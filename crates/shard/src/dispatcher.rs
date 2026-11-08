//! Matching for one Shard, under a `Policy`:
//! - Greedy, the baseline: each Ride Request is offered, in arrival order and as soon as it
//!   arrives, to the nearest idle driver. It's deliberately naive, so later policies have something to beat.
//! - Auction: waiting Ride Requests are collected for a Batch Window, then the auction solver
//!   picks Direct Offers that match the most rides at the least total pickup distance.
//! - Widening, with either matcher: a Ride Request whose Direct Offer failed, or whose area is
//!   short of idle drivers, goes to a Broadcast instead, and the first Accept wins.
//!
//! - Sharding, with any policy: candidates include peer Shards' idle drivers near the boundary.
//!   Offers to them are relayed through their Shard, which reserves the driver on Accept and
//!   asks this Shard to confirm. Drivers and riders on another Shard's Cells are redirected.
//!   See ARCHITECTURE.md → Sharding.
//!
//! The dispatcher holds no connections and does no I/O. It returns effects, and Postgres
//! results come back to it as calls, so Accepts can race in Postgres while matching goes on.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use assignment::{AcceptOutcome, OfferClosed, OfferKind, TripStage};
use geo::shard_map::{MAP_RESOLUTION, ShardId, ShardMap};
use geo::{CellIndex, LocationIndex, Position, Resolution};

use crate::auction;

pub const OFFER_TIMEOUT_SECONDS: f64 = 20.0;
/// Starting values with 4s pings: 3 missed pings for an idle driver, 2 for one holding an offer.
pub const SILENT_IDLE_SECONDS: f64 = 12.0;
pub const SILENT_OFFER_SECONDS: f64 = 8.0;
const INDEX_RESOLUTION: Resolution = Resolution::Nine;

/// Direct Offer candidates per Ride Request in the auction: its nearest idle drivers.
const AUCTION_CANDIDATES: usize = 8;
/// The answer is within this much total pickup distance per ride of the best.
const AUCTION_EPSILON_KM: f64 = 0.001;
/// The solver stops early past this, so a large batch can't hold up the dispatcher.
const AUCTION_BUDGET: Duration = Duration::from_millis(50);
pub const BROADCAST_SIZE: usize = 5;
/// A driver holding only Broadcasts can hold this many at once.
pub const MAX_BROADCASTS_PER_DRIVER: usize = 3;
/// Supply is judged over a resolution-7 Cell and its neighbours, about 36 km².
const SUPPLY_AREA: Resolution = Resolution::Seven;
/// Idle drivers per unassigned Ride Request near the pickup.
pub const SUPPLY_ENTER_BROADCAST_BELOW: f64 = 1.0;
pub const SUPPLY_LEAVE_BROADCAST_ABOVE: f64 = 1.5;
/// An area keeps its state at least this long, so widening can't flap.
pub const SUPPLY_HOLD_SECONDS: f64 = 60.0;
/// How far past the boundary, in map Cells (about 2.4 km each), to take a peer's idle drivers
/// as candidates.
const BOUNDARY_RINGS: u32 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Matcher {
    Greedy,
    Auction { window: Duration },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Policy {
    pub matcher: Matcher,
    pub widening: bool,
}

impl Policy {
    pub const BASELINE: Policy = Policy { matcher: Matcher::Greedy, widening: false };

    /// How often `batch` must be called, if at all.
    pub fn batch_window(&self) -> Option<Duration> {
        match self.matcher {
            Matcher::Greedy => None,
            Matcher::Auction { window } => Some(window),
        }
    }
}

impl std::fmt::Display for Policy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.matcher {
            Matcher::Greedy => write!(f, "matcher greedy")?,
            Matcher::Auction { window } => write!(f, "matcher auction, batch window {} ms", window.as_millis())?,
        }
        write!(f, ", widening {}", if self.widening { "on" } else { "off" })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Effect {
    // Sent to clients.
    Offer { driver: String, offer: String, ride: String, pickup: Position, expires_at: f64 },
    OfferGone { driver: String, offer: String },
    TripAssigned { driver: String, trip: String, ride: String },
    TripStatus { ride: String, trip: String, status: &'static str },
    // Written to Postgres. Storing a ride or an offer reports back when done.
    StoreRide { ride: String, quote: String, pickup: Position },
    StoreOffer { offer: String, ride: String, driver: String, kind: OfferKind, expires_at: f64 },
    CloseOffers { offers: Vec<String>, reason: OfferClosed },
    /// Reports back with the outcome Postgres decided.
    CommitAccept { offer: String, driver: String, trip: String },
    RecordTripEvent { trip: String, driver: String, stage: TripStage },
    // Calls to peer Shards. On the rider's Shard:
    /// Refresh the peer's idle drivers near the boundary; answered with `boundary_drivers`.
    SearchBoundary { shard: ShardId, cells: Vec<CellIndex> },
    /// Deliver a stored offer to the peer's driver; a refusal comes back as `relay_refused`.
    RelayOffer { shard: ShardId, driver: String, offer: String, ride: String, pickup: Position, expires_at: f64 },
    WithdrawRelayed { shard: ShardId, offer: String },
    /// Decide a peer's Reservation in Postgres; reports back with `confirm_decided`.
    CommitConfirm { shard: ShardId, reservation: String, offer: String, driver: String, ride: String, trip: String },
    ReleaseLease { shard: ShardId, reservation: String },
    // On the driver's Shard:
    CloseRelayed { shard: ShardId, offer: String, reason: OfferClosed },
    /// Reserve in Postgres, then ask the peer to confirm; reports back with `reservation_settled`.
    Reserve { shard: ShardId, reservation: String, offer: String, driver: String, ride: String },
    /// Ask the peer whether the Trip behind a lease has ended; reports back with `lease_released`.
    CheckLease { shard: ShardId, reservation: String },
    // To clients: connect to another Shard.
    Redirect { driver: String, shard: ShardId },
    RedirectRider { ride: String, shard: ShardId },
}

/// The rider's Shard's answer to a Reservation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerAnswer {
    Confirmed { trip: String },
    Refused,
}

#[derive(Debug)]
struct Sharding {
    me: ShardId,
    map: ShardMap,
    /// Each peer's Cells within `BOUNDARY_RINGS` of this Shard's.
    bands: BTreeMap<ShardId, Vec<CellIndex>>,
}

impl Sharding {
    fn new(me: ShardId, map: ShardMap) -> Self {
        let mut bands: BTreeMap<ShardId, BTreeSet<CellIndex>> = BTreeMap::new();
        for (cell, owner) in map.cells() {
            if *owner != me {
                continue;
            }
            for near in cell.grid_disk::<Vec<_>>(BOUNDARY_RINGS) {
                if let Some(peer) = map.cells().get(&near).filter(|peer| **peer != me) {
                    bands.entry(*peer).or_default().insert(near);
                }
            }
        }
        let bands = bands.into_iter().map(|(peer, cells)| (peer, cells.into_iter().collect())).collect();
        Self { me, map, bands }
    }

    /// The Shard that should serve `position`, if not this one. `client_version` is the map
    /// version of a redirect the client followed: a newer one than ours means our map is
    /// stale, so the client stays until ours catches up.
    fn elsewhere(&self, position: Position, client_version: u32) -> Option<ShardId> {
        if client_version > self.map.version() {
            return None;
        }
        self.map.owner(position).filter(|owner| *owner != self.me)
    }
}

#[derive(Debug)]
struct Driver {
    position: Position,
    online: bool,
    last_ping: f64,
    silent: bool,
}

#[derive(Debug)]
struct Ride {
    pickup: Position,
    area: Option<CellIndex>,
    tried: HashSet<String>,
    /// Set once a Direct Offer for it has failed; with widening on, it then goes to Broadcasts.
    direct_failed: bool,
}

#[derive(Debug, Clone, Copy)]
struct Area {
    broadcast: bool,
    since: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stage {
    /// Waiting for Postgres before it is sent.
    Storing,
    Open,
    /// The Accept is in Postgres; only its outcome closes the offer now.
    Accepting,
}

#[derive(Debug)]
struct OpenOffer {
    driver: String,
    ride: String,
    kind: OfferKind,
    expires_at: f64,
    stage: Stage,
    /// On the rider's Shard: the driver belongs to this peer, which delivers the offer.
    relay_to: Option<ShardId>,
    /// On the driver's Shard: the offer came from this peer, which owns the ride.
    from: Option<ShardId>,
}

impl OpenOffer {
    fn local(driver: String, ride: String, kind: OfferKind, expires_at: f64) -> Self {
        Self { driver, ride, kind, expires_at, stage: Stage::Storing, relay_to: None, from: None }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Sizes {
    pub drivers: usize,
    pub idle: usize,
    pub rides: usize,
    pub waiting: usize,
    pub offers: usize,
    pub trips: usize,
    pub broadcast_areas: usize,
    pub area_switches: u64,
    pub batches: u64,
    pub batch_deadline_hits: u64,
    pub broadcast_rounds: u64,
    pub boundary_drivers: usize,
    pub relayed_out: u64,
    pub relayed_in: u64,
    pub reservations: u64,
    pub leases: usize,
    pub leased_trips: usize,
    pub redirects: u64,
}

#[derive(Debug)]
pub struct Dispatcher {
    policy: Policy,
    /// Keeps ids unique across Shard restarts, since Postgres keeps the old ones.
    id_prefix: String,
    drivers: HashMap<String, Driver>,
    /// Drivers holding nothing: candidates for any offer.
    idle: LocationIndex,
    /// Drivers holding only Broadcasts, below the limit: candidates for more Broadcasts.
    broadcast_ready: LocationIndex,
    /// Unassigned Ride Requests; stored ones are also in `waiting` or on an offer.
    rides: HashMap<String, Ride>,
    waiting: VecDeque<String>,
    offers: HashMap<String, OpenOffer>,
    trips: HashMap<String, (String, String)>,
    /// Kept alongside `offers` and `trips`, so checking whether someone is busy isn't a scan.
    offers_by_driver: HashMap<String, usize>,
    offers_by_ride: HashMap<String, usize>,
    broadcasts_by_driver: HashMap<String, usize>,
    trips_by_driver: HashMap<String, usize>,
    rides_by_area: HashMap<CellIndex, usize>,
    areas: HashMap<CellIndex, Area>,
    area_switches: u64,
    batches: u64,
    batch_deadline_hits: u64,
    broadcast_rounds: u64,
    next_id: u64,
    sharding: Option<Sharding>,
    /// Peers' idle drivers near the boundary, from the latest search of each peer.
    remote_idle: LocationIndex,
    remote_owner: HashMap<String, ShardId>,
    /// On the driver's Shard: drivers lent to a peer for a Trip there, by Reservation.
    leases: HashMap<String, (String, ShardId)>,
    /// On the rider's Shard: Trips whose driver is leased from a peer.
    leased_trips: HashMap<String, (ShardId, String)>,
    relayed_out: u64,
    relayed_in: u64,
    reservations: u64,
    redirects: u64,
}

impl Default for Dispatcher {
    fn default() -> Self {
        Self::new("")
    }
}

impl Dispatcher {
    pub fn new(id_prefix: &str) -> Self {
        Self::with_policy(id_prefix, Policy::BASELINE)
    }

    pub fn with_policy(id_prefix: &str, policy: Policy) -> Self {
        Self {
            policy,
            id_prefix: id_prefix.to_owned(),
            drivers: HashMap::new(),
            idle: LocationIndex::new(INDEX_RESOLUTION),
            broadcast_ready: LocationIndex::new(INDEX_RESOLUTION),
            rides: HashMap::new(),
            waiting: VecDeque::new(),
            offers: HashMap::new(),
            trips: HashMap::new(),
            offers_by_driver: HashMap::new(),
            offers_by_ride: HashMap::new(),
            broadcasts_by_driver: HashMap::new(),
            trips_by_driver: HashMap::new(),
            rides_by_area: HashMap::new(),
            areas: HashMap::new(),
            area_switches: 0,
            batches: 0,
            batch_deadline_hits: 0,
            broadcast_rounds: 0,
            next_id: 0,
            sharding: None,
            remote_idle: LocationIndex::new(INDEX_RESOLUTION),
            remote_owner: HashMap::new(),
            leases: HashMap::new(),
            leased_trips: HashMap::new(),
            relayed_out: 0,
            relayed_in: 0,
            reservations: 0,
            redirects: 0,
        }
    }

    /// One of several Shards: `me` in `map`.
    pub fn sharded(id_prefix: &str, policy: Policy, me: ShardId, map: ShardMap) -> Self {
        let mut dispatcher = Self::with_policy(id_prefix, policy);
        dispatcher.sharding = Some(Sharding::new(me, map));
        dispatcher
    }

    /// Serves a newer map. Drivers and riders it moves are redirected as they next show up;
    /// Trips stay where they are.
    pub fn set_map(&mut self, map: ShardMap) {
        if let Some(sharding) = &mut self.sharding
            && map.version() > sharding.map.version()
        {
            *sharding = Sharding::new(sharding.me, map);
            let peers: HashSet<ShardId> = sharding.bands.keys().copied().collect();
            let gone: Vec<String> = self.remote_owner.iter().filter(|(_, shard)| !peers.contains(shard)).map(|(id, _)| id.clone()).collect();
            for driver in gone {
                self.remote_owner.remove(&driver);
                self.remote_idle.remove(&driver);
            }
        }
    }

    pub fn map_version(&self) -> Option<u32> {
        self.sharding.as_ref().map(|sharding| sharding.map.version())
    }

    /// A lease recovered from Postgres after a restart.
    pub fn recover_lease(&mut self, driver: String, reservation: String, shard: ShardId) {
        self.leases.insert(driver, (reservation, shard));
    }

    /// A Trip with a leased driver, recovered from Postgres after a restart.
    pub fn recover_leased_trip(&mut self, trip: String, shard: ShardId, reservation: String) {
        self.leased_trips.insert(trip, (shard, reservation));
    }

    /// Active Trips loaded from Postgres after a restart.
    pub fn recover_trip(&mut self, trip: String, driver: String, ride: String) {
        self.add_trip(trip, driver, ride);
    }

    fn add_trip(&mut self, trip: String, driver: String, ride: String) {
        *self.trips_by_driver.entry(driver.clone()).or_default() += 1;
        self.trips.insert(trip, (driver, ride));
    }

    fn take_trip(&mut self, trip: &str) -> Option<(String, String)> {
        let (driver, ride) = self.trips.remove(trip)?;
        decrement(&mut self.trips_by_driver, &driver);
        Some((driver, ride))
    }

    fn add_offer(&mut self, id: String, offer: OpenOffer) {
        *self.offers_by_driver.entry(offer.driver.clone()).or_default() += 1;
        *self.offers_by_ride.entry(offer.ride.clone()).or_default() += 1;
        if offer.kind == OfferKind::Broadcast {
            *self.broadcasts_by_driver.entry(offer.driver.clone()).or_default() += 1;
        }
        self.offers.insert(id, offer);
    }

    fn take_offer(&mut self, id: &str) -> Option<OpenOffer> {
        let offer = self.offers.remove(id)?;
        decrement(&mut self.offers_by_driver, &offer.driver);
        decrement(&mut self.offers_by_ride, &offer.ride);
        if offer.kind == OfferKind::Broadcast {
            decrement(&mut self.broadcasts_by_driver, &offer.driver);
        }
        Some(offer)
    }

    fn remove_ride(&mut self, ride: &str) {
        if let Some(area) = self.rides.remove(ride).and_then(|ride| ride.area) {
            decrement(&mut self.rides_by_area, &area);
        }
    }

    pub fn sizes(&self) -> Sizes {
        Sizes {
            drivers: self.drivers.len(),
            idle: self.idle.len(),
            rides: self.rides.len(),
            waiting: self.waiting.len(),
            offers: self.offers.len(),
            trips: self.trips.len(),
            broadcast_areas: self.areas.values().filter(|area| area.broadcast).count(),
            area_switches: self.area_switches,
            batches: self.batches,
            batch_deadline_hits: self.batch_deadline_hits,
            broadcast_rounds: self.broadcast_rounds,
            boundary_drivers: self.remote_idle.len(),
            relayed_out: self.relayed_out,
            relayed_in: self.relayed_in,
            reservations: self.reservations,
            leases: self.leases.len(),
            leased_trips: self.leased_trips.len(),
            redirects: self.redirects,
        }
    }

    fn new_id(&mut self, kind: char) -> String {
        self.next_id += 1;
        format!("{kind}{}{}", self.id_prefix, self.next_id)
    }

    pub fn location_ping(&mut self, driver: &str, position: Position, now: f64) -> Vec<Effect> {
        self.location_ping_from(driver, position, 0, now)
    }

    /// `map_version` is that of the redirect the driver followed here, or 0.
    pub fn location_ping_from(&mut self, driver: &str, position: Position, map_version: u32, now: f64) -> Vec<Effect> {
        if !position.is_finite() {
            return Vec::new();
        }
        if let Some(shard) = self.sharding.as_ref().and_then(|sharding| sharding.elsewhere(position, map_version))
            && self.movable(driver)
        {
            return self.redirect(driver, shard, now);
        }
        if !self.drivers.contains_key(driver) {
            self.remote_idle.remove(driver);
            self.remote_owner.remove(driver);
        }
        let entry = self
            .drivers
            .entry(driver.to_owned())
            .or_insert(Driver { position, online: false, last_ping: now, silent: false });
        entry.position = position;
        entry.last_ping = now;
        let came_back = !entry.online || entry.silent;
        if !came_back {
            // Only available drivers are indexed, and upserting one follows their move.
            if self.idle.contains(driver) {
                self.idle.upsert(driver, position);
            }
            if self.broadcast_ready.contains(driver) {
                self.broadcast_ready.upsert(driver, position);
            }
            return Vec::new();
        }
        let reconnected = !entry.online;
        entry.online = true;
        entry.silent = false;
        let mut effects = Vec::new();
        if reconnected && self.trips_by_driver.contains_key(driver) {
            // The driver may have missed the assignment, for example across a Shard restart.
            for (trip, (trip_driver, ride)) in &self.trips {
                if trip_driver == driver {
                    effects.push(Effect::TripAssigned { driver: driver.to_owned(), trip: trip.clone(), ride: ride.clone() });
                }
            }
        }
        if reconnected && let Some((reservation, shard)) = self.leases.get(driver) {
            // The release may have been lost; until the peer answers, the driver stays held.
            effects.push(Effect::CheckLease { shard: *shard, reservation: reservation.clone() });
        }
        self.refresh(driver);
        effects.extend(self.dispatch(now));
        effects
    }

    /// A driver can move to another Shard unless a Trip or an Accept in progress pins them here.
    fn movable(&self, driver: &str) -> bool {
        !self.trips_by_driver.contains_key(driver)
            && !self.offers.values().any(|offer| offer.driver == driver && offer.stage == Stage::Accepting)
    }

    fn redirect(&mut self, driver: &str, shard: ShardId, now: f64) -> Vec<Effect> {
        let mut effects = self.withdraw_offers_of(driver);
        effects.push(Effect::Redirect { driver: driver.to_owned(), shard });
        self.redirects += 1;
        self.drivers.remove(driver);
        self.idle.remove(driver);
        self.broadcast_ready.remove(driver);
        effects.extend(self.dispatch(now));
        effects
    }

    /// Closes every offer the driver holds that isn't being accepted, putting local rides back
    /// in the queue.
    fn withdraw_offers_of(&mut self, driver: &str) -> Vec<Effect> {
        let ids: Vec<String> = self
            .offers
            .iter()
            .filter(|(_, offer)| offer.driver == driver && offer.stage != Stage::Accepting)
            .map(|(id, _)| id.clone())
            .collect();
        let mut effects = Vec::new();
        let mut closed = Vec::new();
        for id in ids {
            let offer = self.take_offer(&id).expect("listed above");
            effects.extend(self.gone(&id, &offer, OfferClosed::Withdrawn));
            if offer.from.is_none() {
                self.requeue(&offer);
                closed.push(id);
            }
        }
        if !closed.is_empty() {
            effects.push(Effect::CloseOffers { offers: closed, reason: OfferClosed::Withdrawn });
        }
        effects
    }

    /// Who to tell that an offer this Shard closed is gone: its driver, the peer that relays
    /// it, or the driver and the peer that sent it.
    fn gone(&self, id: &str, offer: &OpenOffer, reason: OfferClosed) -> Vec<Effect> {
        let mut effects = Vec::new();
        match (offer.relay_to, offer.from) {
            (Some(shard), _) => {
                if offer.stage != Stage::Storing {
                    effects.push(Effect::WithdrawRelayed { shard, offer: id.to_owned() });
                }
            }
            (None, from) => {
                if offer.stage != Stage::Storing {
                    effects.push(Effect::OfferGone { driver: offer.driver.clone(), offer: id.to_owned() });
                }
                if let Some(shard) = from {
                    effects.push(Effect::CloseRelayed { shard, offer: id.to_owned(), reason });
                }
            }
        }
        effects
    }

    pub fn driver_disconnected(&mut self, driver: &str) {
        if let Some(entry) = self.drivers.get_mut(driver) {
            entry.online = false;
        }
        self.idle.remove(driver);
        self.broadcast_ready.remove(driver);
    }

    /// The ride is matched only once Postgres has stored it.
    pub fn ride_request(&mut self, ride: String, quote: String, pickup: Position) -> Vec<Effect> {
        self.ride_request_from(ride, quote, pickup, 0)
    }

    /// `map_version` is that of the redirect the rider followed here, or 0.
    pub fn ride_request_from(&mut self, ride: String, quote: String, pickup: Position, map_version: u32) -> Vec<Effect> {
        if self.rides.contains_key(&ride) {
            return Vec::new();
        }
        if let Some(shard) = self.sharding.as_ref().and_then(|sharding| sharding.elsewhere(pickup, map_version)) {
            self.redirects += 1;
            return vec![Effect::RedirectRider { ride, shard }];
        }
        let area = pickup.cell(SUPPLY_AREA);
        if let Some(area) = area {
            *self.rides_by_area.entry(area).or_default() += 1;
        }
        self.rides.insert(ride.clone(), Ride { pickup, area, tried: HashSet::new(), direct_failed: false });
        vec![Effect::StoreRide { ride, quote, pickup }]
    }

    pub fn ride_stored(&mut self, ride: String, now: f64) -> Vec<Effect> {
        if !self.rides.contains_key(&ride) {
            return Vec::new();
        }
        self.waiting.push_back(ride);
        self.dispatch(now)
    }

    /// Sends a stored offer, unless it was overtaken while being stored.
    pub fn offer_stored(&mut self, offer_id: &str, now: f64) -> Vec<Effect> {
        let sendable = self.offers.get(offer_id).is_some_and(|offer| {
            offer.stage == Stage::Storing && offer.expires_at > now && self.rides.contains_key(&offer.ride)
        });
        if sendable {
            let offer = self.offers.get_mut(offer_id).expect("checked above");
            offer.stage = Stage::Open;
            let pickup = self.rides[&offer.ride].pickup;
            let (driver, ride, expires_at) = (offer.driver.clone(), offer.ride.clone(), offer.expires_at);
            return vec![match offer.relay_to {
                Some(shard) => Effect::RelayOffer { shard, driver, offer: offer_id.to_owned(), ride, pickup, expires_at },
                None => Effect::Offer { driver, offer: offer_id.to_owned(), ride, pickup, expires_at },
            }];
        }
        let mut effects = vec![Effect::CloseOffers { offers: vec![offer_id.to_owned()], reason: OfferClosed::Withdrawn }];
        if self.offers.get(offer_id).is_some_and(|offer| offer.stage == Stage::Storing) {
            self.reopen(offer_id);
            effects.extend(self.dispatch(now));
        }
        effects
    }

    /// Checked against memory first; Postgres decides the rest.
    pub fn accept(&mut self, driver: &str, offer_id: &str) -> Vec<Effect> {
        let open = self
            .offers
            .get(offer_id)
            .is_some_and(|offer| offer.driver == driver && offer.stage == Stage::Open);
        if !open {
            return vec![Effect::OfferGone { driver: driver.to_owned(), offer: offer_id.to_owned() }];
        }
        if let Some(shard) = self.offers[offer_id].from {
            let reservation = self.new_id('v');
            self.reservations += 1;
            let offer = self.offers.get_mut(offer_id).expect("checked above");
            offer.stage = Stage::Accepting;
            let ride = offer.ride.clone();
            return vec![Effect::Reserve { shard, reservation, offer: offer_id.to_owned(), driver: driver.to_owned(), ride }];
        }
        let trip = self.new_id('t');
        self.offers.get_mut(offer_id).expect("checked above").stage = Stage::Accepting;
        vec![Effect::CommitAccept { offer: offer_id.to_owned(), driver: driver.to_owned(), trip }]
    }

    pub fn accept_decided(&mut self, offer_id: &str, outcome: AcceptOutcome, now: f64) -> Vec<Effect> {
        let Some(offer) = self.take_offer(offer_id) else { return Vec::new() };
        let mut effects = Vec::new();
        match outcome {
            AcceptOutcome::Assigned { trip, ride } => self.assigned(trip, offer.driver.clone(), ride, true, &mut effects),
            AcceptOutcome::Refused(_) => {
                effects.push(Effect::OfferGone { driver: offer.driver.clone(), offer: offer_id.to_owned() });
                self.requeue(&offer);
                self.refresh(&offer.driver);
            }
        }
        effects.extend(self.dispatch(now));
        effects
    }

    /// Records a committed assignment in memory. `tell_driver` is false when the driver is a
    /// peer's, who hears through their own Shard.
    fn assigned(&mut self, trip: String, driver: String, ride: String, tell_driver: bool, effects: &mut Vec<Effect>) {
        self.remove_ride(&ride);
        self.add_trip(trip.clone(), driver.clone(), ride.clone());
        if tell_driver {
            effects.push(Effect::TripAssigned { driver: driver.clone(), trip: trip.clone(), ride: ride.clone() });
        }
        effects.push(Effect::TripStatus { ride: ride.clone(), trip, status: "assigned" });
        // Postgres has already withdrawn these; memory and the drivers catch up.
        let overtaken: Vec<String> = self
            .offers
            .iter()
            .filter(|(_, other)| other.stage != Stage::Accepting && (other.ride == ride || other.driver == driver))
            .map(|(id, _)| id.clone())
            .collect();
        let mut closed = Vec::new();
        for id in overtaken {
            let other = self.take_offer(&id).expect("listed above");
            if other.stage == Stage::Open {
                effects.extend(self.gone(&id, &other, OfferClosed::Withdrawn));
                if other.from.is_none() {
                    closed.push(id);
                }
            }
            if other.ride != ride && other.from.is_none() {
                self.waiting.push_front(other.ride);
            }
            self.refresh(&other.driver);
        }
        if !closed.is_empty() {
            effects.push(Effect::CloseOffers { offers: closed, reason: OfferClosed::Withdrawn });
        }
    }

    pub fn decline(&mut self, driver: &str, offer_id: &str, now: f64) -> Vec<Effect> {
        let open = self
            .offers
            .get(offer_id)
            .is_some_and(|offer| offer.driver == driver && offer.stage == Stage::Open);
        if !open {
            return Vec::new();
        }
        if let Some(shard) = self.offers[offer_id].from {
            self.take_offer(offer_id);
            self.refresh(driver);
            let mut effects = vec![Effect::CloseRelayed { shard, offer: offer_id.to_owned(), reason: OfferClosed::Declined }];
            effects.extend(self.dispatch(now));
            return effects;
        }
        self.reopen(offer_id);
        let mut effects = vec![Effect::CloseOffers { offers: vec![offer_id.to_owned()], reason: OfferClosed::Declined }];
        effects.extend(self.dispatch(now));
        effects
    }

    pub fn trip_event(&mut self, driver: &str, trip: &str, stage: TripStage, now: f64) -> Vec<Effect> {
        if !self.trips.get(trip).is_some_and(|(trip_driver, _)| trip_driver == driver) {
            return Vec::new();
        }
        let mut effects = vec![Effect::RecordTripEvent { trip: trip.to_owned(), driver: driver.to_owned(), stage }];
        if stage == TripStage::Completed {
            let (_, ride) = self.take_trip(trip).expect("checked above");
            self.refresh(driver);
            effects.push(Effect::TripStatus { ride, trip: trip.to_owned(), status: "completed" });
            if let Some((shard, reservation)) = self.leased_trips.remove(trip) {
                effects.push(Effect::ReleaseLease { shard, reservation });
            }
            effects.extend(self.dispatch(now));
        }
        effects
    }

    /// Expires unanswered offers, handles silent drivers, and retries waiting rides.
    pub fn tick(&mut self, now: f64) -> Vec<Effect> {
        for (id, driver) in &mut self.drivers {
            if driver.online && !driver.silent && now - driver.last_ping > SILENT_IDLE_SECONDS {
                driver.silent = true;
                self.idle.remove(id);
                self.broadcast_ready.remove(id);
            }
        }
        let mut expired = Vec::new();
        let mut withdrawn = Vec::new();
        for (id, offer) in &self.offers {
            if offer.stage != Stage::Open {
                continue;
            }
            // A peer's driver is watched by their own Shard, which closes the offer if they go quiet.
            let silent_for = match offer.relay_to {
                Some(_) => 0.0,
                None => self.drivers.get(&offer.driver).map_or(f64::INFINITY, |driver| now - driver.last_ping),
            };
            if offer.expires_at <= now {
                expired.push(id.clone());
            } else if silent_for > SILENT_OFFER_SECONDS {
                withdrawn.push(id.clone());
            }
        }
        let mut effects = Vec::new();
        for (ids, reason) in [(expired, OfferClosed::Expired), (withdrawn, OfferClosed::Withdrawn)] {
            if ids.is_empty() {
                continue;
            }
            let mut closed = Vec::new();
            for id in &ids {
                if let Some(offer) = self.reopen(id) {
                    effects.extend(self.gone(id, &offer, reason));
                    if offer.from.is_none() {
                        closed.push(id.clone());
                    }
                }
            }
            if !closed.is_empty() {
                effects.push(Effect::CloseOffers { offers: closed, reason });
            }
        }
        effects.extend(self.dispatch(now));
        if let Some(sharding) = &self.sharding {
            for (shard, cells) in &sharding.bands {
                effects.push(Effect::SearchBoundary { shard: *shard, cells: cells.clone() });
            }
        }
        effects
    }

    /// Returns the offer's ride to the front of the queue, and frees its driver.
    fn reopen(&mut self, offer_id: &str) -> Option<OpenOffer> {
        let offer = self.take_offer(offer_id)?;
        if offer.from.is_none() {
            self.requeue(&offer);
        }
        self.refresh(&offer.driver);
        Some(offer)
    }

    /// The offer's driver won't be asked again for now, and a failed Direct Offer widens the ride.
    fn requeue(&mut self, offer: &OpenOffer) {
        if let Some(ride) = self.rides.get_mut(&offer.ride) {
            ride.tried.insert(offer.driver.clone());
            if offer.kind == OfferKind::Direct {
                ride.direct_failed = true;
            }
            self.waiting.push_front(offer.ride.clone());
        }
    }

    /// Puts the driver in the index that matches what they hold, or in neither.
    fn refresh(&mut self, driver: &str) {
        let available = self.drivers.get(driver).filter(|entry| entry.online && !entry.silent).map(|entry| entry.position);
        let offers = self.offers_by_driver.get(driver).copied().unwrap_or(0);
        let broadcasts = self.broadcasts_by_driver.get(driver).copied().unwrap_or(0);
        let free = available.is_some() && !self.trips_by_driver.contains_key(driver) && !self.leases.contains_key(driver);
        let idle = free && offers == 0;
        let ready = free && offers > 0 && offers == broadcasts && broadcasts < MAX_BROADCASTS_PER_DRIVER;
        for (index, wanted) in [(&mut self.idle, idle), (&mut self.broadcast_ready, ready)] {
            match available {
                Some(position) if wanted => {
                    index.upsert(driver, position);
                }
                _ => index.remove(driver),
            }
        }
    }

    /// Matches at once under Greedy. Under Auction, rides wait for the next `batch`.
    fn dispatch(&mut self, now: f64) -> Vec<Effect> {
        match self.policy.matcher {
            Matcher::Greedy => self.match_waiting(now),
            Matcher::Auction { .. } => Vec::new(),
        }
    }

    /// Runs the auction over the rides collected since the last call. The server calls it
    /// once per Batch Window.
    pub fn batch(&mut self, now: f64) -> Vec<Effect> {
        match self.policy.matcher {
            Matcher::Greedy => Vec::new(),
            Matcher::Auction { .. } => self.match_waiting(now),
        }
    }

    fn match_waiting(&mut self, now: f64) -> Vec<Effect> {
        let mut effects = Vec::new();
        let mut still_waiting = VecDeque::new();
        let mut queued = HashSet::new();
        let mut direct = Vec::new();
        let mut judged = HashSet::new();
        while let Some(ride_id) = self.waiting.pop_front() {
            // A ride can be queued twice when offers close together; assigned rides drop out,
            // and so do rides still holding offers.
            if !self.rides.contains_key(&ride_id) || !queued.insert(ride_id.clone()) {
                continue;
            }
            if self.offers_by_ride.contains_key(&ride_id) {
                continue;
            }
            if self.policy.widening && self.wants_broadcast(&ride_id, now, &mut judged) {
                if !self.broadcast(&ride_id, now, &mut effects) {
                    still_waiting.push_back(ride_id);
                }
            } else {
                direct.push(ride_id);
            }
        }
        match self.policy.matcher {
            Matcher::Greedy => self.greedy(direct, now, &mut effects, &mut still_waiting),
            Matcher::Auction { .. } => self.auction(direct, now, &mut effects, &mut still_waiting),
        }
        self.waiting = still_waiting;
        effects
    }

    /// Updates the area's supply state at most once per pass, then reads it.
    fn wants_broadcast(&mut self, ride_id: &str, now: f64, judged: &mut HashSet<CellIndex>) -> bool {
        let ride = &self.rides[ride_id];
        if ride.direct_failed {
            return true;
        }
        let Some(area) = ride.area else { return false };
        if judged.insert(area) {
            let mut idle = 0;
            let mut unassigned = 0;
            for cell in area.grid_disk::<Vec<_>>(1) {
                idle += self.idle.count_in(cell);
                unassigned += self.rides_by_area.get(&cell).copied().unwrap_or(0);
            }
            // This ride is unassigned, so the ratio is defined.
            let supply = idle as f64 / unassigned.max(1) as f64;
            let state = self.areas.entry(area).or_insert(Area { broadcast: false, since: f64::NEG_INFINITY });
            let held = now - state.since >= SUPPLY_HOLD_SECONDS;
            let switch = held
                && if state.broadcast { supply > SUPPLY_LEAVE_BROADCAST_ABOVE } else { supply < SUPPLY_ENTER_BROADCAST_BELOW };
            if switch {
                *state = Area { broadcast: !state.broadcast, since: now };
                self.area_switches += 1;
            }
        }
        self.areas.get(&area).is_some_and(|state| state.broadcast)
    }

    /// Offers the ride to its nearest available drivers at once. False if there were none.
    fn broadcast(&mut self, ride_id: &str, now: f64, effects: &mut Vec<Effect>) -> bool {
        let ride = &self.rides[ride_id];
        let allowed = |driver: &str| !ride.tried.contains(driver);
        let mut found = self.nearest_candidates(ride, BROADCAST_SIZE);
        found.extend(self.broadcast_ready.nearest_k(ride.pickup, BROADCAST_SIZE, allowed).into_iter().map(|(id, km)| (id.to_owned(), km)));
        found.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
        found.truncate(BROADCAST_SIZE);
        let drivers: Vec<String> = found.into_iter().map(|(driver, _)| driver).collect();
        if drivers.is_empty() {
            // Every available driver has refused: let them be asked again next time.
            if let Some(ride) = self.rides.get_mut(ride_id) {
                ride.tried.clear();
            }
            return false;
        }
        self.broadcast_rounds += 1;
        for driver in drivers {
            effects.push(self.make_offer(ride_id, driver, OfferKind::Broadcast, now));
        }
        true
    }

    fn greedy(&mut self, rides: Vec<String>, now: f64, effects: &mut Vec<Effect>, still_waiting: &mut VecDeque<String>) {
        for ride_id in rides {
            if !self.any_available() {
                still_waiting.push_back(ride_id);
                continue;
            }
            let Some((driver, _)) = self.nearest_candidates(&self.rides[&ride_id], 1).pop() else {
                // Every idle driver has refused: let them be asked again next time.
                if let Some(ride) = self.rides.get_mut(&ride_id) {
                    ride.tried.clear();
                }
                still_waiting.push_back(ride_id);
                continue;
            };
            effects.push(self.make_offer(&ride_id, driver, OfferKind::Direct, now));
        }
    }

    fn auction(&mut self, rides: Vec<String>, now: f64, effects: &mut Vec<Effect>, still_waiting: &mut VecDeque<String>) {
        let mut drivers: Vec<String> = Vec::new();
        let mut driver_index: HashMap<String, usize> = HashMap::new();
        let mut bidders = Vec::new();
        let mut candidates = Vec::new();
        for ride_id in rides {
            let found = self.nearest_candidates(&self.rides[&ride_id], AUCTION_CANDIDATES);
            if found.is_empty() {
                if self.any_available() {
                    // Every idle driver has refused: let them be asked again next time.
                    if let Some(ride) = self.rides.get_mut(&ride_id) {
                        ride.tried.clear();
                    }
                }
                still_waiting.push_back(ride_id);
                continue;
            }
            let list = found
                .into_iter()
                .map(|(driver, km)| {
                    let next = drivers.len();
                    let index = *driver_index.entry(driver.clone()).or_insert_with(|| {
                        drivers.push(driver);
                        next
                    });
                    (index, km)
                })
                .collect();
            candidates.push(list);
            bidders.push(ride_id);
        }
        if bidders.is_empty() {
            return;
        }
        self.batches += 1;
        let solution = auction::solve(drivers.len(), &candidates, AUCTION_EPSILON_KM, Some(Instant::now() + AUCTION_BUDGET));
        if !solution.complete {
            self.batch_deadline_hits += 1;
        }
        for (ride_id, assigned) in bidders.into_iter().zip(solution.assigned) {
            match assigned {
                Some(index) => {
                    let driver = drivers[index].clone();
                    effects.push(self.make_offer(&ride_id, driver, OfferKind::Direct, now));
                }
                None => still_waiting.push_back(ride_id),
            }
        }
    }

    fn make_offer(&mut self, ride_id: &str, driver: String, kind: OfferKind, now: f64) -> Effect {
        let offer = self.new_id('o');
        let expires_at = now + OFFER_TIMEOUT_SECONDS;
        let mut open = OpenOffer::local(driver.clone(), ride_id.to_owned(), kind, expires_at);
        if self.remote_idle.contains(&driver) {
            // A peer's driver: offered once at a time from here, until the offer closes.
            self.remote_idle.remove(&driver);
            open.relay_to = self.remote_owner.remove(&driver);
            self.relayed_out += 1;
        }
        self.add_offer(offer.clone(), open);
        self.refresh(&driver);
        Effect::StoreOffer { offer, ride: ride_id.to_owned(), driver, kind, expires_at }
    }

    /// The `k` nearest available drivers, here and near the boundary on peers, nearest first.
    fn nearest_candidates(&self, ride: &Ride, k: usize) -> Vec<(String, f64)> {
        let allowed = |id: &str| !ride.tried.contains(id);
        let mut found: Vec<(String, f64)> =
            self.idle.nearest_k(ride.pickup, k, allowed).into_iter().map(|(id, km)| (id.to_owned(), km)).collect();
        found.extend(self.remote_idle.nearest_k(ride.pickup, k, allowed).into_iter().map(|(id, km)| (id.to_owned(), km)));
        found.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
        found.truncate(k);
        found
    }

    fn any_available(&self) -> bool {
        !self.idle.is_empty() || !self.remote_idle.is_empty()
    }

    // --- Calls from peer Shards ----------------------------------------------------------

    /// On the driver's Shard: idle drivers inside `cells`, for a peer's boundary search.
    pub fn boundary_snapshot(&self, cells: &[CellIndex]) -> Vec<(String, Position)> {
        cells
            .iter()
            .filter(|cell| cell.resolution() == MAP_RESOLUTION)
            .flat_map(|cell| self.idle.drivers_in(*cell))
            .map(|(id, position)| (id.to_owned(), position))
            .collect()
    }

    /// On the rider's Shard: the peer's idle drivers near the boundary, replacing the last answer.
    pub fn boundary_drivers(&mut self, shard: ShardId, drivers: Vec<(String, Position)>) {
        let stale: Vec<String> = self.remote_owner.iter().filter(|(_, owner)| **owner == shard).map(|(id, _)| id.clone()).collect();
        for id in stale {
            self.remote_owner.remove(&id);
            self.remote_idle.remove(&id);
        }
        for (id, position) in drivers {
            // A driver who has moved here, or already holds an offer from here, isn't a candidate.
            let here = self.drivers.get(&id).is_some_and(|driver| driver.online);
            if here || self.offers_by_driver.contains_key(&id) || !position.is_finite() {
                continue;
            }
            self.remote_idle.upsert(&id, position);
            self.remote_owner.insert(id, shard);
        }
    }

    /// On the driver's Shard: delivers a peer's offer if the driver is still free here.
    /// Returns whether it was delivered.
    pub fn relay_offer(
        &mut self,
        from: ShardId,
        offer: String,
        driver: String,
        ride: String,
        pickup: Position,
        expires_at: f64,
    ) -> (bool, Vec<Effect>) {
        let available = self.drivers.get(&driver).is_some_and(|entry| entry.online && !entry.silent)
            && !self.trips_by_driver.contains_key(&driver)
            && !self.leases.contains_key(&driver)
            && !self.offers_by_driver.contains_key(&driver)
            && !self.offers.contains_key(&offer);
        if !available {
            return (false, Vec::new());
        }
        self.relayed_in += 1;
        let mut open = OpenOffer::local(driver.clone(), ride.clone(), OfferKind::Direct, expires_at);
        open.stage = Stage::Open;
        open.from = Some(from);
        self.add_offer(offer.clone(), open);
        self.refresh(&driver);
        (true, vec![Effect::Offer { driver, offer, ride, pickup, expires_at }])
    }

    /// On the driver's Shard: the peer closed its offer. One being accepted is left to the
    /// Reservation, which the peer will refuse.
    pub fn withdraw_relayed(&mut self, offer_id: &str, now: f64) -> Vec<Effect> {
        let relayed_open = self.offers.get(offer_id).is_some_and(|offer| offer.from.is_some() && offer.stage == Stage::Open);
        if !relayed_open {
            return Vec::new();
        }
        let offer = self.take_offer(offer_id).expect("checked above");
        self.refresh(&offer.driver);
        let mut effects = vec![Effect::OfferGone { driver: offer.driver, offer: offer_id.to_owned() }];
        effects.extend(self.dispatch(now));
        effects
    }

    /// On the driver's Shard: the peer's answer to a Reservation. The offer may be gone from
    /// memory after a restart; the answer still settles the driver.
    pub fn reservation_settled(
        &mut self,
        offer_id: &str,
        driver: &str,
        reservation: String,
        shard: ShardId,
        answer: PeerAnswer,
        now: f64,
    ) -> Vec<Effect> {
        let offer = self.take_offer(offer_id);
        match answer {
            PeerAnswer::Confirmed { .. } => {
                // Lent to the peer until the Trip ends there; the driver goes there now.
                self.leases.insert(driver.to_owned(), (reservation, shard));
                let mut effects = self.withdraw_offers_of(driver);
                if self.drivers.remove(driver).is_some() {
                    effects.push(Effect::Redirect { driver: driver.to_owned(), shard });
                    self.redirects += 1;
                }
                self.idle.remove(driver);
                self.broadcast_ready.remove(driver);
                effects.extend(self.dispatch(now));
                effects
            }
            PeerAnswer::Refused => {
                let mut effects = Vec::new();
                if offer.is_some() {
                    effects.push(Effect::OfferGone { driver: driver.to_owned(), offer: offer_id.to_owned() });
                }
                self.refresh(driver);
                effects.extend(self.dispatch(now));
                effects
            }
        }
    }

    /// On the driver's Shard: the Trip behind a lease has ended, so the driver is free here.
    pub fn lease_released(&mut self, reservation: &str, now: f64) -> Vec<Effect> {
        let Some(driver) = self.leases.iter().find(|(_, (id, _))| id == reservation).map(|(driver, _)| driver.clone()) else {
            return Vec::new();
        };
        self.leases.remove(&driver);
        self.refresh(&driver);
        self.dispatch(now)
    }

    /// On the driver's Shard: the peer still has the leased driver's Trip, so a driver who
    /// turned up here is sent back to it.
    pub fn lease_held(&mut self, reservation: &str, now: f64) -> Vec<Effect> {
        let Some((driver, shard)) =
            self.leases.iter().find(|(_, (id, _))| id == reservation).map(|(driver, (_, shard))| (driver.clone(), *shard))
        else {
            return Vec::new();
        };
        if !self.drivers.get(&driver).is_some_and(|entry| entry.online) {
            return Vec::new();
        }
        self.redirect(&driver, shard, now)
    }

    /// On the rider's Shard: the peer refused to deliver an offer, or its driver closed it.
    pub fn relayed_closed(&mut self, offer_id: &str, reason: OfferClosed, now: f64) -> Vec<Effect> {
        let relayed = self.offers.get(offer_id).is_some_and(|offer| offer.relay_to.is_some() && offer.stage == Stage::Open);
        if !relayed {
            return Vec::new();
        }
        self.reopen(offer_id);
        let mut effects = vec![Effect::CloseOffers { offers: vec![offer_id.to_owned()], reason }];
        effects.extend(self.dispatch(now));
        effects
    }

    /// On the rider's Shard: the peer's driver accepted and is reserved there. Postgres
    /// decides; memory holds the offer meanwhile so it doesn't expire under the Confirm.
    pub fn remote_accept(&mut self, shard: ShardId, reservation: String, offer_id: &str, driver: &str, ride: &str) -> Vec<Effect> {
        if let Some(offer) = self.offers.get_mut(offer_id)
            && offer.driver == driver
            && offer.relay_to == Some(shard)
            && offer.stage == Stage::Open
        {
            offer.stage = Stage::Accepting;
        }
        let trip = self.new_id('t');
        vec![Effect::CommitConfirm { shard, reservation, offer: offer_id.to_owned(), driver: driver.to_owned(), ride: ride.to_owned(), trip }]
    }

    /// On the rider's Shard: what Postgres decided for a peer's Reservation. `trip` is set
    /// when it was confirmed.
    #[allow(clippy::too_many_arguments)]
    pub fn confirm_decided(
        &mut self,
        offer_id: &str,
        driver: &str,
        ride: &str,
        shard: ShardId,
        reservation: String,
        trip: Option<String>,
        now: f64,
    ) -> Vec<Effect> {
        let offer = self.take_offer(offer_id);
        let mut effects = Vec::new();
        match trip {
            Some(trip) if !self.trips.contains_key(&trip) => {
                self.leased_trips.insert(trip.clone(), (shard, reservation));
                self.assigned(trip, driver.to_owned(), ride.to_owned(), false, &mut effects);
            }
            Some(_) => {}
            None => {
                if let Some(offer) = offer {
                    self.requeue(&offer);
                }
            }
        }
        effects.extend(self.dispatch(now));
        effects
    }
}

fn decrement<K: std::hash::Hash + Eq + ?Sized, Q: std::hash::Hash + Eq + std::borrow::Borrow<K>>(counts: &mut HashMap<Q, usize>, key: &K) {
    if let Some(count) = counts.get_mut(key) {
        *count -= 1;
        if *count == 0 {
            counts.remove(key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assignment::Refusal;

    const R1: Position = Position { lat: 40.700, lng: -74.0 };
    const R2: Position = Position { lat: 40.720, lng: -74.0 };
    // Each 0.009 degrees of latitude is about one km.
    const D1: Position = Position { lat: 40.709, lng: -74.0 };
    const D2: Position = Position { lat: 40.790, lng: -74.0 };

    /// Plays Postgres: stores rides and offers at once, and assigns every Accept.
    fn settle(dispatcher: &mut Dispatcher, effects: Vec<Effect>, now: f64) -> Vec<Effect> {
        let mut pending = effects;
        let mut out = Vec::new();
        while !pending.is_empty() {
            let mut next = Vec::new();
            for effect in pending {
                match &effect {
                    Effect::StoreRide { ride, .. } => next.extend(dispatcher.ride_stored(ride.clone(), now)),
                    Effect::StoreOffer { offer, .. } => next.extend(dispatcher.offer_stored(offer, now)),
                    _ => out.push(effect),
                }
            }
            pending = next;
        }
        out
    }

    fn request(dispatcher: &mut Dispatcher, ride: &str, pickup: Position, now: f64) -> Vec<Effect> {
        let effects = dispatcher.ride_request(ride.into(), "q".into(), pickup);
        settle(dispatcher, effects, now)
    }

    fn ping(dispatcher: &mut Dispatcher, driver: &str, position: Position, now: f64) -> Vec<Effect> {
        let effects = dispatcher.location_ping(driver, position, now);
        settle(dispatcher, effects, now)
    }

    fn offer_of(effects: &[Effect]) -> (String, String, String) {
        effects
            .iter()
            .find_map(|effect| match effect {
                Effect::Offer { driver, offer, ride, .. } => Some((driver.clone(), offer.clone(), ride.clone())),
                _ => None,
            })
            .expect("an offer")
    }

    fn commit_of(effects: &[Effect]) -> (String, String) {
        effects
            .iter()
            .find_map(|effect| match effect {
                Effect::CommitAccept { offer, trip, .. } => Some((offer.clone(), trip.clone())),
                _ => None,
            })
            .expect("a commit")
    }

    fn assign(dispatcher: &mut Dispatcher, driver: &str, offer: &str, ride: &str, now: f64) -> (String, Vec<Effect>) {
        let (offer, trip) = commit_of(&dispatcher.accept(driver, offer));
        let outcome = AcceptOutcome::Assigned { trip: trip.clone(), ride: ride.into() };
        let effects = dispatcher.accept_decided(&offer, outcome, now);
        (trip, settle(dispatcher, effects, now))
    }

    #[test]
    fn offers_the_nearest_idle_driver_once_ride_and_offer_are_stored() {
        let mut dispatcher = Dispatcher::default();
        ping(&mut dispatcher, "far", D2, 0.0);
        ping(&mut dispatcher, "near", D1, 0.0);
        let stored = dispatcher.ride_request("r1".into(), "q".into(), R1);
        assert!(matches!(stored[..], [Effect::StoreRide { .. }]), "nothing is offered before the ride is stored");
        let offer = dispatcher.ride_stored("r1".into(), 0.0);
        let Effect::StoreOffer { offer, driver, .. } = &offer[0] else { panic!("{offer:?}") };
        assert_eq!(driver, "near");
        let (driver, _, ride) = offer_of(&dispatcher.offer_stored(offer, 0.0));
        assert_eq!((driver.as_str(), ride.as_str()), ("near", "r1"));
    }

    #[test]
    fn baseline_is_greedy_even_when_a_swap_would_be_better() {
        let mut dispatcher = Dispatcher::default();
        ping(&mut dispatcher, "d1", D1, 0.0);
        ping(&mut dispatcher, "d2", D2, 0.0);
        let (first, ..) = offer_of(&request(&mut dispatcher, "r1", R1, 0.0));
        let (second, ..) = offer_of(&request(&mut dispatcher, "r2", R2, 0.0));
        assert_eq!((first.as_str(), second.as_str()), ("d1", "d2"));
    }

    #[test]
    fn an_accept_waits_for_postgres_and_a_second_accept_is_refused_in_memory() {
        let mut dispatcher = Dispatcher::default();
        ping(&mut dispatcher, "d1", D1, 0.0);
        let (_, offer, _) = offer_of(&request(&mut dispatcher, "r1", R1, 0.0));
        let commit = dispatcher.accept("d1", &offer);
        assert!(matches!(commit[..], [Effect::CommitAccept { .. }]), "nothing is told before the commit");
        assert_eq!(dispatcher.accept("d1", &offer), vec![Effect::OfferGone { driver: "d1".into(), offer: offer.clone() }]);
        let (_, trip) = commit_of(&commit);
        let effects = dispatcher.accept_decided(&offer, AcceptOutcome::Assigned { trip, ride: "r1".into() }, 1.0);
        assert!(matches!(effects[0], Effect::TripAssigned { .. }));
    }

    #[test]
    fn a_refused_accept_tells_the_driver_and_moves_on() {
        let mut dispatcher = Dispatcher::default();
        ping(&mut dispatcher, "d1", D1, 0.0);
        ping(&mut dispatcher, "d2", D2, 0.0);
        let (_, offer, _) = offer_of(&request(&mut dispatcher, "r1", R1, 0.0));
        dispatcher.accept("d1", &offer);
        let effects = dispatcher.accept_decided(&offer, AcceptOutcome::Refused(Refusal::OfferNotOpen), 1.0);
        assert!(effects.contains(&Effect::OfferGone { driver: "d1".into(), offer }));
        let effects = settle(&mut dispatcher, effects, 1.0);
        assert_eq!(offer_of(&effects).0, "d2");
    }

    #[test]
    fn an_offer_cannot_be_accepted_by_another_driver() {
        let mut dispatcher = Dispatcher::default();
        ping(&mut dispatcher, "d1", D1, 0.0);
        ping(&mut dispatcher, "d2", D2, 0.0);
        let (_, offer, _) = offer_of(&request(&mut dispatcher, "r1", R1, 0.0));
        assert!(matches!(dispatcher.accept("d2", &offer)[0], Effect::OfferGone { .. }));
    }

    #[test]
    fn a_driver_on_a_trip_gets_no_offers_until_it_completes() {
        let mut dispatcher = Dispatcher::default();
        ping(&mut dispatcher, "d1", D1, 0.0);
        let (_, offer, _) = offer_of(&request(&mut dispatcher, "r1", R1, 0.0));
        let (trip, _) = assign(&mut dispatcher, "d1", &offer, "r1", 1.0);
        assert!(request(&mut dispatcher, "r2", R2, 2.0).is_empty());
        let effects = dispatcher.trip_event("d1", &trip, TripStage::Completed, 3.0);
        assert!(matches!(effects[0], Effect::RecordTripEvent { stage: TripStage::Completed, .. }));
        assert_eq!(offer_of(&settle(&mut dispatcher, effects, 3.0)).2, "r2");
    }

    #[test]
    fn a_decline_moves_to_the_next_nearest_driver() {
        let mut dispatcher = Dispatcher::default();
        ping(&mut dispatcher, "d1", D1, 0.0);
        ping(&mut dispatcher, "d2", D2, 0.0);
        let (_, offer, _) = offer_of(&request(&mut dispatcher, "r1", R1, 0.0));
        let effects = dispatcher.decline("d1", &offer, 1.0);
        assert!(effects.contains(&Effect::CloseOffers { offers: vec![offer], reason: OfferClosed::Declined }));
        assert_eq!(offer_of(&settle(&mut dispatcher, effects, 1.0)).0, "d2");
    }

    #[test]
    fn an_unanswered_offer_expires_and_moves_on() {
        let mut dispatcher = Dispatcher::default();
        ping(&mut dispatcher, "d1", D1, 0.0);
        ping(&mut dispatcher, "d2", D2, 0.0);
        let (_, offer, _) = offer_of(&request(&mut dispatcher, "r1", R1, 0.0));
        // Keep both drivers pinging, so the offer expires rather than being withdrawn.
        for now in (4..20).step_by(4) {
            ping(&mut dispatcher, "d1", D1, f64::from(now));
            ping(&mut dispatcher, "d2", D2, f64::from(now));
            assert!(dispatcher.tick(f64::from(now)).is_empty());
        }
        let effects = dispatcher.tick(OFFER_TIMEOUT_SECONDS);
        assert!(effects.contains(&Effect::OfferGone { driver: "d1".into(), offer }));
        assert_eq!(offer_of(&settle(&mut dispatcher, effects, OFFER_TIMEOUT_SECONDS)).0, "d2");
    }

    #[test]
    fn an_accepting_offer_does_not_expire() {
        let mut dispatcher = Dispatcher::default();
        ping(&mut dispatcher, "d1", D1, 0.0);
        let (_, offer, _) = offer_of(&request(&mut dispatcher, "r1", R1, 0.0));
        dispatcher.accept("d1", &offer);
        assert!(dispatcher.tick(OFFER_TIMEOUT_SECONDS + 5.0).is_empty());
    }

    #[test]
    fn a_ride_refused_by_everyone_is_retried() {
        let mut dispatcher = Dispatcher::default();
        ping(&mut dispatcher, "d1", D1, 0.0);
        let (_, offer, _) = offer_of(&request(&mut dispatcher, "r1", R1, 0.0));
        let effects = dispatcher.decline("d1", &offer, 1.0);
        assert!(settle(&mut dispatcher, effects, 1.0).iter().all(|effect| !matches!(effect, Effect::Offer { .. })));
        let effects = dispatcher.tick(2.0);
        assert_eq!(offer_of(&settle(&mut dispatcher, effects, 2.0)).0, "d1");
    }

    #[test]
    fn a_disconnected_driver_gets_no_offers() {
        let mut dispatcher = Dispatcher::default();
        ping(&mut dispatcher, "d1", D1, 0.0);
        dispatcher.driver_disconnected("d1");
        assert!(request(&mut dispatcher, "r1", R1, 1.0).is_empty());
        assert_eq!(offer_of(&ping(&mut dispatcher, "d1", D1, 2.0)).0, "d1");
    }

    #[test]
    fn a_silent_idle_driver_leaves_the_index_and_returns_on_the_next_ping() {
        let mut dispatcher = Dispatcher::default();
        ping(&mut dispatcher, "d1", D1, 0.0);
        dispatcher.tick(SILENT_IDLE_SECONDS + 0.5);
        assert!(request(&mut dispatcher, "r1", R1, SILENT_IDLE_SECONDS + 0.5).is_empty());
        assert_eq!(offer_of(&ping(&mut dispatcher, "d1", D1, 20.0)).0, "d1");
    }

    #[test]
    fn an_offer_to_a_silent_driver_is_withdrawn() {
        let mut dispatcher = Dispatcher::default();
        ping(&mut dispatcher, "d1", D1, 0.0);
        ping(&mut dispatcher, "d2", D2, 0.0);
        let (_, offer, _) = offer_of(&request(&mut dispatcher, "r1", R1, 0.0));
        ping(&mut dispatcher, "d2", D2, 8.0);
        let effects = dispatcher.tick(SILENT_OFFER_SECONDS + 0.5);
        assert!(effects.contains(&Effect::CloseOffers { offers: vec![offer.clone()], reason: OfferClosed::Withdrawn }));
        assert!(effects.contains(&Effect::OfferGone { driver: "d1".into(), offer }));
        assert_eq!(offer_of(&settle(&mut dispatcher, effects, SILENT_OFFER_SECONDS + 0.5)).0, "d2");
    }

    #[test]
    fn an_assignment_withdraws_overlapping_offers() {
        // Overlaps don't come from this matcher, so build them by hand.
        let mut dispatcher = Dispatcher::default();
        ping(&mut dispatcher, "d1", D1, 0.0);
        ping(&mut dispatcher, "d2", D2, 0.0);
        let (_, first, _) = offer_of(&request(&mut dispatcher, "r1", R1, 0.0));
        let (_, second, _) = offer_of(&request(&mut dispatcher, "r2", R2, 0.0));
        let mut moved = dispatcher.take_offer(&second).unwrap();
        moved.ride = "r1".into();
        dispatcher.add_offer(second.clone(), moved);
        let (_, effects) = assign(&mut dispatcher, "d1", &first, "r1", 1.0);
        assert!(effects.contains(&Effect::OfferGone { driver: "d2".into(), offer: second }));
        // d2 is free again for a new ride.
        assert_eq!(offer_of(&request(&mut dispatcher, "r3", R2, 2.0)).0, "d2");
    }

    #[test]
    fn an_offer_overtaken_while_stored_is_closed_not_sent() {
        let mut dispatcher = Dispatcher::default();
        ping(&mut dispatcher, "d1", D1, 0.0);
        dispatcher.ride_request("r1".into(), "q".into(), R1);
        let stored = dispatcher.ride_stored("r1".into(), 0.0);
        let Effect::StoreOffer { offer, .. } = &stored[0] else { panic!() };
        let effects = dispatcher.offer_stored(offer, OFFER_TIMEOUT_SECONDS + 1.0);
        assert_eq!(effects[0], Effect::CloseOffers { offers: vec![offer.clone()], reason: OfferClosed::Withdrawn });
        assert!(!effects.iter().any(|effect| matches!(effect, Effect::Offer { offer: sent, .. } if sent == offer)));
    }

    #[test]
    fn a_reconnecting_driver_is_told_their_active_trip_again() {
        let mut dispatcher = Dispatcher::default();
        dispatcher.recover_trip("t9".into(), "d1".into(), "r9".into());
        let effects = ping(&mut dispatcher, "d1", D1, 0.0);
        assert_eq!(effects, vec![Effect::TripAssigned { driver: "d1".into(), trip: "t9".into(), ride: "r9".into() }]);
        assert!(request(&mut dispatcher, "r1", R1, 1.0).is_empty(), "a driver on a Trip gets no offers");
    }

    fn offers_of(effects: &[Effect]) -> Vec<(String, String, String)> {
        effects
            .iter()
            .filter_map(|effect| match effect {
                Effect::Offer { driver, offer, ride, .. } => Some((driver.clone(), offer.clone(), ride.clone())),
                _ => None,
            })
            .collect()
    }

    fn auction() -> Dispatcher {
        Dispatcher::with_policy("", Policy { matcher: Matcher::Auction { window: Duration::from_millis(500) }, widening: false })
    }

    fn widening() -> Dispatcher {
        Dispatcher::with_policy("", Policy { matcher: Matcher::Greedy, widening: true })
    }

    fn batch(dispatcher: &mut Dispatcher, now: f64) -> Vec<Effect> {
        let effects = dispatcher.batch(now);
        settle(dispatcher, effects, now)
    }

    /// Along one line, in km from r1: d2 at -2, r1 at 0, d1 at 1, r2 at 3. Greedy in arrival
    /// order gives r1→d1 and r2→d2 (1 + 5 km); swapping gives 2 + 2 km.
    fn km_north(km: f64) -> Position {
        Position { lat: 40.700 + km * 0.009, lng: -74.0 }
    }

    fn pairs(effects: &[Effect]) -> Vec<(String, String)> {
        let mut pairs: Vec<(String, String)> =
            offers_of(effects).into_iter().map(|(driver, _, ride)| (ride, driver)).collect();
        pairs.sort();
        pairs
    }

    fn pair(ride: &str, driver: &str) -> (String, String) {
        (ride.into(), driver.into())
    }

    #[test]
    fn greedy_takes_the_nearest_in_arrival_order() {
        let mut dispatcher = Dispatcher::default();
        ping(&mut dispatcher, "d1", km_north(1.0), 0.0);
        ping(&mut dispatcher, "d2", km_north(-2.0), 0.0);
        let mut effects = request(&mut dispatcher, "r1", km_north(0.0), 0.0);
        effects.extend(request(&mut dispatcher, "r2", km_north(3.0), 0.0));
        assert_eq!(pairs(&effects), [pair("r1", "d1"), pair("r2", "d2")]);
    }

    #[test]
    fn the_auction_waits_for_the_batch_and_swaps() {
        let mut dispatcher = auction();
        ping(&mut dispatcher, "d1", km_north(1.0), 0.0);
        ping(&mut dispatcher, "d2", km_north(-2.0), 0.0);
        assert!(request(&mut dispatcher, "r1", km_north(0.0), 0.0).is_empty(), "nothing before the batch");
        assert!(request(&mut dispatcher, "r2", km_north(3.0), 0.1).is_empty());
        assert_eq!(pairs(&batch(&mut dispatcher, 0.5)), [pair("r1", "d2"), pair("r2", "d1")]);
        assert_eq!(dispatcher.sizes().batches, 1);
    }

    #[test]
    fn an_auction_ride_left_over_waits_for_the_next_batch() {
        let mut dispatcher = auction();
        ping(&mut dispatcher, "d1", km_north(1.0), 0.0);
        request(&mut dispatcher, "r1", km_north(0.0), 0.0);
        request(&mut dispatcher, "r2", km_north(3.0), 0.0);
        assert_eq!(pairs(&batch(&mut dispatcher, 0.5)), [pair("r1", "d1")], "the nearer ride wins the one driver");
        ping(&mut dispatcher, "d2", km_north(4.0), 0.6);
        assert_eq!(pairs(&batch(&mut dispatcher, 1.0)), [pair("r2", "d2")]);
    }

    #[test]
    fn a_declined_direct_offer_widens_to_a_broadcast_and_the_first_accept_wins() {
        let mut dispatcher = widening();
        for i in 0..7 {
            ping(&mut dispatcher, &format!("d{i}"), km_north(0.1 * f64::from(i)), 0.0);
        }
        let (first, offer, _) = offer_of(&request(&mut dispatcher, "r1", km_north(0.0), 0.0));
        assert_eq!(first, "d0", "supply is fine, so a Direct Offer comes first");
        let effects = dispatcher.decline("d0", &offer, 1.0);
        let broadcast = offers_of(&settle(&mut dispatcher, effects, 1.0));
        let drivers: Vec<&str> = broadcast.iter().map(|(driver, ..)| driver.as_str()).collect();
        assert_eq!(drivers, ["d1", "d2", "d3", "d4", "d5"], "the next five nearest, not the one who declined");
        let (winner, winning_offer, _) = broadcast[2].clone();
        let (_, effects) = assign(&mut dispatcher, &winner, &winning_offer, "r1", 2.0);
        let gone: Vec<&str> = effects
            .iter()
            .filter_map(|effect| match effect {
                Effect::OfferGone { driver, .. } => Some(driver.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(gone.len(), 4, "the other Broadcast offers are withdrawn: {effects:?}");
        assert!(!gone.contains(&winner.as_str()));
    }

    #[test]
    fn low_supply_broadcasts_at_once_and_holds_before_switching_back() {
        let mut dispatcher = widening();
        ping(&mut dispatcher, "a", km_north(0.1), 0.0);
        ping(&mut dispatcher, "b", km_north(0.2), 0.0);
        // Two idle drivers for one ride: Direct.
        let first = offers_of(&request(&mut dispatcher, "r1", km_north(0.0), 0.0));
        assert_eq!(first.len(), 1);
        // One idle driver for two rides: below 1.0, so this area now broadcasts.
        let second = offers_of(&request(&mut dispatcher, "r2", km_north(0.0), 1.0));
        assert_eq!(second.iter().map(|(driver, ..)| driver.as_str()).collect::<Vec<_>>(), ["b"]);
        // b holds only a Broadcast, so it can be in another one: a driver-side overlap.
        let third = offers_of(&request(&mut dispatcher, "r3", km_north(0.0), 2.0));
        assert_eq!(third.iter().map(|(driver, ..)| driver.as_str()).collect::<Vec<_>>(), ["b"]);
        assert_eq!(dispatcher.sizes().broadcast_areas, 1);
        // Plenty of drivers arrive, but the area holds its state for a minute.
        for i in 0..20 {
            ping(&mut dispatcher, &format!("n{i}"), km_north(0.3), 10.0);
        }
        let early = offers_of(&request(&mut dispatcher, "r4", km_north(0.0), 30.0));
        assert_eq!(early.len(), BROADCAST_SIZE, "still broadcasting inside the hold");
        let late = offers_of(&request(&mut dispatcher, "r5", km_north(0.0), 1.0 + SUPPLY_HOLD_SECONDS));
        assert_eq!(late.len(), 1, "back to a Direct Offer after the hold");
        assert_eq!(dispatcher.sizes().area_switches, 2);
    }

    #[test]
    fn a_driver_holds_at_most_three_broadcasts_and_no_direct_offers_meanwhile() {
        let mut dispatcher = widening();
        // A ride with no one around puts the area into Broadcast.
        assert!(request(&mut dispatcher, "r1", km_north(0.0), 0.0).is_empty());
        let mut held = offers_of(&ping(&mut dispatcher, "d", km_north(0.1), 0.0));
        for ride in ["r2", "r3", "r4"] {
            held.extend(offers_of(&request(&mut dispatcher, ride, km_north(0.0), 1.0)));
        }
        let rides: Vec<&str> = held.iter().map(|(_, _, ride)| ride.as_str()).collect();
        assert_eq!(rides, ["r1", "r2", "r3"], "r4 waits: d is at the limit");
        assert_eq!(dispatcher.sizes().idle, 0, "a driver holding Broadcasts isn't idle, so gets no Direct Offer");
    }

    // --- Sharding: two dispatchers playing Shard 0 (west Cell) and Shard 1 (east Cell). ---

    use geo::shard_map::centre;

    /// Two neighbouring map Cells: the first is Shard 0's, the second Shard 1's.
    fn two_cells() -> (CellIndex, CellIndex) {
        let west = Position { lat: 40.70, lng: -74.00 }.cell(MAP_RESOLUTION).unwrap();
        let east = west.grid_disk::<Vec<_>>(1).into_iter().filter(|cell| *cell != west).max_by(|a, b| {
            centre(*a).lng.total_cmp(&centre(*b).lng)
        });
        (west, east.unwrap())
    }

    fn map(version: u32, west_owner: ShardId) -> ShardMap {
        let (west, east) = two_cells();
        let shard = |id: ShardId| geo::shard_map::ShardAddress {
            id,
            peer: format!("http://shard-{id}:7100"),
            drivers: vec![format!("ws://shard-{id}:7001")],
            riders: vec![format!("ws://shard-{id}:7002")],
        };
        ShardMap::new(version, vec![shard(0), shard(1)], [(west, west_owner), (east, 1)].into_iter().collect()).unwrap()
    }

    fn shards() -> (Dispatcher, Dispatcher) {
        (Dispatcher::sharded("a", Policy::BASELINE, 0, map(1, 0)), Dispatcher::sharded("b", Policy::BASELINE, 1, map(1, 0)))
    }

    /// Runs one round of B's boundary search against A.
    fn search(rider_shard: &mut Dispatcher, driver_shard: &Dispatcher, now: f64) -> Vec<Effect> {
        let effects = rider_shard.tick(now);
        for effect in &effects {
            if let Effect::SearchBoundary { shard: 0, cells } = effect {
                let found = driver_shard.boundary_snapshot(cells);
                rider_shard.boundary_drivers(0, found);
            }
        }
        let effects: Vec<Effect> = effects.into_iter().filter(|effect| !matches!(effect, Effect::SearchBoundary { .. })).collect();
        let mut out = settle(rider_shard, effects, now);
        let dispatched = rider_shard.tick(now).into_iter().filter(|effect| !matches!(effect, Effect::SearchBoundary { .. })).collect();
        out.extend(settle(rider_shard, dispatched, now));
        out
    }

    fn relay_of(effects: &[Effect]) -> (String, String, String, Position, f64) {
        effects
            .iter()
            .find_map(|effect| match effect {
                Effect::RelayOffer { shard: 0, driver, offer, ride, pickup, expires_at } => {
                    Some((driver.clone(), offer.clone(), ride.clone(), *pickup, *expires_at))
                }
                _ => None,
            })
            .expect("a relayed offer")
    }

    fn reserve_of(effects: &[Effect]) -> String {
        effects
            .iter()
            .find_map(|effect| match effect {
                Effect::Reserve { shard: 1, reservation, .. } => Some(reservation.clone()),
                _ => None,
            })
            .expect("a Reservation")
    }

    #[test]
    fn a_boundary_driver_is_offered_through_their_shard_and_leased_for_the_trip() {
        let (west, east) = two_cells();
        let (mut a, mut b) = shards();
        ping(&mut a, "d1", centre(west), 0.0);
        assert!(request(&mut b, "r1", centre(east), 0.0).is_empty(), "no driver of B's own");

        // B finds A's driver near the boundary and relays an offer through A.
        let (driver, offer, ride, pickup, expires_at) = relay_of(&search(&mut b, &a, 1.0));
        assert_eq!((driver.as_str(), ride.as_str()), ("d1", "r1"));
        let (delivered, effects) = a.relay_offer(1, offer.clone(), driver.clone(), ride.clone(), pickup, expires_at);
        assert!(delivered);
        assert_eq!(offer_of(&effects), ("d1".into(), offer.clone(), "r1".into()));
        assert_eq!(a.sizes().idle, 0, "holding the relayed offer, d1 gets nothing else from A");

        // The Accept reserves on A; B confirms in Postgres and owns the Trip.
        let reservation = reserve_of(&a.accept("d1", &offer));
        let commit = b.remote_accept(0, reservation.clone(), &offer, "d1", "r1");
        let Some(Effect::CommitConfirm { trip, .. }) = commit.first() else { panic!("{commit:?}") };
        let trip = trip.clone();
        let decided = b.confirm_decided(&offer, "d1", "r1", 0, reservation.clone(), Some(trip.clone()), 2.0);
        assert!(decided.contains(&Effect::TripStatus { ride: "r1".into(), trip: trip.clone(), status: "assigned" }));
        assert!(!decided.iter().any(|effect| matches!(effect, Effect::TripAssigned { .. })), "A tells its driver");

        // A leases the driver and sends them to B, which pins them there for the Trip.
        let settled = a.reservation_settled(&offer, "d1", reservation.clone(), 1, PeerAnswer::Confirmed { trip: trip.clone() }, 2.0);
        assert!(settled.contains(&Effect::Redirect { driver: "d1".into(), shard: 1 }));
        assert_eq!(a.sizes().leases, 1);
        let joined = ping(&mut b, "d1", centre(west), 3.0);
        assert!(joined.contains(&Effect::TripAssigned { driver: "d1".into(), trip: trip.clone(), ride: "r1".into() }));
        assert!(!joined.iter().any(|effect| matches!(effect, Effect::Redirect { .. })), "pinned while on the Trip");

        // The Trip ends on B, which releases the lease on A.
        let ended = b.trip_event("d1", &trip, TripStage::Completed, 4.0);
        assert!(ended.contains(&Effect::ReleaseLease { shard: 0, reservation: reservation.clone() }));
        assert_eq!(ping(&mut b, "d1", centre(west), 5.0), vec![Effect::Redirect { driver: "d1".into(), shard: 0 }]);
        a.lease_released(&reservation, 5.0);
        assert_eq!(a.sizes().leases, 0);
        ping(&mut a, "d1", centre(west), 6.0);
        assert_eq!(a.sizes().idle, 1, "free on A again");
    }

    #[test]
    fn a_leased_driver_reconnecting_stays_held_until_the_peer_answers() {
        let (west, _) = two_cells();
        let (mut a, _) = shards();
        a.recover_lease("d1".into(), "v1".into(), 1);
        let effects = ping(&mut a, "d1", centre(west), 0.0);
        assert_eq!(effects, vec![Effect::CheckLease { shard: 1, reservation: "v1".into() }]);
        assert_eq!(a.sizes().idle, 0);
        let (delivered, _) = a.relay_offer(1, "o9".into(), "d1".into(), "r9".into(), centre(west), 30.0);
        assert!(!delivered, "a held driver takes no offers");
        a.lease_released("v1", 1.0);
        assert_eq!(a.sizes().idle, 1);
    }

    #[test]
    fn a_refused_reservation_frees_the_driver() {
        let (west, east) = two_cells();
        let (mut a, mut b) = shards();
        ping(&mut a, "d1", centre(west), 0.0);
        request(&mut b, "r1", centre(east), 0.0);
        let (driver, offer, ride, pickup, expires_at) = relay_of(&search(&mut b, &a, 1.0));
        a.relay_offer(1, offer.clone(), driver, ride, pickup, expires_at);
        let reservation = reserve_of(&a.accept("d1", &offer));
        let settled = a.reservation_settled(&offer, "d1", reservation, 1, PeerAnswer::Refused, 2.0);
        assert!(settled.contains(&Effect::OfferGone { driver: "d1".into(), offer }));
        assert_eq!((a.sizes().idle, a.sizes().leases), (1, 0));
    }

    #[test]
    fn a_declined_relay_comes_back_and_a_relayed_offer_expires_without_silence_checks() {
        let (west, east) = two_cells();
        let (mut a, mut b) = shards();
        ping(&mut a, "d1", centre(west), 0.0);
        ping(&mut a, "d2", Position { lat: centre(west).lat + 0.003, ..centre(west) }, 0.0);
        request(&mut b, "r1", centre(east), 0.0);
        let (driver, offer, ride, pickup, expires_at) = relay_of(&search(&mut b, &a, 1.0));
        a.relay_offer(1, offer.clone(), driver.clone(), ride, pickup, expires_at);

        let declined = a.decline(&driver, &offer, 2.0);
        assert_eq!(declined, vec![Effect::CloseRelayed { shard: 1, offer: offer.clone(), reason: OfferClosed::Declined }]);
        // B re-queues the ride, and at once offers the other boundary driver, also through A.
        let closed = b.relayed_closed(&offer, OfferClosed::Declined, 2.0);
        let reopened = settle(&mut b, closed, 2.0);
        assert!(reopened.contains(&Effect::CloseOffers { offers: vec![offer], reason: OfferClosed::Declined }));
        let (next, second, ..) = relay_of(&reopened);
        assert_ne!(next, driver, "the decliner isn't asked again at once");

        // B never hears pings from A's drivers, so only expiry closes its offer, and A is told.
        let quiet = b.tick(3.0 + SILENT_OFFER_SECONDS + 1.0);
        assert!(!quiet.iter().any(|effect| matches!(effect, Effect::WithdrawRelayed { .. })));
        let expired = b.tick(3.0 + OFFER_TIMEOUT_SECONDS + 1.0);
        assert!(expired.contains(&Effect::WithdrawRelayed { shard: 0, offer: second }));
    }

    #[test]
    fn drivers_and_riders_on_another_shards_cells_are_redirected() {
        let (west, east) = two_cells();
        let (mut a, _) = shards();
        assert_eq!(ping(&mut a, "d1", centre(east), 0.0), vec![Effect::Redirect { driver: "d1".into(), shard: 1 }]);
        assert_eq!(a.sizes().drivers, 0);
        assert_eq!(
            a.ride_request("r1".into(), "q".into(), centre(east)),
            vec![Effect::RedirectRider { ride: "r1".into(), shard: 1 }]
        );
        // Sent here under a newer map than A has: A keeps them rather than bounce them back.
        assert!(a.location_ping_from("d2", centre(east), 2, 0.0).is_empty());
        assert_eq!(a.sizes().drivers, 1);

        // An idle driver crossing the boundary moves, and their open offer is withdrawn.
        ping(&mut a, "d3", centre(west), 0.0);
        let (_, offer, _) = offer_of(&request(&mut a, "r3", centre(west), 0.0));
        let moved = ping(&mut a, "d3", centre(east), 1.0);
        assert!(moved.contains(&Effect::OfferGone { driver: "d3".into(), offer: offer.clone() }));
        assert!(moved.contains(&Effect::Redirect { driver: "d3".into(), shard: 1 }));
    }

    #[test]
    fn a_new_map_moves_drivers_as_they_next_ping_but_never_mid_trip() {
        let (west, _) = two_cells();
        let (mut a, _) = shards();
        ping(&mut a, "idle", centre(west), 0.0);
        ping(&mut a, "busy", centre(west), 0.0);
        a.recover_trip("t1".into(), "busy".into(), "r1".into());
        a.set_map(map(2, 1));
        assert_eq!(a.map_version(), Some(2));
        assert_eq!(ping(&mut a, "idle", centre(west), 1.0), vec![Effect::Redirect { driver: "idle".into(), shard: 1 }]);
        assert!(ping(&mut a, "busy", centre(west), 1.0).is_empty(), "the Trip pins them here");
        // An older map never replaces a newer one.
        a.set_map(map(1, 0));
        assert_eq!(a.map_version(), Some(2));
    }
}
