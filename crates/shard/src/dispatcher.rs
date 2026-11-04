//! Baseline matcher: each Ride Request is offered, in arrival order, to the nearest
//! idle driver, found through the location index. It's deliberately naive, so later policies have something to beat.
//!
//! The dispatcher holds no connections and does no I/O. It returns effects, and Postgres
//! results come back to it as calls, so Accepts can race in Postgres while matching goes on.

use std::collections::{HashMap, HashSet, VecDeque};

use assignment::{AcceptOutcome, OfferClosed, TripStage};
use geo::{LocationIndex, Position, Resolution};

pub const OFFER_TIMEOUT_SECONDS: f64 = 20.0;
/// Starting values with 4s pings: 3 missed pings for an idle driver, 2 for one holding an offer.
pub const SILENT_IDLE_SECONDS: f64 = 12.0;
pub const SILENT_OFFER_SECONDS: f64 = 8.0;
const INDEX_RESOLUTION: Resolution = Resolution::Nine;

#[derive(Debug, Clone, PartialEq)]
pub enum Effect {
    // Sent to clients.
    Offer { driver: String, offer: String, ride: String, pickup: Position, expires_at: f64 },
    OfferGone { driver: String, offer: String },
    TripAssigned { driver: String, trip: String, ride: String },
    TripStatus { ride: String, trip: String, status: &'static str },
    // Written to Postgres. Storing a ride or an offer reports back when done.
    StoreRide { ride: String, quote: String, pickup: Position },
    StoreOffer { offer: String, ride: String, driver: String, expires_at: f64 },
    CloseOffers { offers: Vec<String>, reason: OfferClosed },
    /// Reports back with the outcome Postgres decided.
    CommitAccept { offer: String, driver: String, trip: String },
    RecordTripEvent { trip: String, driver: String, stage: TripStage },
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
    tried: HashSet<String>,
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
    expires_at: f64,
    stage: Stage,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Sizes {
    pub drivers: usize,
    pub idle: usize,
    pub rides: usize,
    pub waiting: usize,
    pub offers: usize,
    pub trips: usize,
}

#[derive(Debug)]
pub struct Dispatcher {
    /// Keeps ids unique across Shard restarts, since Postgres keeps the old ones.
    id_prefix: String,
    drivers: HashMap<String, Driver>,
    idle: LocationIndex,
    /// Unassigned Ride Requests; stored ones are also in `waiting` or on an offer.
    rides: HashMap<String, Ride>,
    waiting: VecDeque<String>,
    offers: HashMap<String, OpenOffer>,
    trips: HashMap<String, (String, String)>,
    /// Kept alongside `offers` and `trips`, so checking whether someone is busy isn't a scan.
    offers_by_driver: HashMap<String, usize>,
    offers_by_ride: HashMap<String, usize>,
    trips_by_driver: HashMap<String, usize>,
    next_id: u64,
}

impl Default for Dispatcher {
    fn default() -> Self {
        Self::new("")
    }
}

impl Dispatcher {
    pub fn new(id_prefix: &str) -> Self {
        Self {
            id_prefix: id_prefix.to_owned(),
            drivers: HashMap::new(),
            idle: LocationIndex::new(INDEX_RESOLUTION),
            rides: HashMap::new(),
            waiting: VecDeque::new(),
            offers: HashMap::new(),
            trips: HashMap::new(),
            offers_by_driver: HashMap::new(),
            offers_by_ride: HashMap::new(),
            trips_by_driver: HashMap::new(),
            next_id: 0,
        }
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
        self.offers.insert(id, offer);
    }

    fn take_offer(&mut self, id: &str) -> Option<OpenOffer> {
        let offer = self.offers.remove(id)?;
        decrement(&mut self.offers_by_driver, &offer.driver);
        decrement(&mut self.offers_by_ride, &offer.ride);
        Some(offer)
    }

    pub fn sizes(&self) -> Sizes {
        Sizes {
            drivers: self.drivers.len(),
            idle: self.idle.len(),
            rides: self.rides.len(),
            waiting: self.waiting.len(),
            offers: self.offers.len(),
            trips: self.trips.len(),
        }
    }

    fn new_id(&mut self, kind: char) -> String {
        self.next_id += 1;
        format!("{kind}{}{}", self.id_prefix, self.next_id)
    }

    pub fn location_ping(&mut self, driver: &str, position: Position, now: f64) -> Vec<Effect> {
        if !position.is_finite() {
            return Vec::new();
        }
        let entry = self
            .drivers
            .entry(driver.to_owned())
            .or_insert(Driver { position, online: false, last_ping: now, silent: false });
        entry.position = position;
        entry.last_ping = now;
        let came_back = !entry.online || entry.silent;
        if !came_back {
            // Only idle drivers are in the index, and upserting one follows their move.
            if self.idle.contains(driver) {
                self.idle.upsert(driver, position);
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
        self.make_idle(driver);
        effects.extend(self.dispatch(now));
        effects
    }

    pub fn driver_disconnected(&mut self, driver: &str) {
        if let Some(entry) = self.drivers.get_mut(driver) {
            entry.online = false;
        }
        self.idle.remove(driver);
    }

    /// The ride is matched only once Postgres has stored it.
    pub fn ride_request(&mut self, ride: String, quote: String, pickup: Position) -> Vec<Effect> {
        if self.rides.contains_key(&ride) {
            return Vec::new();
        }
        self.rides.insert(ride.clone(), Ride { pickup, tried: HashSet::new() });
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
            return vec![Effect::Offer {
                driver: offer.driver.clone(),
                offer: offer_id.to_owned(),
                ride: offer.ride.clone(),
                pickup,
                expires_at: offer.expires_at,
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
        let trip = self.new_id('t');
        self.offers.get_mut(offer_id).expect("checked above").stage = Stage::Accepting;
        vec![Effect::CommitAccept { offer: offer_id.to_owned(), driver: driver.to_owned(), trip }]
    }

    pub fn accept_decided(&mut self, offer_id: &str, outcome: AcceptOutcome, now: f64) -> Vec<Effect> {
        let Some(offer) = self.take_offer(offer_id) else { return Vec::new() };
        let mut effects = Vec::new();
        match outcome {
            AcceptOutcome::Assigned { trip, ride } => {
                self.rides.remove(&ride);
                self.add_trip(trip.clone(), offer.driver.clone(), ride.clone());
                effects.push(Effect::TripAssigned { driver: offer.driver.clone(), trip: trip.clone(), ride: ride.clone() });
                effects.push(Effect::TripStatus { ride: ride.clone(), trip, status: "assigned" });
                // Postgres has already withdrawn these; memory and the drivers catch up.
                let overtaken: Vec<String> = self
                    .offers
                    .iter()
                    .filter(|(_, other)| other.stage != Stage::Accepting && (other.ride == ride || other.driver == offer.driver))
                    .map(|(id, _)| id.clone())
                    .collect();
                let mut closed = Vec::new();
                for id in overtaken {
                    let other = self.take_offer(&id).expect("listed above");
                    if other.stage == Stage::Open {
                        effects.push(Effect::OfferGone { driver: other.driver.clone(), offer: id.clone() });
                        closed.push(id);
                    }
                    if other.ride != ride {
                        self.waiting.push_front(other.ride);
                    }
                    self.make_idle(&other.driver);
                }
                if !closed.is_empty() {
                    effects.push(Effect::CloseOffers { offers: closed, reason: OfferClosed::Withdrawn });
                }
            }
            AcceptOutcome::Refused(_) => {
                effects.push(Effect::OfferGone { driver: offer.driver.clone(), offer: offer_id.to_owned() });
                if let Some(ride) = self.rides.get_mut(&offer.ride) {
                    ride.tried.insert(offer.driver.clone());
                    self.waiting.push_front(offer.ride);
                }
                self.make_idle(&offer.driver);
            }
        }
        effects.extend(self.dispatch(now));
        effects
    }

    pub fn decline(&mut self, driver: &str, offer_id: &str, now: f64) -> Vec<Effect> {
        let open = self
            .offers
            .get(offer_id)
            .is_some_and(|offer| offer.driver == driver && offer.stage == Stage::Open);
        if !open {
            return Vec::new();
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
            self.make_idle(driver);
            effects.push(Effect::TripStatus { ride, trip: trip.to_owned(), status: "completed" });
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
            }
        }
        let mut expired = Vec::new();
        let mut withdrawn = Vec::new();
        for (id, offer) in &self.offers {
            if offer.stage != Stage::Open {
                continue;
            }
            let silent_for = self.drivers.get(&offer.driver).map_or(f64::INFINITY, |driver| now - driver.last_ping);
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
            for id in &ids {
                if let Some(driver) = self.reopen(id) {
                    effects.push(Effect::OfferGone { driver, offer: id.clone() });
                }
            }
            effects.push(Effect::CloseOffers { offers: ids, reason });
        }
        effects.extend(self.dispatch(now));
        effects
    }

    /// Returns the offer's ride to the front of the queue, and frees its driver.
    fn reopen(&mut self, offer_id: &str) -> Option<String> {
        let offer = self.take_offer(offer_id)?;
        if let Some(ride) = self.rides.get_mut(&offer.ride) {
            ride.tried.insert(offer.driver.clone());
            self.waiting.push_front(offer.ride);
        }
        self.make_idle(&offer.driver);
        Some(offer.driver)
    }

    /// Adds the driver to the index if they are online, responsive and hold nothing.
    fn make_idle(&mut self, driver: &str) {
        let Some(entry) = self.drivers.get(driver).filter(|entry| entry.online && !entry.silent) else { return };
        let busy = self.offers_by_driver.contains_key(driver) || self.trips_by_driver.contains_key(driver);
        if !busy {
            self.idle.upsert(driver, entry.position);
        }
    }

    fn nearest_idle(&self, ride: &Ride) -> Option<String> {
        self.idle.nearest(ride.pickup, |id| !ride.tried.contains(id)).map(str::to_owned)
    }

    fn dispatch(&mut self, now: f64) -> Vec<Effect> {
        let mut effects = Vec::new();
        let mut still_waiting = VecDeque::new();
        let mut queued = HashSet::new();
        while let Some(ride_id) = self.waiting.pop_front() {
            // A ride can be queued twice when offers close together; assigned rides drop out.
            if !self.rides.contains_key(&ride_id) || !queued.insert(ride_id.clone()) {
                continue;
            }
            if self.offers_by_ride.contains_key(&ride_id) {
                continue;
            }
            if self.idle.is_empty() {
                still_waiting.push_back(ride_id);
                continue;
            }
            let Some(driver) = self.nearest_idle(&self.rides[&ride_id]) else {
                // Every idle driver has refused: let them be asked again next time.
                if let Some(ride) = self.rides.get_mut(&ride_id) {
                    ride.tried.clear();
                }
                still_waiting.push_back(ride_id);
                continue;
            };
            self.idle.remove(&driver);
            let offer = self.new_id('o');
            let expires_at = now + OFFER_TIMEOUT_SECONDS;
            self.add_offer(
                offer.clone(),
                OpenOffer { driver: driver.clone(), ride: ride_id.clone(), expires_at, stage: Stage::Storing },
            );
            effects.push(Effect::StoreOffer { offer, ride: ride_id, driver, expires_at });
        }
        self.waiting = still_waiting;
        effects
    }
}

fn decrement(counts: &mut HashMap<String, usize>, key: &str) {
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
}
