//! WebSocket front end, and the gRPC service for peer Shards. One task owns the Dispatcher;
//! connections and peer calls exchange messages with it.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use assignment::{AcceptOutcome, Held, OfferClosed, PeerDecision, Store, TripStage};

use futures_util::{SinkExt, StreamExt};
use geo::Position;
use geo::shard_map::{ShardId, ShardMap};
use prost::Message as _;
use protocol::client::{
    DriverToServer, LatLng, Offer, OfferGone, Quote, Redirect, RiderToServer, ServerToDriver, ServerToRider, TripAssigned,
    TripEventKind, TripStatus, driver_to_server, rider_to_server, server_to_driver, server_to_rider,
};
use protocol::shard as peer;
use tokio::net::TcpListener;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::sync::oneshot;
use tokio_tungstenite::accept_async_with_config;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;

use crate::dispatcher::{Dispatcher, Effect, PeerAnswer, Policy};
use crate::peers::{Faults, Peers, Step};
use crate::ping_log::{PingLog, PingRecord};
use crate::sharding::Placement;

const TICK: Duration = Duration::from_millis(250);
const QUOTE_VALID_MS: i64 = 60_000;
const STATS_EVERY: Duration = Duration::from_secs(60);
const RETRY_FIRST: Duration = Duration::from_millis(100);
const RETRY_MAX: Duration = Duration::from_secs(2);
/// Starting value, from ARCHITECTURE.md → Sharding: after this, the rider's Shard refuses to confirm.
const RESERVATION_MS: i64 = 5_000;
/// Fire-and-forget peer calls are retried this often before the backstop takes over.
const NOTIFY_ATTEMPTS: u32 = 5;

type Outbox = UnboundedSender<Vec<u8>>;

struct DriverLink {
    outbox: Outbox,
    reported_at_ms: i64,
}

enum Event {
    DriverPing { driver: String, position: Position, reported_at_ms: i64, map_version: u32, outbox: Outbox },
    DriverGone { driver: String, outbox: Outbox },
    Accept { driver: String, offer: String },
    Decline { driver: String, offer: String },
    TripUpdate { driver: String, trip: String, stage: TripStage },
    Rider { message: rider_to_server::Message, outbox: Outbox },
    RideStored { ride: String },
    OfferStored { offer: String },
    AcceptDecided { offer: String, outcome: AcceptOutcome },
    // Peer calls and their results.
    BoundaryDrivers { shard: ShardId, drivers: Vec<(String, Position)> },
    Snapshot { cells: Vec<geo::CellIndex>, reply: oneshot::Sender<Vec<(String, Position)>> },
    RelayOffer { from: ShardId, request: peer::RelayOfferRequest, reply: oneshot::Sender<bool> },
    WithdrawRelayed { offer: String },
    RelayClosed { offer: String, reason: OfferClosed },
    Confirm { from: ShardId, request: peer::ConfirmRequest, reply: oneshot::Sender<peer::ReservationState> },
    ConfirmDecided { offer: String, driver: String, ride: String, shard: ShardId, reservation: String, trip: Option<String> },
    ReservationSettled { offer: String, driver: String, reservation: String, shard: ShardId, answer: PeerAnswer },
    LeaseReleased { reservation: String },
    LeaseHeld { reservation: String },
    ApplyMap { map: ShardMap },
}

/// Postgres work in flight, for the stats line.
#[derive(Default)]
struct DbStats {
    in_flight: AtomicU64,
    failed_attempts: AtomicU64,
    peer_failures: AtomicU64,
}

fn unix_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |elapsed| elapsed.as_millis() as i64)
}

fn position(point: Option<LatLng>) -> Position {
    let point = point.unwrap_or_default();
    Position { lat: point.lat, lng: point.lng }
}

fn peer_position(point: Option<peer::LatLng>) -> Position {
    let point = point.unwrap_or_default();
    Position { lat: point.lat, lng: point.lng }
}

fn driver_frame(message: server_to_driver::Message) -> Vec<u8> {
    ServerToDriver { message: Some(message) }.encode_to_vec()
}

fn rider_frame(message: server_to_rider::Message) -> Vec<u8> {
    ServerToRider { message: Some(message) }.encode_to_vec()
}

fn offer_close(reason: OfferClosed) -> peer::OfferClose {
    match reason {
        OfferClosed::Declined => peer::OfferClose::Declined,
        OfferClosed::Expired => peer::OfferClose::Expired,
        OfferClosed::Withdrawn => peer::OfferClose::Withdrawn,
    }
}

fn reservation_state(decision: &PeerDecision) -> peer::ReservationState {
    let (status, trip) = match decision {
        PeerDecision::Confirmed { trip } => (peer::ReservationStatus::Confirmed, trip.clone()),
        PeerDecision::Released { trip } => (peer::ReservationStatus::Released, trip.clone()),
        PeerDecision::Aborted => (peer::ReservationStatus::Aborted, String::new()),
        PeerDecision::Expired => (peer::ReservationStatus::Expired, String::new()),
    };
    peer::ReservationState { status: status.into(), trip_id: trip }
}

fn peer_decision(state: &peer::ReservationState) -> PeerDecision {
    match peer::ReservationStatus::try_from(state.status) {
        Ok(peer::ReservationStatus::Confirmed) => PeerDecision::Confirmed { trip: state.trip_id.clone() },
        Ok(peer::ReservationStatus::Released) => PeerDecision::Released { trip: state.trip_id.clone() },
        Ok(peer::ReservationStatus::Expired) => PeerDecision::Expired,
        _ => PeerDecision::Aborted,
    }
}

/// Running as one of several Shards.
pub struct Sharded {
    pub placement: Placement,
    pub peer_listener: TcpListener,
    pub faults: Faults,
}

/// Takes several listeners per kind because one client address can only reach one server
/// port about 28,000 times at once, far short of the load target.
///
/// Active Trips are recovered from Postgres before any connection is served.
pub async fn serve(drivers: Vec<TcpListener>, riders: Vec<TcpListener>, ping_log: Option<PingLog>, store: Store, policy: Policy) {
    serve_sharded(drivers, riders, ping_log, store, policy, None).await;
}

pub async fn serve_sharded(
    drivers: Vec<TcpListener>,
    riders: Vec<TcpListener>,
    ping_log: Option<PingLog>,
    store: Store,
    policy: Policy,
    sharded: Option<Sharded>,
) {
    let db = Arc::new(DbStats::default());
    let recovered = retry(&db, || store.recover()).await;
    eprintln!("recovered {} active Trips from Postgres", recovered.len());
    eprintln!("policy: {policy}");
    // Ids carry the Shard and start time, so they never repeat ids Postgres kept from before a
    // restart, or another Shard's.
    let mut dispatcher = match &sharded {
        Some(sharded) => {
            let placement = &sharded.placement;
            let prefix = format!("{}-{:x}-", placement.shard, unix_ms());
            Dispatcher::sharded(&prefix, policy, placement.shard, placement.map.clone())
        }
        None => Dispatcher::with_policy(&format!("{:x}-", unix_ms()), policy),
    };
    for trip in recovered {
        dispatcher.recover_trip(trip.trip, trip.driver, trip.ride);
    }
    let (events, inbox) = unbounded_channel();
    let peers = match sharded {
        Some(Sharded { placement, peer_listener, faults }) => {
            if let Some(described) = faults.describe() {
                eprintln!("{described}");
            }
            let peers = Arc::new(Peers::new(placement.shard, &placement.map, faults).expect("map addresses checked at load"));
            for leased in retry(&db, || store.leased_trips()).await {
                dispatcher.recover_leased_trip(leased.trip, leased.peer, leased.reservation);
            }
            let held = retry(&db, || store.held_reservations()).await;
            eprintln!("recovered {} held Reservations", held.len());
            for reservation in held {
                match reservation.held {
                    Held::Confirmed => dispatcher.recover_lease(reservation.driver, reservation.id, reservation.peer),
                    // The driver is held, so not indexed, until the peer answers.
                    Held::Reserved => {
                        dispatcher.recover_lease(reservation.driver.clone(), reservation.id.clone(), reservation.peer);
                        let (store, db, events, peers) = (store.clone(), Arc::clone(&db), events.clone(), Arc::clone(&peers));
                        tokio::spawn(async move {
                            let decision = settle_by_status(&peers, reservation.peer, &reservation.id, &db).await;
                            record_settlement(&store, &db, &peers, &reservation.id, &decision).await;
                            let _ = events.send(Event::LeaseReleased { reservation: reservation.id.clone() });
                            let _ = events.send(settled_event(reservation.offer, reservation.driver, reservation.id, reservation.peer, &decision));
                        });
                    }
                }
            }
            let service = PeerService { me: placement.shard, events: events.clone(), store: store.clone() };
            tokio::spawn(async move {
                let incoming = tonic::transport::server::TcpIncoming::from(peer_listener);
                let result = tonic::transport::Server::builder()
                    .add_service(peer::shard_server::ShardServer::new(service))
                    .serve_with_incoming(incoming)
                    .await;
                if let Err(error) = result {
                    eprintln!("peer service stopped: {error}");
                }
            });
            Some((peers, placement.map))
        }
        None => None,
    };
    for listener in drivers {
        tokio::spawn(accept_drivers(listener, events.clone()));
    }
    for listener in riders {
        tokio::spawn(accept_riders(listener, events.clone()));
    }
    let postgres = Postgres { store, events, db };
    let peers = peers.map(|(peers, map)| PeerLinks { peers, map, confirms: HashMap::new(), searching: HashSet::new() });
    run_dispatcher(dispatcher, policy, inbox, ping_log, postgres, peers).await;
}

/// Waits for the next Batch Window, or forever when the policy doesn't batch.
async fn next_batch(batch: &mut Option<tokio::time::Interval>) {
    match batch {
        Some(interval) => {
            interval.tick().await;
        }
        None => std::future::pending().await,
    }
}

struct Postgres {
    store: Store,
    events: UnboundedSender<Event>,
    db: Arc<DbStats>,
}

/// The dispatcher task's view of the other Shards.
struct PeerLinks {
    peers: Arc<Peers>,
    map: ShardMap,
    /// Confirms waiting for Postgres, by Reservation: the reply and the expiry it was sent with.
    confirms: HashMap<String, (oneshot::Sender<peer::ReservationState>, i64)>,
    /// Peers with a boundary search in flight, so a slow peer doesn't pile them up.
    searching: HashSet<ShardId>,
}

struct Links {
    started: Instant,
    drivers: HashMap<String, DriverLink>,
    riders: HashMap<String, Outbox>,
}

impl Links {
    fn now(&self) -> f64 {
        self.started.elapsed().as_secs_f64()
    }

    fn to_unix_ms(&self, at: f64) -> i64 {
        unix_ms() + ((at - self.now()) * 1000.0) as i64
    }

    fn at_unix_ms(&self, ms: i64) -> f64 {
        self.now() + (ms - unix_ms()) as f64 / 1000.0
    }
}

async fn run_dispatcher(
    mut dispatcher: Dispatcher,
    policy: Policy,
    mut inbox: UnboundedReceiver<Event>,
    ping_log: Option<PingLog>,
    postgres: Postgres,
    mut peers: Option<PeerLinks>,
) {
    let mut links = Links { started: Instant::now(), drivers: HashMap::new(), riders: HashMap::new() };
    let mut quotes = 0_u64;
    let mut tick = tokio::time::interval(TICK);
    let mut stats = tokio::time::interval(STATS_EVERY);
    let mut batch = policy.batch_window().map(tokio::time::interval);

    loop {
        let effects = tokio::select! {
            _ = tick.tick() => dispatcher.tick(links.now()),
            _ = next_batch(&mut batch) => dispatcher.batch(links.now()),
            _ = stats.tick() => {
                let sizes = dispatcher.sizes();
                let mut line = format!(
                    "stats: driver links {}, rider links {}, drivers {}, idle {}, rides {}, waiting {}, offers {}, trips {}, postgres in flight {}, postgres failed attempts {}, batches {}, batch deadline hits {}, broadcast rounds {}, broadcast areas {}, area switches {}",
                    links.drivers.len(), links.riders.len(), sizes.drivers, sizes.idle, sizes.rides, sizes.waiting,
                    sizes.offers, sizes.trips,
                    postgres.db.in_flight.load(Ordering::Relaxed), postgres.db.failed_attempts.load(Ordering::Relaxed),
                    sizes.batches, sizes.batch_deadline_hits, sizes.broadcast_rounds, sizes.broadcast_areas, sizes.area_switches,
                );
                if let Some(peers) = &peers {
                    line.push_str(&format!(
                        ", map version {}, boundary drivers {}, relayed out {}, relayed in {}, reservations {}, leases {}, leased trips {}, redirects {}, peer call failures {}, injected faults {}",
                        peers.map.version(), sizes.boundary_drivers, sizes.relayed_out, sizes.relayed_in, sizes.reservations,
                        sizes.leases, sizes.leased_trips, sizes.redirects,
                        postgres.db.peer_failures.load(Ordering::Relaxed), peers.peers.faults.injected.load(Ordering::Relaxed),
                    ));
                }
                eprintln!("{line}");
                Vec::new()
            }
            event = inbox.recv() => {
                let Some(event) = event else { return };
                let now = links.now();
                match event {
                    Event::DriverPing { driver, position, reported_at_ms, map_version, outbox } => {
                        if let Some(log) = ping_log.as_ref().filter(|_| position.is_finite()) {
                            log.append(&PingRecord {
                                driver: driver.clone(),
                                reported_at_ms,
                                received_at_ms: unix_ms(),
                                position,
                            });
                        }
                        links.drivers.insert(driver.clone(), DriverLink { outbox, reported_at_ms });
                        dispatcher.location_ping_from(&driver, position, map_version, now)
                    }
                    Event::DriverGone { driver, outbox } => {
                        // A reconnect may already have replaced this link.
                        if links.drivers.get(&driver).is_some_and(|link| link.outbox.same_channel(&outbox)) {
                            links.drivers.remove(&driver);
                            dispatcher.driver_disconnected(&driver);
                        }
                        Vec::new()
                    }
                    Event::Accept { driver, offer } => dispatcher.accept(&driver, &offer),
                    Event::Decline { driver, offer } => dispatcher.decline(&driver, &offer, now),
                    Event::TripUpdate { driver, trip, stage } => dispatcher.trip_event(&driver, &trip, stage, now),
                    Event::RideStored { ride } => dispatcher.ride_stored(ride, now),
                    Event::OfferStored { offer } => dispatcher.offer_stored(&offer, now),
                    Event::AcceptDecided { offer, outcome } => dispatcher.accept_decided(&offer, outcome, now),
                    Event::Rider { message: rider_to_server::Message::QuoteRequest(_), outbox } => {
                        quotes += 1;
                        let _ = outbox.send(rider_frame(server_to_rider::Message::Quote(Quote {
                            quote_id: format!("q{quotes}"),
                            surge_multiplier: 1.0,
                            expires_at_ms: unix_ms() + QUOTE_VALID_MS,
                        })));
                        Vec::new()
                    }
                    Event::Rider { message: rider_to_server::Message::RideRequest(request), outbox } => {
                        links.riders.insert(request.ride_request_id.clone(), outbox);
                        dispatcher.ride_request_from(request.ride_request_id, request.quote_id, position(request.pickup), request.map_version)
                    }
                    Event::BoundaryDrivers { shard, drivers } => {
                        if let Some(peers) = &mut peers {
                            peers.searching.remove(&shard);
                        }
                        dispatcher.boundary_drivers(shard, drivers);
                        Vec::new()
                    }
                    Event::Snapshot { cells, reply } => {
                        let _ = reply.send(dispatcher.boundary_snapshot(&cells));
                        Vec::new()
                    }
                    Event::RelayOffer { from, request, reply } => {
                        let expires_at = links.at_unix_ms(request.expires_at_ms);
                        let (delivered, effects) = dispatcher.relay_offer(
                            from,
                            request.offer_id,
                            request.driver_id,
                            request.ride_request_id,
                            peer_position(request.pickup),
                            expires_at,
                        );
                        let _ = reply.send(delivered);
                        effects
                    }
                    Event::WithdrawRelayed { offer } => dispatcher.withdraw_relayed(&offer, now),
                    Event::RelayClosed { offer, reason } => dispatcher.relayed_closed(&offer, reason, now),
                    Event::Confirm { from, request, reply } => match &mut peers {
                        Some(peers) => {
                            peers.confirms.insert(request.reservation_id.clone(), (reply, request.expires_at_ms));
                            dispatcher.remote_accept(from, request.reservation_id, &request.offer_id, &request.driver_id, &request.ride_request_id)
                        }
                        None => Vec::new(),
                    },
                    Event::ConfirmDecided { offer, driver, ride, shard, reservation, trip } => {
                        dispatcher.confirm_decided(&offer, &driver, &ride, shard, reservation, trip, now)
                    }
                    Event::ReservationSettled { offer, driver, reservation, shard, answer } => {
                        dispatcher.reservation_settled(&offer, &driver, reservation, shard, answer, now)
                    }
                    Event::LeaseReleased { reservation } => dispatcher.lease_released(&reservation, now),
                    Event::LeaseHeld { reservation } => dispatcher.lease_held(&reservation, now),
                    Event::ApplyMap { map } => {
                        if let Some(peers) = &mut peers
                            && map.version() > peers.map.version()
                        {
                            if let Err(error) = peers.peers.update(&map) {
                                eprintln!("map version {}: {error}", map.version());
                            }
                            eprintln!("serving map version {}", map.version());
                            peers.map = map.clone();
                            dispatcher.set_map(map);
                        }
                        Vec::new()
                    }
                }
            }
        };
        deliver(effects, &mut links, &postgres, peers.as_mut());
    }
}

/// Retries until Postgres answers: the Shard can't make progress on this without it.
async fn retry<T, F, Fut>(db: &DbStats, mut attempt: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, assignment::Error>>,
{
    let mut wait = RETRY_FIRST;
    loop {
        match attempt().await {
            Ok(value) => return value,
            Err(_) => {
                db.failed_attempts.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(wait).await;
                wait = (wait * 2).min(RETRY_MAX);
            }
        }
    }
}

/// Runs Postgres work off the dispatcher task, and reports back as an event if asked.
fn in_postgres<F, Fut>(postgres: &Postgres, work: F)
where
    F: FnOnce(Store, Arc<DbStats>) -> Fut + Send + 'static,
    Fut: Future<Output = Option<Event>> + Send + 'static,
{
    let (store, db, events) = (postgres.store.clone(), Arc::clone(&postgres.db), postgres.events.clone());
    db.in_flight.fetch_add(1, Ordering::Relaxed);
    tokio::spawn(async move {
        let event = work(store, Arc::clone(&db)).await;
        db.in_flight.fetch_sub(1, Ordering::Relaxed);
        if let Some(event) = event {
            let _ = events.send(event);
        }
    });
}

/// Asks the rider's Shard until it answers. Never gives up: guessing could break the
/// invariant (ARCHITECTURE.md → Reservation crash recovery).
async fn settle_by_status(peers: &Peers, shard: ShardId, reservation: &str, db: &DbStats) -> PeerDecision {
    let mut wait = RETRY_FIRST;
    loop {
        let request = peer::ReservationRef { from_shard: u32::from(peers.me), reservation_id: reservation.to_owned() };
        match peers.call(shard, |mut client| async move { client.get_reservation_status(request).await }).await {
            Ok(state) => return peer_decision(&state),
            Err(_) => {
                db.peer_failures.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(wait).await;
                wait = (wait * 2).min(RETRY_MAX);
            }
        }
    }
}

async fn record_settlement(store: &Store, db: &DbStats, peers: &Peers, reservation: &str, decision: &PeerDecision) {
    peers.faults.maybe_crash(Step::BeforeSettle);
    retry(db, || store.settle_reservation(reservation, decision)).await;
}

fn settled_event(offer: String, driver: String, reservation: String, shard: ShardId, decision: &PeerDecision) -> Event {
    let answer = match decision {
        PeerDecision::Confirmed { trip } => PeerAnswer::Confirmed { trip: trip.clone() },
        // Released can only follow a confirm the driver never heard about: they're free again.
        _ => PeerAnswer::Refused,
    };
    Event::ReservationSettled { offer, driver, reservation, shard, answer }
}

/// Sends a call that only speeds things up, retrying a few times; a backstop covers its loss.
fn notify<T, F, Fut>(peers: &Arc<Peers>, db: &Arc<DbStats>, shard: ShardId, call: F)
where
    T: Send + 'static,
    F: Fn(protocol::shard::shard_client::ShardClient<tonic::transport::Channel>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<tonic::Response<T>, tonic::Status>> + Send + 'static,
{
    let (peers, db) = (Arc::clone(peers), Arc::clone(db));
    tokio::spawn(async move {
        let mut wait = RETRY_FIRST;
        for _ in 0..NOTIFY_ATTEMPTS {
            if peers.call(shard, &call).await.is_ok() {
                return;
            }
            db.peer_failures.fetch_add(1, Ordering::Relaxed);
            tokio::time::sleep(wait).await;
            wait = (wait * 2).min(RETRY_MAX);
        }
    });
}

fn deliver(effects: Vec<Effect>, links: &mut Links, postgres: &Postgres, mut peers: Option<&mut PeerLinks>) {
    let to_driver = |links: &Links, driver: &str, frame: Vec<u8>| {
        if let Some(link) = links.drivers.get(driver) {
            let _ = link.outbox.send(frame);
        }
    };
    for effect in effects {
        match effect {
            Effect::Offer { driver, offer, ride, pickup, expires_at } => {
                let now_ms = unix_ms();
                let reported_at_ms = links.drivers.get(&driver).map_or(now_ms, |link| link.reported_at_ms);
                let frame = driver_frame(server_to_driver::Message::Offer(Offer {
                    offer_id: offer,
                    ride_request_id: ride,
                    pickup: Some(LatLng { lat: pickup.lat, lng: pickup.lng }),
                    expires_at_ms: links.to_unix_ms(expires_at),
                    staleness_ms: (now_ms - reported_at_ms).max(0),
                }));
                to_driver(links, &driver, frame);
            }
            Effect::OfferGone { driver, offer } => {
                to_driver(links, &driver, driver_frame(server_to_driver::Message::OfferGone(OfferGone { offer_id: offer })));
            }
            Effect::TripAssigned { driver, trip, ride } => {
                let assigned = TripAssigned { trip_id: trip, ride_request_id: ride };
                to_driver(links, &driver, driver_frame(server_to_driver::Message::TripAssigned(assigned)));
            }
            Effect::TripStatus { ride, trip, status } => {
                if let Some(link) = links.riders.get(&ride) {
                    let _ = link.send(rider_frame(server_to_rider::Message::TripStatus(TripStatus {
                        trip_id: trip,
                        ride_request_id: ride.clone(),
                        status: status.to_owned(),
                    })));
                }
                if status == "completed" {
                    links.riders.remove(&ride);
                }
            }
            Effect::StoreRide { ride, quote, pickup } => in_postgres(postgres, move |store, db| async move {
                retry(&db, || store.store_ride_request(&ride, &quote, pickup)).await;
                Some(Event::RideStored { ride })
            }),
            Effect::StoreOffer { offer, ride, driver, kind, expires_at } => {
                let expires_at_ms = links.to_unix_ms(expires_at);
                in_postgres(postgres, move |store, db| async move {
                    retry(&db, || store.store_offer(&offer, &ride, &driver, kind, expires_at_ms)).await;
                    Some(Event::OfferStored { offer })
                });
            }
            Effect::CloseOffers { offers, reason } => in_postgres(postgres, move |store, db| async move {
                retry(&db, || store.close_offers(&offers, reason)).await;
                None
            }),
            Effect::CommitAccept { offer, driver, trip } => in_postgres(postgres, move |store, db| async move {
                let outcome = match store.accept(&offer, &driver, &trip).await {
                    Ok(outcome) => outcome,
                    // The commit may or may not have happened; ask before telling anyone.
                    Err(_) => {
                        db.failed_attempts.fetch_add(1, Ordering::Relaxed);
                        retry(&db, || store.accept_outcome(&offer)).await
                    }
                };
                Some(Event::AcceptDecided { offer, outcome })
            }),
            Effect::RecordTripEvent { trip, driver, stage } => in_postgres(postgres, move |store, db| async move {
                retry(&db, || store.trip_event(&trip, &driver, stage)).await;
                None
            }),
            Effect::Redirect { driver, shard } => {
                if let Some(peers) = peers.as_deref()
                    && let Some(address) = peers.map.shard(shard)
                {
                    let redirect = Redirect { urls: address.drivers.clone(), map_version: peers.map.version() };
                    to_driver(links, &driver, driver_frame(server_to_driver::Message::Redirect(redirect)));
                }
            }
            Effect::RedirectRider { ride, shard } => {
                if let Some(peers) = peers.as_deref()
                    && let Some(address) = peers.map.shard(shard)
                    && let Some(link) = links.riders.remove(&ride)
                {
                    let redirect = Redirect { urls: address.riders.clone(), map_version: peers.map.version() };
                    let _ = link.send(rider_frame(server_to_rider::Message::Redirect(redirect)));
                }
            }
            peer_effect => {
                if let Some(peers) = peers.as_deref_mut() {
                    deliver_to_peer(peer_effect, links, postgres, peers);
                }
            }
        }
    }
}

fn deliver_to_peer(effect: Effect, links: &Links, postgres: &Postgres, peers: &mut PeerLinks) {
    let me = u32::from(peers.peers.me);
    let (events, db) = (postgres.events.clone(), Arc::clone(&postgres.db));
    match effect {
        Effect::SearchBoundary { shard, cells } => {
            if !peers.searching.insert(shard) {
                return;
            }
            let client = Arc::clone(&peers.peers);
            tokio::spawn(async move {
                let request = peer::SearchBoundaryDriversRequest { from_shard: me, cells: cells.into_iter().map(u64::from).collect() };
                let drivers = match client.call(shard, |mut c| async move { c.search_boundary_drivers(request).await }).await {
                    Ok(response) => response
                        .drivers
                        .into_iter()
                        .map(|driver| (driver.driver_id, peer_position(driver.position)))
                        .collect(),
                    // An unreachable peer's drivers can't be offered anything anyway.
                    Err(_) => {
                        db.peer_failures.fetch_add(1, Ordering::Relaxed);
                        Vec::new()
                    }
                };
                let _ = events.send(Event::BoundaryDrivers { shard, drivers });
            });
        }
        Effect::RelayOffer { shard, driver, offer, ride, pickup, expires_at } => {
            let client = Arc::clone(&peers.peers);
            let request = peer::RelayOfferRequest {
                from_shard: me,
                offer_id: offer.clone(),
                driver_id: driver,
                ride_request_id: ride,
                pickup: Some(peer::LatLng { lat: pickup.lat, lng: pickup.lng }),
                expires_at_ms: links.to_unix_ms(expires_at),
            };
            tokio::spawn(async move {
                // If the reply is lost the offer may have arrived; treating it as refused is safe,
                // because a Confirm for a closed offer is refused here.
                let delivered = match client.call(shard, |mut c| async move { c.relay_offer(request).await }).await {
                    Ok(response) => response.delivered,
                    Err(_) => {
                        db.peer_failures.fetch_add(1, Ordering::Relaxed);
                        false
                    }
                };
                if !delivered {
                    let _ = events.send(Event::RelayClosed { offer, reason: OfferClosed::Withdrawn });
                }
            });
        }
        Effect::WithdrawRelayed { shard, offer } => notify(&peers.peers, &db, shard, move |mut client| {
            let request = peer::OfferRef { from_shard: me, offer_id: offer.clone() };
            async move { client.withdraw_offer(request).await }
        }),
        Effect::CloseRelayed { shard, offer, reason } => notify(&peers.peers, &db, shard, move |mut client| {
            let request = peer::CloseOfferRequest { from_shard: me, offer_id: offer.clone(), reason: offer_close(reason).into() };
            async move { client.close_offer(request).await }
        }),
        Effect::ReleaseLease { shard, reservation } => {
            peers.peers.faults.maybe_crash(Step::BeforeRelease);
            notify(&peers.peers, &db, shard, move |mut client| {
                let request = peer::ReservationRef { from_shard: me, reservation_id: reservation.clone() };
                async move { client.release(request).await }
            });
        }
        Effect::CheckLease { shard, reservation } => {
            let (client, store) = (Arc::clone(&peers.peers), postgres.store.clone());
            tokio::spawn(async move {
                let request = peer::ReservationRef { from_shard: me, reservation_id: reservation.clone() };
                let Ok(state) = client.call(shard, |mut c| async move { c.get_reservation_status(request).await }).await else {
                    // Tried again when the driver next connects.
                    db.peer_failures.fetch_add(1, Ordering::Relaxed);
                    return;
                };
                let decision = peer_decision(&state);
                if matches!(decision, PeerDecision::Confirmed { .. }) {
                    // The Trip is still on the peer: the driver belongs there.
                    let _ = events.send(Event::LeaseHeld { reservation });
                } else {
                    // Released, or never confirmed (a Reservation recovered unresolved): record
                    // what the peer decided, and the driver is free either way.
                    retry(&db, || store.settle_reservation(&reservation, &decision)).await;
                    let _ = events.send(Event::LeaseReleased { reservation });
                }
            });
        }
        Effect::Reserve { shard, reservation, offer, driver, ride } => {
            let (client, store) = (Arc::clone(&peers.peers), postgres.store.clone());
            let expires_at_ms = unix_ms() + RESERVATION_MS;
            db.in_flight.fetch_add(1, Ordering::Relaxed);
            tokio::spawn(async move {
                let reserved =
                    retry(&db, || store.reserve(&reservation, &driver, &offer, &ride, shard, expires_at_ms)).await;
                let decision = if reserved {
                    client.faults.maybe_crash(Step::AfterReserve);
                    let request = peer::ConfirmRequest {
                        from_shard: me,
                        reservation_id: reservation.clone(),
                        offer_id: offer.clone(),
                        driver_id: driver.clone(),
                        ride_request_id: ride.clone(),
                        expires_at_ms,
                    };
                    let decision = match client.call(shard, |mut c| async move { c.confirm(request).await }).await {
                        Ok(state) => peer_decision(&state),
                        Err(_) => {
                            db.peer_failures.fetch_add(1, Ordering::Relaxed);
                            settle_by_status(&client, shard, &reservation, &db).await
                        }
                    };
                    record_settlement(&store, &db, &client, &reservation, &decision).await;
                    decision
                } else {
                    // Busy here: refused before asking the peer, which will see the offer expire.
                    PeerDecision::Aborted
                };
                db.in_flight.fetch_sub(1, Ordering::Relaxed);
                let _ = events.send(settled_event(offer, driver, reservation, shard, &decision));
            });
        }
        Effect::CommitConfirm { shard, reservation, offer, driver, ride, trip } => {
            let Some((reply, expires_at_ms)) = peers.confirms.remove(&reservation) else { return };
            let events = events.clone();
            let (faults_owner, store) = (Arc::clone(&peers.peers), postgres.store.clone());
            in_postgres(postgres, move |_, db| async move {
                let decision = match store.confirm(&reservation, shard, &offer, &driver, &trip, expires_at_ms).await {
                    Ok(decision) => decision,
                    // The commit may or may not have happened; the status check waits for it and
                    // records an abort if it didn't.
                    Err(_) => {
                        db.failed_attempts.fetch_add(1, Ordering::Relaxed);
                        retry(&db, || store.peer_status(&reservation, shard)).await
                    }
                };
                faults_owner.faults.maybe_crash(Step::AfterConfirm);
                let state = reservation_state(&decision);
                let trip = match decision {
                    PeerDecision::Confirmed { trip } => Some(trip),
                    _ => None,
                };
                // Memory first: the driver may connect the moment the peer hears the answer, and
                // must find their Trip here, or they'd be redirected straight back.
                let _ = events.send(Event::ConfirmDecided { offer, driver, ride, shard, reservation, trip });
                let _ = reply.send(state);
                None
            });
        }
        _ => unreachable!("client effects are delivered by `deliver`"),
    }
}

/// Answers peer Shards. Calls that need memory go through the dispatcher task; Reservation
/// status and lease release go straight to Postgres, which decides them.
struct PeerService {
    me: ShardId,
    events: UnboundedSender<Event>,
    store: Store,
}

fn from_shard(id: u32) -> Result<ShardId, tonic::Status> {
    ShardId::try_from(id).map_err(|_| tonic::Status::invalid_argument("bad from_shard"))
}

impl PeerService {
    fn send(&self, event: Event) -> Result<(), tonic::Status> {
        self.events.send(event).map_err(|_| tonic::Status::unavailable("shard stopping"))
    }
}

#[tonic::async_trait]
impl peer::shard_server::Shard for PeerService {
    async fn search_boundary_drivers(
        &self,
        request: tonic::Request<peer::SearchBoundaryDriversRequest>,
    ) -> Result<tonic::Response<peer::SearchBoundaryDriversResponse>, tonic::Status> {
        let cells = request.into_inner().cells.into_iter().filter_map(|cell| geo::CellIndex::try_from(cell).ok()).collect();
        let (reply, answer) = oneshot::channel();
        self.send(Event::Snapshot { cells, reply })?;
        let drivers = answer.await.map_err(|_| tonic::Status::unavailable("shard stopping"))?;
        Ok(tonic::Response::new(peer::SearchBoundaryDriversResponse {
            drivers: drivers
                .into_iter()
                .map(|(id, at)| peer::BoundaryDriver { driver_id: id, position: Some(peer::LatLng { lat: at.lat, lng: at.lng }) })
                .collect(),
        }))
    }

    async fn relay_offer(
        &self,
        request: tonic::Request<peer::RelayOfferRequest>,
    ) -> Result<tonic::Response<peer::RelayOfferResponse>, tonic::Status> {
        let request = request.into_inner();
        let from = from_shard(request.from_shard)?;
        let (reply, answer) = oneshot::channel();
        self.send(Event::RelayOffer { from, request, reply })?;
        let delivered = answer.await.map_err(|_| tonic::Status::unavailable("shard stopping"))?;
        Ok(tonic::Response::new(peer::RelayOfferResponse { delivered }))
    }

    async fn withdraw_offer(&self, request: tonic::Request<peer::OfferRef>) -> Result<tonic::Response<peer::Empty>, tonic::Status> {
        self.send(Event::WithdrawRelayed { offer: request.into_inner().offer_id })?;
        Ok(tonic::Response::new(peer::Empty {}))
    }

    async fn close_offer(&self, request: tonic::Request<peer::CloseOfferRequest>) -> Result<tonic::Response<peer::Empty>, tonic::Status> {
        let request = request.into_inner();
        let reason = match peer::OfferClose::try_from(request.reason) {
            Ok(peer::OfferClose::Declined) => OfferClosed::Declined,
            Ok(peer::OfferClose::Expired) => OfferClosed::Expired,
            _ => OfferClosed::Withdrawn,
        };
        self.send(Event::RelayClosed { offer: request.offer_id, reason })?;
        Ok(tonic::Response::new(peer::Empty {}))
    }

    async fn confirm(&self, request: tonic::Request<peer::ConfirmRequest>) -> Result<tonic::Response<peer::ReservationState>, tonic::Status> {
        let request = request.into_inner();
        let from = from_shard(request.from_shard)?;
        let (reply, answer) = oneshot::channel();
        self.send(Event::Confirm { from, request, reply })?;
        let state = answer.await.map_err(|_| tonic::Status::unavailable("shard stopping"))?;
        Ok(tonic::Response::new(state))
    }

    async fn get_reservation_status(
        &self,
        request: tonic::Request<peer::ReservationRef>,
    ) -> Result<tonic::Response<peer::ReservationState>, tonic::Status> {
        let request = request.into_inner();
        let from = from_shard(request.from_shard)?;
        let decision = self.store.peer_status(&request.reservation_id, from).await.map_err(|error| tonic::Status::unavailable(error.to_string()))?;
        Ok(tonic::Response::new(reservation_state(&decision)))
    }

    async fn release(&self, request: tonic::Request<peer::ReservationRef>) -> Result<tonic::Response<peer::Empty>, tonic::Status> {
        let reservation = request.into_inner().reservation_id;
        self.store.release_reservation(&reservation).await.map_err(|error| tonic::Status::unavailable(error.to_string()))?;
        self.send(Event::LeaseReleased { reservation })?;
        Ok(tonic::Response::new(peer::Empty {}))
    }

    async fn apply_shard_map(&self, request: tonic::Request<peer::ShardMapText>) -> Result<tonic::Response<peer::ShardMapVersion>, tonic::Status> {
        let map = ShardMap::parse(&request.into_inner().text).map_err(tonic::Status::invalid_argument)?;
        if map.shard(self.me).is_none() {
            return Err(tonic::Status::invalid_argument(format!("map version {} has no Shard {}", map.version(), self.me)));
        }
        self.store.save_shard_map(&map).await.map_err(|error| tonic::Status::failed_precondition(error.to_string()))?;
        let version = map.version();
        self.send(Event::ApplyMap { map })?;
        Ok(tonic::Response::new(peer::ShardMapVersion { version }))
    }
}

/// The library's default 128 KiB read buffer per connection would dominate memory at
/// tens of thousands of sockets, and these messages are far smaller than a buffer this size.
fn socket_config() -> WebSocketConfig {
    WebSocketConfig::default().read_buffer_size(4 * 1024)
}

async fn accept_drivers(listener: TcpListener, events: UnboundedSender<Event>) {
    while let Ok((tcp, _)) = listener.accept().await {
        let events = events.clone();
        tokio::spawn(async move {
            let Ok(socket) = accept_async_with_config(tcp, Some(socket_config())).await else { return };
            let (mut sink, mut stream) = socket.split();
            let (outbox, mut outgoing) = unbounded_channel::<Vec<u8>>();
            let mut identity: Option<String> = None;
            loop {
                tokio::select! {
                    Some(frame) = outgoing.recv() => {
                        if sink.send(Message::Binary(frame.into())).await.is_err() { break }
                    }
                    frame = stream.next() => {
                        let Some(Ok(frame)) = frame else { break };
                        let Message::Binary(bytes) = frame else {
                            if frame.is_close() { break }
                            continue
                        };
                        let Ok(decoded) = DriverToServer::decode(bytes) else { break };
                        let Some(message) = decoded.message else { continue };
                        let event = match (message, &identity) {
                            (driver_to_server::Message::LocationPing(ping), _) => {
                                identity = Some(ping.driver_id.clone());
                                Event::DriverPing {
                                    driver: ping.driver_id,
                                    position: position(ping.position),
                                    reported_at_ms: ping.reported_at_ms,
                                    map_version: ping.map_version,
                                    outbox: outbox.clone(),
                                }
                            }
                            (_, None) => continue,
                            (driver_to_server::Message::Accept(accept), Some(driver)) => {
                                Event::Accept { driver: driver.clone(), offer: accept.offer_id }
                            }
                            (driver_to_server::Message::Decline(decline), Some(driver)) => {
                                Event::Decline { driver: driver.clone(), offer: decline.offer_id }
                            }
                            (driver_to_server::Message::TripEvent(event), Some(driver)) => {
                                let stage = match TripEventKind::try_from(event.kind) {
                                    Ok(TripEventKind::Arrived) => TripStage::Arrived,
                                    Ok(TripEventKind::Started) => TripStage::Started,
                                    Ok(TripEventKind::Completed) => TripStage::Completed,
                                    _ => continue,
                                };
                                Event::TripUpdate { driver: driver.clone(), trip: event.trip_id, stage }
                            }
                        };
                        if events.send(event).is_err() { break }
                    }
                }
            }
            if let Some(driver) = identity {
                let _ = events.send(Event::DriverGone { driver, outbox });
            }
        });
    }
}

async fn accept_riders(listener: TcpListener, events: UnboundedSender<Event>) {
    while let Ok((tcp, _)) = listener.accept().await {
        let events = events.clone();
        tokio::spawn(async move {
            let Ok(socket) = accept_async_with_config(tcp, Some(socket_config())).await else { return };
            let (mut sink, mut stream) = socket.split();
            let (outbox, mut outgoing) = unbounded_channel::<Vec<u8>>();
            loop {
                tokio::select! {
                    Some(frame) = outgoing.recv() => {
                        if sink.send(Message::Binary(frame.into())).await.is_err() { break }
                    }
                    frame = stream.next() => {
                        let Some(Ok(Message::Binary(bytes))) = frame else { break };
                        let Ok(decoded) = RiderToServer::decode(bytes) else { break };
                        let Some(message) = decoded.message else { continue };
                        if events.send(Event::Rider { message, outbox: outbox.clone() }).is_err() { break }
                    }
                }
            }
        });
    }
}
