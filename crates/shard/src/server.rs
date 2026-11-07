//! WebSocket front end. One task owns the Dispatcher; connections exchange messages with it.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use assignment::{AcceptOutcome, Store, TripStage};

use futures_util::{SinkExt, StreamExt};
use geo::Position;
use prost::Message as _;
use protocol::client::{
    DriverToServer, LatLng, Offer, OfferGone, Quote, RiderToServer, ServerToDriver, ServerToRider, TripAssigned,
    TripEventKind, TripStatus, driver_to_server, rider_to_server, server_to_driver, server_to_rider,
};
use tokio::net::TcpListener;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio_tungstenite::accept_async_with_config;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::Message;

use crate::dispatcher::{Dispatcher, Effect, Policy};
use crate::ping_log::{PingLog, PingRecord};

const TICK: Duration = Duration::from_millis(250);
const QUOTE_VALID_MS: i64 = 60_000;
const STATS_EVERY: Duration = Duration::from_secs(60);
const RETRY_FIRST: Duration = Duration::from_millis(100);
const RETRY_MAX: Duration = Duration::from_secs(2);

type Outbox = UnboundedSender<Vec<u8>>;

struct DriverLink {
    outbox: Outbox,
    reported_at_ms: i64,
}

enum Event {
    DriverPing { driver: String, position: Position, reported_at_ms: i64, outbox: Outbox },
    DriverGone { driver: String, outbox: Outbox },
    Accept { driver: String, offer: String },
    Decline { driver: String, offer: String },
    TripUpdate { driver: String, trip: String, stage: TripStage },
    Rider { message: rider_to_server::Message, outbox: Outbox },
    RideStored { ride: String },
    OfferStored { offer: String },
    AcceptDecided { offer: String, outcome: AcceptOutcome },
}

/// Postgres work in flight, for the stats line.
#[derive(Default)]
struct DbStats {
    in_flight: AtomicU64,
    failed_attempts: AtomicU64,
}

fn unix_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |elapsed| elapsed.as_millis() as i64)
}

fn position(point: Option<LatLng>) -> Position {
    let point = point.unwrap_or_default();
    Position { lat: point.lat, lng: point.lng }
}

fn driver_frame(message: server_to_driver::Message) -> Vec<u8> {
    ServerToDriver { message: Some(message) }.encode_to_vec()
}

fn rider_frame(message: server_to_rider::Message) -> Vec<u8> {
    ServerToRider { message: Some(message) }.encode_to_vec()
}

/// Takes several listeners per kind because one client address can only reach one server
/// port about 28,000 times at once, far short of the load target.
///
/// Active Trips are recovered from Postgres before any connection is served.
pub async fn serve(drivers: Vec<TcpListener>, riders: Vec<TcpListener>, ping_log: Option<PingLog>, store: Store, policy: Policy) {
    let db = Arc::new(DbStats::default());
    let recovered = retry(&db, || store.recover()).await;
    eprintln!("recovered {} active Trips from Postgres", recovered.len());
    // Ids carry the start time, so they never repeat ids Postgres kept from before a restart.
    eprintln!("policy: {policy}");
    let mut dispatcher = Dispatcher::with_policy(&format!("{:x}-", unix_ms()), policy);
    for trip in recovered {
        dispatcher.recover_trip(trip.trip, trip.driver, trip.ride);
    }
    let (events, inbox) = unbounded_channel();
    for listener in drivers {
        tokio::spawn(accept_drivers(listener, events.clone()));
    }
    for listener in riders {
        tokio::spawn(accept_riders(listener, events.clone()));
    }
    let postgres = Postgres { store, events, db };
    run_dispatcher(dispatcher, policy, inbox, ping_log, postgres).await;
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

async fn run_dispatcher(
    mut dispatcher: Dispatcher,
    policy: Policy,
    mut inbox: UnboundedReceiver<Event>,
    ping_log: Option<PingLog>,
    postgres: Postgres,
) {
    let started = Instant::now();
    let mut driver_links: HashMap<String, DriverLink> = HashMap::new();
    let mut rider_links: HashMap<String, Outbox> = HashMap::new();
    let mut quotes = 0_u64;
    let mut tick = tokio::time::interval(TICK);
    let mut stats = tokio::time::interval(STATS_EVERY);
    let mut batch = policy.batch_window().map(tokio::time::interval);

    loop {
        let effects = tokio::select! {
            _ = tick.tick() => dispatcher.tick(started.elapsed().as_secs_f64()),
            _ = next_batch(&mut batch) => dispatcher.batch(started.elapsed().as_secs_f64()),
            _ = stats.tick() => {
                let sizes = dispatcher.sizes();
                eprintln!(
                    "stats: driver links {}, rider links {}, drivers {}, idle {}, rides {}, waiting {}, offers {}, trips {}, postgres in flight {}, postgres failed attempts {}, batches {}, batch deadline hits {}, broadcast rounds {}, broadcast areas {}, area switches {}",
                    driver_links.len(), rider_links.len(), sizes.drivers, sizes.idle, sizes.rides, sizes.waiting,
                    sizes.offers, sizes.trips,
                    postgres.db.in_flight.load(Ordering::Relaxed), postgres.db.failed_attempts.load(Ordering::Relaxed),
                    sizes.batches, sizes.batch_deadline_hits, sizes.broadcast_rounds, sizes.broadcast_areas, sizes.area_switches,
                );
                Vec::new()
            }
            event = inbox.recv() => {
                let Some(event) = event else { return };
                let now = started.elapsed().as_secs_f64();
                match event {
                    Event::DriverPing { driver, position, reported_at_ms, outbox } => {
                        if let Some(log) = ping_log.as_ref().filter(|_| position.is_finite()) {
                            log.append(&PingRecord {
                                driver: driver.clone(),
                                reported_at_ms,
                                received_at_ms: unix_ms(),
                                position,
                            });
                        }
                        driver_links.insert(driver.clone(), DriverLink { outbox, reported_at_ms });
                        dispatcher.location_ping(&driver, position, now)
                    }
                    Event::DriverGone { driver, outbox } => {
                        // A reconnect may already have replaced this link.
                        if driver_links.get(&driver).is_some_and(|link| link.outbox.same_channel(&outbox)) {
                            driver_links.remove(&driver);
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
                        rider_links.insert(request.ride_request_id.clone(), outbox);
                        dispatcher.ride_request(request.ride_request_id, request.quote_id, position(request.pickup))
                    }
                }
            }
        };
        deliver(effects, started, &driver_links, &mut rider_links, &postgres);
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

fn deliver(
    effects: Vec<Effect>,
    started: Instant,
    drivers: &HashMap<String, DriverLink>,
    riders: &mut HashMap<String, Outbox>,
    postgres: &Postgres,
) {
    let to_unix_ms = |at: f64| unix_ms() + ((at - started.elapsed().as_secs_f64()) * 1000.0) as i64;
    let to_driver = |driver: &str, frame: Vec<u8>| {
        if let Some(link) = drivers.get(driver) {
            let _ = link.outbox.send(frame);
        }
    };
    for effect in effects {
        match effect {
            Effect::Offer { driver, offer, ride, pickup, expires_at } => {
                let now_ms = unix_ms();
                let reported_at_ms = drivers.get(&driver).map_or(now_ms, |link| link.reported_at_ms);
                let frame = driver_frame(server_to_driver::Message::Offer(Offer {
                    offer_id: offer,
                    ride_request_id: ride,
                    pickup: Some(LatLng { lat: pickup.lat, lng: pickup.lng }),
                    expires_at_ms: to_unix_ms(expires_at),
                    staleness_ms: (now_ms - reported_at_ms).max(0),
                }));
                to_driver(&driver, frame);
            }
            Effect::OfferGone { driver, offer } => {
                to_driver(&driver, driver_frame(server_to_driver::Message::OfferGone(OfferGone { offer_id: offer })));
            }
            Effect::TripAssigned { driver, trip, ride } => {
                let assigned = TripAssigned { trip_id: trip, ride_request_id: ride };
                to_driver(&driver, driver_frame(server_to_driver::Message::TripAssigned(assigned)));
            }
            Effect::TripStatus { ride, trip, status } => {
                if let Some(link) = riders.get(&ride) {
                    let _ = link.send(rider_frame(server_to_rider::Message::TripStatus(TripStatus {
                        trip_id: trip,
                        ride_request_id: ride.clone(),
                        status: status.to_owned(),
                    })));
                }
                if status == "completed" {
                    riders.remove(&ride);
                }
            }
            Effect::StoreRide { ride, quote, pickup } => in_postgres(postgres, move |store, db| async move {
                retry(&db, || store.store_ride_request(&ride, &quote, pickup)).await;
                Some(Event::RideStored { ride })
            }),
            Effect::StoreOffer { offer, ride, driver, kind, expires_at } => {
                let expires_at_ms = to_unix_ms(expires_at);
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
        }
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
