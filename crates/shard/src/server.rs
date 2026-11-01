//! WebSocket front end. One task owns the Dispatcher; connections exchange messages with it.

use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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

use crate::dispatcher::{Dispatcher, Effect};

const TICK: Duration = Duration::from_millis(250);
const QUOTE_VALID_MS: i64 = 60_000;

type Outbox = UnboundedSender<Vec<u8>>;

enum Event {
    DriverPing { driver: String, position: Position, outbox: Outbox },
    DriverGone { driver: String, outbox: Outbox },
    Accept { driver: String, offer: String },
    Decline { driver: String, offer: String },
    TripCompleted { driver: String, trip: String },
    Rider { message: rider_to_server::Message, outbox: Outbox },
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

pub async fn serve(drivers: TcpListener, riders: TcpListener) {
    let (events, inbox) = unbounded_channel();
    tokio::spawn(accept_drivers(drivers, events.clone()));
    tokio::spawn(accept_riders(riders, events));
    run_dispatcher(inbox).await;
}

async fn run_dispatcher(mut inbox: UnboundedReceiver<Event>) {
    let started = Instant::now();
    let mut dispatcher = Dispatcher::default();
    let mut driver_links: HashMap<String, Outbox> = HashMap::new();
    let mut rider_links: HashMap<String, Outbox> = HashMap::new();
    let mut quotes = 0_u64;
    let mut tick = tokio::time::interval(TICK);

    loop {
        let effects = tokio::select! {
            _ = tick.tick() => dispatcher.tick(started.elapsed().as_secs_f64()),
            event = inbox.recv() => {
                let Some(event) = event else { return };
                let now = started.elapsed().as_secs_f64();
                match event {
                    Event::DriverPing { driver, position, outbox } => {
                        driver_links.insert(driver.clone(), outbox);
                        dispatcher.location_ping(&driver, position, now)
                    }
                    Event::DriverGone { driver, outbox } => {
                        // A reconnect may already have replaced this link.
                        if driver_links.get(&driver).is_some_and(|link| link.same_channel(&outbox)) {
                            driver_links.remove(&driver);
                            dispatcher.driver_disconnected(&driver);
                        }
                        Vec::new()
                    }
                    Event::Accept { driver, offer } => dispatcher.accept(&driver, &offer, now),
                    Event::Decline { driver, offer } => dispatcher.decline(&driver, &offer, now),
                    Event::TripCompleted { driver, trip } => dispatcher.trip_completed(&driver, &trip, now),
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
                        dispatcher.ride_request(request.ride_request_id, position(request.pickup), now)
                    }
                }
            }
        };
        deliver(effects, started, &driver_links, &mut rider_links);
    }
}

fn deliver(
    effects: Vec<Effect>,
    started: Instant,
    drivers: &HashMap<String, Outbox>,
    riders: &mut HashMap<String, Outbox>,
) {
    let to_driver = |driver: &str, frame: Vec<u8>| {
        if let Some(link) = drivers.get(driver) {
            let _ = link.send(frame);
        }
    };
    for effect in effects {
        match effect {
            Effect::Offer { driver, offer, ride, pickup, expires_at } => {
                let until_expiry = expires_at - started.elapsed().as_secs_f64();
                let frame = driver_frame(server_to_driver::Message::Offer(Offer {
                    offer_id: offer,
                    ride_request_id: ride,
                    pickup: Some(LatLng { lat: pickup.lat, lng: pickup.lng }),
                    expires_at_ms: unix_ms() + (until_expiry * 1000.0) as i64,
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
                                if event.kind != TripEventKind::Completed as i32 { continue }
                                Event::TripCompleted { driver: driver.clone(), trip: event.trip_id }
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
