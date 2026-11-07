//! Runs the generator against a small fake server, at accelerated time.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use futures_util::{SinkExt, StreamExt};
use prost::Message as _;
use protocol::client::{
    DriverToServer, Offer, Quote, RiderToServer, ServerToDriver, ServerToRider, TripAssigned, TripStatus,
    driver_to_server, rider_to_server, server_to_driver, server_to_rider,
};
use simulator::client::{Counters, RunConfig, Silence, run_plan};
use simulator::driver::Dropoff;
use simulator::plan::{DriverPlan, Plan, TripPlan};
use simulator::position::Position;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, mpsc};
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

const TIME_SCALE: f64 = 200.0;
const PICKUP: Position = Position { lat: 40.70, lng: -74.00 };
const DROPOFF: Position = Position { lat: 40.71, lng: -74.00 };

#[derive(Default)]
struct Observed {
    pings: AtomicU64,
    accepts: AtomicU64,
    completed: AtomicU64,
}

type DriverLink = mpsc::UnboundedSender<ServerToDriver>;
type RiderLink = mpsc::UnboundedSender<ServerToRider>;

#[derive(Default)]
struct Server {
    observed: Observed,
    driver: Mutex<Option<DriverLink>>,
    rider: Mutex<Option<RiderLink>>,
    ride_request: Mutex<String>,
}

async fn serve_driver(server: Arc<Server>, listener: TcpListener) {
    while let Ok((tcp, _)) = listener.accept().await {
        let server = Arc::clone(&server);
        tokio::spawn(async move {
            let (mut sink, mut stream) = accept_async(tcp).await.unwrap().split();
            let (sender, mut outgoing) = mpsc::unbounded_channel();
            *server.driver.lock().await = Some(sender.clone());
            loop {
                tokio::select! {
                    Some(message) = outgoing.recv() => {
                        let message: ServerToDriver = message;
                        sink.send(Message::Binary(message.encode_to_vec().into())).await.unwrap();
                    }
                    frame = stream.next() => {
                        let Some(Ok(Message::Binary(bytes))) = frame else { return };
                        match DriverToServer::decode(bytes).unwrap().message {
                            Some(driver_to_server::Message::LocationPing(_)) => {
                                server.observed.pings.fetch_add(1, Ordering::Relaxed);
                            }
                            Some(driver_to_server::Message::Accept(_)) => {
                                server.observed.accepts.fetch_add(1, Ordering::Relaxed);
                                let ride_request_id = server.ride_request.lock().await.clone();
                                let assigned = server_to_driver::Message::TripAssigned(TripAssigned {
                                    trip_id: "t1".into(),
                                    ride_request_id,
                                });
                                sender.send(ServerToDriver { message: Some(assigned) }).unwrap();
                            }
                            Some(driver_to_server::Message::TripEvent(event))
                                if event.kind == protocol::client::TripEventKind::Completed as i32 =>
                            {
                                server.observed.completed.fetch_add(1, Ordering::Relaxed);
                                let status = server_to_rider::Message::TripStatus(TripStatus {
                                    trip_id: event.trip_id,
                                    ride_request_id: String::new(),
                                    status: "completed".into(),
                                });
                                if let Some(rider) = &*server.rider.lock().await {
                                    rider.send(ServerToRider { message: Some(status) }).unwrap();
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        });
    }
}

async fn serve_rider(server: Arc<Server>, listener: TcpListener) {
    while let Ok((tcp, _)) = listener.accept().await {
        let server = Arc::clone(&server);
        tokio::spawn(async move {
            let (mut sink, mut stream) = accept_async(tcp).await.unwrap().split();
            let (sender, mut outgoing) = mpsc::unbounded_channel();
            *server.rider.lock().await = Some(sender.clone());
            loop {
                tokio::select! {
                    Some(message) = outgoing.recv() => {
                        let message: ServerToRider = message;
                        sink.send(Message::Binary(message.encode_to_vec().into())).await.unwrap();
                    }
                    frame = stream.next() => {
                        let Some(Ok(Message::Binary(bytes))) = frame else { return };
                        match RiderToServer::decode(bytes).unwrap().message {
                            Some(rider_to_server::Message::QuoteRequest(_)) => {
                                let quote = server_to_rider::Message::Quote(Quote {
                                    quote_id: "q1".into(),
                                    surge_multiplier: 1.0,
                                    expires_at_ms: 0,
                                });
                                sender.send(ServerToRider { message: Some(quote) }).unwrap();
                            }
                            Some(rider_to_server::Message::RideRequest(request)) => {
                                *server.ride_request.lock().await = request.ride_request_id.clone();
                                let offer = server_to_driver::Message::Offer(Offer {
                                    staleness_ms: 0,
                                    offer_id: "o1".into(),
                                    ride_request_id: request.ride_request_id,
                                    pickup: request.pickup,
                                    expires_at_ms: 0,
                                });
                                if let Some(driver) = &*server.driver.lock().await {
                                    driver.send(ServerToDriver { message: Some(offer) }).unwrap();
                                }
                            }
                            None => {}
                        }
                    }
                }
            }
        });
    }
}

fn plan() -> Plan {
    Plan {
        drivers: vec![DriverPlan {
            id: "d1-0".into(),
            stream: 1,
            // At the pickup, so acceptance is as likely as it gets.
            position: PICKUP,
            online: vec![true],
            busy_until_seconds: 0.0,
        }],
        trips: vec![TripPlan {
            request_at_seconds: 30.0,
            pickup: PICKUP,
            dropoff: Dropoff::Inside(DROPOFF),
            trip_seconds: 120.0,
        }],
        ..Plan::default()
    }
}

fn config(drivers: &TcpListener, riders: &TcpListener) -> RunConfig {
    RunConfig {
        driver_urls: vec![format!("ws://{}", drivers.local_addr().unwrap())],
        rider_urls: vec![format!("ws://{}", riders.local_addr().unwrap())],
        seed: 5,
        ping_interval_seconds: 4.0,
        time_scale: TIME_SCALE,
        seconds_into_first_hour: 0.0,
        silence: Silence::NONE,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_warm_started_driver_connects_when_their_trip_ends() {
    let server = Arc::new(Server::default());
    let drivers = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let riders = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = config(&drivers, &riders);
    tokio::spawn(serve_driver(Arc::clone(&server), drivers));
    tokio::spawn(serve_rider(Arc::clone(&server), riders));

    let mut plan = plan();
    plan.trips.clear();
    plan.drivers[0].busy_until_seconds = 200.0;
    let counters = Arc::new(Counters::default());
    run_plan(config, plan, 300.0, Arc::clone(&counters)).await;

    assert_eq!(Counters::get(&counters.driver_connections), 1);
    let pings = server.observed.pings.load(Ordering::Relaxed);
    assert!((20..=30).contains(&pings), "100s online at one ping per 4s: {pings}");
}

#[tokio::test(flavor = "multi_thread")]
async fn one_trip_runs_from_request_to_completion() {
    let server = Arc::new(Server::default());
    let drivers = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let riders = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = config(&drivers, &riders);
    tokio::spawn(serve_driver(Arc::clone(&server), drivers));
    tokio::spawn(serve_rider(Arc::clone(&server), riders));

    let counters = Arc::new(Counters::default());
    run_plan(config, plan(), 300.0, Arc::clone(&counters)).await;

    assert_eq!(Counters::get(&counters.ride_requests), 1);
    assert_eq!(Counters::get(&counters.offers), 1);
    assert_eq!(Counters::get(&counters.accepts), 1, "seed 5 accepts this offer");
    assert_eq!(Counters::get(&counters.trips_assigned), 1);
    assert_eq!(Counters::get(&counters.trips_completed), 1);
    assert_eq!(Counters::get(&counters.riders_finished), 1);
    assert_eq!(Counters::get(&counters.driver_connect_failures), 0);
    assert_eq!(Counters::get(&counters.rider_connect_failures), 0);
    assert_eq!(server.observed.accepts.load(Ordering::Relaxed), 1);
    assert_eq!(server.observed.completed.load(Ordering::Relaxed), 1);

    let pings = server.observed.pings.load(Ordering::Relaxed);
    assert!((60..=80).contains(&pings), "300s at one ping per 4s: {pings}");
}
