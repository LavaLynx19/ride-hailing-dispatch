//! The real simulator client against the real baseline server, at accelerated time.

use std::sync::Arc;

use geo::Position;
use simulator::client::{Counters, RunConfig, run_plan};
use simulator::driver::Dropoff;
use simulator::metrics::summarise;
use simulator::plan::{DriverPlan, Plan, TripPlan};
use tokio::net::TcpListener;

const TIME_SCALE: f64 = 100.0;

fn position(north_km: f64) -> Position {
    Position { lat: 40.70 + north_km * 0.009, lng: -74.00 }
}

#[tokio::test(flavor = "multi_thread")]
async fn every_trip_completes_with_each_accept_winning_one_trip() {
    let drivers_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let riders_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = RunConfig {
        driver_url: format!("ws://{}", drivers_listener.local_addr().unwrap()),
        rider_url: format!("ws://{}", riders_listener.local_addr().unwrap()),
        seed: 11,
        ping_interval_seconds: 4.0,
        time_scale: TIME_SCALE,
        seconds_into_first_hour: 0.0,
    };
    tokio::spawn(shard::server::serve(drivers_listener, riders_listener));

    let drivers = (0..20)
        .map(|index| DriverPlan {
            id: format!("d{index}"),
            stream: index,
            position: position(index as f64 * 0.1),
            online: vec![true],
        })
        .collect();
    let trips = (0..40)
        .map(|index| TripPlan {
            request_at_seconds: 10.0 + f64::from(index) * 2.0,
            pickup: position(f64::from(index) * 0.05),
            dropoff: Dropoff::Inside(position(1.0)),
            trip_seconds: 60.0,
        })
        .collect();

    let counters = Arc::new(Counters::default());
    let times = run_plan(config, Plan { drivers, trips }, 900.0, Arc::clone(&counters)).await;

    let summary = summarise(&times, 0.0);
    assert_eq!(summary.measured_requests, 40);
    assert_eq!(summary.never_assigned, 0);
    assert_eq!(Counters::get(&counters.connect_failures), 0);
    assert_eq!(Counters::get(&counters.trips_assigned), 40);
    assert_eq!(Counters::get(&counters.trips_completed), 40);
    assert_eq!(Counters::get(&counters.riders_finished), 40);
    assert_eq!(Counters::get(&counters.accepts), 40, "each accept wins exactly one trip");
}
