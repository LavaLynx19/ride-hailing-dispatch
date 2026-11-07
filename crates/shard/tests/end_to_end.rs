//! The real simulator client against the real Shard and Postgres, at accelerated time.
//! Needs `DATABASE_URL`, like the assignment tests.

use std::sync::Arc;
use std::time::Duration;

use geo::Position;
use shard::dispatcher::{Matcher, Policy};
use simulator::client::{Counters, RunConfig, Silence, run_plan};
use simulator::driver::Dropoff;
use simulator::metrics::summarise;
use simulator::plan::{DriverPlan, Plan, TripPlan};
use sqlx::PgPool;
use tokio::net::TcpListener;

const TIME_SCALE: f64 = 100.0;

fn position(north_km: f64) -> Position {
    Position { lat: 40.70 + north_km * 0.009, lng: -74.00 }
}

struct Outcome {
    counters: Arc<Counters>,
    summary: simulator::metrics::Summary,
}

/// Runs `trips` Ride Requests, `request_every` simulated seconds apart, against `drivers` drivers.
async fn run(pool: &PgPool, policy: Policy, drivers: usize, trips: u32, request_every: f64, duration: f64) -> Outcome {
    let drivers_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let riders_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = RunConfig {
        driver_urls: vec![format!("ws://{}", drivers_listener.local_addr().unwrap())],
        rider_urls: vec![format!("ws://{}", riders_listener.local_addr().unwrap())],
        seed: 11,
        ping_interval_seconds: 4.0,
        time_scale: TIME_SCALE,
        seconds_into_first_hour: 0.0,
        silence: Silence::NONE,
    };
    let store = assignment::Store::from_pool(pool.clone());
    tokio::spawn(shard::server::serve(vec![drivers_listener], vec![riders_listener], None, store, policy));

    let drivers = (0..drivers)
        .map(|index| DriverPlan {
            id: format!("d{index}"),
            stream: index as u64,
            position: position(index as f64 * 0.1),
            online: vec![true],
            busy_until_seconds: 0.0,
        })
        .collect();
    let trips = (0..trips)
        .map(|index| TripPlan {
            request_at_seconds: 10.0 + f64::from(index) * request_every,
            pickup: position(f64::from(index) * 0.05),
            dropoff: Dropoff::Inside(position(1.0)),
            trip_seconds: 60.0,
        })
        .collect();

    let counters = Arc::new(Counters::default());
    let times = run_plan(config, Plan { drivers, trips, ..Plan::default() }, duration, Arc::clone(&counters)).await;
    let staleness = counters.offer_staleness.lock().unwrap().clone();
    let summary = summarise(&times, &staleness, 0.0);
    for counter in [
        &counters.driver_connect_failures,
        &counters.driver_session_errors,
        &counters.rider_connect_failures,
        &counters.rider_session_errors,
    ] {
        assert_eq!(Counters::get(counter), 0, "{:?}", counters.failures.lock().unwrap());
    }
    Outcome { counters, summary }
}

async fn assert_invariant(pool: &PgPool, counters: &Counters, trips: i64) {
    let observed = counters.observations.lock().unwrap().clone();
    assert_eq!(observed.drivers.len() as i64, trips);
    assert_eq!(observed.riders.len() as i64, trips);
    let report = checker::check(pool, &observed).await.unwrap();
    assert!(report.passed(), "{:?}", report.examples);
    assert_eq!(report.trips_in_postgres, trips);
}

#[sqlx::test(migrator = "assignment::MIGRATOR")]
async fn every_trip_completes_with_each_accept_winning_one_trip(pool: PgPool) {
    let Outcome { counters, summary } = run(&pool, Policy::BASELINE, 20, 40, 2.0, 900.0).await;
    let at_offer = summary.staleness_at_offer.as_ref().expect("offers report Staleness");
    // Real seconds, since the Shard's clock isn't accelerated: pings are 40 ms apart here.
    assert!(at_offer.max < 2.0, "Staleness max {}", at_offer.max);
    assert_eq!(summary.measured_requests, 40);
    assert_eq!(summary.never_assigned, 0);
    assert_eq!(Counters::get(&counters.trips_assigned), 40);
    assert_eq!(Counters::get(&counters.trips_completed), 40);
    assert_eq!(Counters::get(&counters.riders_finished), 40);
    assert_eq!(Counters::get(&counters.accepts), 40, "each accept wins exactly one trip");
    assert_invariant(&pool, &counters, 40).await;
}

/// Few drivers and a burst of requests: the area broadcasts, rides go to several drivers and
/// drivers hold several Broadcasts, so Accepts race on both sides.
#[sqlx::test(migrator = "assignment::MIGRATOR")]
async fn overlapping_broadcasts_under_the_auction_keep_the_invariant(pool: PgPool) {
    let policy = Policy { matcher: Matcher::Auction { window: Duration::from_millis(200) }, widening: true };
    let Outcome { counters, summary } = run(&pool, policy, 10, 40, 0.5, 4000.0).await;
    assert_eq!(summary.never_assigned, 0);
    assert_eq!(Counters::get(&counters.trips_assigned), 40);
    assert_eq!(Counters::get(&counters.trips_completed), 40);
    assert!(
        Counters::get(&counters.accepts) > 40,
        "some Accepts should lose a race, or the test isn't overlapping offers: {}",
        Counters::get(&counters.accepts)
    );
    assert_invariant(&pool, &counters, 40).await;
}
