//! Kills the real Shard process again and again while simulated drivers accept offers,
//! then runs the invariant checker over Postgres and what clients were told.
//! Needs `DATABASE_URL`, like the other Postgres tests.

mod common;

use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use common::{free_port, fresh_database_url};
use geo::Position;
use simulator::client::{Counters, RunConfig, Silence, run_plan};
use simulator::driver::Dropoff;
use simulator::plan::{DriverPlan, Plan, TripPlan};

const TIME_SCALE: f64 = 100.0;
const KILLS: u32 = 12;

fn position(north_km: f64) -> Position {
    Position { lat: 40.70 + north_km * 0.009, lng: -74.00 }
}

fn start_shard(database_url: &str, driver_port: u16, rider_port: u16) -> Child {
    Command::new(env!("CARGO_BIN_EXE_shard"))
        .env("DATABASE_URL", database_url)
        .args([format!("127.0.0.1:{driver_port}"), format!("127.0.0.1:{rider_port}")])
        .stderr(Stdio::null())
        .spawn()
        .unwrap()
}

/// Dense enough that Accepts are nearly always in flight when the Shard dies.
fn plan() -> Plan {
    let drivers = (0..30)
        .map(|index| DriverPlan {
            id: format!("d{index}"),
            stream: index,
            position: position(index as f64 * 0.1),
            online: vec![true],
            busy_until_seconds: 0.0,
        })
        .collect();
    let trips = (0..400)
        .map(|index| TripPlan {
            request_at_seconds: 5.0 + f64::from(index) * 1.5,
            pickup: position(f64::from(index % 30) * 0.1),
            dropoff: Dropoff::Inside(position(1.0)),
            trip_seconds: 30.0,
        })
        .collect();
    Plan { drivers, trips, ..Plan::default() }
}

#[tokio::test(flavor = "multi_thread")]
async fn killing_the_shard_mid_accept_keeps_the_invariant() {
    let database_url = fresh_database_url("shard_kill").await;
    let (driver_port, rider_port) = (free_port(), free_port());
    let config = RunConfig {
        driver_urls: vec![format!("ws://127.0.0.1:{driver_port}")],
        rider_urls: vec![format!("ws://127.0.0.1:{rider_port}")],
        seed: 5,
        ping_interval_seconds: 4.0,
        time_scale: TIME_SCALE,
        seconds_into_first_hour: 0.0,
        silence: Silence::NONE,
    };
    let mut shard = start_shard(&database_url, driver_port, rider_port);
    let counters = Arc::new(Counters::default());
    let run = tokio::spawn(run_plan(config, plan(), 700.0, Arc::clone(&counters)));

    // Kill at uneven moments, so kills land at different points of an Accept.
    for kill in 0..KILLS {
        tokio::time::sleep(Duration::from_millis(350 + u64::from(kill % 4) * 110)).await;
        shard.kill().unwrap();
        shard.wait().unwrap();
        shard = start_shard(&database_url, driver_port, rider_port);
    }
    run.await.unwrap();
    shard.kill().unwrap();
    shard.wait().unwrap();

    let observed = counters.observations.lock().unwrap().clone();
    let report = checker::check_database(&database_url, &observed).await.unwrap();
    println!(
        "{KILLS} kills: {} accepts sent, {} Trips in Postgres, {} TripAssigned seen by drivers, {} driver session errors",
        Counters::get(&counters.accepts),
        report.trips_in_postgres,
        observed.drivers.len(),
        Counters::get(&counters.driver_session_errors),
    );
    assert!(report.passed(), "{:?}", report.examples);
    assert!(report.trips_in_postgres > 20, "too few Trips for the kills to have hit Accepts");
    assert!(Counters::get(&counters.driver_session_errors) > 0, "the kills never reached the drivers");
}

/// Restarts the Postgres container itself while Accepts are in flight. Ignored by default,
/// because it restarts the server every other test uses. Run it alone, with
/// `TEST_POSTGRES_CONTAINER` naming the container behind `DATABASE_URL`:
/// `cargo test -p shard --test shard_kill -- --ignored --test-threads 1`.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn restarting_postgres_mid_accept_keeps_the_invariant() {
    let container = std::env::var("TEST_POSTGRES_CONTAINER").expect("TEST_POSTGRES_CONTAINER names the Postgres container");
    let database_url = fresh_database_url("postgres_restart").await;
    let (driver_port, rider_port) = (free_port(), free_port());
    let config = RunConfig {
        driver_urls: vec![format!("ws://127.0.0.1:{driver_port}")],
        rider_urls: vec![format!("ws://127.0.0.1:{rider_port}")],
        seed: 6,
        ping_interval_seconds: 4.0,
        time_scale: TIME_SCALE,
        seconds_into_first_hour: 0.0,
        silence: Silence::NONE,
    };
    let mut shard = start_shard(&database_url, driver_port, rider_port);
    let counters = Arc::new(Counters::default());
    let run = tokio::spawn(run_plan(config, plan(), 700.0, Arc::clone(&counters)));

    let restarts = 3;
    for _ in 0..restarts {
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let status = Command::new("docker").args(["restart", "-t", "0", &container]).status().unwrap();
        assert!(status.success());
    }
    run.await.unwrap();
    // Let writes that were retrying while Postgres was down land before checking.
    tokio::time::sleep(Duration::from_secs(3)).await;
    shard.kill().unwrap();
    shard.wait().unwrap();

    let observed = counters.observations.lock().unwrap().clone();
    let report = checker::check_database(&database_url, &observed).await.unwrap();
    println!(
        "{restarts} Postgres restarts: {} accepts sent, {} Trips in Postgres, {} TripAssigned seen by drivers",
        Counters::get(&counters.accepts),
        report.trips_in_postgres,
        observed.drivers.len(),
    );
    assert!(report.passed(), "{:?}", report.examples);
    assert!(report.trips_in_postgres > 20);
}
