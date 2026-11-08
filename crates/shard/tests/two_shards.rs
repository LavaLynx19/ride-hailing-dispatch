//! Two real Shard processes, each with its own Postgres, split down a line of longitude, with
//! the real simulator driving riders and drivers across the boundary. Ends with the invariant
//! checker over both databases. Needs `DATABASE_URL`, like the other Postgres tests.
//!
//! The fault test is the campaign's repeatable core: dropped and delayed peer calls, injected
//! crashes at each Reservation step, and SIGKILLs, with crashed Shards restarted.

mod common;

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use common::{free_port, fresh_database_url};
use geo::Position;
use geo::shard_map::{MAP_RESOLUTION, ShardAddress, ShardMap, centre};
use simulator::client::{Counters, RunConfig, Silence, run_plan};
use simulator::driver::Dropoff;
use simulator::plan::{DriverPlan, Plan, TripPlan};

const TIME_SCALE: f64 = 100.0;
/// The boundary between the Shards.
const BOUNDARY_LNG: f64 = -74.0;

/// A point `km_east` of the boundary (negative is west), on one line of latitude.
fn at(km_east: f64) -> Position {
    Position { lat: 40.70, lng: BOUNDARY_LNG + km_east / 84.3 }
}

struct Cluster {
    urls: Vec<String>,
    driver_urls: Vec<String>,
    rider_urls: Vec<String>,
    map_path: PathBuf,
    ports: Vec<(u16, u16, u16)>,
    shards: Vec<Option<Child>>,
    env: Vec<(String, String)>,
}

impl Cluster {
    async fn new(name: &str, env: &[(&str, &str)]) -> Self {
        let mut urls = Vec::new();
        let mut ports = Vec::new();
        for shard in 0..2 {
            urls.push(fresh_database_url(&format!("{name}_{}", ["a", "b"][shard])).await);
            ports.push((free_port(), free_port(), free_port()));
        }
        let map = Self::map(1, 0.0, &ports);
        let map_path = std::env::temp_dir().join(format!("{name}-{}.map", std::process::id()));
        std::fs::write(&map_path, map.to_text()).unwrap();
        let driver_urls = ports.iter().map(|(drivers, _, _)| format!("ws://127.0.0.1:{drivers}")).collect();
        let rider_urls = ports.iter().map(|(_, riders, _)| format!("ws://127.0.0.1:{riders}")).collect();
        let env = env.iter().map(|(k, v)| ((*k).to_owned(), (*v).to_owned())).collect();
        let mut cluster = Self { urls, driver_urls, rider_urls, map_path, ports, shards: vec![None, None], env };
        for shard in 0..2 {
            cluster.start(shard);
        }
        cluster.wait_ready().await;
        cluster
    }

    /// Cells along the line, each owned by the side of `boundary_km` its centre is on.
    fn map(version: u32, boundary_km: f64, ports: &[(u16, u16, u16)]) -> ShardMap {
        let boundary = at(boundary_km).lng;
        let mut cells = std::collections::BTreeMap::new();
        for step in -40..=40 {
            let cell = at(f64::from(step) * 0.25).cell(MAP_RESOLUTION).unwrap();
            cells.insert(cell, u16::from(centre(cell).lng >= boundary));
        }
        let shards = ports
            .iter()
            .enumerate()
            .map(|(id, (drivers, riders, peer))| ShardAddress {
                id: id as u16,
                peer: format!("http://127.0.0.1:{peer}"),
                drivers: vec![format!("ws://127.0.0.1:{drivers}")],
                riders: vec![format!("ws://127.0.0.1:{riders}")],
            })
            .collect();
        ShardMap::new(version, shards, cells).unwrap()
    }

    /// Sends a new map version to each Shard in turn, as an operator would.
    async fn apply_map(&self, map: &ShardMap) {
        for (_, _, peer) in &self.ports {
            let mut client = protocol::shard::shard_client::ShardClient::connect(format!("http://127.0.0.1:{peer}")).await.unwrap();
            let request = protocol::shard::ShardMapText { text: map.to_text() };
            assert_eq!(client.apply_shard_map(request).await.unwrap().into_inner().version, map.version());
            // A gap, so the Shards really do disagree for a moment.
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Until both Shards accept driver and rider connections.
    async fn wait_ready(&self) {
        for (drivers, riders, _) in &self.ports {
            for port in [drivers, riders] {
                let deadline = std::time::Instant::now() + Duration::from_secs(20);
                while tokio::net::TcpStream::connect(("127.0.0.1", *port)).await.is_err() {
                    assert!(std::time::Instant::now() < deadline, "Shard on port {port} never started");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    }

    fn start(&mut self, shard: usize) {
        let (drivers, riders, peer) = self.ports[shard];
        let child = Command::new(env!("CARGO_BIN_EXE_shard"))
            .env("DATABASE_URL", &self.urls[shard])
            .env("SHARD_ID", shard.to_string())
            .env("SHARD_MAP", &self.map_path)
            .env("PEER_LISTEN", format!("127.0.0.1:{peer}"))
            .envs(self.env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
            .args([format!("127.0.0.1:{drivers}"), format!("127.0.0.1:{riders}")])
            .stderr(match std::env::var("SHARD_TEST_LOG") {
                Ok(dir) => Stdio::from(
                    std::fs::OpenOptions::new().create(true).append(true).open(format!("{dir}/shard-{shard}.log")).unwrap(),
                ),
                Err(_) => Stdio::null(),
            })
            .spawn()
            .unwrap();
        self.shards[shard] = Some(child);
    }

    /// Restarts any Shard that crashed on its own; returns how many.
    fn restart_crashed(&mut self) -> u32 {
        let mut restarted = 0;
        for shard in 0..2 {
            let exited = self.shards[shard].as_mut().is_some_and(|child| child.try_wait().unwrap().is_some());
            if exited {
                self.start(shard);
                restarted += 1;
            }
        }
        restarted
    }

    fn kill(&mut self, shard: usize) {
        if let Some(mut child) = self.shards[shard].take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    fn config(&self, seed: u64) -> RunConfig {
        RunConfig {
            driver_urls: self.driver_urls.clone(),
            rider_urls: self.rider_urls.clone(),
            seed,
            ping_interval_seconds: 4.0,
            time_scale: TIME_SCALE,
            seconds_into_first_hour: 0.0,
            silence: Silence::NONE,
        }
    }

    async fn check(&self, counters: &Counters) -> checker::Report {
        // Let peers settle what was in flight when the run stopped.
        tokio::time::sleep(Duration::from_secs(3)).await;
        let observed = counters.observations.lock().unwrap().clone();
        checker::check_database(&self.urls.join(","), &observed).await.unwrap()
    }

    async fn count(&self, shard: usize, sql: &str) -> i64 {
        let pool = sqlx::PgPool::connect(&self.urls[shard]).await.unwrap();
        let count = sqlx::query_scalar(sqlx::AssertSqlSafe(sql.to_owned())).fetch_one(&pool).await.unwrap();
        pool.close().await;
        count
    }

    async fn summary(&self, shard: usize, sql: &str) -> String {
        let pool = sqlx::PgPool::connect(&self.urls[shard]).await.unwrap();
        let rows: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(sql.to_owned())).fetch_all(&pool).await.unwrap();
        pool.close().await;
        rows.join(", ")
    }

    async fn cross_shard_trips(&self) -> i64 {
        let mut total = 0;
        for shard in 0..2 {
            total += self.count(shard, "SELECT count(*) FROM peer_reservations WHERE status = 'confirmed'").await;
        }
        total
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for shard in 0..2 {
            self.kill(shard);
        }
        let _ = std::fs::remove_file(&self.map_path);
    }
}

/// Most drivers start west of the boundary, and riders' pickups and dropoffs alternate sides,
/// so east-side rides are matched across the boundary and drivers drift between Shards.
/// With `all_cross`, drivers start west and every pickup is east with its dropoff back west,
/// so every match crosses the boundary.
fn plan(drivers: usize, trips: u32, request_every: f64, all_cross: bool) -> Plan {
    let drivers = (0..drivers)
        .map(|index| DriverPlan {
            id: format!("d{index}"),
            stream: index as u64,
            position: if index % 4 == 3 && !all_cross { at(0.5 + index as f64 * 0.1) } else { at(-0.3 - index as f64 * 0.15) },
            online: vec![true],
            busy_until_seconds: 0.0,
        })
        .collect();
    let trips = (0..trips)
        .map(|index| {
            let side = if index % 2 == 0 || all_cross { 1.0 } else { -1.0 };
            TripPlan {
                request_at_seconds: 10.0 + f64::from(index) * request_every,
                pickup: at(side * (0.3 + f64::from(index % 7) * 0.4)),
                dropoff: Dropoff::Inside(at(-side * (0.5 + f64::from(index % 5) * 0.6))),
                trip_seconds: 90.0,
            }
        })
        .collect();
    Plan { drivers, trips, ..Plan::default() }
}

#[tokio::test(flavor = "multi_thread")]
async fn trips_cross_the_boundary_and_the_invariant_holds_across_shards() {
    let cluster = Cluster::new("two_shards", &[]).await;
    let counters = Arc::new(Counters::default());
    run_plan(cluster.config(7), plan(16, 60, 4.0, false), 2500.0, Arc::clone(&counters)).await;
    let report = cluster.check(&counters).await;
    let cross = cluster.cross_shard_trips().await;
    println!(
        "{} Trips, {cross} across Shards, {} redirects, {} completed, {} accepts",
        report.trips_in_postgres,
        Counters::get(&counters.redirects),
        Counters::get(&counters.trips_completed),
        Counters::get(&counters.accepts),
    );
    for shard in 0..2 {
        let rides = cluster.summary(shard, "SELECT status || ' ' || count(*) FROM ride_requests GROUP BY status").await;
        let offers = cluster.summary(shard, "SELECT kind || '/' || status || ' ' || count(*) FROM offers GROUP BY kind, status").await;
        println!("Shard {shard}: rides [{rides}], offers [{offers}]");
    }
    println!(
        "ride requests sent {}, rider connect failures {}, rider session errors {}, failures {:?}",
        Counters::get(&counters.ride_requests),
        Counters::get(&counters.rider_connect_failures),
        Counters::get(&counters.rider_session_errors),
        counters.failures.lock().unwrap()
    );
    assert!(report.passed(), "{:?}", report.examples);
    assert_eq!(report.trips_in_postgres, 60, "every ride was assigned once");
    assert_eq!(Counters::get(&counters.riders_finished), 60, "every Trip completed");
    assert!(cross > 0, "some matches should cross the boundary");
    assert!(Counters::get(&counters.redirects) > 0, "clients should be redirected across the boundary");
    for failures in [&counters.driver_connect_failures, &counters.rider_connect_failures, &counters.rider_session_errors] {
        assert_eq!(Counters::get(failures), 0, "{:?}", counters.failures.lock().unwrap());
    }
}

/// One round of the fault campaign. `CAMPAIGN_FAULTS` and `CAMPAIGN_CRASHES` override the
/// fault mix, `CAMPAIGN_PARTITIONS=1` cuts the Shards apart for 2 s in every 6, and
/// `CAMPAIGN_KILLS=0` stops the SIGKILLs, so the same test runs the whole campaign from a script.
#[tokio::test(flavor = "multi_thread")]
async fn peer_faults_crashes_and_kills_keep_the_invariant_across_shards() {
    let faults = std::env::var("CAMPAIGN_FAULTS").unwrap_or_else(|_| "drop_request=0.05,drop_reply=0.05,delay_ms=300".into());
    let crashes = std::env::var("CAMPAIGN_CRASHES")
        .unwrap_or_else(|_| "after_reserve=0.04,after_confirm=0.04,before_settle=0.04,before_release=0.04".into());
    let seed: u64 = std::env::var("CAMPAIGN_SEED").ok().and_then(|seed| seed.parse().ok()).unwrap_or(11);
    let partitions = std::env::var("CAMPAIGN_PARTITIONS").is_ok_and(|value| value == "1");
    let kills_on = std::env::var("CAMPAIGN_KILLS").map_or(true, |value| value != "0");
    let partition_file = std::env::temp_dir().join(format!("shard-partition-{}", std::process::id()));
    let _ = std::fs::remove_file(&partition_file);
    let partition_path = partition_file.to_string_lossy().into_owned();
    let mut cluster = Cluster::new(
        "shard_faults",
        &[("PEER_FAULTS", &faults), ("CRASH_AT", &crashes), ("PARTITION_FILE", &partition_path)],
    )
    .await;
    let counters = Arc::new(Counters::default());
    let run = tokio::spawn(run_plan(cluster.config(seed), plan(16, 80, 25.0, true), 3000.0, Arc::clone(&counters)));

    let (mut crashes_seen, mut kills) = (0, 0);
    for step in 0..290_u64 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        crashes_seen += cluster.restart_crashed();
        if partitions {
            if step % 60 == 20 {
                std::fs::write(&partition_file, b"").unwrap();
            } else if step % 60 == 40 {
                let _ = std::fs::remove_file(&partition_file);
            }
        }
        // A SIGKILL now and then, at uneven moments, on alternating Shards.
        if kills_on && step % 60 == 30 {
            let shard = (kills % 2) as usize;
            cluster.kill(shard);
            cluster.start(shard);
            kills += 1;
        }
    }
    run.await.unwrap();
    // Heal any partition, so unresolved Reservations can settle before the check.
    let _ = std::fs::remove_file(&partition_file);
    crashes_seen += cluster.restart_crashed();
    let report = cluster.check(&counters).await;
    let cross = cluster.cross_shard_trips().await;
    let mut held = 0;
    for shard in 0..2 {
        held += cluster.count(shard, "SELECT count(*) FROM reservations WHERE status = 'reserved'").await;
    }
    println!(
        "faults [{faults}] crashes [{crashes}] partitions {partitions} seed {seed}: {crashes_seen} injected crashes, {kills} kills, \
         {} Trips, {cross} across Shards, {held} Reservations still unresolved, {} accepts",
        report.trips_in_postgres,
        Counters::get(&counters.accepts),
    );
    for shard in 0..2 {
        let held = cluster.summary(shard, "SELECT status || ' ' || count(*) FROM reservations GROUP BY status ORDER BY 1").await;
        let decided = cluster.summary(shard, "SELECT status || ' ' || count(*) FROM peer_reservations GROUP BY status ORDER BY 1").await;
        println!("Shard {shard}: reservations held [{held}], decided for peers [{decided}]");
    }
    println!("invariant: {} with {} violations", if report.passed() { "passed" } else { "FAILED" }, report.violations);
    assert!(report.passed(), "{:?}", report.examples);
    // Riders on a killed Shard don't reconnect (a known limit), so Trips are fewer than rides.
    assert!(report.trips_in_postgres >= 10, "too few Trips for the faults to have hit Accepts");
    assert!(cross > 0, "faults should still let some cross-shard Accepts through");
}

#[tokio::test(flavor = "multi_thread")]
async fn moving_the_boundary_mid_run_keeps_the_invariant() {
    let cluster = Cluster::new("shard_map_change", &[]).await;
    let counters = Arc::new(Counters::default());
    let run = tokio::spawn(run_plan(cluster.config(9), plan(16, 60, 20.0, false), 2500.0, Arc::clone(&counters)));
    tokio::time::sleep(Duration::from_secs(5)).await;
    // Shard 1 takes over 2 km west of the old boundary, then gives it back.
    cluster.apply_map(&Cluster::map(2, -2.0, &cluster.ports)).await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    cluster.apply_map(&Cluster::map(3, 0.0, &cluster.ports)).await;
    run.await.unwrap();
    let report = cluster.check(&counters).await;
    let mut versions = Vec::new();
    for shard in 0..2 {
        versions.push(cluster.count(shard, "SELECT max(version)::bigint FROM shard_maps").await);
    }
    println!(
        "{} Trips, {} across Shards, {} redirects, {} completed, map versions {versions:?}",
        report.trips_in_postgres,
        cluster.cross_shard_trips().await,
        Counters::get(&counters.redirects),
        Counters::get(&counters.riders_finished),
    );
    assert!(report.passed(), "{:?}", report.examples);
    assert_eq!(versions, [3, 3]);
    assert_eq!(report.trips_in_postgres, 60, "every ride was assigned once");
    assert_eq!(Counters::get(&counters.riders_finished), 60, "every Trip completed");
}
