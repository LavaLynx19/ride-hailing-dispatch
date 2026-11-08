//! The checker across two Shards' databases: clean cross-shard Accepts pass, and a driver
//! busy on both Shards or a Reservation whose halves disagree are flagged. Needs `DATABASE_URL`.

use std::time::{SystemTime, UNIX_EPOCH};

use assignment::{OfferKind, PeerDecision, Store};
use checker::{Observations, check_all};
use geo::Position;
use sqlx::{Connection, PgConnection, PgPool};

const PICKUP: Position = Position { lat: 40.7, lng: -74.0 };

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

/// A second, migrated database for Shard 1, named after the test.
async fn second_shard(name: &str) -> PgPool {
    let base = std::env::var("DATABASE_URL").expect("DATABASE_URL must point at a test Postgres server");
    let mut admin = PgConnection::connect(&base).await.unwrap();
    assert!(name.chars().all(|c| c.is_ascii_lowercase() || c == '_'));
    for statement in [format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"), format!("CREATE DATABASE {name}")] {
        sqlx::raw_sql(sqlx::AssertSqlSafe(statement)).execute(&mut admin).await.unwrap();
    }
    let (server, _) = base.rsplit_once('/').unwrap();
    let pool = PgPool::connect(&format!("{server}/{name}")).await.unwrap();
    assignment::MIGRATOR.run(&pool).await.unwrap();
    pool
}

/// Driver d1 on Shard 0 takes Shard 1's ride r1 through a Reservation.
async fn cross_shard_trip(a: &Store, b: &Store) {
    b.store_ride_request("r1", "q", PICKUP).await.unwrap();
    b.store_offer("o1", "r1", "d1", OfferKind::Direct, now_ms() + 60_000).await.unwrap();
    assert!(a.reserve("v1", "d1", "o1", "r1", 1, now_ms() + 5_000).await.unwrap());
    let decision = b.confirm("v1", 0, "o1", "d1", "t1", now_ms() + 5_000).await.unwrap();
    assert_eq!(decision, PeerDecision::Confirmed { trip: "t1".into() });
    a.settle_reservation("v1", &decision).await.unwrap();
}

#[sqlx::test(migrator = "assignment::MIGRATOR")]
async fn a_clean_cross_shard_trip_passes(pool: PgPool) {
    let peer = second_shard("checker_clean_peer").await;
    cross_shard_trip(&Store::from_pool(pool.clone()), &Store::from_pool(peer.clone())).await;
    let report = check_all(&[pool, peer], &Observations::default()).await.unwrap();
    assert!(report.passed(), "{:?}", report.examples);
    assert_eq!(report.trips_in_postgres, 1);
}

#[sqlx::test(migrator = "assignment::MIGRATOR")]
async fn flags_a_driver_with_active_trips_on_two_shards(pool: PgPool) {
    let peer = second_shard("checker_double_peer").await;
    let (a, b) = (Store::from_pool(pool.clone()), Store::from_pool(peer.clone()));
    cross_shard_trip(&a, &b).await;
    // Past the lease: a local Trip for the same driver on Shard 0, written directly.
    a.store_ride_request("r2", "q", PICKUP).await.unwrap();
    a.store_offer("o2", "r2", "d1", OfferKind::Direct, now_ms() + 60_000).await.unwrap();
    for sql in [
        "UPDATE offers SET status = 'accepted' WHERE id = 'o2'",
        "UPDATE ride_requests SET status = 'assigned' WHERE id = 'r2'",
        "INSERT INTO trips (id, ride_request_id, driver_id, offer_id, status) VALUES ('t2', 'r2', 'd1', 'o2', 'assigned')",
    ] {
        sqlx::query(sql).execute(&pool).await.unwrap();
    }
    let report = check_all(&[pool, peer], &Observations::default()).await.unwrap();
    assert!(!report.passed());
    assert!(report.examples.iter().any(|example| example.contains("driver d1 has 2 active Trips")), "{:?}", report.examples);
}

#[sqlx::test(migrator = "assignment::MIGRATOR")]
async fn flags_a_reservation_whose_halves_disagree(pool: PgPool) {
    let peer = second_shard("checker_split_peer").await;
    let (a, b) = (Store::from_pool(pool.clone()), Store::from_pool(peer.clone()));
    b.store_ride_request("r1", "q", PICKUP).await.unwrap();
    b.store_offer("o1", "r1", "d1", OfferKind::Direct, now_ms() + 60_000).await.unwrap();
    a.reserve("v1", "d1", "o1", "r1", 1, now_ms() + 5_000).await.unwrap();
    b.confirm("v1", 0, "o1", "d1", "t1", now_ms() + 5_000).await.unwrap();
    // The driver's Shard wrongly gives up, as the old expiry rule would have.
    a.settle_reservation("v1", &PeerDecision::Expired).await.unwrap();
    let report = check_all(&[pool, peer], &Observations::default()).await.unwrap();
    assert!(!report.passed());
    assert!(report.examples.iter().any(|example| example.contains("driver's Shard has it expired")), "{:?}", report.examples);
}
