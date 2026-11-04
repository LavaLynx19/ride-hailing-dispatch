//! The checker must pass clean runs and flag deliberately injected Double Assignments.
//! Needs `DATABASE_URL`, like the assignment tests.

use std::time::{SystemTime, UNIX_EPOCH};

use assignment::{OfferKind, Store};
use checker::{DriverDoubleBooked, DriverTold, Observations, RiderTold, check};
use geo::Position;
use sqlx::PgPool;

async fn assigned(store: &Store, ride: &str, driver: &str) -> String {
    let expires = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64 + 60_000;
    store.store_ride_request(ride, "q", Position { lat: 40.7, lng: -74.0 }).await.unwrap();
    let offer = format!("o-{ride}-{driver}");
    store.store_offer(&offer, ride, driver, OfferKind::Direct, expires).await.unwrap();
    let trip = format!("t-{ride}");
    store.accept(&offer, driver, &trip).await.unwrap();
    trip
}

fn told(driver: &str, trip: &str, ride: &str) -> Observations {
    Observations {
        drivers: vec![DriverTold { driver: driver.into(), trip: trip.into(), ride: ride.into() }],
        riders: vec![RiderTold { ride: ride.into(), trip: trip.into() }],
        double_booked: Vec::new(),
    }
}

#[sqlx::test(migrator = "assignment::MIGRATOR")]
async fn a_clean_run_passes(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let trip = assigned(&store, "r1", "d1").await;
    let report = check(&pool, &told("d1", &trip, "r1")).await.unwrap();
    assert!(report.passed(), "{:?}", report.examples);
    assert_eq!(report.trips_in_postgres, 1);
}

#[sqlx::test(migrator = "assignment::MIGRATOR")]
async fn flags_a_driver_with_two_trips_injected_past_the_constraint(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    assigned(&store, "r1", "d1").await;
    store.store_ride_request("r2", "q", Position { lat: 40.7, lng: -74.0 }).await.unwrap();
    store.store_offer("o-inj", "r2", "d1", OfferKind::Direct, 4_102_444_800_000).await.unwrap();
    sqlx::query("DROP INDEX trips_one_active_per_driver").execute(&pool).await.unwrap();
    sqlx::query("UPDATE offers SET status = 'accepted' WHERE id = 'o-inj'").execute(&pool).await.unwrap();
    sqlx::query("UPDATE ride_requests SET status = 'assigned' WHERE id = 'r2'").execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO trips (id, ride_request_id, driver_id, offer_id, status) VALUES ('t-inj', 'r2', 'd1', 'o-inj', 'assigned')")
        .execute(&pool)
        .await
        .unwrap();
    let report = check(&pool, &Observations::default()).await.unwrap();
    assert_eq!(report.violations, 1, "{:?}", report.examples);
    assert!(report.examples[0].contains("driver d1 has 2 active Trips"));
}

#[sqlx::test(migrator = "assignment::MIGRATOR")]
async fn flags_a_ride_with_two_trips_injected_past_the_constraint(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    assigned(&store, "r1", "d1").await;
    store.store_offer("o-inj", "r1", "d2", OfferKind::Direct, 4_102_444_800_000).await.unwrap();
    sqlx::query("DROP INDEX trips_one_per_ride_request").execute(&pool).await.unwrap();
    sqlx::query("UPDATE offers SET status = 'accepted' WHERE id = 'o-inj'").execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO trips (id, ride_request_id, driver_id, offer_id, status) VALUES ('t-inj', 'r1', 'd2', 'o-inj', 'assigned')")
        .execute(&pool)
        .await
        .unwrap();
    let report = check(&pool, &Observations::default()).await.unwrap();
    assert!(report.examples.iter().any(|example| example.contains("Ride Request r1 has 2 Trips")), "{:?}", report.examples);
}

#[sqlx::test(migrator = "assignment::MIGRATOR")]
async fn flags_clients_told_about_trips_without_a_commit(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let trip = assigned(&store, "r1", "d1").await;
    let mut observed = told("d1", &trip, "r1");
    observed.drivers.push(DriverTold { driver: "d2".into(), trip: "t-ghost".into(), ride: "r1".into() });
    observed.riders.push(RiderTold { ride: "r1".into(), trip: "t-ghost".into() });
    observed.double_booked.push(DriverDoubleBooked { driver: "d1".into(), held_trip: trip, new_trip: "t-ghost".into() });
    let report = check(&pool, &observed).await.unwrap();
    // The driver's ghost Trip, the rider's ghost Trip, the rider told two Trips, and the double booking.
    assert_eq!(report.violations, 4, "{:?}", report.examples);
}
