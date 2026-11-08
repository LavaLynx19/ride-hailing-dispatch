//! Cross-shard Reservations against a real Postgres. Needs `DATABASE_URL`, like the Accept
//! tests. Each test plays both Shards on one database: `reservations` belongs to the driver's
//! Shard and `peer_reservations` to the rider's, so they don't collide.

use std::time::{SystemTime, UNIX_EPOCH};

use assignment::{AcceptOutcome, Held, OfferKind, PeerDecision, Refusal, Store, TripStage};
use geo::Position;
use sqlx::PgPool;

const PICKUP: Position = Position { lat: 40.7, lng: -74.0 };
const PEER: u16 = 1;

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

async fn ride_with_offer(store: &Store, ride: &str, offer: &str, driver: &str) {
    store.store_ride_request(ride, "q", PICKUP).await.unwrap();
    store.store_offer(offer, ride, driver, OfferKind::Direct, now_ms() + 60_000).await.unwrap();
}

async fn trips_for(pool: &PgPool, ride: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM trips WHERE ride_request_id = $1 AND status <> 'cancelled'")
        .bind(ride)
        .fetch_one(pool)
        .await
        .unwrap()
}

#[sqlx::test(migrator = "assignment::MIGRATOR")]
async fn a_reserved_driver_takes_no_local_trip_and_no_second_reservation(pool: PgPool) {
    let store = Store::from_pool(pool);
    assert!(store.reserve("v1", "d1", "remote-o1", "remote-r1", PEER, now_ms() + 5_000).await.unwrap());
    assert!(!store.reserve("v2", "d1", "remote-o2", "remote-r2", PEER, now_ms() + 5_000).await.unwrap());
    // Retrying the same Reservation is fine.
    assert!(store.reserve("v1", "d1", "remote-o1", "remote-r1", PEER, now_ms() + 5_000).await.unwrap());

    ride_with_offer(&store, "r1", "o1", "d1").await;
    assert_eq!(store.accept("o1", "d1", "t1").await.unwrap(), AcceptOutcome::Refused(Refusal::DriverBusy));

    // Confirmed is the lease: still held. Released frees the driver.
    store.settle_reservation("v1", &PeerDecision::Confirmed { trip: "remote-t1".into() }).await.unwrap();
    assert_eq!(store.held_reservations().await.unwrap()[0].held, Held::Confirmed);
    store.release_reservation("v1").await.unwrap();
    assert!(store.held_reservations().await.unwrap().is_empty());
    store.store_offer("o2", "r1", "d1", OfferKind::Direct, now_ms() + 60_000).await.unwrap();
    assert!(matches!(store.accept("o2", "d1", "t1").await.unwrap(), AcceptOutcome::Assigned { .. }));

    // With a local Trip, the driver can't be reserved.
    assert!(!store.reserve("v3", "d1", "remote-o3", "remote-r3", PEER, now_ms() + 5_000).await.unwrap());
}

#[sqlx::test(migrator = "assignment::MIGRATOR")]
async fn an_aborted_reservation_frees_the_driver_and_a_late_answer_cant_revive_it(pool: PgPool) {
    let store = Store::from_pool(pool);
    store.reserve("v1", "d1", "o", "r", PEER, now_ms() + 5_000).await.unwrap();
    store.settle_reservation("v1", &PeerDecision::Aborted).await.unwrap();
    store.settle_reservation("v1", &PeerDecision::Confirmed { trip: "t".into() }).await.unwrap();
    assert!(store.held_reservations().await.unwrap().is_empty());
    assert!(store.reserve("v2", "d1", "o", "r", PEER, now_ms() + 5_000).await.unwrap());
}

#[sqlx::test(migrator = "assignment::MIGRATOR")]
async fn confirm_assigns_once_and_answers_retries_the_same(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    ride_with_offer(&store, "r1", "o1", "d1").await;
    let decision = store.confirm("v1", PEER, "o1", "d1", "t1", now_ms() + 5_000).await.unwrap();
    assert_eq!(decision, PeerDecision::Confirmed { trip: "t1".into() });
    // A retry with a fresh Trip id gets the first answer.
    assert_eq!(store.confirm("v1", PEER, "o1", "d1", "t9", now_ms() + 5_000).await.unwrap(), decision);
    assert_eq!(store.peer_status("v1", PEER).await.unwrap(), decision);
    assert_eq!(trips_for(&pool, "r1").await, 1);

    store.trip_event("t1", "d1", TripStage::Completed).await.unwrap();
    assert_eq!(store.peer_status("v1", PEER).await.unwrap(), PeerDecision::Released { trip: "t1".into() });
    assert!(store.leased_trips().await.unwrap().is_empty());
}

#[sqlx::test(migrator = "assignment::MIGRATOR")]
async fn confirm_refuses_a_taken_ride_a_closed_offer_and_an_expired_reservation(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    ride_with_offer(&store, "r1", "o1", "d1").await;
    store.store_offer("o2", "r1", "d2", OfferKind::Broadcast, now_ms() + 60_000).await.unwrap();
    assert!(matches!(store.accept("o1", "d1", "t1").await.unwrap(), AcceptOutcome::Assigned { .. }));
    assert_eq!(store.confirm("v1", PEER, "o2", "d2", "t2", now_ms() + 5_000).await.unwrap(), PeerDecision::Aborted);
    assert_eq!(trips_for(&pool, "r1").await, 1);

    ride_with_offer(&store, "r2", "o3", "d3").await;
    // Well past, since Postgres checks with its own clock, which may trail the test's.
    assert_eq!(store.confirm("v2", PEER, "o3", "d3", "t3", now_ms() - 5_000).await.unwrap(), PeerDecision::Expired);
    assert_eq!(store.peer_status("v2", PEER).await.unwrap(), PeerDecision::Expired);
    assert_eq!(trips_for(&pool, "r2").await, 0);
    // The offer is still open for a local Accept.
    assert!(matches!(store.accept("o3", "d3", "t3").await.unwrap(), AcceptOutcome::Assigned { .. }));

    ride_with_offer(&store, "r3", "o4", "d4").await;
    assert_eq!(store.confirm("v3", PEER, "o4", "someone-else", "t4", now_ms() + 5_000).await.unwrap(), PeerDecision::Aborted);
}

#[sqlx::test(migrator = "assignment::MIGRATOR")]
async fn a_status_check_first_means_a_late_confirm_loses(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    ride_with_offer(&store, "r1", "o1", "d1").await;
    assert_eq!(store.peer_status("v1", PEER).await.unwrap(), PeerDecision::Aborted);
    assert_eq!(store.confirm("v1", PEER, "o1", "d1", "t1", now_ms() + 5_000).await.unwrap(), PeerDecision::Aborted);
    assert_eq!(trips_for(&pool, "r1").await, 0);
}

/// The race the architecture names: a Confirm and a status check for the same Reservation,
/// at the same moment. Whichever commits first decides, and both see that answer.
#[sqlx::test(migrator = "assignment::MIGRATOR")]
async fn confirm_and_status_check_racing_always_agree(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let (mut confirmed, mut aborted) = (0, 0);
    for round in 0..200 {
        let (ride, offer, driver, id) = (format!("r{round}"), format!("o{round}"), format!("d{round}"), format!("v{round}"));
        ride_with_offer(&store, &ride, &offer, &driver).await;
        let trip = format!("t{round}");
        let (by_confirm, by_check) = tokio::join!(
            store.confirm(&id, PEER, &offer, &driver, &trip, now_ms() + 5_000),
            store.peer_status(&id, PEER),
        );
        let (by_confirm, by_check) = (by_confirm.unwrap(), by_check.unwrap());
        assert_eq!(by_confirm, by_check, "round {round}");
        let trips = trips_for(&pool, &ride).await;
        match by_confirm {
            PeerDecision::Confirmed { .. } => {
                confirmed += 1;
                assert_eq!(trips, 1, "round {round}");
            }
            PeerDecision::Aborted => {
                aborted += 1;
                assert_eq!(trips, 0, "round {round}");
            }
            other => panic!("round {round}: {other:?}"),
        }
    }
    println!("{confirmed} confirmed first, {aborted} checked first");
    assert!(confirmed > 0 && aborted > 0, "the race should go both ways: {confirmed} / {aborted}");
}
