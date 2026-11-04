//! Accept races and failures against a real Postgres. Needs `DATABASE_URL` pointing at a
//! server where the test may create databases; each test gets its own.

use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};

use assignment::{AcceptOutcome, OfferClosed, OfferKind, Refusal, Store, TripStage};
use geo::Position;
use sqlx::PgPool;

const PICKUP: Position = Position { lat: 40.7, lng: -74.0 };

fn in_a_minute_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64 + 60_000
}

async fn ride(store: &Store, ride: &str) {
    store.store_ride_request(ride, "q", PICKUP).await.unwrap();
}

async fn offer(store: &Store, offer: &str, ride: &str, driver: &str) {
    store.store_offer(offer, ride, driver, OfferKind::Direct, in_a_minute_ms()).await.unwrap();
}

/// The invariant, straight from the tables, so these tests don't depend on the checker.
async fn assert_invariant(pool: &PgPool) {
    let drivers: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM (SELECT driver_id FROM trips WHERE status IN ('assigned','arrived','started')
         GROUP BY driver_id HAVING count(*) > 1) d",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    let rides: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM (SELECT ride_request_id FROM trips WHERE status <> 'cancelled'
         GROUP BY ride_request_id HAVING count(*) > 1) r",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    let mismatched: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM trips t JOIN offers o ON o.id = t.offer_id
         WHERE o.status <> 'accepted' OR o.driver_id <> t.driver_id OR o.ride_request_id <> t.ride_request_id",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!((drivers, rides, mismatched), (0, 0, 0), "drivers with two Trips, rides with two Trips, bad offers");
}

async fn count(pool: &PgPool, sql: &'static str) -> i64 {
    sqlx::query_scalar(sql).fetch_one(pool).await.unwrap()
}

#[sqlx::test(migrator = "assignment::MIGRATOR")]
async fn accept_assigns_once(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    ride(&store, "r1").await;
    offer(&store, "o1", "r1", "d1").await;
    let first = store.accept("o1", "d1", "t1").await.unwrap();
    assert_eq!(first, AcceptOutcome::Assigned { trip: "t1".into(), ride: "r1".into() });
    assert_eq!(store.accept("o1", "d1", "t2").await.unwrap(), AcceptOutcome::Refused(Refusal::OfferNotOpen));
    assert_eq!(count(&pool, "SELECT count(*) FROM trips").await, 1);
}

#[sqlx::test(migrator = "assignment::MIGRATOR")]
async fn another_drivers_offer_and_expired_offers_are_refused(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    ride(&store, "r1").await;
    offer(&store, "o1", "r1", "d1").await;
    assert_eq!(store.accept("o1", "d2", "t1").await.unwrap(), AcceptOutcome::Refused(Refusal::OfferNotOpen));
    store.store_offer("o2", "r1", "d2", OfferKind::Direct, in_a_minute_ms() - 120_000).await.unwrap();
    assert_eq!(store.accept("o2", "d2", "t2").await.unwrap(), AcceptOutcome::Refused(Refusal::OfferNotOpen));
    store.close_offers(&["o1".into()], OfferClosed::Declined).await.unwrap();
    assert_eq!(store.accept("o1", "d1", "t3").await.unwrap(), AcceptOutcome::Refused(Refusal::OfferNotOpen));
    assert_eq!(count(&pool, "SELECT count(*) FROM trips").await, 0);
}

#[sqlx::test(migrator = "assignment::MIGRATOR")]
async fn one_ride_offered_to_many_drivers_gets_one_trip(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    ride(&store, "r1").await;
    for driver in 0..20 {
        offer(&store, &format!("o{driver}"), "r1", &format!("d{driver}")).await;
    }
    let accepts = (0..20).map(|driver| {
        let store = store.clone();
        tokio::spawn(async move { store.accept(&format!("o{driver}"), &format!("d{driver}"), &format!("t{driver}")).await })
    });
    let outcomes: Vec<_> = futures_join(accepts).await;
    let assigned = outcomes.iter().filter(|outcome| matches!(outcome, AcceptOutcome::Assigned { .. })).count();
    assert_eq!(assigned, 1);
    assert_eq!(count(&pool, "SELECT count(*) FROM offers WHERE status = 'open'").await, 0);
    assert_invariant(&pool).await;
}

#[sqlx::test(migrator = "assignment::MIGRATOR")]
async fn one_driver_holding_many_offers_gets_one_trip(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    for index in 0..20 {
        ride(&store, &format!("r{index}")).await;
        offer(&store, &format!("o{index}"), &format!("r{index}"), "d1").await;
    }
    let accepts = (0..20).map(|index| {
        let store = store.clone();
        tokio::spawn(async move { store.accept(&format!("o{index}"), "d1", &format!("t{index}")).await })
    });
    let outcomes: Vec<_> = futures_join(accepts).await;
    let assigned = outcomes.iter().filter(|outcome| matches!(outcome, AcceptOutcome::Assigned { .. })).count();
    assert_eq!(assigned, 1);
    assert!(outcomes.iter().all(|outcome| matches!(
        outcome,
        AcceptOutcome::Assigned { .. } | AcceptOutcome::Refused(Refusal::DriverBusy | Refusal::OfferNotOpen)
    )));
    // The other Ride Requests are still free for other drivers.
    assert_eq!(count(&pool, "SELECT count(*) FROM ride_requests WHERE status = 'open'").await, 19);
    assert_eq!(count(&pool, "SELECT count(*) FROM offers WHERE status = 'open'").await, 0);
    assert_invariant(&pool).await;
}

/// Every driver holds offers for several rides and every ride is offered to several
/// drivers, all accepted at once. Repeated, because a race may only show up sometimes.
#[sqlx::test(migrator = "assignment::MIGRATOR")]
async fn overlapping_offers_accepted_at_once_keep_the_invariant(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let (rides, drivers, per_driver) = (12, 12, 4);
    for round in 0..15 {
        let mut accepts = Vec::new();
        for r in 0..rides {
            ride(&store, &format!("r{round}-{r}")).await;
        }
        for d in 0..drivers {
            for k in 0..per_driver {
                let (r, id) = ((d + k * 3) % rides, format!("o{round}-{d}-{k}"));
                offer(&store, &id, &format!("r{round}-{r}"), &format!("d{round}-{d}")).await;
                let store = store.clone();
                let (driver, trip) = (format!("d{round}-{d}"), format!("t{round}-{d}-{k}"));
                accepts.push(tokio::spawn(async move { store.accept(&id, &driver, &trip).await }));
            }
        }
        let outcomes: Vec<_> = futures_join(accepts).await;
        let assigned: Vec<_> = outcomes
            .iter()
            .filter_map(|outcome| match outcome {
                AcceptOutcome::Assigned { ride, .. } => Some(ride.clone()),
                AcceptOutcome::Refused(_) => None,
            })
            .collect();
        let unique: HashSet<_> = assigned.iter().collect();
        assert_eq!(unique.len(), assigned.len(), "a ride was assigned twice in round {round}");
        assert!(!assigned.is_empty());
    }
    assert_eq!(count(&pool, "SELECT count(*) FROM offers WHERE status = 'open'").await, 0);
    assert_invariant(&pool).await;
}

/// Kills database sessions in the middle of Accepts, the closest a test can get to Postgres
/// dying mid-commit without restarting the server. Every lost result is then settled with
/// `accept_outcome`, and what each caller would tell its driver must match the tables.
#[sqlx::test(migrator = "assignment::MIGRATOR")]
async fn killed_sessions_mid_accept_keep_the_invariant(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let killer_pool = pool.clone();
    let killer = tokio::spawn(async move {
        loop {
            let _ = sqlx::query(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity
                 WHERE datname = current_database() AND pid <> pg_backend_pid() AND state <> 'idle'",
            )
            .execute(&killer_pool)
            .await;
            tokio::time::sleep(std::time::Duration::from_millis(3)).await;
        }
    });
    let mut lost = 0;
    let mut told: Vec<(String, AcceptOutcome)> = Vec::new();
    for round in 0..10 {
        let mut accepts = Vec::new();
        for r in 0..8 {
            ride_retrying(&store, &format!("r{round}-{r}")).await;
        }
        for d in 0..8 {
            for k in 0..3 {
                let (r, id) = ((d + k) % 8, format!("o{round}-{d}-{k}"));
                offer_retrying(&store, &id, &format!("r{round}-{r}"), &format!("d{round}-{d}")).await;
                let store = store.clone();
                let (driver, trip) = (format!("d{round}-{d}"), format!("t{round}-{d}-{k}"));
                accepts.push(tokio::spawn(async move {
                    let result = store.accept(&id, &driver, &trip).await;
                    (id, result)
                }));
            }
        }
        for task in accepts {
            let (id, result) = task.await.unwrap();
            let outcome = match result {
                Ok(outcome) => outcome,
                Err(_) => {
                    lost += 1;
                    settle(&store, &id).await
                }
            };
            told.push((id, outcome));
        }
    }
    killer.abort();
    assert!(lost > 0, "the killer never interrupted an Accept, so this test proved nothing");
    for (offer, outcome) in &told {
        let trip: Option<String> = sqlx::query_scalar("SELECT id FROM trips WHERE offer_id = $1")
            .bind(offer)
            .fetch_optional(&pool)
            .await
            .unwrap();
        match outcome {
            AcceptOutcome::Assigned { trip: told_trip, .. } => assert_eq!(trip.as_ref(), Some(told_trip)),
            AcceptOutcome::Refused(_) => assert_eq!(trip, None, "offer {offer} was refused but has a Trip"),
        }
    }
    assert_eq!(count(&pool, "SELECT count(*) FROM offers WHERE status = 'open'").await, 0);
    assert_invariant(&pool).await;
    println!("lost results settled afterwards: {lost} of {}", told.len());
}

#[sqlx::test(migrator = "assignment::MIGRATOR")]
async fn completing_a_trip_frees_the_driver(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    for r in ["r1", "r2"] {
        ride(&store, r).await;
    }
    offer(&store, "o1", "r1", "d1").await;
    offer(&store, "o2", "r2", "d1").await;
    store.accept("o1", "d1", "t1").await.unwrap();
    // The Accept withdrew d1's other offer, so a new one is needed.
    offer(&store, "o3", "r2", "d1").await;
    assert_eq!(store.accept("o3", "d1", "t2").await.unwrap(), AcceptOutcome::Refused(Refusal::DriverBusy));
    // A trip event from the wrong driver is ignored.
    store.trip_event("t1", "d2", TripStage::Completed).await.unwrap();
    offer(&store, "o4", "r2", "d1").await;
    assert_eq!(store.accept("o4", "d1", "t3").await.unwrap(), AcceptOutcome::Refused(Refusal::DriverBusy));
    for stage in [TripStage::Arrived, TripStage::Started, TripStage::Completed] {
        store.trip_event("t1", "d1", stage).await.unwrap();
    }
    offer(&store, "o5", "r2", "d1").await;
    assert!(matches!(store.accept("o5", "d1", "t4").await.unwrap(), AcceptOutcome::Assigned { .. }));
    assert_invariant(&pool).await;
}

#[sqlx::test(migrator = "assignment::MIGRATOR")]
async fn recovery_keeps_trips_and_closes_the_rest(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    for r in ["r1", "r2"] {
        ride(&store, r).await;
    }
    offer(&store, "o1", "r1", "d1").await;
    offer(&store, "o2", "r2", "d2").await;
    store.accept("o1", "d1", "t1").await.unwrap();
    let trips = store.recover().await.unwrap();
    assert_eq!(trips.len(), 1);
    assert_eq!((trips[0].trip.as_str(), trips[0].driver.as_str(), trips[0].ride.as_str()), ("t1", "d1", "r1"));
    assert_eq!(store.accept("o2", "d2", "t2").await.unwrap(), AcceptOutcome::Refused(Refusal::OfferNotOpen));
    assert_eq!(count(&pool, "SELECT count(*) FROM ride_requests WHERE status = 'cancelled'").await, 1);
}

#[sqlx::test(migrator = "assignment::MIGRATOR")]
async fn accept_outcome_reports_what_was_committed(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    ride(&store, "r1").await;
    offer(&store, "o1", "r1", "d1").await;
    offer(&store, "o2", "r1", "d2").await;
    store.accept("o1", "d1", "t1").await.unwrap();
    assert_eq!(store.accept_outcome("o1").await.unwrap(), AcceptOutcome::Assigned { trip: "t1".into(), ride: "r1".into() });
    assert_eq!(store.accept_outcome("o2").await.unwrap(), AcceptOutcome::Refused(Refusal::OfferNotOpen));
}

async fn futures_join<T: Send + 'static>(
    tasks: impl IntoIterator<Item = tokio::task::JoinHandle<Result<T, assignment::Error>>>,
) -> Vec<T> {
    let mut results = Vec::new();
    for task in tasks {
        results.push(task.await.unwrap().expect("no database errors without fault injection"));
    }
    results
}

async fn settle(store: &Store, offer: &str) -> AcceptOutcome {
    loop {
        if let Ok(outcome) = store.accept_outcome(offer).await {
            return outcome;
        }
    }
}

async fn ride_retrying(store: &Store, id: &str) {
    while store.store_ride_request(id, "q", PICKUP).await.is_err() {}
}

async fn offer_retrying(store: &Store, id: &str, ride: &str, driver: &str) {
    while store.store_offer(id, ride, driver, OfferKind::Direct, in_a_minute_ms()).await.is_err() {}
}
