//! Invariant checker, run after each experiment. A result only counts if this passes.
//!
//! It checks Postgres, and also what simulated clients were actually told, which catches a
//! TripAssigned sent without a commit behind it.

use std::collections::HashMap;

use sqlx::PgPool;

const MAX_LISTED: usize = 20;

/// A TripAssigned message a driver received.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DriverTold {
    pub driver: String,
    pub trip: String,
    pub ride: String,
}

/// An "assigned" Trip status a rider received.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RiderTold {
    pub ride: String,
    pub trip: String,
}

/// A driver was told about a new Trip while still on another.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DriverDoubleBooked {
    pub driver: String,
    pub held_trip: String,
    pub new_trip: String,
}

#[derive(Debug, Clone, Default)]
pub struct Observations {
    pub drivers: Vec<DriverTold>,
    pub riders: Vec<RiderTold>,
    pub double_booked: Vec<DriverDoubleBooked>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Report {
    pub trips_in_postgres: i64,
    pub violations: usize,
    /// The first few violations, described.
    pub examples: Vec<String>,
}

impl Report {
    pub fn passed(&self) -> bool {
        self.violations == 0
    }
}

#[derive(Default)]
struct Findings {
    count: usize,
    examples: Vec<String>,
}

impl Findings {
    fn add(&mut self, description: String) {
        self.count += 1;
        if self.examples.len() < MAX_LISTED {
            self.examples.push(description);
        }
    }
}

pub async fn check(pool: &PgPool, observed: &Observations) -> Result<Report, sqlx::Error> {
    check_all(std::slice::from_ref(pool), observed).await
}

/// Checks every Shard's Postgres, in Shard order, and the invariant across them: no driver
/// holds two active Trips anywhere, and each Reservation's two halves agree.
pub async fn check_all(pools: &[PgPool], observed: &Observations) -> Result<Report, sqlx::Error> {
    let mut findings = Findings::default();
    let label = |shard: usize| if pools.len() > 1 { format!("Shard {shard}: ") } else { String::new() };

    // Within each Shard.
    for (shard, pool) in pools.iter().enumerate() {
        let at = label(shard);
        for row in sqlx::query!(
            r#"SELECT ride_request_id AS "ride!", count(*) AS "trips!" FROM trips
               WHERE status <> 'cancelled' GROUP BY ride_request_id HAVING count(*) > 1"#
        )
        .fetch_all(pool)
        .await?
        {
            findings.add(format!("{at}Ride Request {} has {} Trips", row.ride, row.trips));
        }
        for row in sqlx::query!(
            r#"SELECT t.id AS "trip!" FROM trips t JOIN offers o ON o.id = t.offer_id
               WHERE o.status <> 'accepted' OR o.driver_id <> t.driver_id OR o.ride_request_id <> t.ride_request_id"#
        )
        .fetch_all(pool)
        .await?
        {
            findings.add(format!("{at}Trip {} doesn't match an accepted offer", row.trip));
        }
        for row in sqlx::query!(
            r#"SELECT r.id AS "ride!", r.status AS "status!", count(t.id) AS "trips!" FROM ride_requests r
               LEFT JOIN trips t ON t.ride_request_id = r.id AND t.status <> 'cancelled'
               GROUP BY r.id, r.status
               HAVING (r.status = 'assigned') <> (count(t.id) > 0)"#
        )
        .fetch_all(pool)
        .await?
        {
            findings.add(format!("{at}Ride Request {} is {} with {} Trips", row.ride, row.status, row.trips));
        }
    }

    // Across Shards: every Trip, by id, with its Shard.
    let mut trips: HashMap<String, Trip> = HashMap::new();
    let mut active_by_driver: HashMap<String, Vec<String>> = HashMap::new();
    for (shard, pool) in pools.iter().enumerate() {
        for row in sqlx::query!("SELECT id, driver_id, ride_request_id, status FROM trips WHERE status <> 'cancelled'")
            .fetch_all(pool)
            .await?
        {
            let active = matches!(row.status.as_str(), "assigned" | "arrived" | "started");
            if active {
                active_by_driver.entry(row.driver_id.clone()).or_default().push(format!("{}{}", label(shard), row.id));
            }
            trips.insert(row.id, Trip { shard, driver: row.driver_id, ride: row.ride_request_id });
        }
    }
    let mut doubled: Vec<_> = active_by_driver.into_iter().filter(|(_, held)| held.len() > 1).collect();
    doubled.sort();
    for (driver, held) in doubled {
        findings.add(format!("driver {driver} has {} active Trips: {}", held.len(), held.join(", ")));
    }

    // Reservations: the driver's Shard's record against the rider's Shard's decision.
    let mut held: HashMap<String, (usize, String, String)> = HashMap::new();
    for (shard, pool) in pools.iter().enumerate() {
        for row in sqlx::query!("SELECT id, driver_id, peer_shard, status, trip_id FROM reservations").fetch_all(pool).await? {
            let peer = row.peer_shard as usize;
            if matches!(row.status.as_str(), "confirmed" | "released") {
                match row.trip_id.as_ref().and_then(|trip| trips.get(trip)) {
                    Some(trip) if trip.shard == peer && trip.driver == row.driver_id => {}
                    _ => findings.add(format!(
                        "{}Reservation {} is {} but Shard {peer} has no matching Trip {:?}",
                        label(shard),
                        row.id,
                        row.status,
                        row.trip_id
                    )),
                }
            }
            held.insert(row.id, (shard, row.status, row.driver_id));
        }
    }
    for (shard, pool) in pools.iter().enumerate() {
        for row in sqlx::query!("SELECT id, peer_shard, status, trip_id FROM peer_reservations WHERE status = 'confirmed'")
            .fetch_all(pool)
            .await?
        {
            let at = label(shard);
            match held.get(&row.id) {
                // Still reserved: the driver's Shard hadn't heard the answer when the run ended.
                Some((peer, status, _)) if *peer == row.peer_shard as usize && matches!(status.as_str(), "confirmed" | "released" | "reserved") => {}
                Some((_, status, driver)) => findings.add(format!(
                    "{at}confirmed Reservation {} for driver {driver}, but the driver's Shard has it {status}",
                    row.id
                )),
                None => findings.add(format!("{at}confirmed Reservation {} that the driver's Shard never made", row.id)),
            }
            if row.trip_id.as_ref().is_none_or(|trip| !trips.contains_key(trip)) {
                findings.add(format!("{at}confirmed Reservation {} has no Trip", row.id));
            }
        }
    }

    for told in &observed.drivers {
        match trips.get(&told.trip) {
            Some(trip) if trip.driver == told.driver && trip.ride == told.ride => {}
            Some(_) => findings.add(format!("driver {} was told Trip {} with the wrong details", told.driver, told.trip)),
            None => findings.add(format!("driver {} was told Trip {}, which has no commit", told.driver, told.trip)),
        }
    }
    let mut told_rides: HashMap<&str, &str> = HashMap::new();
    for told in &observed.riders {
        match trips.get(&told.trip) {
            Some(trip) if trip.ride == told.ride => {}
            _ => findings.add(format!("rider of {} was told Trip {}, which has no matching commit", told.ride, told.trip)),
        }
        if let Some(earlier) = told_rides.insert(&told.ride, &told.trip).filter(|earlier| *earlier != told.trip) {
            findings.add(format!("rider of {} was told two Trips, {} and {}", told.ride, earlier, told.trip));
        }
    }
    for booked in &observed.double_booked {
        findings.add(format!(
            "driver {} was told Trip {} while still on Trip {}",
            booked.driver, booked.new_trip, booked.held_trip
        ));
    }

    Ok(Report { trips_in_postgres: trips.len() as i64, violations: findings.count, examples: findings.examples })
}

struct Trip {
    shard: usize,
    driver: String,
    ride: String,
}

/// Connects, then checks, for callers without their own pool. `url` may list one database
/// per Shard, comma-separated, in Shard order.
pub async fn check_database(url: &str, observed: &Observations) -> Result<Report, sqlx::Error> {
    let mut pools = Vec::new();
    for url in url.split(',').filter(|url| !url.is_empty()) {
        pools.push(PgPool::connect(url).await?);
    }
    let report = check_all(&pools, observed).await;
    for pool in pools {
        pool.close().await;
    }
    report
}
