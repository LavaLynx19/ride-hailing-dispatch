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
    let mut findings = Findings::default();

    for row in sqlx::query!(
        r#"SELECT driver_id AS "driver!", count(*) AS "trips!" FROM trips
           WHERE status IN ('assigned', 'arrived', 'started') GROUP BY driver_id HAVING count(*) > 1"#
    )
    .fetch_all(pool)
    .await?
    {
        findings.add(format!("driver {} has {} active Trips", row.driver, row.trips));
    }
    for row in sqlx::query!(
        r#"SELECT ride_request_id AS "ride!", count(*) AS "trips!" FROM trips
           WHERE status <> 'cancelled' GROUP BY ride_request_id HAVING count(*) > 1"#
    )
    .fetch_all(pool)
    .await?
    {
        findings.add(format!("Ride Request {} has {} Trips", row.ride, row.trips));
    }
    for row in sqlx::query!(
        r#"SELECT t.id AS "trip!" FROM trips t JOIN offers o ON o.id = t.offer_id
           WHERE o.status <> 'accepted' OR o.driver_id <> t.driver_id OR o.ride_request_id <> t.ride_request_id"#
    )
    .fetch_all(pool)
    .await?
    {
        findings.add(format!("Trip {} doesn't match an accepted offer", row.trip));
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
        findings.add(format!("Ride Request {} is {} with {} Trips", row.ride, row.status, row.trips));
    }

    let trips: HashMap<String, (String, String)> = sqlx::query!(
        "SELECT id, driver_id, ride_request_id FROM trips WHERE status <> 'cancelled'"
    )
    .fetch_all(pool)
    .await?
    .into_iter()
    .map(|row| (row.id, (row.driver_id, row.ride_request_id)))
    .collect();

    for told in &observed.drivers {
        match trips.get(&told.trip) {
            Some((driver, ride)) if *driver == told.driver && *ride == told.ride => {}
            Some(_) => findings.add(format!("driver {} was told Trip {} with the wrong details", told.driver, told.trip)),
            None => findings.add(format!("driver {} was told Trip {}, which has no commit", told.driver, told.trip)),
        }
    }
    let mut told_rides: HashMap<&str, &str> = HashMap::new();
    for told in &observed.riders {
        match trips.get(&told.trip) {
            Some((_, ride)) if *ride == told.ride => {}
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

/// Connects, then checks, for callers without their own pool.
pub async fn check_database(url: &str, observed: &Observations) -> Result<Report, sqlx::Error> {
    let pool = PgPool::connect(url).await?;
    let report = check(&pool, observed).await;
    pool.close().await;
    report
}
