//! Assignment state in Postgres: Ride Requests, offers and Trips for one Shard.
//!
//! Postgres decides every Accept. The partial unique indexes on `trips` are the backstop
//! for the assignment invariant, so a bug in the Shard's memory can't create a Double
//! Assignment, only a refused Accept.

use geo::Position;
use sqlx::postgres::{PgPool, PgPoolOptions};

pub use sqlx::Error;

mod reservation;
mod shard_map;
pub use reservation::{Held, HeldReservation, LeasedTrip, PeerDecision};
pub use shard_map::MapSaved;

pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");

const ONE_ACTIVE_TRIP_PER_DRIVER: &str = "trips_one_active_per_driver";
const ONE_TRIP_PER_RIDE_REQUEST: &str = "trips_one_per_ride_request";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OfferKind {
    Direct,
    Broadcast,
}

impl OfferKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Broadcast => "broadcast",
        }
    }
}

/// Why an open offer was closed without an Accept.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OfferClosed {
    Declined,
    Expired,
    Withdrawn,
}

impl OfferClosed {
    fn as_str(self) -> &'static str {
        match self {
            Self::Declined => "declined",
            Self::Expired => "expired",
            Self::Withdrawn => "withdrawn",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TripStage {
    Arrived,
    Started,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AcceptOutcome {
    Assigned { trip: String, ride: String },
    Refused(Refusal),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// Already closed, expired, or not this driver's.
    OfferNotOpen,
    /// Another Accept assigned the Ride Request first.
    RideTaken,
    /// The driver already holds an active Trip.
    DriverBusy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveTrip {
    pub trip: String,
    pub driver: String,
    pub ride: String,
}

#[derive(Debug, Clone)]
pub struct Store {
    pool: PgPool,
}

impl Store {
    /// Connects and applies migrations.
    pub async fn connect(url: &str, max_connections: u32) -> Result<Self, Error> {
        let pool = PgPoolOptions::new().max_connections(max_connections).connect(url).await?;
        MIGRATOR.run(&pool).await?;
        Ok(Self { pool })
    }

    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Run once at Shard start, before serving. Offers and waiting Ride Requests from before
    /// the restart are closed, because their connections are gone; active Trips carry on.
    pub async fn recover(&self) -> Result<Vec<ActiveTrip>, Error> {
        let mut tx = self.pool.begin().await?;
        sqlx::query!("UPDATE offers SET status = 'withdrawn', closed_at = now() WHERE status = 'open'")
            .execute(&mut *tx)
            .await?;
        sqlx::query!("UPDATE ride_requests SET status = 'cancelled' WHERE status = 'open'")
            .execute(&mut *tx)
            .await?;
        let trips = sqlx::query_as!(
            ActiveTrip,
            r#"SELECT id AS "trip!", driver_id AS "driver!", ride_request_id AS "ride!"
               FROM trips WHERE status IN ('assigned', 'arrived', 'started')"#
        )
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(trips)
    }

    /// Safe to retry: storing the same Ride Request twice is a no-op.
    pub async fn store_ride_request(&self, ride: &str, quote: &str, pickup: Position) -> Result<(), Error> {
        sqlx::query!(
            "INSERT INTO ride_requests (id, quote_id, pickup_lat, pickup_lng, status)
             VALUES ($1, $2, $3, $4, 'open') ON CONFLICT (id) DO NOTHING",
            ride,
            quote,
            pickup.lat,
            pickup.lng
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Safe to retry. `expires_at_ms` is Unix time.
    pub async fn store_offer(
        &self,
        offer: &str,
        ride: &str,
        driver: &str,
        kind: OfferKind,
        expires_at_ms: i64,
    ) -> Result<(), Error> {
        sqlx::query!(
            "INSERT INTO offers (id, ride_request_id, driver_id, kind, status, expires_at)
             VALUES ($1, $2, $3, $4, 'open', to_timestamp($5::bigint / 1000.0)) ON CONFLICT (id) DO NOTHING",
            offer,
            ride,
            driver,
            kind.as_str(),
            expires_at_ms
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Closes offers that are still open. Safe to retry.
    pub async fn close_offers(&self, offers: &[String], reason: OfferClosed) -> Result<(), Error> {
        sqlx::query!(
            "UPDATE offers SET status = $2, closed_at = now() WHERE id = ANY($1) AND status = 'open'",
            offers,
            reason.as_str()
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// The only commit point. An error means the outcome is unknown: call
    /// [`Store::accept_outcome`] before telling anyone.
    pub async fn accept(&self, offer: &str, driver: &str, trip: &str) -> Result<AcceptOutcome, Error> {
        let mut tx = self.pool.begin().await?;
        // A driver held for a peer Shard's Ride Request can't take a Trip here meanwhile.
        sqlx::query(reservation::LOCK_DRIVER).bind(driver).execute(&mut *tx).await?;
        let reserved = sqlx::query_scalar!(
            r#"SELECT EXISTS (SELECT 1 FROM reservations WHERE driver_id = $1 AND status IN ('reserved', 'confirmed')) AS "reserved!""#,
            driver
        )
        .fetch_one(&mut *tx)
        .await?;
        if reserved {
            tx.rollback().await?;
            self.close_offers(&[offer.to_owned()], OfferClosed::Withdrawn).await?;
            return Ok(AcceptOutcome::Refused(Refusal::DriverBusy));
        }
        let Some(ride) = sqlx::query_scalar!(
            "UPDATE offers SET status = 'accepted', closed_at = now()
             WHERE id = $1 AND driver_id = $2 AND status = 'open' AND expires_at > now()
             RETURNING ride_request_id",
            offer,
            driver
        )
        .fetch_optional(&mut *tx)
        .await?
        else {
            return Ok(AcceptOutcome::Refused(Refusal::OfferNotOpen));
        };

        let claimed = sqlx::query!(
            "UPDATE ride_requests SET status = 'assigned' WHERE id = $1 AND status = 'open'",
            ride
        )
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if claimed == 0 {
            tx.rollback().await?;
            self.close_offers(&[offer.to_owned()], OfferClosed::Withdrawn).await?;
            return Ok(AcceptOutcome::Refused(Refusal::RideTaken));
        }

        let inserted = sqlx::query!(
            "INSERT INTO trips (id, ride_request_id, driver_id, offer_id, status) VALUES ($1, $2, $3, $4, 'assigned')",
            trip,
            ride,
            driver,
            offer
        )
        .execute(&mut *tx)
        .await;
        if let Err(error) = inserted {
            let refusal = match error.as_database_error().and_then(|db| db.constraint()) {
                Some(ONE_ACTIVE_TRIP_PER_DRIVER) => Refusal::DriverBusy,
                Some(ONE_TRIP_PER_RIDE_REQUEST) => Refusal::RideTaken,
                _ => return Err(error),
            };
            tx.rollback().await?;
            self.close_offers(&[offer.to_owned()], OfferClosed::Withdrawn).await?;
            return Ok(AcceptOutcome::Refused(refusal));
        }

        // Offers locked by a concurrent Accept are skipped: that Accept will be refused and
        // close its own offer. Waiting for them instead would deadlock.
        sqlx::query!(
            "UPDATE offers SET status = 'withdrawn', closed_at = now()
             WHERE id IN (SELECT id FROM offers
                          WHERE status = 'open' AND (ride_request_id = $1 OR driver_id = $2)
                          FOR UPDATE SKIP LOCKED)",
            ride,
            driver
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(AcceptOutcome::Assigned { trip: trip.to_owned(), ride })
    }

    /// Settles an Accept whose result was lost. Closing the offer first waits for any
    /// transaction still holding it, so the answer is final.
    pub async fn accept_outcome(&self, offer: &str) -> Result<AcceptOutcome, Error> {
        let mut tx = self.pool.begin().await?;
        sqlx::query!(
            "UPDATE offers SET status = 'withdrawn', closed_at = now() WHERE id = $1 AND status = 'open'",
            offer
        )
        .execute(&mut *tx)
        .await?;
        let trip = sqlx::query!(
            "SELECT id, ride_request_id FROM trips WHERE offer_id = $1 AND status <> 'cancelled'",
            offer
        )
        .fetch_optional(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(match trip {
            Some(trip) => AcceptOutcome::Assigned { trip: trip.id, ride: trip.ride_request_id },
            None => AcceptOutcome::Refused(Refusal::OfferNotOpen),
        })
    }

    /// Records a trip event from the Trip's driver. Safe to retry; stages never move backwards.
    pub async fn trip_event(&self, trip: &str, driver: &str, stage: TripStage) -> Result<(), Error> {
        match stage {
            TripStage::Arrived => sqlx::query!(
                "UPDATE trips SET status = 'arrived', arrived_at = now()
                 WHERE id = $1 AND driver_id = $2 AND status = 'assigned'",
                trip,
                driver
            )
            .execute(&self.pool)
            .await?,
            TripStage::Started => sqlx::query!(
                "UPDATE trips SET status = 'started', started_at = now()
                 WHERE id = $1 AND driver_id = $2 AND status IN ('assigned', 'arrived')",
                trip,
                driver
            )
            .execute(&self.pool)
            .await?,
            TripStage::Completed => sqlx::query!(
                "UPDATE trips SET status = 'completed', completed_at = now()
                 WHERE id = $1 AND driver_id = $2 AND status IN ('assigned', 'arrived', 'started')",
                trip,
                driver
            )
            .execute(&self.pool)
            .await?,
        };
        Ok(())
    }
}
