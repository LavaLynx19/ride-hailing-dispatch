//! Cross-shard Accepts, as Postgres sees them. The driver's Shard (A) holds `reservations`;
//! the rider's Shard (B) decides in `peer_reservations`. See ARCHITECTURE.md → Sharding.

use crate::{Error, Store};

/// What the rider's Shard decided for a Reservation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerDecision {
    Confirmed { trip: String },
    Aborted,
    Expired,
    /// Confirmed, and the Trip has since ended or been cancelled.
    Released { trip: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Held {
    /// Waiting for the peer's answer.
    Reserved,
    /// The lease: the peer has the Trip.
    Confirmed,
}

/// A Reservation the driver's Shard still holds, for recovery after a restart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeldReservation {
    pub id: String,
    pub driver: String,
    pub offer: String,
    pub ride: String,
    pub peer: u16,
    pub held: Held,
}

/// A Trip on this Shard whose driver belongs to a peer, which holds the lease.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeasedTrip {
    pub trip: String,
    pub peer: u16,
    pub reservation: String,
}

/// Serialises everything that can give one driver a Trip on this Shard: a local Accept and a
/// Reservation. The lock ends with the transaction.
pub(crate) const LOCK_DRIVER: &str = "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))";

impl Store {
    // --- On the driver's Shard -----------------------------------------------------------

    /// Holds the driver for a peer's Ride Request. False if the driver already has an active
    /// Trip here or another open Reservation.
    pub async fn reserve(
        &self,
        id: &str,
        driver: &str,
        offer: &str,
        ride: &str,
        peer: u16,
        expires_at_ms: i64,
    ) -> Result<bool, Error> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(LOCK_DRIVER).bind(driver).execute(&mut *tx).await?;
        let busy = sqlx::query_scalar!(
            r#"SELECT EXISTS (SELECT 1 FROM trips WHERE driver_id = $1 AND status IN ('assigned', 'arrived', 'started'))
                   OR EXISTS (SELECT 1 FROM reservations WHERE driver_id = $1 AND id <> $2 AND status IN ('reserved', 'confirmed'))
               AS "busy!""#,
            driver,
            id
        )
        .fetch_one(&mut *tx)
        .await?;
        if busy {
            return Ok(false);
        }
        sqlx::query!(
            "INSERT INTO reservations (id, driver_id, offer_id, ride_request_id, peer_shard, expires_at, status)
             VALUES ($1, $2, $3, $4, $5, to_timestamp($6::bigint / 1000.0), 'reserved') ON CONFLICT (id) DO NOTHING",
            id,
            driver,
            offer,
            ride,
            peer as i16,
            expires_at_ms
        )
        .execute(&mut *tx)
        .await?;
        let mine = sqlx::query_scalar!(r#"SELECT status AS "status!" FROM reservations WHERE id = $1"#, id)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(mine == "reserved")
    }

    /// Records the peer's answer. Safe to retry, and a later answer never undoes a release.
    pub async fn settle_reservation(&self, id: &str, decision: &PeerDecision) -> Result<(), Error> {
        let (status, trip) = match decision {
            PeerDecision::Confirmed { trip } => ("confirmed", Some(trip.as_str())),
            PeerDecision::Released { trip } => ("released", Some(trip.as_str())),
            PeerDecision::Aborted => ("aborted", None),
            PeerDecision::Expired => ("expired", None),
        };
        sqlx::query!(
            "UPDATE reservations SET status = $2, trip_id = coalesce($3, trip_id), settled_at = now()
             WHERE id = $1 AND (status = 'reserved' OR (status = 'confirmed' AND $2 = 'released'))",
            id,
            status,
            trip
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// The peer says the Trip ended: the driver is free here again.
    pub async fn release_reservation(&self, id: &str) -> Result<(), Error> {
        sqlx::query!(
            "UPDATE reservations SET status = 'released', settled_at = now() WHERE id = $1 AND status = 'confirmed'",
            id
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn held_reservations(&self) -> Result<Vec<HeldReservation>, Error> {
        let rows = sqlx::query!(
            "SELECT id, driver_id, offer_id, ride_request_id, peer_shard, status FROM reservations
             WHERE status IN ('reserved', 'confirmed') ORDER BY created_at"
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| HeldReservation {
                id: row.id,
                driver: row.driver_id,
                offer: row.offer_id,
                ride: row.ride_request_id,
                peer: row.peer_shard as u16,
                held: if row.status == "confirmed" { Held::Confirmed } else { Held::Reserved },
            })
            .collect())
    }

    // --- On the rider's Shard ------------------------------------------------------------

    /// Assigns the Ride Request to the peer's reserved driver, if the offer is still open, the
    /// ride unassigned and the Reservation unexpired. Decides once per Reservation: a retry, or
    /// a Confirm after a status check, gets the first answer back.
    pub async fn confirm(
        &self,
        id: &str,
        peer: u16,
        offer: &str,
        driver: &str,
        trip: &str,
        expires_at_ms: i64,
    ) -> Result<PeerDecision, Error> {
        let mut tx = self.pool.begin().await?;
        let claimed = sqlx::query!(
            "INSERT INTO peer_reservations (id, peer_shard, driver_id, offer_id, status)
             VALUES ($1, $2, $3, $4, 'aborted') ON CONFLICT (id) DO NOTHING",
            id,
            peer as i16,
            driver,
            offer
        )
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if claimed == 0 {
            tx.rollback().await?;
            return self.peer_decision(id).await.map(|decision| decision.unwrap_or(PeerDecision::Aborted));
        }
        let expired = sqlx::query_scalar!(r#"SELECT to_timestamp($1::bigint / 1000.0) <= now() AS "expired!""#, expires_at_ms)
            .fetch_one(&mut *tx)
            .await?;
        if expired {
            sqlx::query!("UPDATE peer_reservations SET status = 'expired' WHERE id = $1", id).execute(&mut *tx).await?;
            tx.commit().await?;
            return Ok(PeerDecision::Expired);
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
            tx.commit().await?;
            return Ok(PeerDecision::Aborted);
        };
        let withdraw_offer = "UPDATE offers SET status = 'withdrawn' WHERE id = $1";
        let ride_open = sqlx::query!("UPDATE ride_requests SET status = 'assigned' WHERE id = $1 AND status = 'open'", ride)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        if ride_open == 0 {
            sqlx::query(withdraw_offer).bind(offer).execute(&mut *tx).await?;
            tx.commit().await?;
            return Ok(PeerDecision::Aborted);
        }
        // A constraint failure aborts only the savepoint, so the refusal can still be recorded.
        sqlx::query("SAVEPOINT trip_insert").execute(&mut *tx).await?;
        let inserted = sqlx::query!(
            "INSERT INTO trips (id, ride_request_id, driver_id, offer_id, status) VALUES ($1, $2, $3, $4, 'assigned')",
            trip,
            ride,
            driver,
            offer
        )
        .execute(&mut *tx)
        .await;
        match inserted {
            Ok(_) => {
                sqlx::query("RELEASE SAVEPOINT trip_insert").execute(&mut *tx).await?;
            }
            Err(error) if error.as_database_error().and_then(|db| db.constraint()).is_some() => {
                sqlx::query("ROLLBACK TO SAVEPOINT trip_insert").execute(&mut *tx).await?;
                sqlx::query!("UPDATE ride_requests SET status = 'open' WHERE id = $1", ride).execute(&mut *tx).await?;
                sqlx::query(withdraw_offer).bind(offer).execute(&mut *tx).await?;
                tx.commit().await?;
                return Ok(PeerDecision::Aborted);
            }
            Err(error) => return Err(error),
        }
        sqlx::query!("UPDATE peer_reservations SET status = 'confirmed', trip_id = $2 WHERE id = $1", id, trip)
            .execute(&mut *tx)
            .await?;
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
        Ok(PeerDecision::Confirmed { trip: trip.to_owned() })
    }

    /// The decision for a Reservation, recording it as aborted if none was made yet. Waits for
    /// a Confirm still in progress, so the answer is final.
    pub async fn peer_status(&self, id: &str, peer: u16) -> Result<PeerDecision, Error> {
        sqlx::query!(
            "INSERT INTO peer_reservations (id, peer_shard, status) VALUES ($1, $2, 'aborted') ON CONFLICT (id) DO NOTHING",
            id,
            peer as i16
        )
        .execute(&self.pool)
        .await?;
        Ok(self.peer_decision(id).await?.unwrap_or(PeerDecision::Aborted))
    }

    async fn peer_decision(&self, id: &str) -> Result<Option<PeerDecision>, Error> {
        let row = sqlx::query!(
            r#"SELECT p.status, p.trip_id, t.status AS "trip_status?" FROM peer_reservations p
               LEFT JOIN trips t ON t.id = p.trip_id WHERE p.id = $1"#,
            id
        )
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|row| match (row.status.as_str(), row.trip_id, row.trip_status.as_deref()) {
            ("confirmed", Some(trip), Some("completed" | "cancelled")) => PeerDecision::Released { trip },
            ("confirmed", Some(trip), _) => PeerDecision::Confirmed { trip },
            ("expired", _, _) => PeerDecision::Expired,
            _ => PeerDecision::Aborted,
        }))
    }

    /// Active Trips whose drivers are leased from peers, for recovery after a restart.
    pub async fn leased_trips(&self) -> Result<Vec<LeasedTrip>, Error> {
        let rows = sqlx::query!(
            r#"SELECT p.trip_id AS "trip!", p.peer_shard, p.id FROM peer_reservations p
               JOIN trips t ON t.id = p.trip_id
               WHERE p.status = 'confirmed' AND t.status IN ('assigned', 'arrived', 'started')"#
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|row| LeasedTrip { trip: row.trip, peer: row.peer_shard as u16, reservation: row.id }).collect())
    }
}
