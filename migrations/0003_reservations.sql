-- Cross-shard Accepts. See ARCHITECTURE.md → Sharding.

-- On the driver's Shard: the driver is held for a Ride Request on a peer Shard. While a
-- Reservation is reserved or confirmed, the driver can't be reserved again or take a local
-- Trip. A confirmed one is the lease, held until the peer's Trip ends.
CREATE TABLE reservations (
    id text PRIMARY KEY,
    driver_id text NOT NULL,
    offer_id text NOT NULL,
    ride_request_id text NOT NULL,
    peer_shard smallint NOT NULL,
    expires_at timestamptz NOT NULL,
    status text NOT NULL CHECK (status IN ('reserved', 'confirmed', 'aborted', 'expired', 'released')),
    trip_id text,
    created_at timestamptz NOT NULL DEFAULT now(),
    settled_at timestamptz
);

CREATE UNIQUE INDEX reservations_one_open_per_driver ON reservations (driver_id)
    WHERE status IN ('reserved', 'confirmed');

-- On the rider's Shard: what it decided for each Reservation id. The first writer of a row
-- wins, whether a Confirm or a status check, so a late Confirm can't reverse an answer.
CREATE TABLE peer_reservations (
    id text PRIMARY KEY,
    peer_shard smallint NOT NULL,
    driver_id text,
    offer_id text,
    status text NOT NULL CHECK (status IN ('confirmed', 'aborted', 'expired')),
    trip_id text REFERENCES trips (id),
    decided_at timestamptz NOT NULL DEFAULT now()
);
