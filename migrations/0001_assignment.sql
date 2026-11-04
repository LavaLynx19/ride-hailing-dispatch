-- Assignment state for one Shard. The partial unique indexes on trips enforce the
-- assignment invariant: a Ride Request ends with at most one Trip, and a driver never
-- holds two active Trips.

CREATE TABLE ride_requests (
    id text PRIMARY KEY,
    quote_id text NOT NULL,
    pickup_lat double precision NOT NULL,
    pickup_lng double precision NOT NULL,
    status text NOT NULL CHECK (status IN ('open', 'assigned', 'cancelled')),
    requested_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE offers (
    id text PRIMARY KEY,
    ride_request_id text NOT NULL REFERENCES ride_requests (id),
    driver_id text NOT NULL,
    kind text NOT NULL CHECK (kind IN ('direct', 'broadcast')),
    status text NOT NULL CHECK (status IN ('open', 'accepted', 'declined', 'expired', 'withdrawn')),
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL,
    closed_at timestamptz
);

CREATE INDEX offers_open_by_ride ON offers (ride_request_id) WHERE status = 'open';
CREATE INDEX offers_open_by_driver ON offers (driver_id) WHERE status = 'open';

CREATE TABLE trips (
    id text PRIMARY KEY,
    ride_request_id text NOT NULL REFERENCES ride_requests (id),
    driver_id text NOT NULL,
    offer_id text NOT NULL UNIQUE REFERENCES offers (id),
    status text NOT NULL CHECK (status IN ('assigned', 'arrived', 'started', 'completed', 'cancelled')),
    assigned_at timestamptz NOT NULL DEFAULT now(),
    arrived_at timestamptz,
    started_at timestamptz,
    completed_at timestamptz
);

CREATE UNIQUE INDEX trips_one_active_per_driver ON trips (driver_id)
    WHERE status IN ('assigned', 'arrived', 'started');
CREATE UNIQUE INDEX trips_one_per_ride_request ON trips (ride_request_id)
    WHERE status <> 'cancelled';
