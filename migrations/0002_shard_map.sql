-- Versions of the Cell → Shard map. Every Shard stores the same versions and serves the
-- highest. A version is never changed once stored.

CREATE TABLE shard_maps (
    version integer PRIMARY KEY CHECK (version > 0),
    stored_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE shard_map_shards (
    version integer NOT NULL REFERENCES shard_maps (version),
    shard smallint NOT NULL CHECK (shard >= 0),
    peer text NOT NULL,
    drivers text NOT NULL,
    riders text NOT NULL,
    PRIMARY KEY (version, shard)
);

CREATE TABLE shard_map_cells (
    version integer NOT NULL REFERENCES shard_maps (version),
    cell bigint NOT NULL,
    shard smallint NOT NULL,
    PRIMARY KEY (version, cell),
    FOREIGN KEY (version, shard) REFERENCES shard_map_shards (version, shard)
);
