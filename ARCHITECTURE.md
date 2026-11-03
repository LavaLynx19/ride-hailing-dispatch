# Architecture

Requirements are in [README.md](README.md), terms in [CONTEXT.md](CONTEXT.md).

## System topology

```
 Simulator (demand replay + synthetic drivers)
        │ WebSockets
        ▼
 ┌─ Shard A ───────────────┐   gRPC    ┌─ Shard B ───────────────┐
 │ Location index (memory) │◄─────────►│ Same components         │
 │ Matcher, ETA, Surge     │           │                         │
 │ Ping log (disk)         │           │                         │
 │ Postgres (Trips, etc.)  │           │                         │
 └─────────────────────────┘           └─────────────────────────┘
        All containers run inside one Linux VM on macOS
```

- **Shards:** one container per Shard, plus one Postgres per Shard.
- **Load generator:** runs inside the VM with its own CPU limit, so latency isn't distorted by the macOS network boundary.
- **Known limit:** the VM's CPU and memory cap all results, and every report must state them.

## Decisions and alternatives

| Concern | Chosen | Alternatives considered | Why chosen |
|---|---|---|---|
| Language | Rust | Go (GC tail latency), Java (memory footprint) | Predictable latency and small footprint leave more of the laptop for load |
| Shard isolation | Containers per Shard | OS processes (harder fault injection), one process with swappable transport (two setups) | Easy partitions and resource limits |
| Live location | In-memory index per Shard, with a ping log behind it | Redis-style store (extra hop, hides the index), broker first (more Staleness, heavy) | Fastest path, and still supports replay |
| Ping retention | Rolling window, compressed | Runs only (no history for Reconciliation), keep everything (about 100 GB a day at 100k) | Predictable disk use, and supports disputes |
| Trip and Reservation state | Postgres per Shard | Shard-local WAL (you own storage correctness), Raft per Shard (triples processes, repeats KV store project) | Constraints enforce one active Trip per driver within a Shard |
| Geo grid | H3 | S2 (less uniform neighbours), geohash (shape varies by latitude) | Uniform neighbour distance suits radius search and surge blending |
| Shard map | Explicit, versioned Cell → Shard table | Hash of parent (nearly every match is cross-shard), parent ranges (can't split downtown) | Handles uneven density, with boundaries drawn deliberately |
| Shard RPC | gRPC | Custom TCP (you own framing and retries), NATS (message system in the commit path) | Typed contracts and mature Rust support |
| Client connection | WebSockets | gRPC streams (weak browser support), UDP pings (blurs the no-drops test) | One connection for pings and offers, and suits later apps |
| Batch solver | Auction algorithm | Hungarian (run time threatens 2s p99), greedy (weak answer on whether batching helps) | Near-optimal, and can stop early at a deadline |
| Simple ETA | Live speeds from pings | Fixed speed, learned zone-hour speeds | Reacts to conditions using the system's own data |
| Surge | Feedback controller per Cell | Smoothed ratio (slow on spikes), stepped hysteresis (jumps at Cell edges) | Directly addresses oscillation |
| Demand data | NYC TLC HVFHV records | Chicago TNC (times rounded to 15 minutes), Porto GPS (about 442 taxis) | Largest and most recent option with usable timing, pending the checks below |
| Pickup points | OSM-weighted points, calibrated from older NYC GPS taxi data | Uniform within zone (erases hot spots) | Records only give zones; this uses both street/venue evidence and observed patterns |

### Dependencies

Versions and maintenance status are checked before each is added.

| Need | Chosen | Alternatives considered | Why chosen |
|---|---|---|---|
| Async runtime | Tokio | None separately | Required by `tonic` |
| gRPC | `tonic` | See Shard RPC above | Main Rust gRPC option |
| Postgres client | `sqlx` | `tokio-postgres` (no query checks), `diesel` (more abstraction) | SQL checked at compile time; builds need a database or cached metadata |
| H3 | `h3o` | C bindings (poorly maintained), own subset (big detour) | See Decision Log: "h3o instead of C H3 bindings" |
| WebSockets | `tokio-tungstenite` | `axum` (more overhead) | Bare socket server for the ping path |
| Dataset preprocessing | Python + DuckDB, offline only | Rust `parquet`/`arrow` (slower to iterate), GeoPandas (heavier) | Quick to change; handles remote Parquet well; Python stays out of the services |
| Ping log compression | `zstd` | LZ4 (larger files), gzip (slower), none (far more disk) | Usually the smallest files, so more history fits; adds C bindings to the build |
| Message encoding | Protobuf | JSON (larger, slower to parse) | Compact, and shares schemas with gRPC; harder to inspect while debugging |
| Stream splitting | `futures-util` | Hand-written socket polling (more code on the hot path) | Splits each WebSocket into separate send and receive halves |
| Protobuf runtime | `prost` | None separately | Used by `tonic`'s generated code |
| Seeded randomness | Own SplitMix64 | `rand` (a dependency, and its streams can change between versions) | A few lines of stdlib-only code keep seeded runs stable |

### Dataset facts (checked 2026-10-31)

- **HVFHV demand:** Parquet, available to September 2026, about 510 MB for that month. `request_datetime` has one-second values and locations are taxi zone IDs. September 2026 averages about 8.0 requests/s after filtering, with a busiest minute of 1033 (about 17/s).
- **Round-time spikes:** the highest per-second counts (135 at most) fall exactly on times such as 05:00:00 and 05:30:00. They look like scheduled rides rather than live requests, though the data doesn't label them. They stay in the demand file unchanged, as deliberate test scenarios for batched matching and surge.
- **Dropped rows:** pickups in unknown zones and impossible trips, including about 250k rows where pickup precedes the request. Trips with an unknown or outside-NYC dropoff are kept, so dropoff points need separate handling.
- **Older GPS calibration:** TLC's current 2015 and 2016 Parquet files have zone IDs only. Exact coordinates come from NYC Open Data, datasets `2yzn-sicd` (2015) and `uacg-pexx` (2016). The sample is ten minutes from every hour of one 2016 week, about 451k usable rows, because the portal serves only about 36 KB/s.
- **Pickup points:** 65,133 weighted cells of about 100 m across all 263 zones. Each zone blends GPS and OSM shares, trusting GPS more as its pickup count grows. The venue weight of 5 and the 100-pickup half-trust point are assumptions, not measurements. OSM data requires attribution under its licence (ODbL).
- **Driver supply:** derived per zone and hour of week from September trips, assuming 60% utilisation. Citywide online drivers range from 3,165 to 35,083, median 22,695, at supply scale 1. The data only records served trips, so it can't show real shortages; the separate supply and demand scales create them. Reaching 100k drivers needs a supply scale of about 2.9 at peak.
- **Driver model:** offer acceptance falls with pickup distance and response delay is uniform from 2s to 15s, both assumed because the data records no declines. Drivers don't yet respond to price, which Phase 6's surge experiments will need.

### Phase 1 baseline (checked 2026-10-31)

- **Matcher:** one Shard, in memory, nearest idle driver first, one open offer per driver. This is deliberately simpler than the overlapping offers in the final design.
- **Assumed values:** offer timeout 20s and pickup speed 25 km/h. Neither is measured yet.
- **Setup:** `deploy/baseline.yml` runs one Shard and the load generator, without Postgres.
- **WebSocket read buffer:** 4 KiB per connection on both sides. The library default of 128 KiB ran the VM out of memory with 25,250 drivers; each side fell from about 3.3 GiB to about 350 MiB early in a peak run.

### Location index (checked 2026-11-01)

- **Search:** walks H3 rings outward from the query point, and stops once no unsearched Cell could hold anything closer. It matches a full scan on sampled queries, including moved and removed drivers.
- **Contents:** only idle drivers, matching the current matcher. Drivers on offers or Trips are added back when they become idle.
- **Resolution:** 9, kept after the Phase 2 ramp, where first offer was 2 ms at p99 at 100k drivers (`experiments/load_ramp.md`). The table below is from synthetic clustered points:

| Idle drivers | Res 8 | Res 9 | Res 10 | Full scan |
|---|---:|---:|---:|---:|
| 100,000 | 119 µs | 23 µs | 6 µs | 762 µs |
| 25,000 | 29 µs | 6 µs | 2 µs | 190 µs |
| 1,000 | 2 µs | 2 µs | 6 µs | 8 µs |
| 20 | 5 µs | 12 µs | 19 µs | 0.2 µs |

Resolution 9 stays fast at every load measured. Resolution 10 is faster when dense, but costs more when supply is thin and splits drivers across more Cells.

### Ping log

- **Format:** segment files of records, each with a length and checksum. Closed segments are compressed with zstd. A torn or damaged record ends reading of that segment, and anything after it is lost.
- **Writing:** a background thread writes pings and syncs every 1s, so matching never waits on the disk. A crash can lose about the last second of pings.
- **When the writer falls behind:** a queue holds 200,000 pings, about 8s at 100k drivers. Beyond that, pings are dropped and counted, and any drop fails the strict 100k test.
- **Retention:** 24 hours with a 20 GB cap, and the cap wins. Only closed segments are deleted, oldest first, so open segments can briefly push the total over the cap.
- **Rotation:** at 64 MiB or 5 minutes.
- **Not yet measured:** compression ratio and throughput under real load, which task 2.4 covers.

## Matching and assignment

1. A Ride Request gets a Quote, with its Surge Multiplier locked for a validity window.
2. The auction solver picks a Direct Offer each Batch Window. Boundary searches query neighbouring Shards over gRPC.
3. On timeout or decline, or based on local supply, the request widens to a Broadcast.
4. Offers can overlap. Accept is atomic and is the only commit point.
5. A cross-shard Accept uses a Reservation: reserve the driver on their Shard, then confirm the rider on theirs.

**Reservation crash recovery:** peer check is the fast path and expiry is the backstop. A restarted Shard asks its peer whether to complete or abort. If that doesn't resolve it in time, the Reservation expires and any late confirm is rejected. Whichever resolves first must win, and tests must cover the race between them.

**Driver ownership:** an idle driver belongs to the Shard for their current location. A driver on a Trip stays with the Shard that owns the Trip until it ends.

**Failure rule:** Postgres enforces the invariant within a Shard, and the Reservation enforces it across Shards.

## Data model

Conceptual only; schemas are set in PLAN or CODE.

| Entity | Lives in | Notes |
|---|---|---|
| Driver location | Memory, plus ping log | Latest position by Cell; history kept in the log |
| Ride Request | Postgres | Pickup, Quote, status |
| Offer | Postgres | Direct Offer or Broadcast; many can be open per driver |
| Reservation | Postgres | Driver's Shard, with expiry |
| Trip | Postgres | At most one active per driver (unique constraint) |
| Shard map | Versioned table | Cell → Shard |

## API contract (outline)

- **Driver WebSocket:** Location Pings in; offers and offer-gone notices out; Accept and decline in; buffered trip events in on reconnect.
- **Rider WebSocket:** Quote and Ride Request (with pickup) in; offer progress, Trip assignment and Trip status out.
- **Shard gRPC:** boundary driver search, reserve, confirm, abort, Reservation status.

## Experiments

Each run records its seed, load, Staleness budget and VM limits. A result counts only if the assignment invariant held.

- **Run mode:** all runs use the wall clock on the normal runtime. Demand and driver decisions are seeded, so policies see the same inputs. Timing still varies, so runs are compared statistically over repeats, and a failing seed may not fail again.
- **Staleness budget:** p99 under 5s with 4s pings. If load is too much, shift to p99 under 8s with 4s pings. Each result states which applied.
- **100k feasibility:** a strict 30-minute run with the 2s p99 met, no pings dropped and no invariant violations, generator included. Each attempt also reports the knee, the load where latency starts to bend.

## Known risks

- **Synthetic echo:** with live-speed ETA and feedback-controlled surge, simulated drivers partly reflect the model that was given to them. Those results show control behaviour and don't prove real-world accuracy.
- **Synthesised hot spots:** Cell-level hot spots come from the pickup-point method, so Cell-level results inherit its assumptions.
- **Postgres cost:** a commit on each Accept and one Postgres per Shard reduce the load the VM can reach.
- **Connection cost:** 100k WebSockets use memory and file descriptors inside the VM.

## Open items

- Docker VM: 14 CPUs and about 7.7 GiB of memory as currently configured, on a 24 GiB machine. Enough for a 30-minute 100k run without Postgres, at about 3.3 GiB peak, but memory keeps growing during runs (`experiments/load_ramp.md`). Revisit once Postgres is added.
- Values: silent-driver, offer and Reservation timeouts; Batch Window (must fit 2s p99); number of Shards.
- Supply threshold for widening, and how to stop it flapping.
- Quote validity window and controller tuning.
- Hot-Shard behaviour under a 50x spike.
- Reusing `payment-ledger` for the final phase; reading it needs explicit permission.

## Decision Log

### Results must not depend on a single time mode

Status: superseded by "Seeded inputs instead of a simulated clock"

Latency numbers need real time, but policy comparisons need reproducible runs, so the system supports both a wall clock and a seeded simulated clock. This forces time, randomness and I/O behind interfaces from the start, which is hard to retrofit.

### Accept owner is a two-step Reservation, not a global store

A global assignment store would be the easiest way to keep assignments correct, but it would be a bottleneck and would undercut geo-partitioning. Reservations keep ownership in the Shards, at the cost of a crash window that expiry and peer checks must close.

### Seeded inputs instead of a simulated clock

Experiments seed demand and driver decisions but run on the normal runtime, with no simulated clock or fake I/O. This avoids building and maintaining a simulation layer. The cost is that runs are only statistically comparable, and timing-dependent bugs such as Reservation races may not reproduce from a seed.

### h3o instead of C H3 bindings

C bindings were chosen for reference behaviour, but Phase 0 found them poorly maintained (`h3ron-h3-sys` last updated 2023), while `h3o` is active. `h3o` also removes the C build step and FFI on the hot path. Its results are spot-checked against reference H3 output. Note that the `h3` crate is an unrelated HTTP/3 library.

### Use NYC trip data without a published licence

No licence or explicit grant was found: NYC.gov reserves rights in its content, while the Open Data Law says data is "open by default". The project uses the data, publishes only aggregate results with attribution and never redistributes raw files. A small residual risk remains.
