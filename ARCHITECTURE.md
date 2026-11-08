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

**Reservation crash recovery (revised 2026-11-07):** the peer check decides; expiry only limits when a Confirm can succeed. A Shard holding an unresolved Reservation asks the rider's Shard for its status, at restart and on a retry timer, and never frees the driver on its own. The earlier rule, where expiry freed the driver if the peer check didn't answer in time, could break the invariant: if the Confirm succeeded but its reply was lost and the rider's Shard then stayed unreachable past expiry, the driver would be free on one Shard with a Trip on the other. The race between a late Confirm and a status check is settled in the rider's Shard's Postgres (see Sharding), and tests must cover it.

**Driver ownership:** an idle driver belongs to the Shard for their current location. A driver on a Trip stays with the Shard that owns the Trip until it ends.

**Failure rule:** Postgres enforces the invariant within a Shard, and the Reservation enforces it across Shards.

### Assignment on one Shard (decided 2026-11-03)

- **Accept path:** the dispatcher checks an Accept against memory, marks the offer pending and hands the transaction to a separate task, so matching never waits on Postgres. The result comes back as an event. Concurrent Accepts race in Postgres, and its constraints decide the winner.
- **Lost result:** if the connection fails mid-Accept, the outcome is looked up by offer once Postgres answers again, and only then is the driver told.
- **Write order:** a Ride Request is stored before it is matched, and an offer is stored before it is sent, so every Accept refers to rows that exist.
- **Overlapping offers:** Accept, the schema and the tests support a driver holding several offers and a Ride Request offered to several drivers. The Phase 3 matcher still sends one offer per driver, so results stay comparable with Phase 2. Broadcast in Phase 4 is the first policy to send overlaps.
- **Silent drivers:** with the socket open but no pings, an idle driver leaves the location index after 12s (3 missed pings), and an open offer is withdrawn after 8s (2 missed pings). These are starting values, checked once the simulator produces silent drivers.
- **Shard restart:** active Trips are loaded from Postgres. Open offers are withdrawn, and open Ride Requests are cancelled, because their riders' connections were lost.
- **Invariant checker:** runs after each run. It checks Postgres (no driver with two active Trips, no Ride Request with two Trips) and cross-checks what simulated clients were told, which catches a TripAssigned sent without a commit.

### Matching policy on one Shard (decided 2026-11-04)

The Shard runs one policy per run, set by `MATCHER`, `BATCH_WINDOW_MS` and `WIDENING`. Unset means the Phase 1 baseline, so earlier results stay comparable.

- **Greedy (baseline):** each Ride Request is offered as soon as it is stored, to the nearest idle driver.
- **Auction:** Ride Requests collect for one Batch Window. Each gets its 8 nearest idle drivers as candidates, with straight-line pickup distance as the cost. The solver matches as many rides as it can first, then minimises total pickup distance, to within 1 m per ride. It stops at 50 ms and offers what it has; the stats line counts these deadline hits. Rides left over wait for the next window.
- **Widening:** a Ride Request goes to a Broadcast if its Direct Offer failed (declined, expired, withdrawn or refused), or if its area is short of supply. The Broadcast goes to the 5 nearest available drivers, and the first Accept wins.
- **Supply signal:** idle drivers per unassigned Ride Request, over the pickup's resolution-7 Cell and its 6 neighbours (about 36 km²). An area starts broadcasting below 1.0 and stops above 1.5. It keeps each state for at least 60 s, so it can switch at most once a minute.
- **Overlap:** a driver holding only Broadcasts can be in up to 3 at once, and gets no Direct Offer meanwhile. This is the first policy where drivers hold overlapping offers.
- **Values:** 8 candidates, 50 ms, 5 drivers, 3 Broadcasts, and the 1.0 / 1.5 / 60 s thresholds are starting values, not yet measured. The Batch Window comes from the sweep below.
- **Choosing the Batch Window:** sweep 250 ms, 500 ms and 1 s with the auction in the scarce scenario. Keep only windows with request-to-first-offer p99 of 1.5 s or less, leaving 0.5 s of the 2 s target for everything else. Of those, pick the one with the lowest mean pickup leg (assignment to pickup arrival). If two are within 2%, take the shorter window. **Chosen: 250 ms** (2026-11-07, warm 0.65×, seed 1). All three passed the filter (first-offer p99 0.25, 0.50 and 1.00 s), and mean pickup legs were 52.99, 52.93 and 52.91 s, within 0.2%. A longer window buys no shorter pickups at this load: about 2 Ride Requests arrive per 250 ms citywide, so rides rarely compete for the same drivers within one batch.
- **Experiment scenarios:** abundant supply (supply scale equal to demand scale), and scarce supply (supply at 0.65× the demand scale). The warm 0.65× check (2026-11-07) qualified: idle drivers fell from about 2,600 to 1,750 of 16,247 without running out, at most 4 rides waited, and the mean pickup leg was 53 s, against 28 s at 0.75×. Runs are warm-started (see Experiments). The warm 0.75× check (2026-11-07) reached a steady state but wasn't scarce: about 4,000 of 18,742 drivers stayed idle (21%, against 20% estimated below) and at most 6 rides waited.
- **Why cold runs couldn't be scarce (2026-11-04):** with every driver idle at the start and few Trips finishing within 20 minutes, drivers only get used up. At 0.5× idle drivers fell to about 2,600 but never ran out, batches held about one ride, and every window gave the same 26.7 s mean pickup leg. At 0.35× and 0.25× idle drivers ran out after about 16 and 11 minutes, then the queue grew without limit (880 and 3,476 waiting rides at the end, first-offer p99 76 s and 381 s). These runs passed the invariant checker and are kept, but don't count as scenario results.
- **Why 0.65× for scarce:** the supply table assumes 60% utilisation, so at a supply-to-demand ratio r, trip time alone keeps 0.6 / r of drivers busy. At 0.5× that is 120%, so a warm run would be overloaded in steady state. At 0.75× it is 80%, which the check confirmed leaves too many idle. At 0.65× it is about 92%, plus pickup time, which should leave a small idle margin with local shortages.
- **PLAN's verify example:** P4's example (r1: d1 1 min, d2 4 min; r2: d1 3 min, d2 15 min) can't come from straight-line distances, because no positions satisfy the triangle inequality for it. It is checked against the solver with a cost table instead. Dispatcher tests use a swap that positions can produce.

### Sharding (decided 2026-11-07)

- **Shard map:** resolution-7 Cells (about 5 km²) grouped into N contiguous regions of roughly equal demand, built from September demand by `build_shard_map`. It halves the map again and again across its wider side, at the weighted median, so N is a power of two. Resolution 6 was chosen first but is too coarse: NYC has only 43 such Cells and one in Midtown holds about 10% of demand, giving 58/42 at 2 Shards and 47.5/10.8/21.7/20.1 at 4. At resolution 7 (217 Cells) the splits are 52/48 and 26/26/24/24. The maps are in `deploy/shard-maps/`. Each version is a numbered list of Cell → Shard rows plus each Shard's addresses, stored in every Shard's Postgres. A Cell missing from the map belongs to the Shard of its nearest mapped Cell. Correctness work and fault tests use 2 Shards; the single vs multi-Shard experiment uses 1, 2 and 4.
- **Routing by redirect:** clients know every Shard's address but not the map. A Shard that gets a connection, Ride Request or ping from a Cell it doesn't own replies with a redirect to the owner and the map version. The client reconnects there, and the old Shard drops the driver from its index. A redirect to a Shard with an older map version is ignored by that Shard, which answers with its own redirect only once it has the newer map.
- **Boundary search (revised 2026-11-08):** every 250 ms tick, each Shard asks each neighbour for its idle drivers in the neighbour's Cells within 2 rings (about 5 km) of its own, and keeps the answer as a second index. Remote drivers join the same greedy, auction or Broadcast pass as local ones. This replaced a query per Ride Request, which would have held each boundary ride for a round trip and made the synchronous matcher wait on the network; the cost is a snapshot up to one tick old, which is safe because the driver's Shard refuses a relayed offer to a driver no longer free.
- **Cross-shard offers:** the rider's Shard B stores the offer in its own Postgres and asks the driver's Shard A to deliver it. A delivers only to a driver who is online, free and holding no other offer, and keeps it in memory only. Declines and A's withdrawals (silence, redirect, expiry) go back to B; B's expiry goes to A. B doesn't run silence checks on a peer's driver, since it gets no pings from them.
- **Cross-shard Accept:** the Accept arrives on A.
  1. A reserves the driver in its Postgres, with an expiry 5 s ahead. A driver with an open or confirmed Reservation can't be reserved or assigned again on A.
  2. A asks B to confirm. In one transaction, B checks the Ride Request is open and that the expiry hasn't passed, then creates the Trip and records the Reservation as confirmed, or records it as aborted or expired.
  3. A records the outcome and tells the driver. On confirm, the confirmed Reservation becomes a lease, and the driver is redirected to B, pinned there until the Trip ends.
- **Settling races:** B keys Reservations by id. A status check for an id B has never seen records it as aborted before answering, so a Confirm still in flight is rejected. Confirm and status check go through the same row, so whichever commits first wins.
- **Lease release:** when the Trip ends or is cancelled, B tells A to release the lease. If that call is lost, A asks B for the Trip's status when the driver next connects to A, or on a retry timer.
- **Trip ownership:** the Trip, Ride Request and Offer rows stay on B, the rider's Shard. Each Shard's unique indexes hold locally, and the lease keeps A from assigning a driver who has a Trip on B.
- **Ordering:** the rider's Shard records a confirmed Trip in memory before it replies, because the driver connects to it as soon as their Shard hears the answer, and must find their Trip there. A leased driver who reconnects to their own Shard while the peer still has the Trip is sent back to the peer.
- **Values:** 5 s Reservation expiry, kept after the fault campaign (2026-11-08): peer-call delays up to 4.5 s caused no expiries, and delays up to 9 s caused expiries that both Shards recorded alike. With a 2 s call timeout, a Confirm and one retry fit inside it. Boundary snapshot over 2 res-7 rings, every 250 ms tick. At 1× load, 1, 2 and 4 Shards gave the same waits within the spread over seeds, so the number of Shards is left for the Phase 8 capacity runs (`experiments/sharding.md`).
- **Fault hooks:** `PEER_FAULTS` drops requests or replies and delays peer calls; `CRASH_AT` aborts the process at a Reservation step (after reserving, after confirming, before recording the answer, before releasing a lease). Both are off unless set.
- **Invariant checker:** reads every Shard's Postgres, and checks the invariant across them plus that every confirmed Reservation matches exactly one Trip.

## Data model

The schema is in `migrations/`. Partial unique indexes on `trips` enforce the invariant: one active Trip per driver, and one Trip per Ride Request unless cancelled.

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

Each run records its seed, load, Staleness budget and VM limits. A result counts only if the assignment invariant held: the simulator runs the invariant checker against the Shard's Postgres after each run, and records `invariant_checked` and `invariant_passed`.

- **Experiment database:** `deploy/baseline.yml` runs Postgres with trust auth and no published port, so no secret is needed. Its data is wiped before and after each run.

- **Run mode:** all runs use the wall clock on the normal runtime. Demand and driver decisions are seeded, so policies see the same inputs. Timing still varies, so runs are compared statistically over repeats, and a failing seed may not fail again.
- **Warm start (decided 2026-11-05, used from Phase 4):** the simulator also reads the 2 hours of demand before the run starts. Ride Requests from then that are still in progress at the start (request time plus recorded trip time, pickup not counted) are scaled and seeded like run demand. Each goes to an online driver: one in its pickup zone if any is left, otherwise any. That driver connects only when the trip ends, at its dropoff, or at the exit point for trips leaving the city. Trips beyond the online drivers are dropped and counted. The Shard just sees drivers arriving later: no Trip or Accept state is created, so the assignment invariant is untouched. The run record adds the counts of warm-started and dropped Trips, and the plan fingerprint covers them.
- **Staleness budget:** p99 under 5s with 4s pings. If load is too much, shift to p99 under 8s with 4s pings. Each result states which applied.
- **100k feasibility:** a strict 30-minute run with the 2s p99 met, no pings dropped and no invariant violations, generator included. Each attempt also reports the knee, the load where latency starts to bend.

## Known risks

- **Synthetic echo:** with live-speed ETA and feedback-controlled surge, simulated drivers partly reflect the model that was given to them. Those results show control behaviour and don't prove real-world accuracy.
- **Synthesised hot spots:** Cell-level hot spots come from the pickup-point method, so Cell-level results inherit its assumptions.
- **Postgres cost:** a commit on each Accept and one Postgres per Shard reduce the load the VM can reach.
- **Connection cost:** 100k WebSockets use memory and file descriptors inside the VM.

## Open items

- Docker VM: 14 CPUs and about 7.7 GiB of memory as currently configured, on a 24 GiB machine. Enough for a 30-minute 100k run with Postgres, at about 3.5 GiB peak. Shard memory grows with Trips in progress, about 9 KB each, and should level off once completions match new Trips; a run longer than 30 minutes would confirm it (`experiments/assignment.md`).
- Values: offer timeouts. Reservation expiry is set (see Sharding); number of Shards waits for Phase 8 load. Batch Window is set at 250 ms (see Matching policy on one Shard). Silent-driver timeouts have starting values (see Assignment on one Shard); Phase 3 runs at 5% assumed silence didn't press them, so they stay until a harsher silence model does.
- Supply threshold for widening: the 1.0 / 1.5 / 60 s starting values showed no flapping in Phase 4 (48–71 switches per 20-minute run, at most 22 areas broadcasting), but switches are only counted in total, not per area. Widening made scarce waits about 1 s worse at that load, so tuning it waits for a denser-demand test such as the Phase 6 spikes (`experiments/matching.md`).
- Quote validity window and controller tuning.
- First-offer max of about 3.8 s with Postgres, against p99 of 3–4 ms; to explain before the Phase 8 strict test (`experiments/assignment.md`). Likely the startup surge of every idle driver connecting at once: 5–7.6 s in Phase 4 runs that start with about 11,000 idle drivers, and 0.68 s with about 4,900. A separate 20 s max appears on scarce seed 1 under every policy, so it most likely comes from that seed's demand. Neither is confirmed by timestamps yet (`experiments/matching.md`).
- Connection failures: the ones seen so far are Docker DNS lookups failing at about 10 s, during the startup surge ("Temporary failure in name resolution"). This is a test-setup artefact; retries recover the drivers.
- Hot-Shard compute: a 50x Midtown spike at 1x load drained the hot Shard's drivers but barely raised its CPU (11% of a core, against 10% for the others), so a spike that overloads one Shard is untested. Phase 8 load should show it (`experiments/sharding.md`).
- Reusing `payment-ledger` for the final phase; reading it needs explicit permission.

## Decision Log

### Results must not depend on a single time mode

Status: superseded by "Seeded inputs instead of a simulated clock"

Latency numbers need real time, but policy comparisons need reproducible runs, so the system supports both a wall clock and a seeded simulated clock. This forces time, randomness and I/O behind interfaces from the start, which is hard to retrofit.

### Accept owner is a two-step Reservation, not a global store

A global assignment store would be the easiest way to keep assignments correct, but it would be a bottleneck and would undercut geo-partitioning. Reservations keep ownership in the Shards, at the cost of a crash window that expiry and peer checks must close. Revised 2026-11-07: the peer check alone closes it. A Shard that can't reach its peer keeps the reserved driver out of matching rather than guess, because a guess can break the invariant. That costs availability for the few drivers caught mid-Reservation while a peer is down.

### Accept commits outside the dispatcher task

Waiting for each commit inside the dispatcher would be simpler, but it would block all matching for each commit and serialize Accepts in memory, so the Postgres constraints would rarely be what stops a Double Assignment. Running the transaction in a separate task lets Accepts really race in Postgres, at the cost of a pending state and handling for lost results.

### Seeded inputs instead of a simulated clock

Experiments seed demand and driver decisions but run on the normal runtime, with no simulated clock or fake I/O. This avoids building and maintaining a simulation layer. The cost is that runs are only statistically comparable, and timing-dependent bugs such as Reservation races may not reproduce from a seed.

### h3o instead of C H3 bindings

C bindings were chosen for reference behaviour, but Phase 0 found them poorly maintained (`h3ron-h3-sys` last updated 2023), while `h3o` is active. `h3o` also removes the C build step and FFI on the hot path. Its results are spot-checked against reference H3 output. Note that the `h3` crate is an unrelated HTTP/3 library.

### Use NYC trip data without a published licence

No licence or explicit grant was found: NYC.gov reserves rights in its content, while the Open Data Law says data is "open by default". The project uses the data, publishes only aggregate results with attribution and never redistributes raw files. A small residual risk remains.
