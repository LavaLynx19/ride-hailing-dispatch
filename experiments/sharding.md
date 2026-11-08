# Phase 5 geo-sharding

Tests whether splitting the city into Shards keeps the invariant, and what it costs and gains against one Shard. Run on 2026-11-07 and 2026-11-08.

**The invariant held everywhere.**
- 33 fault-campaign rounds on two Shards, with crashes at every Reservation step, dropped and late calls, partitions and kills: 0 violations.
- A shard-map change mid-run: 0 violations.
- 15 experiment runs: 0 violations each, checked across every Shard's Postgres.

**At this load, sharding cost nothing measurable and gained nothing measurable.**
- 1, 2 and 4 Shards matched within the spread over seeds on every latency and wait measure.
- Under a 50× Midtown spike, 4 Shards and 1 Shard gave the same waits. The hot Shard drew 568–582 drivers from its neighbours through cross-Shard Reservations.
- The whole system peaked under 1 CPU of 14 in every run, so neither setup was short of compute. What limited the spike was drivers near Midtown, which sharding can't add.

## Fault campaign (5.5)

Each round ran two real Shard processes, each with its own Postgres, on a map split along a line of longitude. Every rider sat on Shard 1 and every driver on Shard 0, so every match crossed the boundary. Riders were spaced 25 s apart over the round. Faults came from the hooks in ARCHITECTURE.md → Sharding → Fault hooks. Runner: `experiments/fault_campaign.sh`; per-round records in `runs/p5-fault-campaign.txt`.

10 mixes × seeds 1–3. Every round passed the invariant checker with 0 violations.

| Fault mix | Injected crashes | Trips | Cross-Shard Trips | Unresolved at end |
|---|---|---|---|---|
| Crash after reserving (p 0.3) | 14–17 | 25–33 | 25–33 | 0 |
| Crash after confirming | 17–23 | 52–56 | 52–56 | 0 |
| Crash before recording the answer | 23–27 | 58–75 | 57–74 | 0–1 |
| Crash before releasing a lease | 7–19 | 40–52 | 40–52 | 0 |
| Dropped requests and replies (20% each) | 0 | 43–47 | 42–47 | 0 |
| Delays up to 4.5 s | 0 | 29–32 | 29–32 | 0–1 |
| Delays up to 9 s, past the 5 s expiry | 0 | 11–12 | 11–12 | 0–2 |
| Partitions, 2 s in every 6 | 0 | 64–66 | 64–66 | 0 |
| SIGKILLs, 5 per round | 0 | 57–61 | 57–61 | 0 |
| Everything at once | 4–5, plus 5 kills | 34–38 | 34–37 | 0 |

- **Expiry:** delays up to 4.5 s caused no expiries. Delays up to 9 s caused 6–10 per round, and both Shards recorded the same outcome every time.
- **Dropped calls:** 21–25 Reservations aborted per round, each recorded the same on both sides.
- **Unresolved:** at most 2 Reservations per round were still waiting on their peer when the round stopped. None was guessed; the driver stayed out of matching.
- **Confirm vs status-check race:** a store test runs it 200 times in both orders (`crates/assignment/tests/reservation.rs`). The two sides always agreed.
- **Shard-map change:** `moving_the_boundary_mid_run_keeps_the_invariant` moves the boundary twice while Trips cross it. Both Shards end on map version 3, with 60 of 60 rides assigned and completed.

**Bugs the tests caught:**
- The rider's Shard replied to a Confirm before its memory knew the Trip. The driver, redirected there at once, found no Trip and was sent back. Memory is now updated first, with a backstop for a leased driver who reconnects to their own Shard.
- A lease check recorded "released" for an aborted Reservation. It now records the peer's actual decision.
- A retried Reserve refused itself because its own row counted as "busy".

## Scenarios (5.6 and 5.7)

Each run covers 17:55–18:15 on Monday 2026-09-14, with the first 5 minutes as warmup, warm-started as in Phase 4, at abundant supply (0.981 for both scales). Policy: auction with a 250 ms Batch Window, widening off. Seeds 1–3.

**Maps** (`deploy/shard-maps/`), resolution-7 Cells split by September demand:

| Shards | Demand share per Shard |
|---|---|
| 1 | 100% |
| 2 | 52 / 48 |
| 4 | 26 / 26 / 24 / 24 |

**Spike:** demand in taxi zone 161 (Midtown Center) is multiplied by 50 from minute 5 to minute 10, after warmup. On 4 Shards, zone 161 is on Shard 1, which has 11 of the 217 Cells.

Runner: `experiments/run_sharded.sh` with `deploy/sharded.yml`. Each Shard has its own Postgres.

## Results: 1, 2 and 4 Shards (5.6)

Ranges are min–max over the three seeds. Wait is request to pickup arrival, for measured requests that reached pickup before the run ended. Pickup leg is assignment to pickup arrival. Cross-Shard counts cover the whole run, warmup included.

| Measure | 1 Shard | 2 Shards | 4 Shards |
|---|---|---|---|
| Invariant passed, violations | 3/3, 0 | 3/3, 0 | 3/3, 0 |
| Trips assigned | 11,353–11,388 | 11,365–11,381 | 11,365–11,383 |
| Requests reaching pickup | 8,377–8,409 | 8,389–8,410 | 8,384–8,416 |
| Never assigned | 75–93 | 79–82 | 72–82 |
| First offer p99 (s) | 0.25 | 0.25 | 0.25–0.26 |
| Assignment p50 (s) | 8.90–8.99 | 8.81–8.95 | 8.90–8.99 |
| Assignment p95 (s) | 14.90–14.94 | 14.90–14.97 | 14.91–14.95 |
| Mean pickup leg (s) | 15.06–15.13 | 15.09–15.19 | 15.07–15.18 |
| Wait p50 (s) | 18.90–19.31 | 18.77–19.35 | 18.69–19.37 |
| Wait p95 (s) | 62.25–63.06 | 61.98–63.24 | 62.26–63.43 |
| Total wait (hours) | 56.1–56.5 | 56.3–56.5 | 56.3–56.7 |
| Cross-Shard Trips | 0 | 32–47 | 49–58 |
| Reservations made | 0 | 40–63 | 66–82 |
| Redirects | 0 | 16,217–16,371 | 24,305–24,492 |
| Peer call failures | 0 | 1 | 6 |

**Paired by seed**, total wait in hours: 1 Shard 56.4 / 56.1 / 56.5; 2 Shards 56.5 / 56.3 / 56.4; 4 Shards 56.7 / 56.3 / 56.6. The 4-Shard runs are 0.1–0.3 hours higher on every seed, about 0.4%, which is inside the spread between seeds.

- **Cross-Shard Trips are rare:** 0.3–0.5% of Trips. Most boundary matches still find a local driver first. Between 8 and 24 Reservations per run were aborted, mostly because the ride had already been taken.
- **Redirects** come from drivers crossing a boundary while driving. Each costs a reconnect, and no driver or rider session failed because of one.
- **Peer call failures** all came in the first minute, while peers were still starting.
- **Connection failures:** one 4-Shard run had 30 driver connection failures, all Docker DNS lookups failing during the startup surge (the known test-setup artefact). Retries recovered them.

## Results: 50× spike, 4 Shards vs 1 (5.7)

Ranges are min–max over the three seeds, measured as in 5.6.

| Measure | 1 Shard | 4 Shards |
|---|---|---|
| Invariant passed, violations | 3/3, 0 | 3/3, 0 |
| Trips assigned | 13,611–13,649 | 13,625–13,648 |
| Requests reaching pickup | 10,389–10,458 | 10,402–10,489 |
| Never assigned | 75–93 | 76–80 |
| First offer p99 (s) | 0.25 | 0.26 |
| Assignment p50 (s) | 9.16–9.23 | 9.19–9.37 |
| Assignment p95 (s) | 16.96–17.52 | 17.33–17.59 |
| Mean pickup leg (s) | 86.4–87.6 | 86.6–87.4 |
| Pickup leg p95 (s) | 409–416 | 413–420 |
| Wait p50 (s) | 36.9–37.0 | 36.6–37.0 |
| Wait p95 (s) | 420–427 | 423–430 |
| Total wait (hours) | 278.5–281.3 | 280.0–280.5 |
| Declines | 1,253–1,314 | 1,222–1,269 |
| Cross-Shard Trips | 0 | 642–658 |
| Redirects | 0 | 26,628–26,725 |

**Paired by seed**, total wait in hours: 1 Shard 281.3 / 280.1 / 278.5; 4 Shards 280.0 / 280.5 / 280.3. Neither setup is ahead on every seed. Assignment p95 (1 Shard 17.52 / 16.96 / 17.16 s; 4 Shards 17.33 / 17.59 / 17.33 s) has no consistent direction either.

**On the hot Shard** (4 Shards, seed 1, sampled every minute):
- Its idle drivers fell from about 2,600 to about 350 within the 5 minutes of the spike, and stayed near that until the run ended.
- Open rides peaked at 240 in the last spike minute, against about 25 before it. The 1-Shard run peaked at 295 citywide.
- Waiting rides (no driver found yet) peaked at 12, and fell back within a minute.
- Shard 1 relayed about 810 offers to its neighbours and took 568–582 Trips with their drivers over the three seeds, about 12% of its Trips. Almost all came in the last two spike minutes, after its own idle drivers ran low.
- Its CPU peaked at 11% of one core, against 10% for the other three Shards. The extra matching work is small next to handling every driver's pings.

**What limited the spike was drivers, not the Shard.** About 2,000 extra requests landed in a few Cells in 5 minutes. Drivers came from further away, so the p95 pickup leg rose from about 51 s to about 410 s on both setups. One Shard sees every driver in the city at once; four Shards see the same drivers through the boundary snapshot and Reservations, and the results show no difference.

## What not to read into this

- **Load:** about 1× real load, far below the 100k target. Every setup peaked under 1 CPU of 14, so these runs show sharding is correct and cheap, not that it scales. Capacity is for Phase 8.
- **One machine:** all Shards share one VM, so peer calls cross a Docker network, not a real one. Real network latency would make Reservations slower, but the 4.5 s campaign mix shows that's tolerated.
- **Hot-Shard compute:** a spike big enough to overload one Shard's CPU wasn't tested. At this load the spike was 50× in one zone, and the Shard barely noticed.
- **Spike shape:** a step up and down in one zone, not a real event's ramp. Drivers don't move toward the spike on their own.

## Known limits

- Riders connected to a killed Shard don't reconnect to the restarted one. The fault campaign counted those rides as lost, not as violations.
- The boundary snapshot is up to one 250 ms tick old. The driver's Shard refuses a relayed offer to a driver no longer free, so a stale snapshot costs an offer, not correctness.
- A Shard that can't reach its peer keeps a reserved driver out of matching until the peer answers. That was at most 2 drivers per round in the campaign.

## Assumptions

- Driver behaviour and warm start as in `experiments/matching.md` → Assumptions.
- Every client knows every Shard's address; the map itself is learned through redirects.
- Cross-Shard values: 2 res-7 rings for the boundary snapshot, 5 s Reservation expiry, 2 s per peer call, 1 s to connect.

## Environment

- **VM:** 14 CPUs and 7.7 GiB, shared by all containers on one M4 Pro laptop.
- **Simulator:** limited to 2 CPUs.
- **Peak CPU, whole system, sampled every minute:** 0.55–0.63 cores on 1 Shard, 0.58–0.66 on 2, 0.73–0.82 on 4. Each extra Shard and Postgres adds a small fixed cost.
- **Peak memory, whole system:** 1.0 GiB on 1 Shard, 1.1 GiB on 2, 1.3 GiB on 4. One Shard peaked at 330–350 MiB; each of 4 Shards at 130–150 MiB.
- **Sleep:** the laptop slept from 03:34 on 2026-11-08 during the seed-3 spike runs. Both were discarded and rerun.
- **Code:** based on commit `0c9e378` with uncommitted Phase 5 changes, so records are marked `-dirty`.

## Data attribution

Demand comes from NYC Taxi & Limousine Commission HVFHV trip records. Calibration uses NYC Open Data taxi records. Map data is © OpenStreetMap contributors, under the ODbL. Only aggregate results are published here.
