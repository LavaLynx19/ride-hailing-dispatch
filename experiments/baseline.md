# Phase 1 baseline

Replays one weekday's real demand against a deliberately naive matcher, so later phases have something to beat. Run on 2026-10-31.

**These results don't pass the experiment gate.** The invariant checker arrives in Phase 3, so every record has `invariant_checked: false`. They are a reference point, not counted results.

## Results

All windows are on Monday 2026-09-14, with seed 1 and supply and demand scales of 1. Each runs for 30 minutes, and the first 15 are warmup, excluded from the measurements.

| Measure | Spike A | Spike B | Peak |
|---|---|---|---|
| Window (NYC time) | 05:15–05:45 | 05:15–05:45 | 17:45–18:15 |
| Drivers connected | 10,088 | 10,088 | 25,474 |
| Ride Requests, whole run | 7,033 | 7,033 | 17,247 |
| Location Pings per second | 2,521 | 2,521 | 6,080 |
| Requests measured | 3,718 | 3,718 | 8,804 |
| Request to first offer, p50 | 4 ms | 4 ms | 7 ms |
| Request to first offer, p99 | 7 ms | 7 ms | 11 ms |
| Request to assignment, p50 | 8.78 s | 8.80 s | 8.71 s |
| Request to assignment, p99 | 23.46 s | 23.47 s | 22.59 s |
| Request to pickup arrival, p50 | 24.8 s | 24.8 s | 14.7 s |
| Request to pickup arrival, p99 | 133.9 s | 133.9 s | 102.1 s |
| Measured requests never assigned | 33 | 32 | 86 |
| Connection failures | 0 | 0 | 0 |
| Late memory, simulator / Shard | 198 / 167 MiB | 197 / 167 MiB | 460 / 391 MiB |

Records are in [runs/](runs/). Resource use was sampled every 5 minutes, so memory figures are approximate.

## What this shows

- **Repeatability:** spike runs A and B have the same plan fingerprint, so demand and each driver's plan match. Timing isn't seeded, so what happens during the run differs slightly: 7,435 and 7,438 offers, 402 and 405 declines.
- **First offer:** p99 is well under the 2s target. This says little about the final design, because the naive matcher has no Batch Window, Postgres or location index.
- **Assignment time:** this mostly reflects the assumed driver response of 2–15s and the 20s offer timeout, not system speed.

## What not to read into this

- **Pickup times look too short.** A p50 arrival of 14.7s, less about 8.7s to assign, leaves about 6s of travel at peak, roughly 40 m at the assumed speed. Drivers start at sampled pickup points, supply is estimated generously, and there is no road network. Use these times to compare matchers on identical inputs, not as real wait times.
- **Unassigned counts may be overstated.** Assignment was not checked against requests still open when the run ended, so some of these may simply be cut off by the run ending.
- **Few trips complete.** Most Trips outlast the 30-minute run, so completion counts aren't a throughput measure.
- **Staleness isn't measured.** Records state the budget of p99 under 5s, but measuring against it is task 2.3.

## Assumptions

- **Driver supply:** derived from September trips, assuming 60% utilisation.
- **Driver behaviour:** acceptance is 95% at zero distance and halves at 5 km, with response delay uniform from 2s to 15s. None of this is measured, and drivers don't respond to price.
- **Pickup speed:** 25 km/h.
- **Matcher:** one Shard, in memory, nearest idle driver first, one open offer per driver.
- **Pickup points:** blended from OSM and 2016 GPS data, with assumed weights.

## Environment

- **VM:** 14 CPUs and 7.7 GiB, shared by all containers on one M4 Pro laptop. The simulator has a limit of 2 CPUs, which is a placeholder. It was not the bottleneck, using about 0.3 of a core at peak.
- **Code:** based on commit `c0cf0fd` with uncommitted changes, so records are marked `-dirty` and can't be rebuilt from that commit alone.
- **Builds:** the peak run includes a fix for supply changes on the clock hour. The spike runs were built just before it. Their window sits inside one clock hour, so the fix doesn't affect them.

## Problems found while running

- **Out of memory:** the first peak attempt was killed by the VM about 12 minutes in. The WebSocket library's default 128 KiB read buffer per connection was the cause, and reducing it to 4 KiB fixed it.
- **Supply timing:** supply hours were counted from the start of the run, so the 18:00 change never happened in the 17:45 window. This is fixed and covered by a test, and those peak results were discarded.
- **Unexplained:** one spike run before the buffer fix recorded 38 connection failures. They didn't recur in either rerun, and the cause is unknown.
- **Memory growth:** both containers grew by about 10–20 MiB every 5 minutes during the peak run. The cause isn't confirmed, and longer runs should watch it.

## Data attribution

Demand comes from NYC Taxi & Limousine Commission HVFHV trip records. Calibration uses NYC Open Data taxi records. Map data is © OpenStreetMap contributors, under the ODbL. Only aggregate results are published here.
