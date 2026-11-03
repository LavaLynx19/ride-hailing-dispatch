# Phase 2 load ramp

Runs the location index and ping log at four load levels, to find the knee: the load where latency starts to bend. Run on 2026-11-01.

**No knee was found up to 100k drivers.** Latency stayed flat at every level, so the knee, if there is one on this setup, is above the highest load tried.

**These results don't pass the experiment gate.** The invariant checker arrives in Phase 3, so every record has `invariant_checked: false`. This is also not the strict 100k test: there is no Postgres, no Batch Window and only one Shard, so the Phase 8 knee may be much lower.

## Results

All windows are 17:45–18:15 on Monday 2026-09-14, with seed 1 and one run per level, so there is no measure of spread. Supply and demand are scaled together. Each runs for 30 minutes, and the first 15 are warmup, excluded from the measurements.

| Measure | 25k | 50k | 75k | 100k |
|---|---:|---:|---:|---:|
| Supply and demand scale | 0.981 | 1.963 | 2.944 | 3.926 |
| Drivers connected | 24,985 | 50,009 | 74,996 | 100,009 |
| Location Pings, whole run | 10.7M | 21.4M | 32.1M | 42.6M |
| Ride Requests, whole run | 16,912 | 33,831 | 50,747 | 67,646 |
| Requests measured | 8,620 | 17,264 | 25,897 | 34,536 |
| Request to first offer, p99 | 1 ms | 1 ms | 2 ms | 2 ms |
| Request to first offer, max | 27 ms | 18 ms | 19 ms | 163 ms |
| Request to assignment, p50 | 8.68 s | 8.77 s | 8.82 s | 8.79 s |
| Request to assignment, p99 | 22.89 s | 22.13 s | 22.25 s | 22.47 s |
| Staleness at offer, p50 | 2.01 s | 1.98 s | 1.99 s | 1.98 s |
| Staleness at offer, p99 | 3.95 s | 3.96 s | 3.96 s | 3.96 s |
| Staleness at offer, max | 4.00 s | 4.00 s | 4.00 s | 4.74 s |
| Measured requests never assigned | 76 | 150 | 241 | 319 |
| Pings dropped by the ping log | 0 | 0 | 0 | 0 |
| Ping log errors | 0 | 0 | 0 | 0 |
| Ping log on disk | 217 MB | 399 MB | 616 MB | 792 MB |
| Connection failures | 0 | 18 | 0 | 99 |
| Shard CPU, late mean / peak (cores) | 0.20 / 0.24 | 0.36 / 0.41 | 0.44 / 0.59 | 0.60 / 0.90 |
| Simulator CPU, late mean / peak (cores) | 0.32 / 0.41 | 0.58 / 0.66 | 0.75 / 0.83 | 0.83 / 1.98 |
| Peak memory, Shard (MiB) | 409 | 809 | 1,198 | 1,570 |
| Peak memory, simulator (MiB) | 454 | 889 | 1,323 | 1,763 |

Records are in [runs/](runs/). CPU and memory were sampled every minute, so short peaks may be missed.

## What this shows

- **First offer:** p99 is 2 ms at 100k, against the 2s target and 11 ms in the Phase 1 peak baseline at 25k. The max rose to 163 ms at 100k, in one run, so it may be a one-off.
- **Staleness:** p99 is about 4 s at every level, under the 5s budget, leaving about 1 s of margin. It is mostly the 4 s ping interval, so it barely depends on load. A growing backlog in the Shard would have raised it, and it didn't.
- **Assignment time:** this mostly reflects the assumed 2–15 s driver response, so it can't show a knee.
- **Headroom:** the Shard used under one core at every level. The dispatcher is a single task, so its limit is about one core, and the Shard's 0.90 peak at 100k includes socket work on other threads. The dispatcher's own use wasn't measured, so the remaining headroom isn't known.
- **Location index resolution:** 9 stays. First offer at 2 ms p99 at 100k leaves nothing to gain from changing it. The index's own time wasn't measured under replay.
- **VM memory:** 7.7 GiB was enough, with about 3.3 GiB used by the two containers at peak. Postgres isn't included yet, so this needs checking again.

## Ping log retention

- **Disk use:** about 19 bytes per ping, measured across all four runs. The figure includes the open segment, which isn't compressed yet.
- **Projection:** 100k drivers held all day would write about 38 GB, about twice the 20 GB cap. The cap would then win over the 24h limit, keeping about 12.6 hours. Real supply falls outside peak hours, so a real day would write less.
- **Tested:** the 24h and 20 GB limits are enforced in tests. These runs stayed far under both, so neither limit was reached under load.

## What not to read into this

- **100k is a scaled load.** Scaling multiplies drivers and trips on the same geography, about four times real supply. Drivers get denser, so pickup arrival improves with load (p50 14.8 s at 25k, 11.2 s at 100k). That comes from the setup, not the system.
- **Staleness is sampled at offer time.** It only covers idle drivers who received an offer, not all drivers.
- **Unassigned counts may be overstated.** These stay near 0.9% at every level, and some may simply be cut off by the run ending.
- **Few Trips complete.** As in the baseline, most outlast the 30-minute run.

## Assumptions

- **Driver supply:** derived from September trips, assuming 60% utilisation.
- **Driver behaviour:** acceptance is 95% at zero distance and halves at 5 km, with response delay uniform from 2 s to 15 s and a 20 s offer timeout. None of this is measured, and drivers don't respond to price.
- **Pickup speed:** 25 km/h.
- **Matcher:** one Shard, in memory, nearest idle driver first through the location index, one open offer per driver.
- **Location index:** H3 resolution 9.
- **Ping log:** zstd, synced every 1 s, retention 24h with a 20 GB cap.
- **Pickup points:** blended from OSM and 2016 GPS data, with assumed weights.

## Environment

- **VM:** 14 CPUs and 7.7 GiB, shared by all containers on one M4 Pro laptop.
- **Simulator:** limited to 2 CPUs and shares the VM with the Shard. It hit about 1.98 cores in the first minute of the 100k run, while drivers connected, so it was at its limit then. After that it stayed under one core.
- **Ports:** the Shard listens on 4 driver and 4 rider ports, because one client address can only open about 28,000 connections to one server port.
- **Code:** based on commit `eb28b5d` with uncommitted changes, so records are marked `-dirty` and can't be rebuilt from that commit alone.

## Problems found while running

- **Connection failures, cause unknown:** 18 at 50k and 99 at 100k, but 0 at 25k and 75k. The counter doesn't record the reason or the time. No driver or rider was lost: successful driver connections and Ride Requests both match the plan exactly, which points to driver connection attempts that failed and then succeeded on retry. The simulator being at its CPU limit while connecting at 100k may be related, but that doesn't explain 50k. The strict test needs these explained, so the counter should separate the reasons before Phase 8.
- **Memory growth:** at 100k the Shard grew about 13 MiB a minute and the simulator about 9. The Shard's rate increases with load, which fits per-Trip or per-rider state, but the cause isn't confirmed. If it stays linear, the two would use up the VM in about 3 hours, so runs much longer than 30 minutes at this load aren't safe yet.

## Data attribution

Demand comes from NYC Taxi & Limousine Commission HVFHV trip records. Calibration uses NYC Open Data taxi records. Map data is © OpenStreetMap contributors, under the ODbL. Only aggregate results are published here.
