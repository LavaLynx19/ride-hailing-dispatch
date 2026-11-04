# Phase 3 assignment

Adds Postgres-backed Accept, Trips, Silent Driver handling and the invariant checker to the Phase 2 load ramp, then reruns its 25k and 100k levels. Run on 2026-11-03.

**Both runs passed the invariant checker with 0 violations**, over 16,836 and 67,326 Trips. These are the first results that pass the experiment gate.

**Postgres didn't move the percentiles.** First offer p99 stayed at 2–4 ms and assignment p99 at about 22 s. The first-offer max rose to about 3.8 s at both levels, which is unexplained (see Problems found).

## Results

Windows, seed and scaling match the Phase 2 ramp: 17:45–18:15 on Monday 2026-09-14, seed 1, one run per level, so there is no measure of spread. Each runs 30 minutes and the first 15 are warmup. Phase 2 figures are from [load_ramp.md](load_ramp.md).

| Measure | 25k Phase 2 | 25k Phase 3 | 100k Phase 2 | 100k Phase 3 |
|---|---:|---:|---:|---:|
| Invariant checked / passed | no | yes / yes | no | yes / yes |
| Trips in Postgres | – | 16,836 | – | 67,326 |
| Requests measured | 8,620 | 8,620 | 34,536 | 34,536 |
| Request to first offer, p99 | 1 ms | 4 ms | 2 ms | 3 ms |
| Request to first offer, max | 27 ms | 3.73 s | 163 ms | 3.90 s |
| Request to assignment, p50 | 8.68 s | 8.69 s | 8.79 s | 8.81 s |
| Request to assignment, p99 | 22.89 s | 22.91 s | 22.47 s | 22.32 s |
| Staleness at offer, p99 | 3.95 s | 3.96 s | 3.96 s | 3.96 s |
| Staleness at offer, max | 4.00 s | 4.00 s | 4.74 s | 4.22 s |
| Measured requests never assigned | 76 | 76 | 319 | 320 |
| Silent periods | – | 905 | – | 3,526 |
| Offers withdrawn or refused (`offers_gone`) | 0 | 4 | 0 | 208 |
| Connection failures | 0 | 0 | 99 | 0 |
| Postgres failed attempts | – | 0 | – | 0 |
| Shard CPU, late mean / peak (cores) | 0.20 / 0.24 | 0.22 / 0.45 | 0.60 / 0.90 | 0.61 / 0.79 |
| Postgres CPU, late mean / peak (cores) | – | 0.05 / 0.07 | – | 0.11 / 0.38 |
| Peak memory, Shard (MiB) | 409 | 421 | 1,570 | 1,619 |
| Peak memory, Postgres (MiB) | – | 85 | – | 123 |
| Peak memory, simulator (MiB) | 454 | 462 | 1,763 | 1,798 |

Records are in [runs/](runs/) as `phase3-*`, with failure lines in `.stderr.txt` and the Shard's per-minute counts in `.shard-stats.txt`. CPU and memory were sampled every minute.

## Fault tests

These run in `cargo test` against a local Postgres. Each ends with the invariant checker.

| Test | Result |
|---|---|
| Postgres sessions killed every 3 ms during 240 Accepts | 54–106 results lost per run, all settled by lookup; what drivers were told matched Postgres |
| 20 drivers accepting one Ride Request; one driver accepting 20 offers | 1 Trip each time |
| Shard killed with SIGKILL 12 times while 30 drivers accept | 130–144 Trips per run, 6 runs, 0 violations |
| Postgres container restarted 3 times during Accepts (ignored by default) | 160 Trips, 0 violations |
| Double Assignment injected past the constraints | flagged by the checker |

A Shard kill can't be shown to land inside a given Accept. The evidence it does is that a few Accepts sent in each run never became Trips. The session-kill test is the one that reliably hits a commit midway.

## What this shows

- **Accept cost is off the matching path.** Assignment time is unchanged to within 0.2 s, because matching doesn't wait on the commit.
- **Postgres is light at this load.** At 100k it averaged 0.11 cores and peaked at 123 MiB. It is not yet a bottleneck, but this is one Shard with no Batch Window.
- **VM memory:** about 3.5 GiB used at 100k across the Shard, Postgres and simulator, out of 7.7 GiB.
- **Silent Drivers didn't raise unassigned requests.** Never-assigned counts match Phase 2 to within one request at both levels, even with 5% of drivers going silent. Staleness at offer stayed under the 5 s budget, so no offer went to a driver long past their last ping. The 12 s and 8 s values stay as starting values, since this load never pressed them.

## Phase 2 open points

- **Connection failures:** the counter now records driver and rider failures separately, connecting versus mid-session, with reasons and times. None occurred in these runs, including at 100k where Phase 2 saw 99, so their cause is still unknown. The breakdown will name it if they come back.
- **Memory growth, explained:** the Shard holds a rider connection and a Trip for every Trip in progress, and few Trips complete within 30 minutes. At 100k, active Trips grew from 4,141 to 40,815 over the last 28 minutes while Shard memory grew by about 320 MiB, about 9 KB per Trip in progress. At 25k the figure is about 12 KB. Growth should level off once Trip completions match new Trips, which a 30-minute window doesn't reach. A longer run would confirm this.
- **Driver links fall late in the run:** from 99,101 to 91,084 at 100k. These are drivers reaching the end of their shift, not failures: session errors were 0.

## Problems found while running

- **First-offer max of about 3.8 s:** at both levels, against 27 ms and 163 ms in Phase 2. p99 stayed at 3–4 ms, so this affects under 1% of requests, but it is over the 2 s target for those. A Ride Request is stored in Postgres before its first offer, so a stalled write would delay it; no Postgres attempt failed, which points to a slow write rather than a lost connection. The run records only percentiles, so how many requests were affected and when isn't known. This needs explaining before the Phase 8 strict test.

## What not to read into this

- **100k is a scaled load**, as in Phase 2: same geography, about four times real supply.
- **Silence is assumed:** 5% of drivers, once per run, for 10–60 s. It isn't measured from data.
- **Few Trips complete**, so Trip lifecycle events after pickup get little load.

## Assumptions

As in [load_ramp.md](load_ramp.md), plus:

- **Silent Drivers:** idle drivers go silent with probability 0.05, for 10–60 s, once per run. Drivers holding a Trip never go silent.
- **Silent Driver timeouts:** idle drivers leave the location index after 12 s without a ping; an open offer is withdrawn after 8 s.
- **Postgres:** postgres:18 in the same VM, trust auth, no published port, wiped each run, 16 Shard connections.

## Environment

- **VM:** 14 CPUs and 7.7 GiB, shared by all containers on one M4 Pro laptop.
- **Simulator:** limited to 2 CPUs; it peaked at 0.74 cores.
- **Code:** based on commit `e98d16b` with uncommitted Phase 3 changes, so records are marked `-dirty`.

## Data attribution

Demand comes from NYC Taxi & Limousine Commission HVFHV trip records. Calibration uses NYC Open Data taxi records. Map data is © OpenStreetMap contributors, under the ODbL. Only aggregate results are published here.
