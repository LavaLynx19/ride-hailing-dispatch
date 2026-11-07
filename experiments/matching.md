# Phase 4 matching policy

Compares greedy matching with the batched auction, and Direct Offers alone with direct-then-Broadcast widening, on one Shard. Run on 2026-11-04 to 2026-11-07.

**All 15 comparison runs passed the invariant checker with 0 violations**, over 11,353 to 11,390 Trips each. So did every sweep and check run.

**No policy beat the greedy baseline at this load.**
- The auction matched greedy's pickup legs to within noise in both scenarios. It added about 0.13 s to assignment, the cost of waiting for the batch.
- Widening made scarce supply slightly worse on every seed: about 1 s more wait and nearly twice as many declines.
- Request to first offer stayed far under the 2 s p99 target for every policy.

## Scenarios

Each run covers 17:55–18:15 on Monday 2026-09-14, with the first 5 minutes as warmup, and seeds 1–3. Demand scale is 0.981 throughout.

| Scenario | Supply scale | Drivers planned | Idle drivers late in the run |
|---|---:|---:|---|
| Abundant | 0.981 (1×) | about 24,990 | not tight |
| Scarce | 0.638 (0.65×) | about 16,250 | 1,750–2,600, never 0 |

**Runs are warm-started.** Trips from the 2 hours before the start that are still in progress at the start occupy drivers. Each of those drivers comes online only when that Trip ends, at its dropoff. That was 13,748–13,787 Trips per run, with none dropped. See ARCHITECTURE.md → Experiments → Warm start.

Without it, every driver starts idle and few Trips finish within 20 minutes, so a run either never gets scarce or ends in overload (see "How the scarce level was found").

**Policies:**
- **Greedy:** the Phase 1 baseline.
- **Auction:** 250 ms Batch Window.
- **Widening:** the auction plus Broadcasts after a failed Direct Offer or below the supply threshold.

The values are in ARCHITECTURE.md → Matching policy on one Shard.

## Results

Ranges are min–max over the three seeds. Wait is request to pickup arrival, counted for measured requests that reached pickup before the run ended. Pickup leg is assignment to pickup arrival.

| Measure | Greedy abundant | Auction abundant | Greedy scarce | Auction scarce | Widening scarce |
|---|---|---|---|---|---|
| Invariant passed, violations | 3/3, 0 | 3/3, 0 | 3/3, 0 | 3/3, 0 | 3/3, 0 |
| Requests reaching pickup | 8,378–8,409 | 8,377–8,409 | 7,732–7,760 | 7,733–7,765 | 7,677–7,720 |
| Total wait (hours) | 55.9–56.2 | 56.1–56.5 | 132.0–135.6 | 132.4–134.4 | 133.1–137.0 |
| Mean wait (s) | 24.0–24.1 | 24.1–24.2 | 61.4–62.9 | 61.6–62.5 | 62.4–63.9 |
| Wait p95 (s) | 62.1–63.0 | 62.2–63.1 | 170.3–171.1 | 168.2–171.6 | 172.0–174.4 |
| Worst wait (s) | 280–528 | 280–528 | 610–737 | 610–732 | 631–736 |
| Mean pickup leg (s) | 15.1 | 15.1 | 52.4–53.9 | 52.4–53.3 | 53.6–55.1 |
| Request to assignment, p50 (s) | 8.79–8.86 | 8.92–9.00 | 8.90–8.95 | 9.02–9.06 | 8.73–8.79 |
| Request to first offer, p99 (s) | 0.005 | 0.252 | 0.005–0.006 | 0.251–0.252 | 0.252 |
| Request to first offer, max (s) | 5.06–7.32 | 5.52–7.56 | 0.52–20.03 | 0.58–20.29 | 0.70–20.29 |
| Measured requests never assigned | 73–93 | 75–94 | 78–83 | 82–89 | 75–80 |
| Declines | 594–633 | 595–629 | 790–850 | 783–846 | 1,390–1,486 |
| Connection failures | 0 | 0, 0, 16 | 0 | 0 | 0 |

Auction scarce seed 1 is the 250 ms sweep run `p4-warm-window250-scarce065-seed1`. The other records are `p4-{greedy,auction,widening}-{abundant,scarce}-seed{1,2,3}` in [runs/](runs/). Shard counts per minute are in `.shard-stats.txt`, and failure lines are in `.stderr.txt`.

### Paired by seed

The same seed gives every policy the same demand and drivers, so differences per seed are a fairer test than the ranges above.

| Difference (s) | Seed 1 | Seed 2 | Seed 3 |
|---|---:|---:|---:|
| Auction − greedy, mean wait, scarce | −0.2 | −0.4 | +0.2 |
| Auction − greedy, mean wait, abundant | +0.2 | +0.1 | +0.1 |
| Widening − auction, mean wait, scarce | +0.6 | +1.4 | +0.8 |
| Widening − auction, mean pickup leg, scarce | +1.0 | +1.8 | +1.2 |

## Batch Window

Swept at 250 ms, 500 ms and 1 s, with the auction and no widening, warm 0.65×, seed 1. The rule was fixed before the runs: keep windows with first-offer p99 ≤ 1.5 s, then take the lowest mean pickup leg, and the shorter window if two are within 2%.

| Window | First offer p99 | Mean pickup leg | Mean wait | Batches | Deadline hits |
|---|---:|---:|---:|---:|---:|
| 250 ms | 0.252 s | 52.99 s | 62.20 s | 4,439 | 0 |
| 500 ms | 0.500 s | 52.93 s | 62.28 s | 2,383 | 0 |
| 1 s | 0.996 s | 52.91 s | 62.52 s | 1,199 | 0 |

All three passed the filter, and the pickup legs are within 0.2%, so **250 ms** was chosen. The solver never hit its 50 ms deadline.

## Widening and flapping

- **Broadcast rounds:** about 1,030–1,050 per run.
- **Failed Direct Offers:** 104–281 of those rounds came before any area went short, so they followed a failed Direct Offer.
- **Low supply:** areas started broadcasting 5–9 minutes into each run, and up to 22 were broadcasting at once by the end.
- **Area switches:** 48, 71 and 63 per run, counting both entering and leaving. That is about 3 per broadcasting area over the 10–14 minutes they were active. The 60 s hold caps each area at one switch a minute, and no run came near that cap in total.
- **Flapping:** none seen at this load. But the Shard counts switches only in total, not per area, so this doesn't rule out one area switching back and forth while others hold. A per-area count would be needed to show that.

## What this shows

- **The auction has nothing to optimise here.** Only about 2 Ride Requests arrive per 250 ms citywide, spread over the whole map. Two rides in one batch rarely share candidate drivers, so the auction usually gives the greedy answer, about 0.13 s later. The 1 s window didn't help either. A benefit would need dense local demand, such as an event letting out. Phase 6's spike scenarios could test that.
- **Widening costs more than it gains at this load.** A Broadcast goes to the 5 nearest drivers, including farther ones, and the first Accept wins, so pickups get longer. Farther drivers also decline more often (assumed acceptance halves at 5 km), so declines nearly doubled. Widening cut assignment p50 by about 0.27 s, which doesn't make up for 1–2 s of longer pickups.
- **Lost Accept races were rare.** Widening runs had 11,389 Accepts for 11,388 Trips at most, so overlapping Broadcasts almost never had two drivers accept the same ride. The concurrent race is covered by the end-to-end test.
- **First offer meets the target** for every policy, with p99 at 0.25 s at most against the 2 s target. The auction's p99 is the window itself.
- **The first-offer max** (open since Phase 3):
  - **Abundant runs:** 5–7.6 s on every seed. These runs start with about 11,000 idle drivers connecting at once, so a startup surge fits. The cold 0.5× runs showed about 5.6 s; the warm 0.75× check, with about 4,900 drivers idle at the start, showed 0.68 s.
  - **Scarce runs:** about 20 s on seed 1 under all three policies, but 0.5–5.7 s on seeds 2 and 3. Since it appears in every policy for that seed only, it most likely comes from that seed's demand, a ride with no idle driver near enough, rather than from the matcher.

  Neither cause is confirmed with timestamps yet.

## Problems found while running

- **The first scarce level didn't work.** See the next section; it cost 4 extra runs and a simulator change.
- **A run name collided** with an existing record and overwrote it. The JSON was restored byte for byte from a copy printed earlier in the session; the stderr file was rebuilt with the same lines. Cold runs were renamed `p4-cold-*`, with the supply level in the name, so their `name` field no longer matches their file name.
- **Connection failures:** 16 driver connects failed at 10.2 s in `p4-auction-abundant-seed3`, and 10 at 10.1 s in a cold sweep run. Every one was "Temporary failure in name resolution": Docker's DNS failed to resolve the Shard's name during the startup surge. This is the first cause found for the connection failures flagged in Phase 2. It's an artefact of the test setup, not a Shard failure. Retries recovered those drivers, and the invariant still passed.

## How the scarce level was found

All four checks were seed 1, with the auction at 250 ms and no widening. They passed the invariant checker but don't count as scenario results.

| Run | Start | Supply | What happened |
|---|---|---:|---|
| `p4-cold-window{250,500,1000}-scarce050-seed1` | cold | 0.5× | Idle drivers fell to about 2,600 and never ran out; at most 8 rides waited. |
| `p4-cold-window250-scarce035-seed1` | cold | 0.35× | Idle ran out about 16 minutes in; 880 rides waiting at the end. |
| `p4-cold-window250-scarce025-seed1` | cold | 0.25× | Idle ran out about 11 minutes in; 3,476 waiting, first-offer p99 381 s. |
| `p4-warm-window250-scarce075-seed1` | warm | 0.75× | Steady, but 21% of drivers idle; at most 6 waited. |
| `p4-warm-window250-scarce065-seed1` | warm | 0.65× | Idle 11–16% and falling slowly; at most 4 waited; pickup leg 53 s. Chosen. |

The supply table assumes 60% utilisation. So at a supply-to-demand ratio r, trip time alone keeps 0.6 / r of drivers busy: 80% at 0.75×, 92% at 0.65×, and over 100% below 0.6×. The 0.75× check matched that estimate, at 21% idle measured against 20% predicted.

## What not to read into this

- **The scarce scenario isn't fully steady.** Idle drivers were still falling slowly at the end, from about 2,600 to 1,750. A longer run might reach overload.
- **Waits only count riders who reached pickup.** Riders who didn't reach pickup before the run ended are left out: 235–266 per run in abundant and 879–973 in scarce. They are mostly the latest and longest waits, so the totals and percentiles are low estimates. Widening left the most out, so its disadvantage is if anything understated.
- **Distances are straight lines** at an assumed 25 km/h pickup speed. Roads would change pickup costs, and might change what the auction can gain.
- **Driver behaviour is assumed:**
  - Acceptance falls with distance.
  - Responses take 2–15 s.
  - 5% of idle drivers go silent once.
  - Nothing reacts to the policy.

  Widening's result depends on the acceptance curve in particular.
- **The supply threshold values aren't tuned.** The 1.0 / 1.5 / 60 s thresholds, 5 drivers per Broadcast and 3 Broadcasts per driver are untested starting values. A tighter threshold might make widening break even, but this load gives it little to gain.

## Assumptions

As in [assignment.md](assignment.md), plus:

- **Warm start:** described above. Pickup time before the run isn't counted, so warm drivers come free slightly early.
- **Policies:** 8 auction candidates, 1 m epsilon, 50 ms solver deadline. Supply is measured over the resolution-7 Cell and its 6 neighbours.

## Environment

- **VM:** 14 CPUs and 7.7 GiB, shared by all containers on one M4 Pro laptop.
- **Simulator:** limited to 2 CPUs.
- **Peak memory, sampled every minute:** Shard 333 MiB, Postgres 83 MiB, simulator 387 MiB.
- **Peak CPU:** Shard 0.23 cores, Postgres 0.29, simulator 0.32.
- **Load:** about 1× real load, far below the Phase 2 100k level, so these runs say nothing about capacity.
- **Code:** based on commit `705c52b` with uncommitted Phase 4 changes, so records are marked `-dirty`. The warm-start simulator change came after the cold runs, which used the same commit marker.

## Data attribution

Demand comes from NYC Taxi & Limousine Commission HVFHV trip records. Calibration uses NYC Open Data taxi records. Map data is © OpenStreetMap contributors, under the ODbL. Only aggregate results are published here.
