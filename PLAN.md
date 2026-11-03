# Plan

Requirements are in [README.md](README.md), design in [ARCHITECTURE.md](ARCHITECTURE.md), terms in [CONTEXT.md](CONTEXT.md).

## Implementation Checklist

### Phase 0: Verification (§P0)
- [x] 0.1 Confirm NYC HVFHV and older GPS data terms of use (§P0; no licence found, see Decision Log)
- [x] 0.2 Confirm HVFHV request timestamp precision (§P0)
- [x] 0.3 Check versions and maintenance of chosen crates (§P0)
- [x] 0.3a Choose an H3 library (§P0; `h3o`, see Decision Log)
- [x] 0.4 Choose the tooling for dataset preprocessing (§P0; Python + DuckDB)
- [x] 0.5 Create Cargo workspace and protobuf schemas (§P0)
- [x] 0.5a Add protobuf code generation (§P0)
- [x] 0.6 Add Dockerfile and compose setup (§P0)
- [>] 0.7 Verify `sqlx` metadata in the container build (§P0) → defer until: Phase 3 adds the first query

### Phase 1: Simulator and baseline (§P1)
- [x] 1.1 Prepare one month of demand from HVFHV records (§P1)
- [x] 1.2 Generate pickup points from OSM weights and older GPS calibration (§P1)
- [x] 1.3 Build synthetic driver behaviour model (§P1)
- [x] 1.4 Build seeded load generator over WebSockets (§P1)
- [x] 1.5 Build single-Shard naive greedy matcher (§P1)
- [x] 1.6 Build metrics and run record (§P1)
- [x] 1.7 Publish baseline result (§P1; `experiments/baseline.md`)

### Phase 2: Ingestion and geo-index (§P2)
- [x] 2.1 Build in-memory location index on H3 (§P2)
- [x] 2.2 Build ping log with rolling retention (§P2)
- [x] 2.3 Measure Staleness and spot-check `h3o` against reference H3 output (§P2)
- [x] 2.4 Run first load ramp and knee report (§P2; `experiments/load_ramp.md`)

### Phase 3: Assignment on one Shard (§P3)
- [ ] 3.1 Build Postgres schema and Trip lifecycle
- [ ] 3.2 Build atomic Accept with overlapping offers
- [ ] 3.3 Build silent-driver handling for idle and offer stages
- [ ] 3.4 Build invariant checker for run gating

### Phase 4: Matching policy (§P4)
- [ ] 4.1 Build auction solver with Batch Window
- [ ] 4.2 Build direct-then-Broadcast widening with supply signal
- [ ] 4.3 Run greedy vs auction experiment
- [ ] 4.4 Run Direct Offer vs widening experiment

### Phase 5: Geo-sharding (§P5)
- [ ] 5.1 Build versioned Cell → Shard map
- [ ] 5.2 Build boundary search over gRPC
- [ ] 5.3 Build cross-shard Reservation with expiry and peer check
- [ ] 5.4 Build idle driver handoff and Trip pinning
- [ ] 5.5 Run fault injection campaign
- [ ] 5.6 Run single vs multi-Shard experiment
- [ ] 5.7 Run hot-Shard spike experiment

### Phase 6: ETA and pricing (§P6)
- [ ] 6.1 Build live-speed ETA
- [ ] 6.2 Build Quote with locked Surge Multiplier
- [ ] 6.3 Build feedback controller for surge
- [ ] 6.4 Run surge stability experiment
- [>] 6.5 Discuss artificial spike creation (for example at airports) using the user's proposed solution, and refine it (§P6) → defer until: start of Phase 6

### Phase 7: Reconciliation (§P7)
- [ ] 7.1 Build mid-trip tolerance and driver-event buffering
- [ ] 7.2 Build plausibility rules using driver logs and rider data

### Phase 8: 100k attempt and report (§P8)
- [ ] 8.1 Run 100k feasibility test
- [ ] 8.2 Publish tradeoff report

### Later phases
- [>] 9.1 Road-network routing for ETA (§P9) → defer until: Phase 8 report published
- [>] 10.1 Payment and complaint systems (§P10) → defer until: Phase 9 complete
- [>] 10.2 Rider and driver apps (§P10) → defer until: payment and complaint systems complete

## Phase specs

Each phase builds depth on one module. A phase is complete when its verification passes. Tunable values are set by that phase's measurements, and ARCHITECTURE.md is updated with them.

### P0: Verification
Settle what ARCH left unverified, then set up the build. If timestamps are too coarse or the terms prohibit this use, stop and revisit the dataset decision in ARCHITECTURE.md.

**Verify:** the workspace builds in a container, including `sqlx` metadata.

### P1: Simulator and baseline
Replay real demand against a deliberately naive matcher, so every later experiment has something to beat. A driver model that can't produce plausible acceptance and movement blocks later results.

**Verify:** two runs with the same seed get the same demand and driver decisions; the run record contains seed, load, Staleness budget and VM limits; request-to-first-offer latency is reported.

### P2: Ingestion and geo-index
Replace the naive lookup with the location index and start storing pings.

**Verify:** nearest-driver results match a brute-force check on sampled queries; ping log survives a restart; retention keeps disk use within its limit; Staleness p99 is reported against the 5s budget; the report includes the knee.

### P3: Assignment on one Shard
Make the correctness rules real before optimising matching.

**Verify:** concurrent Accept tests never break the assignment invariant; killing the Shard or Postgres mid-Accept doesn't either; the checker flags a deliberately injected Double Assignment.

### P4: Matching policy
Add the auction solver and widening, and start producing tradeoff results.

**Verify:** with r1 at d1 = 1 min and d2 = 4 min, and r2 at d1 = 3 min and d2 = 15 min, the auction assigns r1→d2 and r2→d1 (7 min total) where greedy gives 16; experiments report total, p95 and worst-case wait; auction results are compared with brute force on small cases; request-to-first-offer stays under 2s at p99 with the chosen Batch Window; policies are compared over repeated runs with the spread reported; widening doesn't flap under the test load.

### P5: Geo-sharding
Split the metro into Shards, the riskiest phase for correctness. Seeded runs won't reliably reproduce races, so tests must rely on repeated fault runs and detailed logs.

**Verify:** invariant holds through Shard kills, partitions and message delays at every Reservation step, with repeated runs of the expiry vs peer-check race; a shard-map change causes no invariant violation; results show what sharding cost and gained compared with one Shard.

### P6: ETA and pricing
Add live-speed ETA and the surge controller.

Start with task 6.5, before the controller design is final. Experiments cover both sharp round-time spikes and broad peak-hour demand, and check whether surge should react to the pre-booked-looking spikes.

**Verify:** ETA error is reported against simulated arrival; a rider's locked Quote is honoured; surge settles after a demand step without sustained oscillation; the report states the limits of synthetic driver behaviour.

### P7: Reconciliation
Handle drivers who reconnect mid-trip or after it has ended. Disputes are recorded for the later payment and complaint systems.

**Verify:** scripted scenarios cover a plausible late completion, an implausible one and a conflict with rider data, each with the expected outcome.

### P8: 100k attempt and report
Run the strict test from ARCHITECTURE.md, then publish the report. If the test fails, the report gives the highest sustained load, the knee and the scaling curve.

**Verify:** every result in the report has a run record and passed the invariant gate.

### P9: Road-network routing
Add road-graph ETA and measure it against live-speed ETA. Graph data and engine need an ARCH update first.

### P10: Payments, complaints and apps
Add these once the core runs smoothly. Reusing `payment-ledger` needs a decision and permission to read it first, and an ARCH update.

## Commit sequence

Work on a feature branch per phase, `phase-N-<slug>`, and commit one checklist item at a time when asked. The first commit holds the project docs.

## Project structure

```
ride-hailing-dispatch/
├── README.md, CONTEXT.md, ARCHITECTURE.md, PLAN.md, CLAUDE.md
├── Cargo.toml             workspace
├── proto/                 protobuf schemas for clients and gRPC
├── crates/
│   ├── protocol/          code generated from proto/
│   ├── shard/             Shard service and module wiring
│   ├── geo/               location index and shard map
│   ├── matching/          solver and offer policy
│   ├── assignment/        Trips, Accept and Reservations
│   ├── pricing/           Quote and surge
│   ├── eta/               live-speed ETA
│   ├── simulator/         load generator and driver model
│   └── checker/           invariant checker
├── migrations/            Postgres schema
├── data/                  dataset preparation (raw data not committed)
├── experiments/           run configs, records and report
└── deploy/                container setup
```
