# Ride-Hailing Dispatch

Backend for a ride-hailing platform in one metro area: it ingests driver locations, matches riders to drivers, estimates arrival times and prices rides by local supply and demand.

Terms are defined in [CONTEXT.md](CONTEXT.md).

## Problem

Dispatch has to match moving supply to demand within seconds, using locations that are already out of date, and across geographic shards that can fail independently. Most designs look fine under uniform random load, so this project measures how the design choices behave under realistic demand and injected faults.

## Goals

- Answer the core dispatch tradeoffs with measurements, not opinion.
- Build every module to real depth over time, in phases: ingestion and geo-index, matching, ETA, pricing.
- Hold the assignment invariant under races and faults.

## Matching behaviour

- Each Ride Request starts with a Direct Offer, and the driver for it is chosen by batched bipartite matching.
- If the Direct Offer isn't accepted, the request widens to a Broadcast where the first Accept wins. Local supply also influences widening.
- A driver can hold many overlapping offers. Accept is the only commit point, and it is atomic.
- A cross-shard Accept uses a two-step Reservation: reserve the driver, then confirm the rider.

**Assignment invariant:** a rider ends with exactly one driver, and a driver never holds two trips.

## Silent drivers

| Stage | Behaviour |
|---|---|
| Idle | Strict: quickly stop matching the driver |
| Holding an offer | Strict: quickly move the request on |
| Mid-trip | Tolerant, then Reconciliation on reconnect |

Reconciliation applies plausibility rules (timeline, location) using driver-app logs and rider trip data. Disputes go to the payment and complaint systems.

## Pricing

The Surge Multiplier is locked at Quote for a validity window.

## User stories

- As a rider, I get an offer to a nearby driver quickly, at the price I was quoted.
- As a rider, I'm never left with two drivers, or with a driver who has gone silent.
- As a driver, I can see several offers, and I'm told clearly if one is gone by the time I accept.
- As a driver, a trip I completed in a dead zone is still recognised after I reconnect.
- As the experimenter, I can replay the same load against different policies and compare the results.

## Constraints

- Solo project, developed on one M4 Pro laptop.
- One metro area, split into shards.
- Scale target is 100k simulated drivers if the laptop can sustain that load; otherwise, the highest feasible load plus a measured scaling curve.
- Staleness budget is on the order of a few seconds.
- Location Pings are stored for replay.
- Load uses a public trip dataset for demand and synthetic driver behaviour. Results depend on that behaviour model, and reports must state its assumptions.

## Scope

| When | What |
|---|---|
| Phase 1 | Simulator plus a naive baseline matcher |
| Core | Backend for one metro: ingestion, geo-index, matching, simple ETA, pricing |
| Late | Road-network routing for ETA, measured against the simpler ETA |
| Final | Payment and complaint systems, then rider and driver apps, once the core runs smoothly |
| Out | Multi-city |

## Success criteria

- Request to first offer is under 2s at p99.
- A published tradeoff report covers the experiments.
- **Gate:** an experiment result only counts if the assignment invariant held during that run.

## Open questions

Technical decisions and their open items are tracked in [ARCHITECTURE.md](ARCHITECTURE.md). No requirements are open here. The scaling curve uses 25k, 50k, 75k and 100k drivers.
