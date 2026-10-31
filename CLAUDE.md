# Ride-Hailing Dispatch

Phase: PLAN complete. CODE starts at PLAN.md Phase 0 once the user approves.

## Docs
- Requirements: README.md
- Terms: CONTEXT.md. Use its terms exactly in code, logs and reports.
- Stack, design and open values: ARCHITECTURE.md
- Tasks and progress: PLAN.md

## Stack
Locked in ARCHITECTURE.md → Dependencies. Changing or adding to it needs a tradeoff discussion and an ARCHITECTURE.md update.

## Conventions
- **Assignment invariant:** every change touching Accept, Reservation or Trip state gets concurrent and fault tests. Postgres constraints enforce it within a Shard, and the Reservation enforces it across Shards.
- **Experiments:** every result has a run record, and counts only if the invariant checker passed. Compare policies over repeated runs and report the spread.
- **Seeds:** all demand and driver randomness comes from the run seed. Timing isn't seeded, so log enough at each Reservation step to diagnose a race that won't reproduce.
- **Hot path:** pings stay in memory and on the ping log; Postgres is reserved for assignment state.
- **Tunable values:** set them from a measurement, then record them in ARCHITECTURE.md.

## Constraints
- All containers share one Linux VM, including the load generator with its own CPU limit. Every report states the VM limits.
- Raw datasets stay out of git.
- `sqlx` builds need a database or cached query metadata.
- Publish only aggregate results from NYC data, with attribution. Raw files are never redistributed.

## Overrides
- Isolation exception: `payment-ledger/` may be read only with explicit user permission, for PLAN.md P10.
