# Ride-Hailing Dispatch

The shared language for matching riders to drivers within one metro area, and for pricing and settling the resulting rides.

## Language

### People and trips

**Rider**:
A person requesting transport.
_Avoid_: Customer, passenger, user

**Driver**:
A person who provides transport and can receive offers.
_Avoid_: Partner, supply unit

**Ride Request**:
A rider's ask for a ride from a pickup point, made at a quoted price.
_Avoid_: Booking, order

**Trip**:
The ride that exists once a driver has been committed to a Ride Request, through to its end.
_Avoid_: Ride, journey

### Matching

**Direct Offer**:
An invitation to one chosen driver to take a Ride Request.
_Avoid_: Dispatch, ping

**Broadcast**:
An invitation to several drivers at once to take the same Ride Request, where the first Accept wins.
_Avoid_: Fan-out, multi-offer

**Accept**:
A driver's response to an offer, and the only point at which a driver can be committed to a Ride Request.
_Avoid_: Claim, confirm

**Reservation**:
A temporary hold on a driver during an Accept that crosses a Shard Boundary, before the rider is confirmed.
_Avoid_: Lock, hold

**Batch Window**:
The short period over which Ride Requests are collected so their Direct Offers are chosen together.
_Avoid_: Tick, matching interval

**Double Assignment**:
Any outcome where a rider ends with more than one driver, or a driver holds more than one Trip.
_Avoid_: Double booking, conflict

### Location and geography

**Location Ping**:
A driver's reported position at a point in time.
_Avoid_: GPS update, heartbeat

**Staleness**:
The age of a driver's latest Location Ping.
_Avoid_: Lag, delay

**Silent Driver**:
A driver who has stopped communicating, whatever the reason.
_Avoid_: Offline driver, ghost driver

**Cell**:
The smallest geographic area used to group drivers, demand and prices.
_Avoid_: Zone, tile

**Shard**:
A group of Cells owned and served together, able to fail independently of other Shards.
_Avoid_: Region, partition

**Shard Boundary**:
The line between two Shards, where a rider and a driver can belong to different owners.
_Avoid_: Border, edge

### Pricing and settlement

**Quote**:
The price shown to a rider, including its Surge Multiplier, held for a validity window.
_Avoid_: Estimate, fare

**Surge Multiplier**:
The factor applied to a price because of supply and demand in a Cell.
_Avoid_: Boost, dynamic price

**Reconciliation**:
Deciding what really happened on a Trip when a driver's reports arrive late or conflict with what the system recorded.
_Avoid_: Sync, dispute
