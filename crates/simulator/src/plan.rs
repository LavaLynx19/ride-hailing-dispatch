//! The run's seeded inputs: which drivers exist, when they're online and which trips
//! are requested. Building a plan touches no clock or network, so a seed fully fixes it.

use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};

use crate::driver::Dropoff;
use crate::position::Position;
use crate::rng::Rng;
use crate::supply::SupplyTable;

const HOURS_PER_WEEK: i64 = 168;
const SECONDS_PER_HOUR: i64 = 3600;
// 1970-01-01 was a Thursday, 72 hours into a week that starts on Monday.
const EPOCH_HOUR_OF_WEEK: i64 = 72;
const UNKNOWN_ZONES: [u16; 2] = [264, 265];
const DRIVER_STREAM: u64 = 1 << 62;
const RIDER_STREAM: u64 = 1 << 61;

pub fn hour_of_week(epoch_seconds: i64) -> u16 {
    ((epoch_seconds.div_euclid(SECONDS_PER_HOUR) + EPOCH_HOUR_OF_WEEK) % HOURS_PER_WEEK) as u16
}

/// Weighted pickup points per zone.
#[derive(Debug, Default)]
pub struct ZonePoints {
    zones: HashMap<u16, (Vec<Position>, Vec<f64>)>,
}

impl ZonePoints {
    /// Parses `zone,lat,lng,weight` rows, with a header.
    pub fn parse(csv: &str) -> Result<Self, String> {
        let mut zones: HashMap<u16, (Vec<Position>, Vec<f64>)> = HashMap::new();
        for line in csv.lines().skip(1) {
            let (zone, lat, lng, weight) = four_fields(line)?;
            let (points, cumulative) = zones.entry(zone).or_default();
            points.push(Position { lat, lng });
            cumulative.push(cumulative.last().copied().unwrap_or(0.0) + weight);
        }
        Ok(Self { zones })
    }

    pub fn sample(&self, rng: &mut Rng, zone: u16) -> Option<Position> {
        let (points, cumulative) = self.zones.get(&zone)?;
        let draw = rng.next_f64() * cumulative.last()?;
        let index = cumulative.partition_point(|total| *total <= draw);
        points.get(index.min(points.len() - 1)).copied()
    }
}

fn four_fields(line: &str) -> Result<(u16, f64, f64, f64), String> {
    let bad = || format!("bad row: {line}");
    let mut fields = line.split(',');
    let mut next = || fields.next().ok_or_else(bad);
    Ok((
        next()?.parse().map_err(|_| bad())?,
        next()?.parse().map_err(|_| bad())?,
        next()?.parse().map_err(|_| bad())?,
        next()?.parse().map_err(|_| bad())?,
    ))
}

/// Parses `zone,lat,lng` rows, with a header.
pub fn parse_zone_exits(csv: &str) -> Result<HashMap<u16, Position>, String> {
    let mut exits = HashMap::new();
    for line in csv.lines().skip(1) {
        let bad = || format!("bad row: {line}");
        let mut fields = line.split(',');
        let mut next = || fields.next().ok_or_else(bad);
        let zone = next()?.parse().map_err(|_| bad())?;
        let lat = next()?.parse().map_err(|_| bad())?;
        let lng = next()?.parse().map_err(|_| bad())?;
        exits.insert(zone, Position { lat, lng });
    }
    Ok(exits)
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DemandRow {
    pub requested_at: i64,
    pub pickup_zone: u16,
    pub dropoff_zone: u16,
    pub trip_seconds: f64,
}

/// Parses `requested_at,pickup_zone,dropoff_zone,trip_seconds` rows, with a header.
pub fn parse_demand(csv: &str) -> Result<Vec<DemandRow>, String> {
    csv.lines()
        .skip(1)
        .map(|line| {
            let bad = || format!("bad row: {line}");
            let mut fields = line.split(',');
            let mut next = || fields.next().ok_or_else(bad);
            Ok(DemandRow {
                requested_at: next()?.parse().map_err(|_| bad())?,
                pickup_zone: next()?.parse().map_err(|_| bad())?,
                dropoff_zone: next()?.parse().map_err(|_| bad())?,
                trip_seconds: next()?.parse().map_err(|_| bad())?,
            })
        })
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Scenario {
    pub seed: u64,
    pub start_epoch: i64,
    pub duration_seconds: i64,
    pub supply_scale: f64,
    pub demand_scale: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DriverPlan {
    pub id: String,
    pub stream: u64,
    pub position: Position,
    /// One entry per hour of the run.
    pub online: Vec<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TripPlan {
    pub request_at_seconds: f64,
    pub pickup: Position,
    pub dropoff: Dropoff,
    pub trip_seconds: f64,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Plan {
    pub drivers: Vec<DriverPlan>,
    pub trips: Vec<TripPlan>,
}

impl Plan {
    /// A fingerprint of the plan, so run records can show two runs had the same inputs.
    pub fn fingerprint(&self) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        let hash_position = |hasher: &mut std::collections::hash_map::DefaultHasher, position: Position| {
            position.lat.to_bits().hash(hasher);
            position.lng.to_bits().hash(hasher);
        };
        for driver in &self.drivers {
            driver.id.hash(&mut hasher);
            driver.online.hash(&mut hasher);
            hash_position(&mut hasher, driver.position);
        }
        for trip in &self.trips {
            trip.request_at_seconds.to_bits().hash(&mut hasher);
            trip.trip_seconds.to_bits().hash(&mut hasher);
            hash_position(&mut hasher, trip.pickup);
            match trip.dropoff {
                Dropoff::Inside(at) | Dropoff::Outside { exit: at } => hash_position(&mut hasher, at),
            }
        }
        hasher.finish()
    }
}

/// Rounds a fractional count up with probability equal to its fraction.
fn round_randomly(rng: &mut Rng, value: f64) -> usize {
    let floor = value.floor();
    floor as usize + usize::from(rng.next_f64() < value - floor)
}

fn run_hours(scenario: &Scenario) -> Vec<u16> {
    let seconds = scenario.duration_seconds.max(1);
    let first = scenario.start_epoch.div_euclid(SECONDS_PER_HOUR);
    let last = (scenario.start_epoch + seconds - 1).div_euclid(SECONDS_PER_HOUR);
    (first..=last).map(|hour| hour_of_week(hour * SECONDS_PER_HOUR)).collect()
}

fn plan_drivers(scenario: &Scenario, supply: &SupplyTable, points: &ZonePoints) -> Vec<DriverPlan> {
    let hours = run_hours(scenario);
    // Sorted so that plan order, and so a seeded plan, doesn't depend on hash order.
    let mut targets: BTreeMap<u16, Vec<usize>> = BTreeMap::new();
    for zone in supply.zones() {
        let counts = hours
            .iter()
            .map(|hour| {
                let id = (u64::from(zone) << 16) | u64::from(*hour);
                let mut rng = Rng::stream(scenario.seed, DRIVER_STREAM | id);
                round_randomly(&mut rng, supply.target(zone, *hour, scenario.supply_scale))
            })
            .collect();
        targets.insert(zone, counts);
    }

    let mut drivers = Vec::new();
    for (zone, counts) in targets {
        let peak = counts.iter().copied().max().unwrap_or(0);
        for index in 0..peak {
            let stream = DRIVER_STREAM | (1 << 40) | (u64::from(zone) << 24) | index as u64;
            let mut rng = Rng::stream(scenario.seed, stream);
            let Some(position) = points.sample(&mut rng, zone) else { continue };
            drivers.push(DriverPlan {
                id: format!("d{zone}-{index}"),
                stream,
                position,
                online: counts.iter().map(|count| index < *count).collect(),
            });
        }
    }
    drivers
}

fn plan_trips(
    scenario: &Scenario,
    demand: &[DemandRow],
    points: &ZonePoints,
    exits: &HashMap<u16, Position>,
) -> Vec<TripPlan> {
    let end = scenario.start_epoch + scenario.duration_seconds;
    let mut trips = Vec::new();
    for (row_index, row) in demand.iter().enumerate() {
        if row.requested_at < scenario.start_epoch || row.requested_at >= end {
            continue;
        }
        let mut rng = Rng::stream(scenario.seed, RIDER_STREAM | row_index as u64);
        for _ in 0..round_randomly(&mut rng, scenario.demand_scale) {
            let Some(pickup) = points.sample(&mut rng, row.pickup_zone) else { continue };
            let dropoff = if UNKNOWN_ZONES.contains(&row.dropoff_zone) {
                exits.get(&row.pickup_zone).map(|exit| Dropoff::Outside { exit: *exit })
            } else {
                points.sample(&mut rng, row.dropoff_zone).map(Dropoff::Inside)
            };
            let Some(dropoff) = dropoff else { continue };
            trips.push(TripPlan {
                request_at_seconds: (row.requested_at - scenario.start_epoch) as f64 + rng.next_f64(),
                pickup,
                dropoff,
                trip_seconds: row.trip_seconds,
            });
        }
    }
    trips.sort_by(|a, b| a.request_at_seconds.total_cmp(&b.request_at_seconds));
    trips
}

pub fn build_plan(
    scenario: &Scenario,
    supply: &SupplyTable,
    points: &ZonePoints,
    exits: &HashMap<u16, Position>,
    demand: &[DemandRow],
) -> Plan {
    Plan {
        drivers: plan_drivers(scenario, supply, points),
        trips: plan_trips(scenario, demand, points, exits),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Monday 2026-09-14 08:00:00, read as local time.
    const MONDAY_8AM: i64 = 1_789_372_800;

    fn scenario(seed: u64) -> Scenario {
        Scenario {
            seed,
            start_epoch: MONDAY_8AM,
            duration_seconds: 7200,
            supply_scale: 1.0,
            demand_scale: 1.0,
        }
    }

    fn fixtures() -> (SupplyTable, ZonePoints, HashMap<u16, Position>, Vec<DemandRow>) {
        let supply = SupplyTable::parse("zone,hour,drivers\n1,8,10\n1,9,4\n2,8,0.5\n").unwrap();
        let points = ZonePoints::parse(
            "zone,lat,lng,weight\n1,40.70,-74.00,0.25\n1,40.71,-74.00,0.75\n2,40.80,-73.90,1.0\n",
        )
        .unwrap();
        let exits = parse_zone_exits("zone,lat,lng\n1,40.60,-74.20\n").unwrap();
        let demand = (0..1000)
            .map(|index| DemandRow {
                requested_at: MONDAY_8AM + index,
                pickup_zone: 1,
                dropoff_zone: if index % 2 == 0 { 2 } else { 265 },
                trip_seconds: 600.0,
            })
            .collect();
        (supply, points, exits, demand)
    }

    fn plan(scenario: &Scenario) -> Plan {
        let (supply, points, exits, demand) = fixtures();
        build_plan(scenario, &supply, &points, &exits, &demand)
    }

    #[test]
    fn hour_of_week_starts_on_monday() {
        assert_eq!(hour_of_week(MONDAY_8AM), 8);
        assert_eq!(hour_of_week(MONDAY_8AM - 9 * 3600), 167);
    }

    #[test]
    fn same_seed_gives_same_plan() {
        assert_eq!(plan(&scenario(5)), plan(&scenario(5)));
    }

    #[test]
    fn different_seed_gives_different_plan() {
        assert_ne!(plan(&scenario(5)), plan(&scenario(6)));
    }

    #[test]
    fn fingerprint_follows_the_plan() {
        assert_eq!(plan(&scenario(5)).fingerprint(), plan(&scenario(5)).fingerprint());
        assert_ne!(plan(&scenario(5)).fingerprint(), plan(&scenario(6)).fingerprint());
    }

    #[test]
    fn drivers_follow_hourly_targets() {
        let drivers = plan(&scenario(5)).drivers;
        let zone_one: Vec<_> = drivers.iter().filter(|d| d.id.starts_with("d1-")).collect();
        assert_eq!(zone_one.len(), 10);
        assert_eq!(zone_one.iter().filter(|d| d.online[0]).count(), 10);
        assert_eq!(zone_one.iter().filter(|d| d.online[1]).count(), 4);
    }

    #[test]
    fn supply_scale_multiplies_drivers() {
        let drivers = plan(&Scenario { supply_scale: 3.0, ..scenario(5) }).drivers;
        assert_eq!(drivers.iter().filter(|d| d.id.starts_with("d1-")).count(), 30);
    }

    #[test]
    fn demand_scale_changes_trip_count_proportionally() {
        let half = plan(&Scenario { demand_scale: 0.5, ..scenario(5) }).trips.len();
        assert_eq!(plan(&Scenario { demand_scale: 2.0, ..scenario(5) }).trips.len(), 2000);
        assert!((430..=570).contains(&half), "{half}");
    }

    #[test]
    fn trips_are_ordered_and_inside_the_window() {
        let trips = plan(&Scenario { duration_seconds: 100, ..scenario(5) }).trips;
        assert_eq!(trips.len(), 100);
        assert!(trips.windows(2).all(|t| t[0].request_at_seconds <= t[1].request_at_seconds));
        assert!(trips.iter().all(|t| t.request_at_seconds < 101.0));
    }

    #[test]
    fn unknown_dropoffs_leave_through_the_pickup_zones_exit() {
        let exit = Position { lat: 40.60, lng: -74.20 };
        let trips = plan(&scenario(5)).trips;
        assert_eq!(trips.iter().filter(|t| t.dropoff == Dropoff::Outside { exit }).count(), 500);
    }

    #[test]
    fn sampling_follows_weights() {
        let (_, points, _, _) = fixtures();
        let mut rng = Rng::new(1);
        let heavy = (0..10_000)
            .filter(|_| points.sample(&mut rng, 1).unwrap().lat == 40.71)
            .count();
        assert!((7300..=7700).contains(&heavy), "{heavy}");
    }
}
