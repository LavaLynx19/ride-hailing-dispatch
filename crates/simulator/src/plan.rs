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
const WARM_STREAM: u64 = 1 << 50;

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
    pub spike: Option<Spike>,
}

/// Extra demand in one zone for a while: its Ride Requests are multiplied by `multiplier`
/// on top of the demand scale. Drivers don't react to it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Spike {
    pub zone: u16,
    /// Seconds into the run.
    pub start_seconds: f64,
    pub seconds: f64,
    pub multiplier: f64,
}

impl Spike {
    /// Parses `ZONE,START_SECONDS,SECONDS,MULTIPLIER`.
    pub fn parse(text: &str) -> Result<Self, String> {
        let bad = || format!("spike must be ZONE,START_SECONDS,SECONDS,MULTIPLIER, not {text}");
        let fields: Vec<&str> = text.split(',').map(str::trim).collect();
        let [zone, start, seconds, multiplier] = fields.as_slice() else { return Err(bad()) };
        Ok(Self {
            zone: zone.parse().map_err(|_| bad())?,
            start_seconds: start.parse().map_err(|_| bad())?,
            seconds: seconds.parse().map_err(|_| bad())?,
            multiplier: multiplier.parse().map_err(|_| bad())?,
        })
    }

    fn covers(&self, zone: u16, seconds_into_run: f64) -> bool {
        zone == self.zone && (self.start_seconds..self.start_seconds + self.seconds).contains(&seconds_into_run)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DriverPlan {
    pub id: String,
    pub stream: u64,
    pub position: Position,
    /// One entry per hour of the run.
    pub online: Vec<bool>,
    /// Seconds into the run that a Trip begun before it ends. Until then the driver stays
    /// offline; zero for a driver who starts idle.
    pub busy_until_seconds: f64,
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
    pub warm_start: WarmStart,
}

/// Trips from before the run that are still in progress when it starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WarmStart {
    /// Given to a driver, who comes online when the Trip ends.
    pub trips: usize,
    /// Left out because every driver online at the start already had one.
    pub dropped: usize,
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
            driver.busy_until_seconds.to_bits().hash(&mut hasher);
            hash_position(&mut hasher, driver.position);
        }
        self.warm_start.trips.hash(&mut hasher);
        self.warm_start.dropped.hash(&mut hasher);
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

/// Drivers in zone order; within a zone, those online the most hours come first.
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
                busy_until_seconds: 0.0,
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
        let seconds_into_run = (row.requested_at - scenario.start_epoch) as f64;
        let spiked = scenario.spike.filter(|spike| spike.covers(row.pickup_zone, seconds_into_run));
        let copies = scenario.demand_scale * spiked.map_or(1.0, |spike| spike.multiplier);
        for _ in 0..round_randomly(&mut rng, copies) {
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

fn dropoff_point(rng: &mut Rng, row: &DemandRow, points: &ZonePoints, exits: &HashMap<u16, Position>) -> Option<Position> {
    if UNKNOWN_ZONES.contains(&row.dropoff_zone) {
        exits.get(&row.pickup_zone).copied()
    } else {
        points.sample(rng, row.dropoff_zone)
    }
}

/// Gives each Trip still in progress at the start to a driver online at the start, from its
/// pickup zone if one is free, otherwise from any zone. The driver comes online at the dropoff
/// when the Trip ends. Demand rows from inside the run are ignored.
fn warm_start(
    scenario: &Scenario,
    demand: &[DemandRow],
    points: &ZonePoints,
    exits: &HashMap<u16, Position>,
    drivers: &mut [DriverPlan],
) -> WarmStart {
    let start = scenario.start_epoch;
    // Free drivers per zone, popped from the back so lower indices go first.
    let mut free: BTreeMap<u16, Vec<usize>> = BTreeMap::new();
    for (index, driver) in drivers.iter().enumerate().rev() {
        if driver.online.first() == Some(&true) {
            free.entry(zone_of(&driver.id)).or_default().push(index);
        }
    }
    let mut result = WarmStart::default();
    for (row_index, row) in demand.iter().enumerate() {
        let ends = row.requested_at as f64 + row.trip_seconds;
        if row.requested_at >= start || ends <= start as f64 {
            continue;
        }
        let mut rng = Rng::stream(scenario.seed, RIDER_STREAM | WARM_STREAM | row_index as u64);
        for _ in 0..round_randomly(&mut rng, scenario.demand_scale) {
            let Some(dropoff) = dropoff_point(&mut rng, row, points, exits) else { continue };
            let own_zone = free.get_mut(&row.pickup_zone).and_then(Vec::pop);
            let Some(index) = own_zone.or_else(|| free.values_mut().find_map(Vec::pop)) else {
                result.dropped += 1;
                continue;
            };
            drivers[index].position = dropoff;
            drivers[index].busy_until_seconds = ends - start as f64;
            result.trips += 1;
        }
    }
    result
}

/// The zone in an id made by `plan_drivers`.
fn zone_of(id: &str) -> u16 {
    id[1..].split('-').next().and_then(|zone| zone.parse().ok()).expect("driver ids are d{zone}-{index}")
}

/// `demand` may start before the run: rows from before it only feed the warm start.
pub fn build_plan(
    scenario: &Scenario,
    supply: &SupplyTable,
    points: &ZonePoints,
    exits: &HashMap<u16, Position>,
    demand: &[DemandRow],
) -> Plan {
    let mut drivers = plan_drivers(scenario, supply, points);
    let warm_start = warm_start(scenario, demand, points, exits, &mut drivers);
    Plan { drivers, trips: plan_trips(scenario, demand, points, exits), warm_start }
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
            spike: None,
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
    fn a_spike_multiplies_its_zones_requests_inside_its_window_only() {
        // Fixture demand: 1000 requests in zone 1, one a second from the start.
        let spike = Spike { zone: 1, start_seconds: 100.0, seconds: 50.0, multiplier: 10.0 };
        let trips = plan(&Scenario { spike: Some(spike), ..scenario(5) }).trips;
        assert_eq!(trips.len(), 1000 + 50 * 9);
        let inside = trips.iter().filter(|t| (100.0..151.0).contains(&t.request_at_seconds)).count();
        assert!((499..=510).contains(&inside), "{inside}");
        let elsewhere = Spike { zone: 2, ..spike };
        assert_eq!(plan(&Scenario { spike: Some(elsewhere), ..scenario(5) }).trips.len(), 1000);
        assert_eq!(Spike::parse("161, 300, 300, 50").unwrap(), Spike { zone: 161, start_seconds: 300.0, seconds: 300.0, multiplier: 50.0 });
        assert!(Spike::parse("161,300").is_err());
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

    fn before_start(seconds_before: i64, pickup_zone: u16, dropoff_zone: u16, trip_seconds: f64) -> DemandRow {
        DemandRow { requested_at: MONDAY_8AM - seconds_before, pickup_zone, dropoff_zone, trip_seconds }
    }

    #[test]
    fn trips_in_progress_at_the_start_keep_drivers_offline_until_they_end() {
        let (supply, points, exits, _) = fixtures();
        let demand = vec![
            before_start(300, 1, 2, 900.0),  // ends 600 s in, at a zone 2 point
            before_start(900, 1, 265, 600.0), // ended before the start
            before_start(100, 1, 265, 400.0), // ends 300 s in, at zone 1's exit
        ];
        let plan = build_plan(&scenario(5), &supply, &points, &exits, &demand);
        assert_eq!(plan.warm_start, WarmStart { trips: 2, dropped: 0 });
        assert!(plan.trips.is_empty(), "rows before the start aren't run demand");

        let busy: Vec<_> = plan.drivers.iter().filter(|d| d.busy_until_seconds > 0.0).collect();
        assert_eq!(busy.len(), 2);
        assert!(busy.iter().all(|d| d.id.starts_with("d1-")), "pickup zone first");
        let zone_two = Position { lat: 40.80, lng: -73.90 };
        let exit = Position { lat: 40.60, lng: -74.20 };
        let mut ends: Vec<_> = busy.iter().map(|d| (d.busy_until_seconds, d.position)).collect();
        ends.sort_by(|a, b| a.0.total_cmp(&b.0));
        assert_eq!(ends, [(300.0, exit), (600.0, zone_two)]);
    }

    #[test]
    fn warm_trips_spill_to_other_zones_then_drop() {
        // Zone 1 has 10 drivers online at the start, zone 2 none or one.
        let (supply, points, exits, _) = fixtures();
        let demand: Vec<_> = (0..15).map(|_| before_start(60, 1, 2, 600.0)).collect();
        let plan = build_plan(&scenario(5), &supply, &points, &exits, &demand);
        let online = plan.drivers.iter().filter(|d| d.online[0]).count();
        assert!(online < 15);
        assert_eq!(plan.warm_start, WarmStart { trips: online, dropped: 15 - online });
        assert!(plan.drivers.iter().filter(|d| d.online[0]).all(|d| d.busy_until_seconds == 600.0 - 60.0));
        assert!(plan.drivers.iter().filter(|d| !d.online[0]).all(|d| d.busy_until_seconds == 0.0));
    }

    #[test]
    fn warm_start_is_seeded_and_fingerprinted() {
        let (supply, points, exits, _) = fixtures();
        let demand = vec![before_start(60, 1, 2, 600.0)];
        let warm = build_plan(&scenario(5), &supply, &points, &exits, &demand);
        assert_eq!(warm, build_plan(&scenario(5), &supply, &points, &exits, &demand));
        assert_ne!(warm.fingerprint(), build_plan(&scenario(5), &supply, &points, &exits, &[]).fingerprint());
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
