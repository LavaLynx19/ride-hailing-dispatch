use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use simulator::client::{Counters, RunConfig, Silence, run_plan};
use simulator::metrics::{RunInfo, run_record, summarise};
use simulator::plan::{Scenario, Spike, ZonePoints, build_plan, parse_demand, parse_zone_exits};
use simulator::supply::SupplyTable;

const USAGE: &str = "usage: simulator --data-dir DIR --demand FILE --driver-url URL --rider-url URL \
--seed N --start-epoch SECONDS --duration-seconds N [--supply-scale X] [--demand-scale X] [--ping-seconds X] \
[--warmup-seconds X] [--name S] [--git-commit S] [--policy S] [--vm-cpus S] [--vm-memory-gib S] [--simulator-cpus S] \
[--silent-probability X] [--database-url URL[,URL...]] [--spike ZONE,START_SECONDS,SECONDS,MULTIPLIER]

With --database-url, the invariant checker runs against the Shards' Postgres after the run, one URL per Shard in Shard order.";

/// Assumed, not measured: how often an idle driver goes quiet with the socket still open.
const SILENCE: Silence = Silence { probability: 0.05, min_seconds: 10.0, max_seconds: 60.0 };

fn parse_args() -> Result<HashMap<String, String>, String> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut parsed = HashMap::new();
    for pair in args.chunks(2) {
        match pair {
            [name, value] if name.starts_with("--") => {
                parsed.insert(name[2..].to_owned(), value.clone());
            }
            _ => return Err(USAGE.to_owned()),
        }
    }
    Ok(parsed)
}

fn required<'a>(args: &'a HashMap<String, String>, name: &str) -> Result<&'a str, String> {
    args.get(name).map(String::as_str).ok_or_else(|| format!("missing --{name}\n{USAGE}"))
}

fn number<T: std::str::FromStr>(args: &HashMap<String, String>, name: &str, default: Option<T>) -> Result<T, String> {
    match (args.get(name), default) {
        (Some(value), _) => value.parse().map_err(|_| format!("bad --{name}: {value}")),
        (None, Some(default)) => Ok(default),
        (None, None) => Err(format!("missing --{name}\n{USAGE}")),
    }
}

fn read(path: PathBuf) -> Result<String, String> {
    std::fs::read_to_string(&path).map_err(|error| format!("{}: {error}", path.display()))
}

fn assumptions(silence: Silence) -> [(&'static str, f64); 10] {
    [
        ("utilisation", 0.6),
        ("accept_at_zero_km", 0.95),
        ("accept_half_distance_km", 5.0),
        ("response_min_seconds", 2.0),
        ("response_max_seconds", 15.0),
        ("pickup_speed_kmh", 25.0),
        ("offer_timeout_seconds", 20.0),
        ("silent_probability", silence.probability),
        ("silent_min_seconds", silence.min_seconds),
        ("silent_max_seconds", silence.max_seconds),
    ]
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let args = parse_args()?;
    let data = PathBuf::from(required(&args, "data-dir")?);
    let scenario = Scenario {
        seed: number(&args, "seed", None)?,
        start_epoch: number(&args, "start-epoch", None)?,
        duration_seconds: number(&args, "duration-seconds", None)?,
        supply_scale: number(&args, "supply-scale", Some(1.0))?,
        demand_scale: number(&args, "demand-scale", Some(1.0))?,
        spike: args.get("spike").map(|text| Spike::parse(text)).transpose()?,
    };
    let config = RunConfig {
        driver_urls: required(&args, "driver-url")?.split(',').map(str::to_owned).collect(),
        rider_urls: required(&args, "rider-url")?.split(',').map(str::to_owned).collect(),
        seed: scenario.seed,
        ping_interval_seconds: number(&args, "ping-seconds", Some(4.0))?,
        time_scale: 1.0,
        seconds_into_first_hour: scenario.start_epoch.rem_euclid(3600) as f64,
        silence: Silence { probability: number(&args, "silent-probability", Some(SILENCE.probability))?, ..SILENCE },
    };
    let silence = config.silence;

    let supply = SupplyTable::parse(&read(data.join("supply.csv"))?)?;
    let points = ZonePoints::parse(&read(data.join("pickup_points.csv"))?)?;
    let exits = parse_zone_exits(&read(data.join("zone_exits.csv"))?)?;
    let demand = parse_demand(&read(PathBuf::from(required(&args, "demand")?))?)?;
    let plan = build_plan(&scenario, &supply, &points, &exits, &demand);
    let (drivers_planned, trips_planned) = (plan.drivers.len(), plan.trips.len());
    let plan_hash = plan.fingerprint();
    let warm_start = plan.warm_start;
    eprintln!(
        "plan: {drivers_planned} drivers, {trips_planned} trips, warm start {} Trips in progress ({} dropped)",
        warm_start.trips, warm_start.dropped
    );

    let ping_seconds = config.ping_interval_seconds;
    let counters = Arc::new(Counters::default());
    let duration = Duration::from_secs(scenario.duration_seconds.max(0) as u64);
    let times = run_plan(config, plan, duration.as_secs_f64(), Arc::clone(&counters)).await;

    let invariant = match args.get("database-url") {
        Some(url) => {
            let observed = counters.observations.lock().map_err(|_| "observations unavailable")?.clone();
            let report = checker::check_database(url, &observed).await.map_err(|error| format!("invariant checker: {error}"))?;
            eprintln!(
                "invariant checker: {} with {} violations over {} Trips",
                if report.passed() { "passed" } else { "FAILED" },
                report.violations,
                report.trips_in_postgres
            );
            for example in &report.examples {
                eprintln!("  {example}");
            }
            Some(report)
        }
        None => {
            eprintln!("no --database-url, so the invariant wasn't checked and this run can't pass the gate");
            None
        }
    };
    for failure in counters.failures.lock().map_err(|_| "failures unavailable")?.iter() {
        eprintln!("failure: {failure}");
    }

    let warmup_seconds = number(&args, "warmup-seconds", Some(0.0))?;
    let text = |name: &str| args.get(name).map_or("unknown", String::as_str);
    let info = RunInfo {
        name: text("name"),
        git_commit: text("git-commit"),
        policy: text("policy"),
        seed: scenario.seed,
        plan_hash,
        start_epoch: scenario.start_epoch,
        duration_seconds: scenario.duration_seconds,
        warmup_seconds,
        supply_scale: scenario.supply_scale,
        demand_scale: scenario.demand_scale,
        ping_seconds,
        staleness_budget_p99_seconds: 5.0,
        vm_cpus: text("vm-cpus"),
        vm_memory_gib: text("vm-memory-gib"),
        simulator_cpus: text("simulator-cpus"),
        drivers_planned,
        trips_planned,
        warm_start,
        invariant_checked: invariant.is_some(),
        invariant_passed: invariant.as_ref().is_some_and(checker::Report::passed),
        invariant_violations: invariant.as_ref().map_or(0, |report| report.violations),
    };
    let get = Counters::get;
    let counter_values = [
        ("driver_connections", get(&counters.driver_connections)),
        ("driver_connect_failures", get(&counters.driver_connect_failures)),
        ("driver_session_errors", get(&counters.driver_session_errors)),
        ("rider_connect_failures", get(&counters.rider_connect_failures)),
        ("rider_session_errors", get(&counters.rider_session_errors)),
        ("silent_periods", get(&counters.silent_periods)),
        ("pings", get(&counters.pings)),
        ("offers", get(&counters.offers)),
        ("accepts", get(&counters.accepts)),
        ("declines", get(&counters.declines)),
        ("offers_gone", get(&counters.offers_gone)),
        ("trips_assigned", get(&counters.trips_assigned)),
        ("trips_completed", get(&counters.trips_completed)),
        ("ride_requests", get(&counters.ride_requests)),
        ("riders_finished", get(&counters.riders_finished)),
        ("redirects", get(&counters.redirects)),
    ];
    let offer_staleness = counters.offer_staleness.lock().map_err(|_| "staleness samples unavailable")?;
    let summary = summarise(&times, &offer_staleness, warmup_seconds);
    print!("{}", run_record(&info, &summary, &counter_values, &assumptions(silence)));
    Ok(())
}
