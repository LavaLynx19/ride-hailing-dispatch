//! Per-trip timings and the run record.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};

const UNSET: u64 = 0;

/// Microseconds of simulated time since the run started, plus one so zero means unset.
#[derive(Debug, Default)]
pub struct TripTimes {
    pub requested: AtomicU64,
    pub first_offer: AtomicU64,
    pub assigned: AtomicU64,
    pub arrived: AtomicU64,
}

pub fn record_once(slot: &AtomicU64, sim_seconds: f64) {
    let value = (sim_seconds * 1e6) as u64 + 1;
    let _ = slot.compare_exchange(UNSET, value, Ordering::Relaxed, Ordering::Relaxed);
}

fn read(slot: &AtomicU64) -> Option<f64> {
    match slot.load(Ordering::Relaxed) {
        UNSET => None,
        value => Some((value - 1) as f64 / 1e6),
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Distribution {
    pub count: usize,
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub max: f64,
}

/// Nearest-rank percentiles; `None` when there are no samples.
pub fn distribution(mut samples: Vec<f64>) -> Option<Distribution> {
    if samples.is_empty() {
        return None;
    }
    samples.sort_by(f64::total_cmp);
    let rank = |percent: f64| {
        let index = (percent / 100.0 * samples.len() as f64).ceil() as usize;
        samples[index.saturating_sub(1)]
    };
    Some(Distribution {
        count: samples.len(),
        p50: rank(50.0),
        p95: rank(95.0),
        p99: rank(99.0),
        max: samples[samples.len() - 1],
    })
}

#[derive(Debug, Clone, PartialEq)]
pub struct Summary {
    pub measured_requests: usize,
    pub never_offered: usize,
    pub never_assigned: usize,
    pub never_reached_pickup: usize,
    pub request_to_first_offer: Option<Distribution>,
    pub request_to_assignment: Option<Distribution>,
    pub request_to_pickup_arrival: Option<Distribution>,
    pub staleness_at_offer: Option<Distribution>,
}

/// Summarises requests made at or after `warmup_seconds`, so that the empty start
/// of a run, when every driver is idle, doesn't flatter the result.
pub fn summarise(times: &[TripTimes], offer_staleness: &[(f64, f64)], warmup_seconds: f64) -> Summary {
    let measured: Vec<_> = times
        .iter()
        .filter_map(|trip| read(&trip.requested).filter(|at| *at >= warmup_seconds).map(|at| (at, trip)))
        .collect();
    let since = |slot: fn(&TripTimes) -> &AtomicU64| -> Vec<f64> {
        measured.iter().filter_map(|(at, trip)| read(slot(trip)).map(|time| time - at)).collect()
    };
    let (offers, assigned, arrived) = (since(|t| &t.first_offer), since(|t| &t.assigned), since(|t| &t.arrived));
    Summary {
        measured_requests: measured.len(),
        never_offered: measured.len() - offers.len(),
        never_assigned: measured.len() - assigned.len(),
        never_reached_pickup: measured.len() - arrived.len(),
        request_to_first_offer: distribution(offers),
        request_to_assignment: distribution(assigned),
        request_to_pickup_arrival: distribution(arrived),
        staleness_at_offer: distribution(
            offer_staleness.iter().filter(|(at, _)| *at >= warmup_seconds).map(|(_, age)| *age).collect(),
        ),
    }
}

fn json_distribution(out: &mut String, name: &str, value: &Option<Distribution>) {
    match value {
        None => writeln!(out, "    \"{name}\": null,").unwrap(),
        Some(d) => writeln!(
            out,
            "    \"{name}\": {{\"count\": {}, \"p50\": {:.3}, \"p95\": {:.3}, \"p99\": {:.3}, \"max\": {:.3}}},",
            d.count, d.p50, d.p95, d.p99, d.max
        )
        .unwrap(),
    }
}

/// Fields describing the run, written ahead of its results.
pub struct RunInfo<'a> {
    pub name: &'a str,
    pub git_commit: &'a str,
    pub seed: u64,
    pub plan_hash: u64,
    pub start_epoch: i64,
    pub duration_seconds: i64,
    pub warmup_seconds: f64,
    pub supply_scale: f64,
    pub demand_scale: f64,
    pub ping_seconds: f64,
    pub staleness_budget_p99_seconds: f64,
    pub vm_cpus: &'a str,
    pub vm_memory_gib: &'a str,
    pub simulator_cpus: &'a str,
    pub drivers_planned: usize,
    pub trips_planned: usize,
    pub invariant_checked: bool,
}

pub fn run_record(info: &RunInfo, summary: &Summary, counters: &[(&str, u64)], assumptions: &[(&str, f64)]) -> String {
    let mut out = String::from("{\n");
    writeln!(out, "  \"name\": \"{}\",", info.name).unwrap();
    writeln!(out, "  \"git_commit\": \"{}\",", info.git_commit).unwrap();
    writeln!(out, "  \"seed\": {},", info.seed).unwrap();
    writeln!(out, "  \"plan_hash\": \"{:016x}\",", info.plan_hash).unwrap();
    writeln!(out, "  \"start_epoch\": {},", info.start_epoch).unwrap();
    writeln!(out, "  \"duration_seconds\": {},", info.duration_seconds).unwrap();
    writeln!(out, "  \"warmup_seconds\": {},", info.warmup_seconds).unwrap();
    writeln!(out, "  \"supply_scale\": {},", info.supply_scale).unwrap();
    writeln!(out, "  \"demand_scale\": {},", info.demand_scale).unwrap();
    writeln!(out, "  \"ping_seconds\": {},", info.ping_seconds).unwrap();
    writeln!(out, "  \"staleness_budget_p99_seconds\": {},", info.staleness_budget_p99_seconds).unwrap();
    writeln!(out, "  \"vm_cpus\": \"{}\",", info.vm_cpus).unwrap();
    writeln!(out, "  \"vm_memory_gib\": \"{}\",", info.vm_memory_gib).unwrap();
    writeln!(out, "  \"simulator_cpus\": \"{}\",", info.simulator_cpus).unwrap();
    writeln!(out, "  \"drivers_planned\": {},", info.drivers_planned).unwrap();
    writeln!(out, "  \"trips_planned\": {},", info.trips_planned).unwrap();
    writeln!(out, "  \"invariant_checked\": {},", info.invariant_checked).unwrap();
    out.push_str("  \"assumptions\": {");
    let joined: Vec<String> = assumptions.iter().map(|(name, value)| format!("\"{name}\": {value}")).collect();
    out.push_str(&joined.join(", "));
    out.push_str("},\n  \"summary\": {\n");
    writeln!(out, "    \"measured_requests\": {},", summary.measured_requests).unwrap();
    writeln!(out, "    \"never_offered\": {},", summary.never_offered).unwrap();
    writeln!(out, "    \"never_assigned\": {},", summary.never_assigned).unwrap();
    writeln!(out, "    \"never_reached_pickup\": {},", summary.never_reached_pickup).unwrap();
    json_distribution(&mut out, "request_to_first_offer_seconds", &summary.request_to_first_offer);
    json_distribution(&mut out, "request_to_assignment_seconds", &summary.request_to_assignment);
    json_distribution(&mut out, "request_to_pickup_arrival_seconds", &summary.request_to_pickup_arrival);
    json_distribution(&mut out, "staleness_at_offer_seconds", &summary.staleness_at_offer);
    if out.ends_with(",\n") {
        out.truncate(out.len() - 2);
        out.push('\n');
    }
    out.push_str("  },\n  \"counters\": {");
    let joined: Vec<String> = counters.iter().map(|(name, value)| format!("\"{name}\": {value}")).collect();
    out.push_str(&joined.join(", "));
    out.push_str("}\n}\n");
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trip(requested: f64, offer: Option<f64>) -> TripTimes {
        let times = TripTimes::default();
        record_once(&times.requested, requested);
        if let Some(offer) = offer {
            record_once(&times.first_offer, offer);
        }
        times
    }

    #[test]
    fn percentiles_use_nearest_rank() {
        let result = distribution((1..=100).map(f64::from).collect()).unwrap();
        assert_eq!((result.p50, result.p95, result.p99, result.max), (50.0, 95.0, 99.0, 100.0));
    }

    #[test]
    fn no_samples_gives_no_distribution() {
        assert_eq!(distribution(Vec::new()), None);
    }

    #[test]
    fn only_the_first_time_is_kept() {
        let slot = AtomicU64::default();
        record_once(&slot, 2.0);
        record_once(&slot, 3.0);
        assert_eq!(read(&slot), Some(2.0));
    }

    #[test]
    fn a_time_of_zero_is_still_recorded() {
        let slot = AtomicU64::default();
        record_once(&slot, 0.0);
        assert_eq!(read(&slot), Some(0.0));
    }

    #[test]
    fn warmup_requests_are_excluded_and_unserved_ones_counted() {
        let trips = [trip(1.0, Some(1.5)), trip(10.0, Some(10.5)), trip(11.0, None)];
        let staleness = [(1.0, 99.0), (6.0, 2.0), (7.0, 4.0)];
        let summary = summarise(&trips, &staleness, 5.0);
        let measured = summary.staleness_at_offer.as_ref().expect("offers after warmup");
        assert_eq!(measured.count, 2, "offers during warmup are excluded");
        assert_eq!(measured.max, 4.0);
        assert_eq!(summary.measured_requests, 2);
        assert_eq!(summary.never_offered, 1);
        assert_eq!(summary.request_to_first_offer.unwrap().max, 0.5);
    }
}
