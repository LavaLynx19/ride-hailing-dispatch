//! Draws a shard map from demand: each resolution-7 Cell weighs the Ride Requests expected in
//! it, from requests per zone spread over that zone's weighted pickup points.
//!
//! usage: build_shard_map PICKUP_POINTS_CSV ZONE_DEMAND_CSV SHARDS VERSION [HOST_PATTERN]
//! HOST_PATTERN names each Shard's host, with `{id}` replaced by its number; default `shard-{id}`.
//! Ports match deploy/: drivers 7001-7007 odd, riders 7002-7008 even, peers 7100.

use std::collections::{BTreeMap, HashMap};

use geo::Position;
use geo::shard_map::{MAP_RESOLUTION, ShardAddress, ShardMap, partition};

fn read(path: &str) -> Result<String, String> {
    std::fs::read_to_string(path).map_err(|error| format!("{path}: {error}"))
}

fn main() -> Result<(), String> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let usage = "usage: build_shard_map PICKUP_POINTS_CSV ZONE_DEMAND_CSV SHARDS VERSION [HOST_PATTERN]";
    let [points, demand, shards, version, rest @ ..] = args.as_slice() else { return Err(usage.into()) };
    let shards: u16 = shards.parse().map_err(|_| usage)?;
    let version: u32 = version.parse().map_err(|_| usage)?;
    let host_pattern = rest.first().map_or("shard-{id}", String::as_str);

    let mut requests: HashMap<u16, f64> = HashMap::new();
    for line in read(demand)?.lines().skip(1) {
        let (zone, count) = line.split_once(',').ok_or_else(|| format!("bad demand row: {line}"))?;
        requests.insert(zone.parse().map_err(|_| format!("bad zone: {line}"))?, count.parse().map_err(|_| format!("bad count: {line}"))?);
    }
    let mut weights: BTreeMap<geo::CellIndex, f64> = BTreeMap::new();
    for line in read(points)?.lines().skip(1) {
        let fields: Vec<&str> = line.split(',').collect();
        let [zone, lat, lng, weight] = fields.as_slice() else { return Err(format!("bad point row: {line}")) };
        let parse = |value: &str| value.parse::<f64>().map_err(|_| format!("bad point row: {line}"));
        let zone: u16 = zone.parse().map_err(|_| format!("bad point row: {line}"))?;
        let Some(cell) = (Position { lat: parse(lat)?, lng: parse(lng)? }).cell(MAP_RESOLUTION) else { continue };
        *weights.entry(cell).or_default() += requests.get(&zone).copied().unwrap_or(0.0) * parse(weight)?;
    }

    let weights: Vec<_> = weights.into_iter().collect();
    let cells = partition(&weights, shards)?;
    let addresses = (0..shards)
        .map(|id| {
            let host = host_pattern.replace("{id}", &id.to_string());
            ShardAddress {
                id,
                peer: format!("http://{host}:7100"),
                drivers: [7001, 7003, 7005, 7007].iter().map(|port| format!("ws://{host}:{port}")).collect(),
                riders: [7002, 7004, 7006, 7008].iter().map(|port| format!("ws://{host}:{port}")).collect(),
            }
        })
        .collect();
    let map = ShardMap::new(version, addresses, cells)?;

    let total: f64 = weights.iter().map(|w| w.1).sum();
    for id in 0..shards {
        let share: f64 = weights.iter().filter(|(cell, _)| map.cells()[cell] == id).map(|w| w.1).sum::<f64>() / total;
        let count = map.cells().values().filter(|shard| **shard == id).count();
        eprintln!("Shard {id}: {count} Cells, {:.1}% of demand", share * 100.0);
    }
    print!("# Built by build_shard_map from {points} and {demand}.\n{}", map.to_text());
    Ok(())
}
