//! The versioned Cell → Shard map: which Shard owns each resolution-7 Cell, and where each
//! Shard can be reached. Every Shard holds the same versions; see ARCHITECTURE.md → Sharding.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::str::FromStr;

use h3o::{CellIndex, LatLng, Resolution};

use crate::Position;

/// About 5 km² per Cell: fine enough to balance demand, since one resolution-6 Cell in Midtown
/// holds about a tenth of it. The widening supply area uses the same resolution.
pub const MAP_RESOLUTION: Resolution = Resolution::Seven;

pub type ShardId = u16;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardAddress {
    pub id: ShardId,
    /// gRPC address for other Shards.
    pub peer: String,
    /// WebSocket addresses for drivers and riders.
    pub drivers: Vec<String>,
    pub riders: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardMap {
    version: u32,
    shards: Vec<ShardAddress>,
    cells: BTreeMap<CellIndex, ShardId>,
}

impl ShardMap {
    /// Shards must be numbered 0.. in order, and every Cell must name one of them.
    pub fn new(version: u32, shards: Vec<ShardAddress>, cells: BTreeMap<CellIndex, ShardId>) -> Result<Self, String> {
        if version == 0 {
            return Err("map version must be at least 1".into());
        }
        if shards.is_empty() || cells.is_empty() {
            return Err("a map needs at least one Shard and one Cell".into());
        }
        if let Some((index, shard)) = shards.iter().enumerate().find(|(index, shard)| usize::from(shard.id) != *index) {
            return Err(format!("Shard {} listed in position {index}; Shards must be numbered 0.. in order", shard.id));
        }
        for (cell, shard) in &cells {
            if cell.resolution() != MAP_RESOLUTION {
                return Err(format!("Cell {cell} isn't at resolution {MAP_RESOLUTION}"));
            }
            if usize::from(*shard) >= shards.len() {
                return Err(format!("Cell {cell} names Shard {shard}, which isn't listed"));
            }
        }
        Ok(Self { version, shards, cells })
    }

    pub fn version(&self) -> u32 {
        self.version
    }

    pub fn shards(&self) -> &[ShardAddress] {
        &self.shards
    }

    pub fn cells(&self) -> &BTreeMap<CellIndex, ShardId> {
        &self.cells
    }

    pub fn shard(&self, id: ShardId) -> Option<&ShardAddress> {
        self.shards.get(usize::from(id))
    }

    /// The Shard owning `position`. A position outside every mapped Cell belongs to the Shard of
    /// the mapped Cell whose centre is nearest. `None` only for a position that isn't a real
    /// coordinate.
    pub fn owner(&self, position: Position) -> Option<ShardId> {
        let cell = position.cell(MAP_RESOLUTION)?;
        if let Some(shard) = self.cells.get(&cell) {
            return Some(*shard);
        }
        // Few hundred Cells, and only positions off the map get here.
        self.cells
            .iter()
            .map(|(cell, shard)| (centre(*cell).distance_km(position), *cell, *shard))
            .min_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)))
            .map(|(_, _, shard)| shard)
    }

    /// The text form, which `parse` reads back.
    pub fn to_text(&self) -> String {
        let mut out = format!("version {}\n", self.version);
        for shard in &self.shards {
            writeln!(out, "shard {} peer {} drivers {} riders {}", shard.id, shard.peer, shard.drivers.join(","), shard.riders.join(","))
                .unwrap();
        }
        for (cell, shard) in &self.cells {
            writeln!(out, "cell {cell} {shard}").unwrap();
        }
        out
    }

    /// Reads the text form. Blank lines and lines starting with `#` are skipped.
    pub fn parse(text: &str) -> Result<Self, String> {
        let mut version = None;
        let mut shards = Vec::new();
        let mut cells = BTreeMap::new();
        for (number, line) in text.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let bad = || format!("shard map line {}: {line}", number + 1);
            let words: Vec<&str> = line.split_whitespace().collect();
            match words.as_slice() {
                ["version", value] => version = Some(value.parse().map_err(|_| bad())?),
                ["shard", id, "peer", peer, "drivers", drivers, "riders", riders] => shards.push(ShardAddress {
                    id: id.parse().map_err(|_| bad())?,
                    peer: (*peer).to_owned(),
                    drivers: drivers.split(',').map(str::to_owned).collect(),
                    riders: riders.split(',').map(str::to_owned).collect(),
                }),
                ["cell", cell, shard] => {
                    let cell = CellIndex::from_str(cell).map_err(|_| bad())?;
                    if cells.insert(cell, shard.parse().map_err(|_| bad())?).is_some() {
                        return Err(format!("{}: Cell listed twice", bad()));
                    }
                }
                _ => return Err(bad()),
            }
        }
        Self::new(version.ok_or("shard map has no version line")?, shards, cells)
    }
}

/// The centre of a Cell.
pub fn centre(cell: CellIndex) -> Position {
    let point = LatLng::from(cell);
    Position { lat: point.lat(), lng: point.lng() }
}

/// Splits weighted Cells into `shards` regions of roughly equal weight by cutting in half
/// again and again, each time across the wider side of the region, at the weighted median.
/// Each region is a band or block of the map, so it stays in one piece apart from water.
/// `shards` must be a power of two. The result doesn't depend on input order.
pub fn partition(weights: &[(CellIndex, f64)], shards: u16) -> Result<BTreeMap<CellIndex, ShardId>, String> {
    if shards == 0 || !shards.is_power_of_two() {
        return Err(format!("Shard count must be a power of two, not {shards}"));
    }
    let mut cells: Vec<(CellIndex, Position, f64)> = weights.iter().map(|(cell, weight)| (*cell, centre(*cell), weight.max(0.0))).collect();
    cells.sort_by_key(|(cell, _, _)| *cell);
    cells.dedup_by_key(|(cell, _, _)| *cell);
    if cells.len() < usize::from(shards) {
        return Err(format!("{} Cells can't make {shards} Shards", cells.len()));
    }
    let mut owner = BTreeMap::new();
    split(&mut cells, 0, shards, &mut owner);
    Ok(owner)
}

fn split(cells: &mut [(CellIndex, Position, f64)], first: ShardId, count: ShardId, owner: &mut BTreeMap<CellIndex, ShardId>) {
    if count == 1 {
        owner.extend(cells.iter().map(|(cell, _, _)| (*cell, first)));
        return;
    }
    let (min_lat, max_lat) = bounds(cells.iter().map(|c| c.1.lat));
    let (min_lng, max_lng) = bounds(cells.iter().map(|c| c.1.lng));
    let mid_lat = (min_lat + max_lat) / 2.0;
    let north_south_km = Position { lat: min_lat, lng: 0.0 }.distance_km(Position { lat: max_lat, lng: 0.0 });
    let east_west_km = Position { lat: mid_lat, lng: min_lng }.distance_km(Position { lat: mid_lat, lng: max_lng });
    let along = |c: &(CellIndex, Position, f64)| if north_south_km >= east_west_km { c.1.lat } else { c.1.lng };
    cells.sort_by(|a, b| along(a).total_cmp(&along(b)).then(a.0.cmp(&b.0)));

    // Cut where the running weight first reaches half, leaving each side enough Cells for its Shards.
    let half = count / 2;
    let total: f64 = cells.iter().map(|c| c.2).sum();
    let mut running = 0.0;
    let mut cut = cells.len() - usize::from(half);
    for (index, c) in cells.iter().enumerate() {
        running += c.2;
        if running >= total / 2.0 {
            cut = index + 1;
            break;
        }
    }
    let cut = cut.clamp(usize::from(half), cells.len() - usize::from(half));
    let (low, high) = cells.split_at_mut(cut);
    split(low, first, half, owner);
    split(high, first + half, half, owner);
}

fn bounds(values: impl Iterator<Item = f64>) -> (f64, f64) {
    values.fold((f64::INFINITY, f64::NEG_INFINITY), |(low, high), v| (low.min(v), high.max(v)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cell(lat: f64, lng: f64) -> CellIndex {
        Position { lat, lng }.cell(MAP_RESOLUTION).unwrap()
    }

    fn address(id: ShardId) -> ShardAddress {
        ShardAddress {
            id,
            peer: format!("http://shard-{id}:7100"),
            drivers: vec![format!("ws://shard-{id}:7001"), format!("ws://shard-{id}:7003")],
            riders: vec![format!("ws://shard-{id}:7002")],
        }
    }

    /// A grid of points over part of the NYC area, deduplicated into Cells.
    fn grid() -> Vec<CellIndex> {
        let mut cells: Vec<CellIndex> = (0..20)
            .flat_map(|i| (0..20).map(move |j| cell(40.50 + f64::from(i) * 0.01, -74.25 + f64::from(j) * 0.012)))
            .collect();
        cells.sort();
        cells.dedup();
        cells
    }

    fn two_shard_map() -> ShardMap {
        let cells = [(cell(40.70, -74.00), 0), (cell(40.80, -73.90), 1)].into_iter().collect();
        ShardMap::new(3, vec![address(0), address(1)], cells).unwrap()
    }

    #[test]
    fn text_form_reads_back_the_same_map() {
        let map = two_shard_map();
        assert_eq!(ShardMap::parse(&map.to_text()).unwrap(), map);
        assert!(map.to_text().starts_with("version 3\nshard 0 peer http://shard-0:7100 drivers ws://shard-0:7001,ws://shard-0:7003"));
    }

    #[test]
    fn bad_maps_are_refused_with_the_reason() {
        let cells = || [(cell(40.70, -74.00), 0)].into_iter().collect::<BTreeMap<_, _>>();
        assert!(ShardMap::new(0, vec![address(0)], cells()).unwrap_err().contains("version"));
        assert!(ShardMap::new(1, vec![address(1)], cells()).unwrap_err().contains("in order"));
        let unknown = [(cell(40.70, -74.00), 4)].into_iter().collect();
        assert!(ShardMap::new(1, vec![address(0)], unknown).unwrap_err().contains("isn't listed"));
        let fine = [(Position { lat: 40.7, lng: -74.0 }.cell(Resolution::Eight).unwrap(), 0)].into_iter().collect();
        assert!(ShardMap::new(1, vec![address(0)], fine).unwrap_err().contains("resolution"));
        assert!(ShardMap::parse("version 1\ncell nonsense 0\n").unwrap_err().contains("line 2"));
        let line = format!("cell {} 0\n", cell(40.7, -74.0));
        assert!(ShardMap::parse(&format!("version 1\n{line}{line}")).unwrap_err().contains("twice"));
    }

    #[test]
    fn owner_is_the_mapped_cell_or_else_the_nearest_one() {
        let map = two_shard_map();
        assert_eq!(map.owner(Position { lat: 40.70, lng: -74.00 }), Some(0));
        assert_eq!(map.owner(Position { lat: 40.80, lng: -73.90 }), Some(1));
        // Off the map: nearer the second Cell.
        assert_eq!(map.owner(Position { lat: 41.20, lng: -73.60 }), Some(1));
        assert_eq!(map.owner(Position { lat: 40.20, lng: -74.40 }), Some(0));
        assert_eq!(map.owner(Position { lat: f64::NAN, lng: 0.0 }), None);
    }

    #[test]
    fn partition_balances_weight_and_ignores_input_order() {
        let cells = grid();
        // Heavier towards the south-west, as if there were a busy downtown.
        let weights: Vec<(CellIndex, f64)> = cells
            .iter()
            .map(|c| {
                let p = centre(*c);
                (*c, 1.0 + 50.0 * (-((p.lat - 40.6).powi(2) + (p.lng + 74.1).powi(2)) * 100.0).exp())
            })
            .collect();
        let total: f64 = weights.iter().map(|w| w.1).sum();
        for shards in [1, 2, 4] {
            let owner = partition(&weights, shards).unwrap();
            assert_eq!(owner.len(), cells.len());
            for id in 0..shards {
                let share: f64 = weights.iter().filter(|(c, _)| owner[c] == id).map(|w| w.1).sum::<f64>() / total;
                let fair = 1.0 / f64::from(shards);
                assert!((share - fair).abs() < 0.05, "{shards} Shards: Shard {id} has {share:.3}");
            }
            let mut reversed = weights.clone();
            reversed.reverse();
            assert_eq!(partition(&reversed, shards).unwrap(), owner);
        }
    }

    #[test]
    fn partition_regions_are_contiguous() {
        let cells = grid();
        let weights: Vec<(CellIndex, f64)> = cells.iter().map(|c| (*c, 1.0)).collect();
        let owner = partition(&weights, 4).unwrap();
        for id in 0..4 {
            let region: Vec<CellIndex> = owner.iter().filter(|(_, s)| **s == id).map(|(c, _)| *c).collect();
            // Flood fill from one Cell through neighbours in the same region.
            let mut seen = std::collections::BTreeSet::from([region[0]]);
            let mut stack = vec![region[0]];
            while let Some(next) = stack.pop() {
                for neighbour in next.grid_disk::<Vec<_>>(1) {
                    if owner.get(&neighbour) == Some(&id) && seen.insert(neighbour) {
                        stack.push(neighbour);
                    }
                }
            }
            assert_eq!(seen.len(), region.len(), "Shard {id} is in pieces");
        }
    }

    #[test]
    fn partition_refuses_counts_it_cant_make() {
        let weights = [(cell(40.7, -74.0), 1.0)];
        assert!(partition(&weights, 3).unwrap_err().contains("power of two"));
        assert!(partition(&weights, 2).unwrap_err().contains("can't make"));
    }
}
