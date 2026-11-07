//! Latest position of each available driver, grouped by H3 Cell, for nearest-driver search.

use std::collections::{HashMap, HashSet};

use h3o::{CellIndex, LatLng, Resolution};

use crate::Position;

/// A search this many rings out has covered most of a metro, so it scans what's left instead.
const MAX_RINGS: u32 = 32;

#[derive(Debug)]
struct Entry {
    position: Position,
    cell: CellIndex,
}

#[derive(Debug)]
pub struct LocationIndex {
    resolution: Resolution,
    /// How much distance each ring is guaranteed to add. The true width of a Cell is about
    /// 1.7 edges, so one edge leaves room for Cells that are smaller than average.
    km_per_ring: f64,
    entries: HashMap<String, Entry>,
    cells: HashMap<CellIndex, HashSet<String>>,
}

fn cell_of(position: Position, resolution: Resolution) -> Option<CellIndex> {
    LatLng::new(position.lat, position.lng).ok().map(|point| point.to_cell(resolution))
}

impl LocationIndex {
    pub fn new(resolution: Resolution) -> Self {
        Self {
            resolution,
            km_per_ring: resolution.edge_length_km(),
            entries: HashMap::new(),
            cells: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn contains(&self, driver: &str) -> bool {
        self.entries.contains_key(driver)
    }

    /// Adds a driver or moves them. A position that isn't a real coordinate is refused,
    /// and leaves any existing entry as it was.
    pub fn upsert(&mut self, driver: &str, position: Position) -> bool {
        let Some(cell) = cell_of(position, self.resolution) else { return false };
        if let Some(entry) = self.entries.get_mut(driver) {
            entry.position = position;
            if entry.cell == cell {
                return true;
            }
            let old = std::mem::replace(&mut entry.cell, cell);
            Self::leave_cell(&mut self.cells, old, driver);
        } else {
            self.entries.insert(driver.to_owned(), Entry { position, cell });
        }
        self.cells.entry(cell).or_default().insert(driver.to_owned());
        true
    }

    pub fn remove(&mut self, driver: &str) {
        if let Some(entry) = self.entries.remove(driver) {
            Self::leave_cell(&mut self.cells, entry.cell, driver);
        }
    }

    fn leave_cell(cells: &mut HashMap<CellIndex, HashSet<String>>, cell: CellIndex, driver: &str) {
        if let Some(drivers) = cells.get_mut(&cell) {
            drivers.remove(driver);
            if drivers.is_empty() {
                cells.remove(&cell);
            }
        }
    }

    /// The nearest driver that `allowed` accepts. Equal distances go to the lowest id, so
    /// results don't depend on hash order.
    pub fn nearest(&self, from: Position, allowed: impl Fn(&str) -> bool) -> Option<&str> {
        self.nearest_k(from, 1, allowed).first().map(|(id, _)| *id)
    }

    /// Up to `k` nearest drivers that `allowed` accepts, nearest first, with their distance in km.
    /// Ties are broken by id, as in `nearest`.
    pub fn nearest_k(&self, from: Position, k: usize, allowed: impl Fn(&str) -> bool) -> Vec<(&str, f64)> {
        let Some(center) = cell_of(from, self.resolution) else { return Vec::new() };
        if k == 0 {
            return Vec::new();
        }
        let mut search = Search { index: self, from, allowed, k, best: Vec::with_capacity(k + 1) };
        let mut cells_left = self.cells.len();
        for ring in 0..=MAX_RINGS {
            // Rings from here out have at least `ring - 1` whole Cells between them and the search point.
            let closer_is_impossible = search.best.len() == k
                && ring > 0
                && search.best[k - 1].1 < f64::from(ring - 1) * self.km_per_ring;
            if cells_left == 0 || closer_is_impossible {
                return search.best;
            }
            let Some(cells) = center.grid_ring_fast(ring).collect::<Option<Vec<_>>>() else {
                // Rings can't be walked across a pentagon, so fall back to a full scan.
                break;
            };
            for cell in cells {
                if let Some(drivers) = self.cells.get(&cell) {
                    search.consider(drivers);
                    cells_left -= 1;
                }
            }
        }
        search.best.clear();
        for drivers in self.cells.values() {
            search.consider(drivers);
        }
        search.best
    }

    /// How many drivers are inside `area`, a Cell at this index's resolution or coarser.
    pub fn count_in(&self, area: CellIndex) -> usize {
        if area.resolution() > self.resolution {
            return 0;
        }
        area.children(self.resolution).filter_map(|cell| self.cells.get(&cell)).map(HashSet::len).sum()
    }
}

struct Search<'a, F> {
    index: &'a LocationIndex,
    from: Position,
    allowed: F,
    k: usize,
    /// Sorted nearest first, at most `k` long.
    best: Vec<(&'a str, f64)>,
}

impl<'a, F: Fn(&str) -> bool> Search<'a, F> {
    fn consider(&mut self, drivers: &'a HashSet<String>) {
        for id in drivers {
            if !(self.allowed)(id) {
                continue;
            }
            let distance = self.index.entries[id].position.distance_km(self.from);
            let before = |(best_id, best_distance): &(&str, f64)| {
                distance < *best_distance || (distance == *best_distance && id.as_str() < *best_id)
            };
            let at = self.best.iter().position(before).unwrap_or(self.best.len());
            if at < self.k {
                self.best.insert(at, (id, distance));
                self.best.truncate(self.k);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;

    const NYC: Position = Position { lat: 40.75, lng: -73.98 };

    /// A small seeded generator, so failures reproduce without another dependency.
    struct Sampler(u64);

    impl Sampler {
        fn next_f64(&mut self) -> f64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            ((z ^ (z >> 31)) >> 11) as f64 / (1u64 << 53) as f64
        }

        /// Clustered like a city: most points are close to a few hot spots.
        fn position(&mut self, spread_degrees: f64) -> Position {
            let cluster = (self.next_f64() * 4.0).floor();
            let (scale, offset) = if cluster < 3.0 {
                (spread_degrees * 0.1, cluster * spread_degrees * 0.3)
            } else {
                (spread_degrees, 0.0)
            };
            Position {
                lat: NYC.lat + offset + (self.next_f64() - 0.5) * scale,
                lng: NYC.lng - offset + (self.next_f64() - 0.5) * scale,
            }
        }
    }

    fn brute_force(
        drivers: &HashMap<String, Position>,
        from: Position,
        allowed: impl Fn(&str) -> bool,
    ) -> Option<&str> {
        let mut best: Option<(&str, f64)> = None;
        for (id, position) in drivers {
            if !allowed(id) {
                continue;
            }
            let distance = position.distance_km(from);
            if best.is_none_or(|(best_id, d)| distance < d || (distance == d && id.as_str() < best_id)) {
                best = Some((id, distance));
            }
        }
        best.map(|(id, _)| id)
    }

    fn assert_matches_brute_force(resolution: Resolution, count: usize, spread: f64, seed: u64) {
        let mut sampler = Sampler(seed);
        let mut index = LocationIndex::new(resolution);
        let mut truth = HashMap::new();
        for i in 0..count {
            let (id, position) = (format!("d{i}"), sampler.position(spread));
            index.upsert(&id, position);
            truth.insert(id, position);
        }
        // Move some and remove some, so the index isn't only tested as freshly built.
        for i in 0..count / 2 {
            let id = format!("d{}", (sampler.next_f64() * count as f64) as usize);
            if i % 4 == 0 {
                index.remove(&id);
                truth.remove(&id);
            } else {
                let position = sampler.position(spread);
                index.upsert(&id, position);
                truth.insert(id, position);
            }
        }
        assert_eq!(index.len(), truth.len());
        for query in 0..500 {
            let from = sampler.position(spread * 1.5);
            let skip = format!("d{}", query % count.max(1));
            let allowed = |id: &str| id != skip;
            assert_eq!(
                index.nearest(from, allowed),
                brute_force(&truth, from, allowed),
                "query {query} from {from:?} with {count} drivers at {resolution:?}"
            );
        }
    }

    #[test]
    fn nearest_matches_brute_force_when_dense() {
        for resolution in [Resolution::Seven, Resolution::Eight, Resolution::Nine] {
            assert_matches_brute_force(resolution, 5000, 0.4, 1);
        }
    }

    #[test]
    fn nearest_matches_brute_force_when_sparse() {
        for count in [1, 2, 5, 50] {
            assert_matches_brute_force(Resolution::Nine, count, 0.6, 2);
        }
    }

    #[test]
    fn nearest_k_matches_brute_force() {
        let mut sampler = Sampler(3);
        let mut index = LocationIndex::new(Resolution::Nine);
        let mut truth = HashMap::new();
        for i in 0..3000 {
            let (id, position) = (format!("d{i}"), sampler.position(0.4));
            index.upsert(&id, position);
            truth.insert(id, position);
        }
        for query in 0..300 {
            let from = sampler.position(0.6);
            let k = [1, 5, 8, 40][query % 4];
            let allowed = |id: &str| !id.ends_with('7');
            let mut expected: Vec<(&str, f64)> = truth
                .iter()
                .filter(|(id, _)| allowed(id))
                .map(|(id, position)| (id.as_str(), position.distance_km(from)))
                .collect();
            expected.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(b.0)));
            expected.truncate(k);
            assert_eq!(index.nearest_k(from, k, allowed), expected, "query {query} with k {k}");
        }
    }

    #[test]
    fn nearest_k_returns_everyone_when_there_are_fewer_than_k() {
        let mut index = LocationIndex::new(Resolution::Nine);
        index.upsert("d1", NYC);
        index.upsert("d2", Position { lat: 40.80, lng: -73.90 });
        let found: Vec<&str> = index.nearest_k(NYC, 5, |_| true).into_iter().map(|(id, _)| id).collect();
        assert_eq!(found, ["d1", "d2"]);
    }

    #[test]
    fn count_in_sums_drivers_inside_a_coarser_cell() {
        let mut index = LocationIndex::new(Resolution::Nine);
        index.upsert("d1", NYC);
        index.upsert("d2", Position { lat: NYC.lat + 0.0005, lng: NYC.lng });
        index.upsert("far", Position { lat: 41.5, lng: -73.0 });
        let area = cell_of(NYC, Resolution::Seven).unwrap();
        assert_eq!(index.count_in(area), 2);
        assert_eq!(index.count_in(cell_of(NYC, Resolution::Ten).unwrap()), 0, "finer than the index counts nothing");
    }

    #[test]
    fn a_far_away_driver_is_still_found() {
        let mut index = LocationIndex::new(Resolution::Nine);
        index.upsert("far", Position { lat: 42.0, lng: -71.0 });
        assert_eq!(index.nearest(NYC, |_| true), Some("far"));
    }

    #[test]
    fn empty_cells_are_dropped_when_drivers_move_or_leave() {
        let mut index = LocationIndex::new(Resolution::Eight);
        index.upsert("d1", NYC);
        index.upsert("d1", Position { lat: 40.80, lng: -73.90 });
        assert_eq!(index.cells.len(), 1);
        index.remove("d1");
        assert!(index.cells.is_empty() && index.is_empty());
        assert_eq!(index.nearest(NYC, |_| true), None);
    }

    #[test]
    fn an_invalid_position_is_refused_and_keeps_the_old_one() {
        let mut index = LocationIndex::new(Resolution::Eight);
        index.upsert("d1", NYC);
        assert!(!index.upsert("d1", Position { lat: f64::NAN, lng: 0.0 }));
        assert_eq!(index.nearest(NYC, |_| true), Some("d1"));
    }

    /// Run with: cargo test -p geo --release -- --ignored --nocapture
    #[test]
    #[ignore = "measurement, not a check"]
    fn compare_resolutions() {
        for drivers in [100_000, 25_000, 1_000, 20] {
            for resolution in [Resolution::Seven, Resolution::Eight, Resolution::Nine, Resolution::Ten] {
                let mut sampler = Sampler(7);
                let mut index = LocationIndex::new(resolution);
                let mut truth = HashMap::new();
                for i in 0..drivers {
                    let (id, position) = (format!("d{i}"), sampler.position(0.4));
                    index.upsert(&id, position);
                    truth.insert(id, position);
                }
                let queries: Vec<Position> = (0..500).map(|_| sampler.position(0.4)).collect();
                let timed = |search: &dyn Fn(Position)| {
                    let started = Instant::now();
                    queries.iter().for_each(|from| search(*from));
                    started.elapsed().as_nanos() / queries.len() as u128
                };
                let nearest = timed(&|from| {
                    std::hint::black_box(index.nearest(from, |_| true));
                });
                let scan = timed(&|from| {
                    std::hint::black_box(brute_force(&truth, from, |_| true));
                });
                println!("{drivers} drivers, {resolution:?}: nearest {nearest} ns, full scan {scan} ns");
            }
        }
    }
}
