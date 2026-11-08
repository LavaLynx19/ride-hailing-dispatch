mod index;
pub mod shard_map;

pub use h3o::{CellIndex, Resolution};
pub use index::LocationIndex;

const EARTH_RADIUS_KM: f64 = 6371.0;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Position {
    pub lat: f64,
    pub lng: f64,
}

impl Position {
    pub fn is_finite(self) -> bool {
        self.lat.is_finite() && self.lng.is_finite()
    }

    /// The H3 Cell containing this position, or `None` for a position that isn't a real coordinate.
    pub fn cell(self, resolution: Resolution) -> Option<CellIndex> {
        h3o::LatLng::new(self.lat, self.lng).ok().map(|point| point.to_cell(resolution))
    }

    pub fn distance_km(self, other: Position) -> f64 {
        let (lat1, lat2) = (self.lat.to_radians(), other.lat.to_radians());
        let d_lat = lat2 - lat1;
        let d_lng = (other.lng - self.lng).to_radians();
        let a = (d_lat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (d_lng / 2.0).sin().powi(2);
        2.0 * EARTH_RADIUS_KM * a.sqrt().asin()
    }

    /// The point `fraction` of the way to `other`; close enough to straight over a city.
    pub fn towards(self, other: Position, fraction: f64) -> Position {
        let fraction = fraction.clamp(0.0, 1.0);
        Position {
            lat: self.lat + (other.lat - self.lat) * fraction,
            lng: self.lng + (other.lng - self.lng) * fraction,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_degree_of_latitude_is_about_111_km() {
        let a = Position { lat: 40.0, lng: -74.0 };
        let b = Position { lat: 41.0, lng: -74.0 };
        assert!((a.distance_km(b) - 111.2).abs() < 0.1);
    }

    #[test]
    fn towards_clamps_at_the_destination() {
        let a = Position { lat: 40.0, lng: -74.0 };
        let b = Position { lat: 41.0, lng: -73.0 };
        assert_eq!(a.towards(b, 2.0), b);
    }
}
