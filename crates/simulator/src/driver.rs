use crate::position::Position;
use crate::rng::Rng;

/// Assumed, not measured: the data doesn't record declined offers.
const ACCEPT_AT_ZERO_KM: f64 = 0.95;
const ACCEPT_HALF_DISTANCE_KM: f64 = 5.0;
const MIN_RESPONSE_SECONDS: f64 = 2.0;
const MAX_RESPONSE_SECONDS: f64 = 15.0;

pub fn accept_probability(pickup_km: f64) -> f64 {
    ACCEPT_AT_ZERO_KM / (1.0 + (pickup_km / ACCEPT_HALF_DISTANCE_KM).powi(2))
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OfferResponse {
    pub accept: bool,
    pub after_seconds: f64,
}

pub fn respond_to_offer(rng: &mut Rng, pickup_km: f64) -> OfferResponse {
    OfferResponse {
        accept: rng.next_f64() < accept_probability(pickup_km),
        after_seconds: MIN_RESPONSE_SECONDS + rng.next_f64() * (MAX_RESPONSE_SECONDS - MIN_RESPONSE_SECONDS),
    }
}

/// One straight-line movement between two times.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Leg {
    pub from: Position,
    pub to: Position,
    pub start_seconds: f64,
    pub end_seconds: f64,
}

impl Leg {
    pub fn position_at(&self, now_seconds: f64) -> Position {
        let duration = self.end_seconds - self.start_seconds;
        if duration <= 0.0 {
            return self.to;
        }
        self.from.towards(self.to, (now_seconds - self.start_seconds) / duration)
    }
}

/// Where a recorded trip ends.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Dropoff {
    Inside(Position),
    /// Outside the city or unknown: leave through `exit`, go out of the map for
    /// the rest of the recorded time, then return through the same point.
    Outside { exit: Position },
}

/// The legs for a trip. Total time always equals the recorded trip time.
pub fn trip_legs(pickup: Position, dropoff: Dropoff, start_seconds: f64, trip_seconds: f64) -> Vec<Leg> {
    let end_seconds = start_seconds + trip_seconds;
    match dropoff {
        Dropoff::Inside(to) => vec![Leg { from: pickup, to, start_seconds, end_seconds }],
        Dropoff::Outside { exit } => {
            let midpoint = start_seconds + trip_seconds / 2.0;
            vec![
                Leg { from: pickup, to: exit, start_seconds, end_seconds: midpoint },
                Leg { from: exit, to: exit, start_seconds: midpoint, end_seconds },
            ]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: Position = Position { lat: 40.70, lng: -74.00 };
    const B: Position = Position { lat: 40.80, lng: -73.90 };

    #[test]
    fn farther_pickups_are_accepted_less_often() {
        assert!(accept_probability(1.0) > accept_probability(5.0));
        assert!(accept_probability(5.0) > accept_probability(20.0));
        assert!((accept_probability(5.0) - ACCEPT_AT_ZERO_KM / 2.0).abs() < 1e-9);
    }

    #[test]
    fn same_seed_gives_same_response() {
        let (mut a, mut b) = (Rng::stream(1, 9), Rng::stream(1, 9));
        assert_eq!(respond_to_offer(&mut a, 3.0), respond_to_offer(&mut b, 3.0));
    }

    #[test]
    fn observed_accept_rate_matches_probability() {
        let mut rng = Rng::new(3);
        let accepted = (0..100_000).filter(|_| respond_to_offer(&mut rng, 5.0).accept).count();
        assert!((accepted as f64 / 100_000.0 - accept_probability(5.0)).abs() < 0.01);
    }

    #[test]
    fn responses_arrive_within_bounds() {
        let mut rng = Rng::new(3);
        assert!((0..10_000).all(|_| {
            let seconds = respond_to_offer(&mut rng, 1.0).after_seconds;
            (MIN_RESPONSE_SECONDS..MAX_RESPONSE_SECONDS).contains(&seconds)
        }));
    }

    #[test]
    fn inside_trip_moves_to_dropoff() {
        let legs = trip_legs(A, Dropoff::Inside(B), 10.0, 100.0);
        assert_eq!(legs.len(), 1);
        assert_eq!(legs[0].position_at(110.0), B);
    }

    #[test]
    fn outside_trip_waits_at_exit_for_the_recorded_time() {
        let legs = trip_legs(A, Dropoff::Outside { exit: B }, 0.0, 100.0);
        assert_eq!(legs[0].position_at(50.0), B);
        assert_eq!(legs[1].position_at(80.0), B);
        assert_eq!(legs[1].end_seconds, 100.0);
    }
}
