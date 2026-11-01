//! Baseline matcher: each Ride Request is offered, in arrival order, to the nearest
//! idle driver. It's deliberately naive, so later policies have something to beat.

use std::collections::{HashMap, HashSet, VecDeque};

use geo::Position;

pub const OFFER_TIMEOUT_SECONDS: f64 = 20.0;

#[derive(Debug, Clone, PartialEq)]
pub enum Effect {
    Offer { driver: String, offer: String, ride: String, pickup: Position, expires_at: f64 },
    OfferGone { driver: String, offer: String },
    TripAssigned { driver: String, trip: String, ride: String },
    TripStatus { ride: String, trip: String, status: &'static str },
}

#[derive(Debug)]
struct Driver {
    position: Position,
    online: bool,
}

#[derive(Debug)]
struct Ride {
    pickup: Position,
    tried: HashSet<String>,
}

#[derive(Debug)]
struct OpenOffer {
    driver: String,
    ride: String,
    expires_at: f64,
}

#[derive(Debug, Default)]
pub struct Dispatcher {
    drivers: HashMap<String, Driver>,
    idle: HashSet<String>,
    rides: HashMap<String, Ride>,
    waiting: VecDeque<String>,
    offers: HashMap<String, OpenOffer>,
    trips: HashMap<String, (String, String)>,
    next_id: u64,
}

impl Dispatcher {
    pub fn location_ping(&mut self, driver: &str, position: Position, now: f64) -> Vec<Effect> {
        let entry = self.drivers.entry(driver.to_owned()).or_insert(Driver { position, online: false });
        entry.position = position;
        if entry.online {
            return Vec::new();
        }
        entry.online = true;
        let busy = self.offers.values().any(|offer| offer.driver == driver)
            || self.trips.values().any(|(trip_driver, _)| trip_driver == driver);
        if !busy {
            self.idle.insert(driver.to_owned());
        }
        self.dispatch(now)
    }

    pub fn driver_disconnected(&mut self, driver: &str) {
        if let Some(entry) = self.drivers.get_mut(driver) {
            entry.online = false;
        }
        self.idle.remove(driver);
    }

    pub fn ride_request(&mut self, ride: String, pickup: Position, now: f64) -> Vec<Effect> {
        self.rides.insert(ride.clone(), Ride { pickup, tried: HashSet::new() });
        self.waiting.push_back(ride);
        self.dispatch(now)
    }

    pub fn accept(&mut self, driver: &str, offer_id: &str, now: f64) -> Vec<Effect> {
        let matches = self.offers.get(offer_id).is_some_and(|offer| offer.driver == driver);
        let Some(offer) = matches.then(|| self.offers.remove(offer_id)).flatten() else {
            return vec![Effect::OfferGone { driver: driver.to_owned(), offer: offer_id.to_owned() }];
        };
        self.rides.remove(&offer.ride);
        self.next_id += 1;
        let trip = format!("t{}", self.next_id);
        self.trips.insert(trip.clone(), (driver.to_owned(), offer.ride.clone()));
        let mut effects = vec![
            Effect::TripAssigned { driver: driver.to_owned(), trip: trip.clone(), ride: offer.ride.clone() },
            Effect::TripStatus { ride: offer.ride, trip, status: "assigned" },
        ];
        effects.extend(self.dispatch(now));
        effects
    }

    pub fn decline(&mut self, driver: &str, offer_id: &str, now: f64) -> Vec<Effect> {
        if self.offers.get(offer_id).is_some_and(|offer| offer.driver == driver) {
            self.reopen(offer_id);
        }
        self.dispatch(now)
    }

    pub fn trip_completed(&mut self, driver: &str, trip: &str, now: f64) -> Vec<Effect> {
        let owned = self.trips.get(trip).is_some_and(|(trip_driver, _)| trip_driver == driver);
        let Some((_, ride)) = owned.then(|| self.trips.remove(trip)).flatten() else {
            return Vec::new();
        };
        self.make_idle(driver);
        let mut effects = vec![Effect::TripStatus { ride, trip: trip.to_owned(), status: "completed" }];
        effects.extend(self.dispatch(now));
        effects
    }

    /// Expires unanswered offers, and retries waiting rides.
    pub fn tick(&mut self, now: f64) -> Vec<Effect> {
        let expired: Vec<String> = self
            .offers
            .iter()
            .filter(|(_, offer)| offer.expires_at <= now)
            .map(|(id, _)| id.clone())
            .collect();
        let mut effects = Vec::new();
        for id in expired {
            if let Some(driver) = self.reopen(&id) {
                effects.push(Effect::OfferGone { driver, offer: id });
            }
        }
        effects.extend(self.dispatch(now));
        effects
    }

    /// Returns the offer's ride to the front of the queue, and frees its driver.
    fn reopen(&mut self, offer_id: &str) -> Option<String> {
        let offer = self.offers.remove(offer_id)?;
        if let Some(ride) = self.rides.get_mut(&offer.ride) {
            ride.tried.insert(offer.driver.clone());
        }
        self.waiting.push_front(offer.ride);
        self.make_idle(&offer.driver);
        Some(offer.driver)
    }

    fn make_idle(&mut self, driver: &str) {
        if self.drivers.get(driver).is_some_and(|entry| entry.online) {
            self.idle.insert(driver.to_owned());
        }
    }

    fn nearest_idle(&self, ride: &Ride) -> Option<String> {
        let mut best: Option<(&String, f64)> = None;
        for id in &self.idle {
            if ride.tried.contains(id) {
                continue;
            }
            let distance = self.drivers[id].position.distance_km(ride.pickup);
            // The id breaks ties, so the result doesn't depend on hash order.
            let closer = best.is_none_or(|(best_id, best_distance)| {
                distance < best_distance || (distance == best_distance && id < best_id)
            });
            if closer {
                best = Some((id, distance));
            }
        }
        best.map(|(id, _)| id.clone())
    }

    fn dispatch(&mut self, now: f64) -> Vec<Effect> {
        let mut effects = Vec::new();
        let mut still_waiting = VecDeque::new();
        while let Some(ride_id) = self.waiting.pop_front() {
            if self.idle.is_empty() {
                still_waiting.push_back(ride_id);
                continue;
            }
            let Some(ride) = self.rides.get(&ride_id) else { continue };
            let pickup = ride.pickup;
            let Some(driver) = self.nearest_idle(ride) else {
                // Every idle driver has refused: let them be asked again next time.
                if let Some(ride) = self.rides.get_mut(&ride_id) {
                    ride.tried.clear();
                }
                still_waiting.push_back(ride_id);
                continue;
            };
            self.idle.remove(&driver);
            self.next_id += 1;
            let offer = format!("o{}", self.next_id);
            let expires_at = now + OFFER_TIMEOUT_SECONDS;
            self.offers.insert(
                offer.clone(),
                OpenOffer { driver: driver.clone(), ride: ride_id.clone(), expires_at },
            );
            effects.push(Effect::Offer { driver, offer, ride: ride_id, pickup, expires_at });
        }
        self.waiting = still_waiting;
        effects
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const R1: Position = Position { lat: 40.700, lng: -74.0 };
    const R2: Position = Position { lat: 40.720, lng: -74.0 };
    // Each 0.009 degrees of latitude is about one km.
    const D1: Position = Position { lat: 40.709, lng: -74.0 };
    const D2: Position = Position { lat: 40.790, lng: -74.0 };

    fn offer_of(effects: &[Effect]) -> (String, String, String) {
        effects
            .iter()
            .find_map(|effect| match effect {
                Effect::Offer { driver, offer, ride, .. } => Some((driver.clone(), offer.clone(), ride.clone())),
                _ => None,
            })
            .expect("an offer")
    }

    #[test]
    fn offers_the_nearest_idle_driver() {
        let mut dispatcher = Dispatcher::default();
        dispatcher.location_ping("far", D2, 0.0);
        dispatcher.location_ping("near", D1, 0.0);
        let (driver, _, ride) = offer_of(&dispatcher.ride_request("r1".into(), R1, 0.0));
        assert_eq!((driver.as_str(), ride.as_str()), ("near", "r1"));
    }

    #[test]
    fn baseline_is_greedy_even_when_a_swap_would_be_better() {
        let mut dispatcher = Dispatcher::default();
        dispatcher.location_ping("d1", D1, 0.0);
        dispatcher.location_ping("d2", D2, 0.0);
        let (first, ..) = offer_of(&dispatcher.ride_request("r1".into(), R1, 0.0));
        let (second, ..) = offer_of(&dispatcher.ride_request("r2".into(), R2, 0.0));
        assert_eq!((first.as_str(), second.as_str()), ("d1", "d2"));
    }

    #[test]
    fn accepting_assigns_one_trip_and_a_second_accept_is_refused() {
        let mut dispatcher = Dispatcher::default();
        dispatcher.location_ping("d1", D1, 0.0);
        let (_, offer, _) = offer_of(&dispatcher.ride_request("r1".into(), R1, 0.0));
        let effects = dispatcher.accept("d1", &offer, 1.0);
        assert!(matches!(effects[0], Effect::TripAssigned { .. }));
        assert_eq!(
            dispatcher.accept("d1", &offer, 2.0),
            vec![Effect::OfferGone { driver: "d1".into(), offer }]
        );
    }

    #[test]
    fn an_offer_cannot_be_accepted_by_another_driver() {
        let mut dispatcher = Dispatcher::default();
        dispatcher.location_ping("d1", D1, 0.0);
        dispatcher.location_ping("d2", D2, 0.0);
        let (_, offer, _) = offer_of(&dispatcher.ride_request("r1".into(), R1, 0.0));
        assert!(matches!(dispatcher.accept("d2", &offer, 1.0)[0], Effect::OfferGone { .. }));
    }

    #[test]
    fn a_driver_on_a_trip_gets_no_offers_until_it_completes() {
        let mut dispatcher = Dispatcher::default();
        dispatcher.location_ping("d1", D1, 0.0);
        let (_, offer, _) = offer_of(&dispatcher.ride_request("r1".into(), R1, 0.0));
        let trip = dispatcher
            .accept("d1", &offer, 1.0)
            .into_iter()
            .find_map(|effect| match effect {
                Effect::TripAssigned { trip, .. } => Some(trip),
                _ => None,
            })
            .unwrap();
        assert!(dispatcher.ride_request("r2".into(), R2, 2.0).is_empty());
        let effects = dispatcher.trip_completed("d1", &trip, 3.0);
        assert_eq!(offer_of(&effects).2, "r2");
    }

    #[test]
    fn a_decline_moves_to_the_next_nearest_driver() {
        let mut dispatcher = Dispatcher::default();
        dispatcher.location_ping("d1", D1, 0.0);
        dispatcher.location_ping("d2", D2, 0.0);
        let (_, offer, _) = offer_of(&dispatcher.ride_request("r1".into(), R1, 0.0));
        assert_eq!(offer_of(&dispatcher.decline("d1", &offer, 1.0)).0, "d2");
    }

    #[test]
    fn an_unanswered_offer_expires_and_moves_on() {
        let mut dispatcher = Dispatcher::default();
        dispatcher.location_ping("d1", D1, 0.0);
        dispatcher.location_ping("d2", D2, 0.0);
        let (_, offer, _) = offer_of(&dispatcher.ride_request("r1".into(), R1, 0.0));
        assert!(dispatcher.tick(OFFER_TIMEOUT_SECONDS - 1.0).is_empty());
        let effects = dispatcher.tick(OFFER_TIMEOUT_SECONDS);
        assert!(effects.contains(&Effect::OfferGone { driver: "d1".into(), offer }));
        assert_eq!(offer_of(&effects).0, "d2");
    }

    #[test]
    fn a_ride_refused_by_everyone_is_retried() {
        let mut dispatcher = Dispatcher::default();
        dispatcher.location_ping("d1", D1, 0.0);
        let (_, offer, _) = offer_of(&dispatcher.ride_request("r1".into(), R1, 0.0));
        assert!(dispatcher.decline("d1", &offer, 1.0).is_empty());
        assert_eq!(offer_of(&dispatcher.tick(2.0)).0, "d1");
    }

    #[test]
    fn a_disconnected_driver_gets_no_offers() {
        let mut dispatcher = Dispatcher::default();
        dispatcher.location_ping("d1", D1, 0.0);
        dispatcher.driver_disconnected("d1");
        assert!(dispatcher.ride_request("r1".into(), R1, 1.0).is_empty());
        assert_eq!(offer_of(&dispatcher.location_ping("d1", D1, 2.0)).0, "d1");
    }
}
