//! Drives a plan against a server over WebSockets, in wall-clock time.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use prost::Message as _;
use protocol::client::{
    Accept, Decline, DriverToServer, LatLng, LocationPing, Offer, QuoteRequest, RideRequest, RiderToServer,
    ServerToDriver, ServerToRider, TripEvent, TripEventKind, driver_to_server, rider_to_server, server_to_driver,
    server_to_rider,
};
use tokio::time::{Instant, sleep_until};
use tokio_tungstenite::connect_async_with_config;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::Message;

use checker::{DriverDoubleBooked, DriverTold, Observations, RiderTold};

use crate::driver::{Leg, respond_to_offer, trip_legs};
use crate::metrics::{TripTimes, record_once};
use crate::plan::{DriverPlan, Plan, TripPlan};
use crate::position::Position;
use crate::rng::Rng;

/// Assumed, not measured: drivers head to the pickup at a constant speed.
const PICKUP_SPEED_KM_PER_SECOND: f64 = 25.0 / 3600.0;
const RECONNECT_SECONDS: f64 = 1.0;
const RIDER_GIVES_UP_SECONDS: f64 = 4.0 * 3600.0;
const SECONDS_PER_HOUR: f64 = 3600.0;
/// Keeps silence decisions off the stream that drives offer responses.
const SILENCE_STREAM_SALT: u64 = 0x5113_7ce0_0000_0000;
const FAILURES_KEPT: usize = 50;

/// Assumed, not measured: some drivers go quiet once in a run, keeping the socket open but
/// sending no pings and answering no offers. Only idle drivers go quiet; mid-trip silence
/// belongs to Reconciliation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Silence {
    pub probability: f64,
    pub min_seconds: f64,
    pub max_seconds: f64,
}

impl Silence {
    pub const NONE: Self = Self { probability: 0.0, min_seconds: 0.0, max_seconds: 0.0 };
}

#[derive(Debug, Clone)]
pub struct RunConfig {
    /// Connections spread across these, since one client address can only reach one server
    /// port about 28,000 times at once.
    pub driver_urls: Vec<String>,
    pub rider_urls: Vec<String>,
    pub seed: u64,
    pub ping_interval_seconds: f64,
    /// Simulated seconds per real second. Runs use 1; only tests speed time up.
    pub time_scale: f64,
    /// How far into its clock hour the run starts, because supply changes on the clock hour.
    pub seconds_into_first_hour: f64,
    pub silence: Silence,
}

#[derive(Debug, Default)]
pub struct Counters {
    pub driver_connections: AtomicU64,
    pub driver_connect_failures: AtomicU64,
    /// A driver connection that failed after connecting; the driver reconnects.
    pub driver_session_errors: AtomicU64,
    pub rider_connect_failures: AtomicU64,
    pub rider_session_errors: AtomicU64,
    pub silent_periods: AtomicU64,
    pub pings: AtomicU64,
    pub offers: AtomicU64,
    pub accepts: AtomicU64,
    pub declines: AtomicU64,
    pub offers_gone: AtomicU64,
    pub trips_assigned: AtomicU64,
    pub trips_completed: AtomicU64,
    pub ride_requests: AtomicU64,
    pub riders_finished: AtomicU64,
    /// When each offer arrived, and the Staleness of the location it was based on, in seconds.
    pub offer_staleness: Mutex<Vec<(f64, f64)>>,
    /// The first few failures, with time and reason.
    pub failures: Mutex<Vec<String>>,
    /// What clients were told, for the invariant checker.
    pub observations: Mutex<Observations>,
}

impl Counters {
    pub fn get(counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }

    fn add(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    fn failure(&self, counter: &AtomicU64, at_seconds: f64, kind: &str, reason: &str) {
        Self::add(counter);
        if let Ok(mut failures) = self.failures.lock()
            && failures.len() < FAILURES_KEPT
        {
            failures.push(format!("{at_seconds:.1}s {kind}: {reason}"));
        }
    }

    fn observe(&self, record: impl FnOnce(&mut Observations)) {
        if let Ok(mut observations) = self.observations.lock() {
            record(&mut observations);
        }
    }
}

enum Failure {
    Connect(String),
    Session(String),
}

fn session<E: std::fmt::Display>(context: &'static str) -> impl FnOnce(E) -> Failure {
    move |error| Failure::Session(format!("{context}: {error}"))
}

struct Run {
    config: RunConfig,
    trips: Vec<TripPlan>,
    times: Arc<[TripTimes]>,
    counters: Arc<Counters>,
    started: Instant,
    duration_seconds: f64,
}

impl Run {
    fn instant(&self, sim_seconds: f64) -> Instant {
        self.started + Duration::from_secs_f64(sim_seconds.max(0.0) / self.config.time_scale)
    }

    fn sim_now(&self) -> f64 {
        self.started.elapsed().as_secs_f64() * self.config.time_scale
    }
}

fn wall_clock_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |elapsed| elapsed.as_millis() as i64)
}

fn lat_lng(position: Position) -> LatLng {
    LatLng { lat: position.lat, lng: position.lng }
}

fn position(lat_lng: Option<LatLng>) -> Option<Position> {
    lat_lng.map(|point| Position { lat: point.lat, lng: point.lng })
}

/// The library's default 128 KiB read buffer per connection would dominate memory at
/// tens of thousands of sockets, and these messages are far smaller than a buffer this size.
fn socket_config() -> WebSocketConfig {
    WebSocketConfig::default().read_buffer_size(4 * 1024)
}

/// Runs the plan until `duration_seconds` of simulated time has passed.
pub async fn run_plan(
    config: RunConfig,
    plan: Plan,
    duration_seconds: f64,
    counters: Arc<Counters>,
) -> Arc<[TripTimes]> {
    let times: Arc<[TripTimes]> = plan.trips.iter().map(|_| TripTimes::default()).collect();
    let run = Arc::new(Run {
        config,
        trips: plan.trips,
        times: Arc::clone(&times),
        counters,
        started: Instant::now(),
        duration_seconds,
    });
    let mut tasks = Vec::with_capacity(plan.drivers.len() + run.trips.len());
    for driver in plan.drivers {
        tasks.push(tokio::spawn(run_driver(Arc::clone(&run), driver)));
    }
    for index in 0..run.trips.len() {
        tasks.push(tokio::spawn(run_rider(Arc::clone(&run), index)));
    }
    sleep_until(run.instant(duration_seconds)).await;
    for task in &tasks {
        task.abort();
    }
    for task in tasks {
        let _ = task.await;
    }
    times
}

/// Joins consecutive online clock hours into `(start, end)` spans of seconds since the run began.
pub fn online_spans(online: &[bool], seconds_into_first_hour: f64) -> Vec<(f64, f64)> {
    let mut spans: Vec<(f64, f64)> = Vec::new();
    for (hour, is_online) in online.iter().enumerate() {
        if !is_online {
            continue;
        }
        let start = (hour as f64 * SECONDS_PER_HOUR - seconds_into_first_hour).max(0.0);
        let end = (hour + 1) as f64 * SECONDS_PER_HOUR - seconds_into_first_hour;
        match spans.last_mut() {
            Some(last) if last.1 == start => last.1 = end,
            _ => spans.push((start, end)),
        }
    }
    spans
}

async fn run_driver(run: Arc<Run>, plan: DriverPlan) {
    let mut rng = Rng::stream(run.config.seed, plan.stream);
    let mut state = DriverState { at: plan.position, trip: None };
    let silence = silence_window(&run, plan.stream);
    for (start, end) in online_spans(&plan.online, run.config.seconds_into_first_hour) {
        sleep_until(run.instant(start)).await;
        while run.sim_now() < end || state.trip.is_some() {
            let result = driver_session(&run, &plan.id, plan.stream, &mut rng, &mut state, end, silence).await;
            let counters = &run.counters;
            match result {
                Ok(()) => break,
                Err(Failure::Connect(reason)) => {
                    counters.failure(&counters.driver_connect_failures, run.sim_now(), "driver connect", &reason);
                }
                Err(Failure::Session(reason)) => {
                    counters.failure(&counters.driver_session_errors, run.sim_now(), "driver session", &reason);
                }
            }
            tokio::time::sleep(Duration::from_secs_f64(RECONNECT_SECONDS / run.config.time_scale)).await;
        }
    }
}

struct DriverState {
    at: Position,
    trip: Option<ActiveTrip>,
}

struct ActiveTrip {
    id: String,
    ride: usize,
    legs: VecDeque<Leg>,
    events: VecDeque<(f64, TripEventKind)>,
}

struct PendingResponse {
    at_seconds: f64,
    offer_id: String,
    accept: bool,
}

impl DriverState {
    fn position_at(&mut self, now: f64) -> Position {
        if let Some(trip) = &mut self.trip {
            while trip.legs.len() > 1 && trip.legs[0].end_seconds <= now {
                trip.legs.pop_front();
            }
            if let Some(leg) = trip.legs.front() {
                self.at = leg.position_at(now);
            }
        }
        self.at
    }
}

/// When this driver goes quiet, if at all, as `(start, end)` in run seconds.
fn silence_window(run: &Run, stream: u64) -> Option<(f64, f64)> {
    let silence = run.config.silence;
    let mut rng = Rng::stream(run.config.seed ^ SILENCE_STREAM_SALT, stream);
    if rng.next_f64() >= silence.probability {
        return None;
    }
    let start = rng.next_f64() * run.duration_seconds;
    Some((start, start + silence.min_seconds + rng.next_f64() * (silence.max_seconds - silence.min_seconds)))
}

fn start_trip(run: &Run, state: &mut DriverState, trip_id: String, ride_request_id: &str, now: f64) -> bool {
    let Some((ride, plan)) = ride_index(ride_request_id).and_then(|index| Some((index, run.trips.get(index)?))) else {
        return false;
    };
    record_once(&run.times[ride].assigned, now);
    let from = state.at;
    let arrive = now + from.distance_km(plan.pickup) / PICKUP_SPEED_KM_PER_SECOND;
    let finish = arrive + plan.trip_seconds;
    let mut legs = VecDeque::from([Leg { from, to: plan.pickup, start_seconds: now, end_seconds: arrive }]);
    legs.extend(trip_legs(plan.pickup, plan.dropoff, arrive, plan.trip_seconds));
    let events = VecDeque::from([
        (arrive, TripEventKind::Arrived),
        (arrive, TripEventKind::Started),
        (finish, TripEventKind::Completed),
    ]);
    state.trip = Some(ActiveTrip { id: trip_id, ride, legs, events });
    true
}

fn ride_request_id(index: usize) -> String {
    format!("r{index}")
}

fn ride_index(id: &str) -> Option<usize> {
    id.strip_prefix('r')?.parse().ok()
}

fn pick_url(urls: &[String], key: u64) -> &str {
    &urls[(key % urls.len() as u64) as usize]
}

async fn driver_session(
    run: &Run,
    id: &str,
    plan_stream: u64,
    rng: &mut Rng,
    state: &mut DriverState,
    end_seconds: f64,
    silence: Option<(f64, f64)>,
) -> Result<(), Failure> {
    let (socket, _) = connect_async_with_config(pick_url(&run.config.driver_urls, plan_stream), Some(socket_config()), false)
        .await
        .map_err(|error| Failure::Connect(error.to_string()))?;
    Counters::add(&run.counters.driver_connections);
    let (mut sink, mut stream) = socket.split();

    let ping_every = run.config.ping_interval_seconds;
    // Spread first pings across the interval, so a shift start isn't a burst.
    let mut next_ping = run.sim_now() + rng.next_f64() * ping_every;
    let mut pending: Vec<PendingResponse> = Vec::new();
    let mut was_silent = false;

    loop {
        let now = run.sim_now();
        let silent = state.trip.is_none() && silence.is_some_and(|(start, end)| (start..end).contains(&now));
        if silent && !was_silent {
            Counters::add(&run.counters.silent_periods);
            pending.clear();
        }
        was_silent = silent;
        if now >= end_seconds && state.trip.is_none() {
            let _ = sink.close().await;
            return Ok(());
        }
        let next_event = state.trip.as_ref().and_then(|trip| trip.events.front()).map(|(at, _)| *at);
        let next_response = pending.iter().map(|response| response.at_seconds).reduce(f64::min);
        let mut wake = next_ping;
        let silence_edge = silence.map(|(start, end)| if now < start { start } else { end }).filter(|edge| *edge > now);
        for candidate in [next_event, next_response, state.trip.is_none().then_some(end_seconds), silence_edge] {
            wake = candidate.map_or(wake, |at| wake.min(at));
        }

        tokio::select! {
            () = sleep_until(run.instant(wake)) => {
                let now = run.sim_now();
                if now >= next_ping && silent {
                    next_ping += ping_every;
                } else if now >= next_ping {
                    let ping = driver_to_server::Message::LocationPing(LocationPing {
                        driver_id: id.to_owned(),
                        position: Some(lat_lng(state.position_at(now))),
                        reported_at_ms: wall_clock_ms(),
                    });
                    send_driver(&mut sink, ping).await.map_err(session("send ping"))?;
                    Counters::add(&run.counters.pings);
                    next_ping += ping_every;
                }
                let mut index = 0;
                while index < pending.len() {
                    if pending[index].at_seconds > now {
                        index += 1;
                        continue;
                    }
                    let response = pending.swap_remove(index);
                    let message = if response.accept {
                        Counters::add(&run.counters.accepts);
                        driver_to_server::Message::Accept(Accept { offer_id: response.offer_id })
                    } else {
                        Counters::add(&run.counters.declines);
                        driver_to_server::Message::Decline(Decline { offer_id: response.offer_id })
                    };
                    send_driver(&mut sink, message).await.map_err(session("send response"))?;
                }
                loop {
                    let due = state.trip.as_ref().and_then(|trip| trip.events.front()).filter(|(at, _)| *at <= now);
                    let Some(&(_, kind)) = due else { break };
                    let at = state.position_at(now);
                    let Some(trip) = state.trip.as_mut() else { break };
                    trip.events.pop_front();
                    if kind == TripEventKind::Arrived {
                        record_once(&run.times[trip.ride].arrived, now);
                    }
                    let event = TripEvent {
                        trip_id: trip.id.clone(),
                        kind: kind.into(),
                        position: Some(lat_lng(at)),
                        occurred_at_ms: wall_clock_ms(),
                        buffered: false,
                    };
                    send_driver(&mut sink, driver_to_server::Message::TripEvent(event)).await.map_err(session("send trip event"))?;
                    if kind == TripEventKind::Completed {
                        state.trip = None;
                        Counters::add(&run.counters.trips_completed);
                    }
                }
            }
            frame = stream.next() => {
                let frame = match frame {
                    Some(Ok(frame)) => frame,
                    Some(Err(error)) => return Err(Failure::Session(format!("receive: {error}"))),
                    None => return Err(Failure::Session("closed by server".into())),
                };
                let Message::Binary(bytes) = frame else { continue };
                let decoded = ServerToDriver::decode(bytes).map_err(session("decode"))?;
                match decoded.message {
                    // A quiet driver doesn't see offers; the server must give up on them.
                    Some(server_to_driver::Message::Offer(_)) if silent => {}
                    Some(server_to_driver::Message::Offer(offer)) => {
                        Counters::add(&run.counters.offers);
                        if let Ok(mut samples) = run.counters.offer_staleness.lock() {
                            samples.push((run.sim_now(), offer.staleness_ms as f64 / 1000.0));
                        }
                        if let Some(times) = ride_index(&offer.ride_request_id).and_then(|index| run.times.get(index)) {
                            record_once(&times.first_offer, run.sim_now());
                        }
                        pending.push(answer_offer(run, rng, state, offer));
                    }
                    Some(server_to_driver::Message::OfferGone(gone)) => {
                        Counters::add(&run.counters.offers_gone);
                        pending.retain(|response| response.offer_id != gone.offer_id);
                    }
                    Some(server_to_driver::Message::TripAssigned(assigned)) => {
                        run.counters.observe(|observed| observed.drivers.push(DriverTold {
                            driver: id.to_owned(),
                            trip: assigned.trip_id.clone(),
                            ride: assigned.ride_request_id.clone(),
                        }));
                        match &state.trip {
                            // Told again, for example after a reconnect.
                            Some(trip) if trip.id == assigned.trip_id => continue,
                            Some(trip) => {
                                let held_trip = trip.id.clone();
                                run.counters.observe(|observed| observed.double_booked.push(DriverDoubleBooked {
                                    driver: id.to_owned(),
                                    held_trip,
                                    new_trip: assigned.trip_id.clone(),
                                }));
                                continue;
                            }
                            None => {}
                        }
                        Counters::add(&run.counters.trips_assigned);
                        pending.clear();
                        let now = run.sim_now();
                        state.position_at(now);
                        if !start_trip(run, state, assigned.trip_id, &assigned.ride_request_id, now) {
                            return Err(Failure::Session(format!("unknown Ride Request {}", assigned.ride_request_id)));
                        }
                    }
                    None => {}
                }
            }
        }
    }
}

fn answer_offer(run: &Run, rng: &mut Rng, state: &mut DriverState, offer: Offer) -> PendingResponse {
    let now = run.sim_now();
    let at = state.position_at(now);
    // A driver on a Trip can't take another; the server should learn that quickly.
    if state.trip.is_some() {
        return PendingResponse { at_seconds: now, offer_id: offer.offer_id, accept: false };
    }
    let pickup_km = position(offer.pickup).map_or(0.0, |pickup| at.distance_km(pickup));
    let response = respond_to_offer(rng, pickup_km);
    PendingResponse { at_seconds: now + response.after_seconds, offer_id: offer.offer_id, accept: response.accept }
}

async fn send_driver<S>(sink: &mut S, message: driver_to_server::Message) -> Result<(), &'static str>
where
    S: SinkExt<Message> + Unpin,
{
    let frame = DriverToServer { message: Some(message) }.encode_to_vec();
    sink.send(Message::Binary(frame.into())).await.map_err(|_| "connection closed")
}

async fn send_rider<S>(sink: &mut S, message: rider_to_server::Message) -> Result<(), &'static str>
where
    S: SinkExt<Message> + Unpin,
{
    let frame = RiderToServer { message: Some(message) }.encode_to_vec();
    sink.send(Message::Binary(frame.into())).await.map_err(|_| "connection closed")
}

async fn run_rider(run: Arc<Run>, index: usize) {
    let trip = run.trips[index];
    sleep_until(run.instant(trip.request_at_seconds)).await;
    let give_up = run.instant(trip.request_at_seconds + RIDER_GIVES_UP_SECONDS);
    let counters = &run.counters;
    match rider_session(&run, index, trip.pickup, give_up).await {
        Ok(()) => {}
        Err(Failure::Connect(reason)) => {
            counters.failure(&counters.rider_connect_failures, run.sim_now(), "rider connect", &reason);
        }
        Err(Failure::Session(reason)) => {
            counters.failure(&counters.rider_session_errors, run.sim_now(), "rider session", &reason);
        }
    }
}

async fn rider_session(run: &Run, index: usize, pickup: Position, give_up: Instant) -> Result<(), Failure> {
    let (socket, _) = connect_async_with_config(pick_url(&run.config.rider_urls, index as u64), Some(socket_config()), false)
        .await
        .map_err(|error| Failure::Connect(error.to_string()))?;
    let (mut sink, mut stream) = socket.split();
    let id = ride_request_id(index);
    let quote = QuoteRequest { rider_id: format!("rider-{index}"), pickup: Some(lat_lng(pickup)) };
    send_rider(&mut sink, rider_to_server::Message::QuoteRequest(quote)).await.map_err(session("send quote request"))?;

    loop {
        let frame = tokio::time::timeout_at(give_up, stream.next()).await.map_err(session("gave up waiting"))?;
        let frame = match frame {
            Some(Ok(frame)) => frame,
            Some(Err(error)) => return Err(Failure::Session(format!("receive: {error}"))),
            None => return Err(Failure::Session("closed by server".into())),
        };
        let Message::Binary(bytes) = frame else { continue };
        match ServerToRider::decode(bytes).map_err(session("decode"))?.message {
            Some(server_to_rider::Message::Quote(quote)) => {
                let request = RideRequest {
                    ride_request_id: id.clone(),
                    quote_id: quote.quote_id,
                    pickup: Some(lat_lng(pickup)),
                };
                send_rider(&mut sink, rider_to_server::Message::RideRequest(request)).await.map_err(session("send ride request"))?;
                Counters::add(&run.counters.ride_requests);
                record_once(&run.times[index].requested, run.sim_now());
            }
            Some(server_to_rider::Message::TripStatus(status)) if status.status == "assigned" => {
                run.counters.observe(|observed| observed.riders.push(RiderTold { ride: id.clone(), trip: status.trip_id }));
            }
            Some(server_to_rider::Message::TripStatus(status)) if status.status == "completed" => {
                Counters::add(&run.counters.riders_finished);
                let _ = sink.close().await;
                return Ok(());
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn online_spans_join_consecutive_hours() {
        assert_eq!(
            online_spans(&[true, true, false, true], 0.0),
            vec![(0.0, 7200.0), (10800.0, 14400.0)]
        );
    }

    #[test]
    fn online_spans_follow_the_clock_hour_when_a_run_starts_mid_hour() {
        // A 17:45 start: the 18:00 hour begins 900 seconds in, not 3600.
        assert_eq!(online_spans(&[true, false], 2700.0), vec![(0.0, 900.0)]);
        assert_eq!(online_spans(&[false, true], 2700.0), vec![(900.0, 4500.0)]);
    }

    #[test]
    fn ride_ids_round_trip() {
        assert_eq!(ride_index(&ride_request_id(42)), Some(42));
        assert_eq!(ride_index("nope"), None);
    }
}
