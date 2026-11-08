//! gRPC clients for peer Shards, with optional fault injection for the fault campaign.
//!
//! Faults come from the environment, so the real binary can be tested as it runs:
//! - `PEER_FAULTS=drop_request=0.1,drop_reply=0.1,delay_ms=200`: each outbound call is
//!   dropped before sending, or sent with its reply thrown away, with these probabilities,
//!   after a random delay of up to `delay_ms`.
//! - `CRASH_AT=after_reserve=0.05,after_confirm=0.05,before_settle=0.05,before_release=0.05`:
//!   the process aborts at that Reservation step with that probability.
//! - `PARTITION_FILE=path`: while that file exists, every peer call fails unsent, as if the
//!   network between Shards were cut. Tests create and remove it to open and heal a partition.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use geo::shard_map::{ShardId, ShardMap};
use protocol::shard::shard_client::ShardClient;
use tonic::transport::{Channel, Endpoint};

const CALL_TIMEOUT: Duration = Duration::from_secs(2);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(1);

/// The Reservation steps a crash can be injected at.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Step {
    /// Driver's Shard: the Reservation is in Postgres, the Confirm not yet sent.
    AfterReserve,
    /// Rider's Shard: the Confirm is committed, the reply not yet sent.
    AfterConfirm,
    /// Driver's Shard: the peer answered, the answer not yet recorded.
    BeforeSettle,
    /// Rider's Shard: the leased Trip ended, the release not yet sent.
    BeforeRelease,
}

#[derive(Debug, Default)]
pub struct Faults {
    drop_request: f64,
    drop_reply: f64,
    delay_ms: u64,
    crashes: HashMap<Step, f64>,
    partition_file: Option<std::path::PathBuf>,
    state: Mutex<u64>,
    pub injected: AtomicU64,
}

impl Faults {
    pub fn none() -> Self {
        Self::default()
    }

    /// From `PEER_FAULTS` and `CRASH_AT`; unset means no faults.
    pub fn from_env() -> Result<Self, String> {
        let mut faults = Self::none();
        for (name, value) in pairs("PEER_FAULTS")? {
            match name.as_str() {
                "drop_request" => faults.drop_request = value,
                "drop_reply" => faults.drop_reply = value,
                "delay_ms" => faults.delay_ms = value as u64,
                other => return Err(format!("PEER_FAULTS: unknown fault {other}")),
            }
        }
        for (name, value) in pairs("CRASH_AT")? {
            let step = match name.as_str() {
                "after_reserve" => Step::AfterReserve,
                "after_confirm" => Step::AfterConfirm,
                "before_settle" => Step::BeforeSettle,
                "before_release" => Step::BeforeRelease,
                other => return Err(format!("CRASH_AT: unknown step {other}")),
            };
            faults.crashes.insert(step, value);
        }
        faults.partition_file = std::env::var("PARTITION_FILE").ok().filter(|path| !path.is_empty()).map(Into::into);
        let seed = SystemTime::now().duration_since(UNIX_EPOCH).map_or(1, |elapsed| elapsed.as_nanos() as u64) | 1;
        *faults.state.lock().expect("unpoisoned") = seed;
        Ok(faults)
    }

    pub fn describe(&self) -> Option<String> {
        if self.drop_request == 0.0
            && self.drop_reply == 0.0
            && self.delay_ms == 0
            && self.crashes.is_empty()
            && self.partition_file.is_none()
        {
            return None;
        }
        let mut crashes: Vec<String> = self.crashes.iter().map(|(step, p)| format!("{step:?} {p}")).collect();
        crashes.sort();
        Some(format!(
            "faults: drop request {}, drop reply {}, delay up to {} ms, crash at [{}], partition file {:?}",
            self.drop_request,
            self.drop_reply,
            self.delay_ms,
            crashes.join(", "),
            self.partition_file
        ))
    }

    fn chance(&self) -> f64 {
        let mut state = self.state.lock().expect("unpoisoned");
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        (*state >> 11) as f64 / (1u64 << 53) as f64
    }

    /// Aborts the process here, if a crash is configured for this step and the draw says so.
    pub fn maybe_crash(&self, step: Step) {
        if let Some(p) = self.crashes.get(&step)
            && self.chance() < *p
        {
            eprintln!("injected crash at {step:?}");
            std::process::abort();
        }
    }
}

fn pairs(var: &str) -> Result<Vec<(String, f64)>, String> {
    let Ok(text) = std::env::var(var) else { return Ok(Vec::new()) };
    text.split(',')
        .filter(|pair| !pair.trim().is_empty())
        .map(|pair| {
            let (name, value) = pair.split_once('=').ok_or_else(|| format!("{var}: expected name=value, got {pair}"))?;
            let value = value.trim().parse().map_err(|_| format!("{var}: bad number in {pair}"))?;
            Ok((name.trim().to_owned(), value))
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallError {
    /// No client for that Shard in the map.
    UnknownShard,
    /// Not sent, or the reply was lost: the call may or may not have happened.
    Failed(String),
}

pub struct Peers {
    pub me: ShardId,
    clients: RwLock<HashMap<ShardId, ShardClient<Channel>>>,
    pub faults: Faults,
}

impl Peers {
    pub fn new(me: ShardId, map: &ShardMap, faults: Faults) -> Result<Self, String> {
        let peers = Self { me, clients: RwLock::new(HashMap::new()), faults };
        peers.update(map)?;
        Ok(peers)
    }

    /// Connects lazily to every other Shard in `map`.
    pub fn update(&self, map: &ShardMap) -> Result<(), String> {
        let mut clients = HashMap::new();
        for shard in map.shards().iter().filter(|shard| shard.id != self.me) {
            let endpoint = Endpoint::from_shared(shard.peer.clone())
                .map_err(|error| format!("Shard {} peer address {}: {error}", shard.id, shard.peer))?
                .timeout(CALL_TIMEOUT)
                .connect_timeout(CONNECT_TIMEOUT);
            clients.insert(shard.id, ShardClient::new(endpoint.connect_lazy()));
        }
        *self.clients.write().expect("unpoisoned") = clients;
        Ok(())
    }

    /// Runs one call to `shard`, with any configured faults around it.
    pub async fn call<T, F, Fut>(&self, shard: ShardId, call: F) -> Result<T, CallError>
    where
        F: FnOnce(ShardClient<Channel>) -> Fut,
        Fut: Future<Output = Result<tonic::Response<T>, tonic::Status>>,
    {
        let client = self.clients.read().expect("unpoisoned").get(&shard).cloned().ok_or(CallError::UnknownShard)?;
        if self.faults.delay_ms > 0 {
            let delay = (self.faults.chance() * self.faults.delay_ms as f64) as u64;
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
        if self.faults.partition_file.as_ref().is_some_and(|path| path.exists()) {
            self.faults.injected.fetch_add(1, Ordering::Relaxed);
            return Err(CallError::Failed("injected: partitioned".into()));
        }
        if self.faults.chance() < self.faults.drop_request {
            self.faults.injected.fetch_add(1, Ordering::Relaxed);
            return Err(CallError::Failed("injected: request dropped".into()));
        }
        let reply = call(client).await.map_err(|status| CallError::Failed(status.to_string()))?;
        if self.faults.chance() < self.faults.drop_reply {
            self.faults.injected.fetch_add(1, Ordering::Relaxed);
            return Err(CallError::Failed("injected: reply dropped".into()));
        }
        Ok(reply.into_inner())
    }
}
