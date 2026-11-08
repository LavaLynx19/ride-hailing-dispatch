use std::path::PathBuf;

use std::time::Duration;

use shard::dispatcher::{Matcher, Policy};
use shard::ping_log::{Config, PingLog};
use tokio::net::TcpListener;

/// Enough for Accepts to race; most Postgres work is short single statements.
const POSTGRES_CONNECTIONS: u32 = 16;

async fn bind_all(addresses: &str) -> std::io::Result<Vec<TcpListener>> {
    let mut listeners = Vec::new();
    for address in addresses.split(',') {
        listeners.push(TcpListener::bind(address).await?);
    }
    Ok(listeners)
}

/// From `MATCHER` (greedy or auction), `BATCH_WINDOW_MS` (auction only) and `WIDENING` (on or off).
/// Unset means the Phase 1 baseline: greedy, no widening.
fn policy_from_env() -> std::io::Result<Policy> {
    let var = |name: &str| std::env::var(name).ok().filter(|value| !value.is_empty());
    let bad = |message: String| std::io::Error::other(message);
    let matcher = match var("MATCHER").as_deref() {
        None | Some("greedy") => Matcher::Greedy,
        Some("auction") => {
            let window = var("BATCH_WINDOW_MS")
                .ok_or_else(|| bad("MATCHER=auction needs BATCH_WINDOW_MS".into()))?
                .parse::<u64>()
                .ok()
                .filter(|ms| *ms > 0)
                .ok_or_else(|| bad("BATCH_WINDOW_MS must be a positive whole number".into()))?;
            Matcher::Auction { window: Duration::from_millis(window) }
        }
        Some(other) => return Err(bad(format!("MATCHER must be greedy or auction, not {other}"))),
    };
    let widening = match var("WIDENING").as_deref() {
        None | Some("off") => false,
        Some("on") => true,
        Some(other) => return Err(bad(format!("WIDENING must be on or off, not {other}"))),
    };
    Ok(Policy { matcher, widening })
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let mut args = std::env::args().skip(1);
    let usage = || std::io::Error::other("usage: shard DRIVER_ADDRS RIDER_ADDRS [PING_LOG_DIR], with addresses comma-separated");
    let driver_addr = args.next().ok_or_else(usage)?;
    let rider_addr = args.next().ok_or_else(usage)?;
    let ping_log = args.next().map(|dir| PingLog::open(Config::new(PathBuf::from(dir)))).transpose()?;
    if ping_log.is_none() {
        eprintln!("no ping log directory given, so pings won't be stored");
    }
    let policy = policy_from_env()?;
    let url = std::env::var("DATABASE_URL").map_err(|_| std::io::Error::other("DATABASE_URL must be set"))?;
    let store = assignment::Store::connect(&url, POSTGRES_CONNECTIONS).await.map_err(std::io::Error::other)?;
    // Sharded when SHARD_ID is set; SHARD_MAP names a map file to store first, and PEER_LISTEN
    // the gRPC address (default 0.0.0.0:7100). Unset means one Shard with no map, as in
    // Phases 1-4.
    let sharded = match std::env::var("SHARD_ID").ok().filter(|value| !value.is_empty()) {
        Some(id) => {
            let id = id.parse().map_err(|_| std::io::Error::other(format!("SHARD_ID must be a whole number, not {id}")))?;
            let text = match std::env::var("SHARD_MAP").ok().filter(|value| !value.is_empty()) {
                Some(path) => Some(std::fs::read_to_string(&path).map_err(|error| std::io::Error::other(format!("{path}: {error}")))?),
                None => None,
            };
            let placement = shard::sharding::place(&store, id, text.as_deref()).await.map_err(std::io::Error::other)?;
            eprintln!("{}", placement.describe());
            let peer_address = std::env::var("PEER_LISTEN").unwrap_or_else(|_| "0.0.0.0:7100".into());
            let peer_listener = TcpListener::bind(&peer_address).await?;
            let faults = shard::peers::Faults::from_env().map_err(std::io::Error::other)?;
            eprintln!("peers on {peer_address}");
            Some(shard::server::Sharded { placement, peer_listener, faults })
        }
        None => None,
    };
    let drivers = bind_all(&driver_addr).await?;
    let riders = bind_all(&rider_addr).await?;
    eprintln!("listening: drivers {driver_addr}, riders {rider_addr}");
    shard::server::serve_sharded(drivers, riders, ping_log, store, policy, sharded).await;
    Ok(())
}
