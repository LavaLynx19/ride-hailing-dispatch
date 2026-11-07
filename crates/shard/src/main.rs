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
    let drivers = bind_all(&driver_addr).await?;
    let riders = bind_all(&rider_addr).await?;
    eprintln!("listening: drivers {driver_addr}, riders {rider_addr}");
    shard::server::serve(drivers, riders, ping_log, store, policy).await;
    Ok(())
}
