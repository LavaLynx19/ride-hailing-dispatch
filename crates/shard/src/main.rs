use std::path::PathBuf;

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
    let url = std::env::var("DATABASE_URL").map_err(|_| std::io::Error::other("DATABASE_URL must be set"))?;
    let store = assignment::Store::connect(&url, POSTGRES_CONNECTIONS).await.map_err(std::io::Error::other)?;
    let drivers = bind_all(&driver_addr).await?;
    let riders = bind_all(&rider_addr).await?;
    eprintln!("listening: drivers {driver_addr}, riders {rider_addr}");
    shard::server::serve(drivers, riders, ping_log, store).await;
    Ok(())
}
