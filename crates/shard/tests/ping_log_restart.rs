//! Kills the real Shard process with pings on the way in, and checks what's on disk.

use std::net::TcpListener as StdListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use futures_util::SinkExt;
use prost::Message as _;
use protocol::client::{DriverToServer, LatLng, LocationPing, driver_to_server};
use shard::ping_log::read_all;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

fn free_port() -> u16 {
    StdListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn start_shard(dir: &Path, driver_port: u16) -> Child {
    Command::new(env!("CARGO_BIN_EXE_shard"))
        .args([format!("127.0.0.1:{driver_port}"), format!("127.0.0.1:{}", free_port())])
        .arg(dir)
        .stderr(Stdio::null())
        .spawn()
        .unwrap()
}

async fn send_pings(driver_port: u16, range: std::ops::Range<i64>) {
    let url = format!("ws://127.0.0.1:{driver_port}");
    let mut socket = loop {
        match connect_async(&url).await {
            Ok((socket, _)) => break socket,
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    };
    for reported_at_ms in range {
        let ping = LocationPing {
            driver_id: "d1".into(),
            position: Some(LatLng { lat: 40.7, lng: -74.0 }),
            reported_at_ms,
        };
        let message = DriverToServer { message: Some(driver_to_server::Message::LocationPing(ping)) };
        socket.send(Message::Binary(message.encode_to_vec().into())).await.unwrap();
    }
}

#[tokio::test]
async fn pings_older_than_the_sync_interval_survive_being_killed() {
    let dir = std::env::temp_dir().join(format!("ping-log-restart-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    let port = free_port();
    let mut shard = start_shard(&dir, port);
    send_pings(port, 0..1000).await;
    // Allow one full sync interval, plus margin, then kill with a further batch in flight.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    send_pings(port, 1000..1100).await;
    shard.kill().unwrap();
    shard.wait().unwrap();

    let after_kill: Vec<i64> = read_all(&dir).unwrap().iter().map(|record| record.reported_at_ms).collect();
    assert!(after_kill.len() >= 1000, "only {} pings survived", after_kill.len());
    assert!(after_kill.iter().copied().eq(0..after_kill.len() as i64), "pings are lost or out of order");

    // A restart keeps the old pings and adds new ones after them.
    let mut shard = start_shard(&dir, port);
    send_pings(port, 2000..2100).await;
    tokio::time::sleep(Duration::from_millis(2500)).await;
    shard.kill().unwrap();
    shard.wait().unwrap();
    let after_restart: Vec<i64> = read_all(&dir).unwrap().iter().map(|record| record.reported_at_ms).collect();
    assert_eq!(after_restart.len(), after_kill.len() + 100);
    assert!(after_restart[..after_kill.len()] == after_kill[..]);
    assert!(after_restart[after_kill.len()..].iter().copied().eq(2000..2100));

    let _ = std::fs::remove_dir_all(&dir);
}
