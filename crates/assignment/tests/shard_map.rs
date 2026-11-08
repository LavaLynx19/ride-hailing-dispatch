//! Stored shard map versions. Needs `DATABASE_URL`, like the Accept tests.

use std::collections::BTreeMap;

use assignment::{MapSaved, Store};
use geo::Position;
use geo::shard_map::{MAP_RESOLUTION, ShardAddress, ShardMap};
use sqlx::PgPool;

fn address(id: u16) -> ShardAddress {
    ShardAddress {
        id,
        peer: format!("http://shard-{id}:7100"),
        drivers: vec![format!("ws://shard-{id}:7001"), format!("ws://shard-{id}:7003")],
        riders: vec![format!("ws://shard-{id}:7002")],
    }
}

fn map(version: u32, second_owner: u16) -> ShardMap {
    let cell = |lat, lng| Position { lat, lng }.cell(MAP_RESOLUTION).unwrap();
    let cells: BTreeMap<_, _> = [(cell(40.70, -74.00), 0), (cell(40.80, -73.90), second_owner)].into_iter().collect();
    ShardMap::new(version, vec![address(0), address(1)], cells).unwrap()
}

#[sqlx::test(migrator = "assignment::MIGRATOR")]
async fn the_latest_version_reads_back_as_stored(pool: PgPool) {
    let store = Store::from_pool(pool);
    assert_eq!(store.latest_shard_map().await.unwrap(), None);
    assert_eq!(store.save_shard_map(&map(1, 1)).await.unwrap(), MapSaved::Stored);
    assert_eq!(store.save_shard_map(&map(2, 0)).await.unwrap(), MapSaved::Stored);
    assert_eq!(store.latest_shard_map().await.unwrap(), Some(map(2, 0)));
    assert_eq!(store.shard_map(1).await.unwrap(), Some(map(1, 1)));
}

#[sqlx::test(migrator = "assignment::MIGRATOR")]
async fn a_stored_version_never_changes(pool: PgPool) {
    let store = Store::from_pool(pool);
    store.save_shard_map(&map(1, 1)).await.unwrap();
    assert_eq!(store.save_shard_map(&map(1, 1)).await.unwrap(), MapSaved::AlreadyStored);
    let error = store.save_shard_map(&map(1, 0)).await.unwrap_err().to_string();
    assert!(error.contains("different content"), "{error}");
    assert_eq!(store.latest_shard_map().await.unwrap(), Some(map(1, 1)));
}
