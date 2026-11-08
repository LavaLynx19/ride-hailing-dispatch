//! Which map a Shard serves at startup. Needs `DATABASE_URL`, like the assignment tests.

use sqlx::PgPool;

fn map_text(version: u32) -> String {
    let cell = |lat, lng| geo::Position { lat, lng }.cell(geo::shard_map::MAP_RESOLUTION).unwrap();
    format!(
        "version {version}\n\
         shard 0 peer http://shard-0:7100 drivers ws://shard-0:7001 riders ws://shard-0:7002\n\
         shard 1 peer http://shard-1:7100 drivers ws://shard-1:7001 riders ws://shard-1:7002\n\
         cell {} 0\ncell {} 1\ncell {} 1\n",
        cell(40.70, -74.00),
        cell(40.80, -73.90),
        cell(40.75, -73.95)
    )
}

#[sqlx::test(migrator = "assignment::MIGRATOR")]
async fn a_shard_serves_the_newest_stored_map(pool: PgPool) {
    let store = assignment::Store::from_pool(pool);
    let placement = shard::sharding::place(&store, 1, Some(&map_text(2))).await.unwrap();
    assert_eq!(placement.describe(), "shard 1 of 2, map version 2, 2 of 3 Cells");
    // Restarted with an older file: version 2 is still stored, so it wins.
    assert_eq!(shard::sharding::place(&store, 0, Some(&map_text(1))).await.unwrap().map.version(), 2);
    assert_eq!(shard::sharding::place(&store, 0, None).await.unwrap().map.version(), 2);
}

#[sqlx::test(migrator = "assignment::MIGRATOR")]
async fn a_shard_refuses_to_start_without_a_place_in_the_map(pool: PgPool) {
    let store = assignment::Store::from_pool(pool);
    assert!(shard::sharding::place(&store, 0, None).await.unwrap_err().contains("no shard map"));
    assert!(shard::sharding::place(&store, 5, Some(&map_text(1))).await.unwrap_err().contains("no Shard 5"));
    assert!(shard::sharding::place(&store, 0, Some("version 1\n")).await.unwrap_err().contains("at least one"));
}
