//! This Shard's place in the shard map. See ARCHITECTURE.md → Sharding.

use geo::shard_map::{ShardId, ShardMap};

/// The map this Shard serves, and which Shard it is.
#[derive(Debug, Clone)]
pub struct Placement {
    pub shard: ShardId,
    pub map: ShardMap,
}

impl Placement {
    pub fn describe(&self) -> String {
        let owned = self.map.cells().values().filter(|shard| **shard == self.shard).count();
        format!(
            "shard {} of {}, map version {}, {owned} of {} Cells",
            self.shard,
            self.map.shards().len(),
            self.map.version(),
            self.map.cells().len()
        )
    }
}

/// Stores the map from `map_text`, if given, then serves the highest version stored, which may
/// be newer than the file. `shard` must be one of that map's Shards.
pub async fn place(store: &assignment::Store, shard: ShardId, map_text: Option<&str>) -> Result<Placement, String> {
    if let Some(text) = map_text {
        let map = ShardMap::parse(text)?;
        store.save_shard_map(&map).await.map_err(|error| format!("storing map version {}: {error}", map.version()))?;
    }
    let map = store
        .latest_shard_map()
        .await
        .map_err(|error| format!("loading the shard map: {error}"))?
        .ok_or("no shard map stored and none given")?;
    if map.shard(shard).is_none() {
        return Err(format!("map version {} has no Shard {shard}", map.version()));
    }
    Ok(Placement { shard, map })
}
