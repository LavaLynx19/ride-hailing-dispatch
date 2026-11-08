//! Stored versions of the Cell → Shard map.

use std::collections::BTreeMap;

use geo::CellIndex;
use geo::shard_map::{ShardAddress, ShardMap};

use crate::{Error, Store};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MapSaved {
    Stored,
    /// The same version with the same content was already there.
    AlreadyStored,
}

impl Store {
    /// Stores a map version. A version can't change once stored, so a different map under an
    /// existing version is refused.
    pub async fn save_shard_map(&self, map: &ShardMap) -> Result<MapSaved, Error> {
        let version = i32::try_from(map.version()).map_err(|_| Error::Protocol("map version too large".into()))?;
        let mut tx = self.pool.begin().await?;
        let inserted = sqlx::query!("INSERT INTO shard_maps (version) VALUES ($1) ON CONFLICT DO NOTHING", version)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        if inserted == 0 {
            tx.rollback().await?;
            return match self.shard_map(map.version()).await? {
                Some(stored) if stored == *map => Ok(MapSaved::AlreadyStored),
                _ => Err(Error::Protocol(format!("map version {} is already stored with different content", map.version()))),
            };
        }
        for shard in map.shards() {
            sqlx::query!(
                "INSERT INTO shard_map_shards (version, shard, peer, drivers, riders) VALUES ($1, $2, $3, $4, $5)",
                version,
                shard.id as i16,
                shard.peer,
                shard.drivers.join(","),
                shard.riders.join(","),
            )
            .execute(&mut *tx)
            .await?;
        }
        let (cells, shards): (Vec<i64>, Vec<i16>) = map.cells().iter().map(|(cell, shard)| (u64::from(*cell) as i64, *shard as i16)).unzip();
        sqlx::query!(
            "INSERT INTO shard_map_cells (version, cell, shard) SELECT $1, * FROM UNNEST($2::bigint[], $3::smallint[])",
            version,
            &cells,
            &shards,
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(MapSaved::Stored)
    }

    /// The highest stored map version, if any.
    pub async fn latest_shard_map(&self) -> Result<Option<ShardMap>, Error> {
        let latest = sqlx::query_scalar!("SELECT max(version) FROM shard_maps").fetch_one(&self.pool).await?;
        match latest {
            Some(version) => self.shard_map(version as u32).await,
            None => Ok(None),
        }
    }

    pub async fn shard_map(&self, version: u32) -> Result<Option<ShardMap>, Error> {
        let version = version as i32;
        let shards = sqlx::query!(
            "SELECT shard, peer, drivers, riders FROM shard_map_shards WHERE version = $1 ORDER BY shard",
            version
        )
        .fetch_all(&self.pool)
        .await?;
        if shards.is_empty() {
            return Ok(None);
        }
        let shards = shards
            .into_iter()
            .map(|row| ShardAddress {
                id: row.shard as u16,
                peer: row.peer,
                drivers: row.drivers.split(',').map(str::to_owned).collect(),
                riders: row.riders.split(',').map(str::to_owned).collect(),
            })
            .collect();
        let mut cells = BTreeMap::new();
        for row in sqlx::query!("SELECT cell, shard FROM shard_map_cells WHERE version = $1", version).fetch_all(&self.pool).await? {
            let cell = CellIndex::try_from(row.cell as u64).map_err(|error| Error::Protocol(format!("stored Cell {}: {error}", row.cell)))?;
            cells.insert(cell, row.shard as u16);
        }
        ShardMap::new(version as u32, shards, cells).map(Some).map_err(Error::Protocol)
    }
}
