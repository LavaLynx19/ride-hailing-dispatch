//! Helpers for tests that run the real Shard binary against Postgres.

#![allow(dead_code)]

use std::net::TcpListener as StdListener;

use sqlx::{Connection, PgConnection};

pub fn free_port() -> u16 {
    StdListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

/// Creates an empty database named `name` on the `DATABASE_URL` server, and returns its URL.
/// The Shard applies migrations itself when it starts.
pub async fn fresh_database_url(name: &str) -> String {
    let base = std::env::var("DATABASE_URL").expect("DATABASE_URL must point at a test Postgres server");
    let mut admin = PgConnection::connect(&base).await.unwrap();
    assert!(name.chars().all(|c| c.is_ascii_lowercase() || c == '_'), "plain database names only");
    // Safe: the name is checked above, and only tests pass it.
    // Separate statements, since Postgres won't run these together in one transaction.
    for statement in [format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"), format!("CREATE DATABASE {name}")] {
        sqlx::raw_sql(sqlx::AssertSqlSafe(statement)).execute(&mut admin).await.unwrap();
    }
    let (server, _) = base.rsplit_once('/').expect("DATABASE_URL ends with a database name");
    format!("{server}/{name}")
}
