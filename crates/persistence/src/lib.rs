//! The `PostgreSQL` pool that Notes owns, the `notes` schema embedded in the binary, and the
//! readiness probe.
//!
//! Scope. This crate owns the `notes` schema and nothing else. Notes writes only its own schema
//! and reads no other schema. One type is the only route to a connection, and that keeps the rule
//! true over time.
//!
//! The schema is ONE file, `schema.sql` at the repository root. It is not a migration directory
//! and not a numbered ledger. No database holds data that has to survive a schema change, so a
//! schema change edits `schema.sql` in place.

#[cfg(feature = "test-support")]
pub mod test_support;

mod connections;
mod files;
mod outbox;
mod reads;

use std::time::Duration;

use secrecy::{ExposeSecret as _, SecretString};
use sqlx::postgres::{PgPool, PgPoolOptions};

pub use crate::connections::{
    ConnectOperation, ConnectionRecord, CreateOutcome, NewConnection, NewSecret, OutboxEvent,
};
pub use crate::files::{FileRow, FileTransaction, LockedConnection, NewCollision, NewRevision};
pub use crate::outbox::PendingEvent;
pub use crate::reads::{ConnectionStatus, FileSummary};

/// The schema, embedded at compile time.
///
/// Embedded rather than read from disk, so that a deployed binary cannot run with a schema other
/// than the one it was built against. `include_str!` makes the file a build input, so an edit
/// rebuilds this crate.
const SCHEMA: &str = include_str!("../../../schema.sql");

/// The advisory-lock key that `apply_schema` holds while it decides and applies.
///
/// One fixed 64-bit value. The lock matters with one process per deployment too: a restart that
/// overlaps the grace window of the previous process is two processes for a few seconds, and both
/// call this method.
const SCHEMA_LOCK: i64 = 0x7261_7461_6e6f_7401;

/// A failure in the pool or in a query.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PersistenceError {
    /// The pool could not be created, or a connection could not be acquired.
    #[error("the database connection could not be established")]
    Connect(#[source] sqlx::Error),

    /// The schema could not be applied.
    #[error("the database schema could not be applied")]
    Schema(#[source] sqlx::Error),

    /// A query failed.
    #[error("a database query failed")]
    Query(#[source] sqlx::Error),

    /// Concurrent writers changed a row between two statements more often than the operation
    /// retries. Repeating the request can succeed.
    #[error("concurrent writers changed the row; repeat the request")]
    Contended,
}

/// The pool, and the only handle through which Notes reaches `PostgreSQL`.
#[derive(Debug, Clone)]
pub struct Database {
    pool: PgPool,
}

impl Database {
    /// Create the pool and verify that it can serve one connection.
    ///
    /// The verification finds wrong credentials at startup, not on the first request.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Connect`] if the URL is unusable or the server refuses the connection
    /// within the acquire timeout.
    pub async fn connect(
        url: &SecretString,
        max_connections: u32,
        acquire_timeout: Duration,
    ) -> Result<Self, PersistenceError> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(acquire_timeout)
            .test_before_acquire(true)
            .connect(url.expose_secret())
            .await
            .map_err(PersistenceError::Connect)?;
        Ok(Self { pool })
    }

    /// Apply [`SCHEMA`] to a database that does not have it yet.
    ///
    /// Idempotent. One transaction takes a transaction-scoped advisory lock, asks whether the
    /// `notes` schema exists, and applies the file only if it does not. `PostgreSQL` DDL is
    /// transactional, so a file that fails halfway leaves the database as it was.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Schema`] if the lock cannot be taken, the catalogue cannot be read, or
    /// a statement in the file fails.
    pub async fn apply_schema(&self) -> Result<(), PersistenceError> {
        let mut transaction = self.pool.begin().await.map_err(PersistenceError::Schema)?;
        sqlx::query("select pg_advisory_xact_lock($1)")
            .bind(SCHEMA_LOCK)
            .execute(&mut *transaction)
            .await
            .map_err(PersistenceError::Schema)?;
        let present: bool = sqlx::query_scalar(
            "select exists (select 1 from pg_namespace where nspname = 'notes')",
        )
        .fetch_one(&mut *transaction)
        .await
        .map_err(PersistenceError::Schema)?;
        if !present {
            sqlx::raw_sql(SCHEMA)
                .execute(&mut *transaction)
                .await
                .map_err(PersistenceError::Schema)?;
        }
        transaction.commit().await.map_err(PersistenceError::Schema)
    }

    /// Answer whether the database is usable now.
    ///
    /// A round trip, not a pool-state inspection: a pool with idle connections to a server that
    /// refuses queries looks healthy from the inside.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] if the round trip fails or times out.
    pub async fn ping(&self) -> Result<(), PersistenceError> {
        sqlx::query("select 1")
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(PersistenceError::Query)
    }

    /// The pool, for the queries of this crate and for tests that inspect rows.
    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Close the pool and wait for its connections to close.
    pub async fn close(&self) {
        self.pool.close().await;
    }
}
