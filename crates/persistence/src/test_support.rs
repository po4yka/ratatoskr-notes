//! A disposable database per test.
//!
//! Enabled by the `test-support` feature, so it is never compiled into a service binary.
//!
//! Each test gets its own database, not its own transaction. A CHECK constraint, an idempotent
//! re-apply and a unique index behave differently inside one transaction, and a suite that cannot
//! see a constraint fire does not test the schema.

use std::env;

use secrecy::SecretString;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use sqlx::{ConnectOptions as _, Executor as _};
use uuid::Uuid;

use crate::{Database, PersistenceError};

/// How many connections one test may hold.
///
/// Two, not the sqlx default of ten. Several test binaries run at once and each test creates its
/// own database, so a default-sized pool per test exhausts `max_connections` of the server.
const TEST_POOL_SIZE: u32 = 2;

/// Where the disposable databases are created.
///
/// `NOTES_TEST_DATABASE_URL` overrides it. The default matches `compose.yaml`, so
/// `docker compose up -d` and then `cargo test` works with no further setup.
#[must_use]
#[expect(
    clippy::disallowed_methods,
    reason = "the workspace bans direct environment reads so that configuration has exactly one \
              loader. This is test-only scaffolding that never runs in a service binary, and the \
              variable is not part of the notes configuration: it names where a test may create \
              and drop databases."
)]
pub fn admin_url() -> String {
    env::var("NOTES_TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://notes:notes@127.0.0.1:5432/notes".to_owned())
}

/// A database that drops itself when the test calls [`TestDatabase::cleanup`].
#[derive(Debug)]
pub struct TestDatabase {
    /// The connected pool, with the schema applied.
    pub database: Database,
    name: String,
}

impl TestDatabase {
    /// Create a fresh database, apply the embedded schema, and connect to it.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Connect`] if the server is unreachable. That is a real failure, not a
    /// reason to skip: a suite that passes without a database proves nothing.
    pub async fn create() -> Result<Self, PersistenceError> {
        let name = format!("notes_test_{}", Uuid::now_v7().simple());
        let admin = admin_url();

        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&admin)
            .await
            .map_err(PersistenceError::Connect)?;
        // The name comes from a UUID, so it cannot carry an injection; `PostgreSQL` has no bind
        // parameters for an identifier in DDL. The locale is stated, not inherited from template1,
        // and is byte-identical to compose.yaml and ci.yml.
        pool.execute(
            format!(
                r#"create database "{name}" template template0
                   locale_provider icu icu_locale 'und-x-icu' encoding 'UTF8'"#
            )
            .as_str(),
        )
        .await
        .map_err(PersistenceError::Query)?;
        pool.close().await;

        let options: PgConnectOptions = admin
            .parse::<PgConnectOptions>()
            .map_err(PersistenceError::Connect)?
            .database(&name)
            .log_statements(tracing::log::LevelFilter::Off);
        let pool = PgPoolOptions::new()
            .max_connections(TEST_POOL_SIZE)
            .connect_with(options)
            .await
            .map_err(PersistenceError::Connect)?;
        let database = Database { pool };
        database.apply_schema().await?;

        Ok(Self { database, name })
    }

    /// The pool under test.
    #[must_use]
    pub fn pool(&self) -> &PgPool {
        self.database.pool()
    }

    /// The connection URL of this database, for a test that starts the real binary against it.
    #[must_use]
    pub fn url(&self) -> String {
        let admin = admin_url();
        let (prefix, _) = admin.rsplit_once('/').unwrap_or((admin.as_str(), ""));
        format!("{prefix}/{}", self.name)
    }

    /// The connection URL as a secret.
    #[must_use]
    pub fn secret_url(&self) -> SecretString {
        SecretString::from(self.url())
    }

    /// Drop the database.
    ///
    /// Explicit rather than a `Drop` impl: the drop needs async work, and a blocking drop inside a
    /// Tokio worker deadlocks. A test that panics leaves its database behind for investigation.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] if the drop fails.
    pub async fn cleanup(self) -> Result<(), PersistenceError> {
        self.database.close().await;
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&admin_url())
            .await
            .map_err(PersistenceError::Connect)?;
        pool.execute(format!(r#"drop database if exists "{}" with (force)"#, self.name).as_str())
            .await
            .map_err(PersistenceError::Query)?;
        pool.close().await;
        Ok(())
    }
}
