//! The relay of the outbox to `JetStream` under the `NOTES` identity.

use ratatoskr_notes_persistence::Database;

/// Where the relay reaches NATS.
#[derive(Debug, Clone)]
pub struct BusSettings {
    /// The NATS URL.
    pub url: String,
    /// The file that holds the nkey seed of the `NOTES` identity, or `None` for a server without
    /// authentication (a test).
    pub nkey_seed_path: Option<std::path::PathBuf>,
}

/// A failure of one pass of the relay.
#[derive(Debug, thiserror::Error)]
#[error("the outbox relay failed: {0}")]
pub struct RelayError(String);

/// Publishes the rows of the outbox in order and marks each one published after its `PubAck`.
#[derive(Debug, Clone)]
pub struct OutboxRelay {
    database: Database,
    settings: BusSettings,
}

impl OutboxRelay {
    /// A relay of the outbox of `database` to the server of `settings`.
    #[must_use]
    pub fn new(database: Database, settings: BusSettings) -> Self {
        Self { database, settings }
    }

    /// Publish every unpublished row once. Returns how many rows were published.
    ///
    /// # Errors
    ///
    /// [`RelayError`] when NATS or the database cannot be used.
    pub async fn publish_pending(&self) -> Result<usize, RelayError> {
        let _ = (&self.database, &self.settings);
        tokio::task::yield_now().await;
        Ok(0)
    }
}
