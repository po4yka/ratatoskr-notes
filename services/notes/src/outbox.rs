//! The relay of the outbox to `JetStream` under the `NOTES` identity (workspace spec
//! `fleet-bus-topology`).
//!
//! The relay publishes each row of the outbox in the order of the outbox, unchanged, on the subject
//! `evt.<event type>`, with the event id as `Nats-Msg-Id`, so that `JetStream` drops a second copy.
//! It marks a row published only after the `PubAck`, so a stop between the two publishes the row
//! again, and the consumer sees one copy. The events of one connection are in the outbox in the
//! order of their connection sequence, and the relay publishes them in that order.
//!
//! The connection uses the inbox prefix `_INBOX_notes`: the deployed ACL lets the `NOTES` identity
//! subscribe to `_INBOX_notes.>` only, and the `PubAck` of a publish arrives there.

use std::path::PathBuf;
use std::time::Duration;

use async_nats::jetstream::{self, message::PublishMessage};
use ratatoskr_notes_persistence::Database;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

/// The inbox prefix of the one NATS connection of this service.
const INBOX_PREFIX: &str = "_INBOX_notes";

/// How many rows one read of the outbox takes.
const BATCH: i64 = 64;

/// How long the connect may take.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

/// How long the relay waits for one `PubAck`.
const PUBLISH_TIMEOUT: Duration = Duration::from_secs(5);

/// How often the relay reads the outbox when nothing failed.
const POLL_INTERVAL: Duration = Duration::from_secs(1);

/// The longest wait after a failed pass.
const LONGEST_BACKOFF: Duration = Duration::from_secs(30);

/// Where the relay reaches NATS.
#[derive(Debug, Clone)]
pub struct BusSettings {
    /// The NATS URL.
    pub url: String,
    /// The file that holds the nkey seed of the `NOTES` identity, or `None` for a server without
    /// authentication (a test).
    pub nkey_seed_path: Option<PathBuf>,
}

/// A failure of one pass of the relay. The message names no event content.
#[derive(Debug, thiserror::Error)]
#[error("the outbox relay failed: {0}")]
pub struct RelayError(String);

/// The subject of an event type that this service produces, or `None` for another type.
fn subject_of(event_type: &str) -> Option<&'static str> {
    Some(match event_type {
        "notes.vault.connected.v1" => "evt.notes.vault.connected.v1",
        "notes.vault.disconnected.v1" => "evt.notes.vault.disconnected.v1",
        "notes.file.changed.v1" => "evt.notes.file.changed.v1",
        "platform.operation.reported.v1" => "evt.platform.operation.reported.v1",
        _ => return None,
    })
}

/// Publishes the rows of the outbox in order and marks each one published after its `PubAck`.
/// Clones share one connection.
#[derive(Debug, Clone)]
pub struct OutboxRelay {
    database: Database,
    settings: BusSettings,
    context: Arc<Mutex<Option<jetstream::Context>>>,
}

impl OutboxRelay {
    /// A relay of the outbox of `database` to the server of `settings`.
    #[must_use]
    pub fn new(database: Database, settings: BusSettings) -> Self {
        Self {
            database,
            settings,
            context: Arc::new(Mutex::new(None)),
        }
    }

    /// Publish every unpublished row once. Returns how many rows were published. A failure drops
    /// the connection, so the next pass connects again and reads the seed file again.
    ///
    /// # Errors
    ///
    /// [`RelayError`] when NATS or the database cannot be used, or a row has a type that this
    /// service does not produce.
    pub async fn publish_pending(&self) -> Result<usize, RelayError> {
        let mut held = self.context.lock().await;
        let context = if let Some(context) = held.as_ref() {
            context.clone()
        } else {
            let context = self.connect().await?;
            *held = Some(context.clone());
            context
        };
        let published = self.publish_with(&context).await;
        if published.is_err() {
            *held = None;
        }
        published
    }

    async fn publish_with(&self, context: &jetstream::Context) -> Result<usize, RelayError> {
        let mut published = 0;
        loop {
            let pending = self
                .database
                .pending_events(BATCH)
                .await
                .map_err(|error| RelayError(error.to_string()))?;
            if pending.is_empty() {
                return Ok(published);
            }
            for event in pending {
                let subject = subject_of(&event.event_type).ok_or_else(|| {
                    RelayError(format!(
                        "the outbox holds the unknown type {}",
                        event.event_type
                    ))
                })?;
                let body = serde_json::to_vec(&event.envelope)
                    .map_err(|error| RelayError(error.to_string()))?;
                let message = PublishMessage::build()
                    .payload(body.into())
                    .message_id(event.event_id.to_string());
                let acknowledgement = context
                    .send_publish(subject, message)
                    .await
                    .map_err(|error| RelayError(error.to_string()))?;
                tokio::time::timeout(PUBLISH_TIMEOUT, acknowledgement)
                    .await
                    .map_err(|_| RelayError("no PubAck in time".to_owned()))?
                    .map_err(|error| RelayError(error.to_string()))?;
                self.database
                    .mark_published(event.event_id)
                    .await
                    .map_err(|error| RelayError(error.to_string()))?;
                published += 1;
            }
        }
    }

    async fn connect(&self) -> Result<jetstream::Context, RelayError> {
        let options = match &self.settings.nkey_seed_path {
            Some(path) => {
                let seed = tokio::fs::read_to_string(path).await.map_err(|error| {
                    RelayError(format!("the nkey seed cannot be read: {error}"))
                })?;
                async_nats::ConnectOptions::with_nkey(seed.trim().to_owned())
            }
            None => async_nats::ConnectOptions::new(),
        };
        let client = options
            .custom_inbox_prefix(INBOX_PREFIX)
            .connection_timeout(CONNECT_TIMEOUT)
            .connect(self.settings.url.as_str())
            .await
            .map_err(|error| RelayError(error.to_string()))?;
        Ok(jetstream::ContextBuilder::new()
            .timeout(PUBLISH_TIMEOUT)
            .build(client))
    }

    /// Publish the outbox every [`POLL_INTERVAL`] until `stop`. After a failed pass it waits twice
    /// as long as before, up to [`LONGEST_BACKOFF`], and logs the failure.
    pub async fn run(self, stop: CancellationToken) {
        let mut wait = POLL_INTERVAL;
        loop {
            tokio::select! {
                biased;
                () = stop.cancelled() => return,
                () = tokio::time::sleep(wait) => {}
            }
            match self.publish_pending().await {
                Ok(_) => wait = POLL_INTERVAL,
                Err(error) => {
                    tracing::warn!(%error, "the outbox relay failed; it tries again");
                    wait = (wait * 2).min(LONGEST_BACKOFF);
                }
            }
        }
    }
}
