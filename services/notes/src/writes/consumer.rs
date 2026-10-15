//! The consumer of the write commands (workspace spec `fleet-bus-topology`).
//!
//! Edge creates the durable [`WRITE_DURABLE`] on [`COMMAND_STREAM`]; notes never creates a stream
//! or a consumer. The consumer looks the durable up by its name and compares its filter, its
//! acknowledgement policy and its ack wait with the values that Edge provisions. Readiness answers
//! 503 until that verification passed, and again as soon as one fails. The connection uses the
//! inbox prefix `_INBOX_notes` of the outbox relay, the only one that the `NOTES` identity may
//! subscribe to.
//!
//! The consumer hands each command to [`Writes::handle`] and acknowledges it after the write path
//! committed what it did with it, so a stop between the two delivers the command again, and the
//! idempotency key of the write makes the repeat a repeat.

use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream::consumer::{AckPolicy, Consumer, pull};
use async_nats::jetstream::{self, AckKind, Context};
use futures_util::StreamExt as _;
use ratatoskr_event_envelope::CommandEnvelope;
use tokio_util::sync::CancellationToken;

use crate::admin::RuntimeState;
use crate::outbox::{self, BusSettings};
use crate::writes::Writes;

/// The command stream that Edge owns.
pub const COMMAND_STREAM: &str = "ratatoskr_commands";

/// The durable of the write commands. Edge creates it; notes only looks it up.
pub const WRITE_DURABLE: &str = "ratatoskr_notes_write_requests";

/// The one subject that the durable delivers.
pub const WRITE_FILTER: &str = "cmd.notes.file.write_requested.v1";

/// The ack wait that Edge gives the durable: the `standard` row of `DOMAIN_CONSUMERS`.
const WRITE_ACK_WAIT: Duration = Duration::from_secs(30);

/// How long one request to the server may take.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// How often the consumer verifies the durable again, and how long it waits before it starts
/// again after a failure.
const VERIFY_INTERVAL: Duration = Duration::from_secs(30);

/// How many messages one pull asks for: a bound on the commands that wait in memory.
const BATCH: usize = 16;

/// How long the server waits before it delivers again a command that the write path failed.
const RETRY_DELAY: Duration = Duration::from_secs(5);

/// The durable is missing, cannot be reached, or differs from what Edge provisions.
#[derive(Debug, thiserror::Error)]
#[error(
    "the durable {WRITE_DURABLE} is missing, cannot be reached or differs from its \
     specification; start ratatoskr-edge first so that it provisions the durable"
)]
pub struct DurableError;

/// Look up [`WRITE_DURABLE`] on [`COMMAND_STREAM`] and compare it with its specification.
///
/// # Errors
///
/// [`DurableError`] when the durable is missing, cannot be read, or differs.
pub(crate) async fn write_durable(
    context: &Context,
) -> Result<Consumer<pull::Config>, DurableError> {
    let consumer = context
        .get_consumer_from_stream::<pull::Config, _, _>(WRITE_DURABLE, COMMAND_STREAM)
        .await
        .map_err(|_| DurableError)?;
    let config = &consumer.cached_info().config;
    let matches = config.durable_name.as_deref() == Some(WRITE_DURABLE)
        && config.filter_subject == WRITE_FILTER
        && config.ack_policy == AckPolicy::Explicit
        && config.ack_wait == WRITE_ACK_WAIT;
    if matches {
        Ok(consumer)
    } else {
        Err(DurableError)
    }
}

/// Reads the write commands through [`WRITE_DURABLE`].
#[derive(Debug)]
pub struct WriteConsumer {
    settings: BusSettings,
    runtime: Arc<RuntimeState>,
    writes: Writes,
}

impl WriteConsumer {
    /// A consumer of the server of `settings` that hands each command to `writes`. From now on
    /// readiness of `runtime` needs the durable.
    #[must_use]
    pub fn new(settings: BusSettings, runtime: Arc<RuntimeState>, writes: Writes) -> Self {
        runtime.require_write_durable();
        Self {
            settings,
            runtime,
            writes,
        }
    }

    /// Look the durable up and verify it, and record the result for readiness.
    ///
    /// # Errors
    ///
    /// [`DurableError`] when the server cannot be reached, or the durable is missing or differs.
    pub async fn verify(&self) -> Result<(), DurableError> {
        let verified = self.context().await;
        let verified = match verified {
            Some(context) => write_durable(&context).await.map(|_| ()),
            None => Err(DurableError),
        };
        self.runtime.set_write_durable_verified(verified.is_ok());
        verified
    }

    /// A `JetStream` context of a new connection, or `None` when the server cannot be reached.
    async fn context(&self) -> Option<Context> {
        match outbox::connect(&self.settings).await {
            Ok(client) => Some(
                jetstream::ContextBuilder::new()
                    .timeout(REQUEST_TIMEOUT)
                    .build(client),
            ),
            Err(error) => {
                tracing::warn!(%error, "the write consumer cannot reach the bus");
                None
            }
        }
    }

    /// Hand each command of the durable to the write path until `stop`, and verify the durable
    /// every [`VERIFY_INTERVAL`]. A command is acknowledged after the write path took it, and
    /// delivered again after [`RETRY_DELAY`] when the write path failed; a message that is not a
    /// command is terminated. When the bus or the durable fails, the service is not ready, and
    /// the consumer starts again after [`VERIFY_INTERVAL`].
    pub async fn run(self, stop: CancellationToken) {
        loop {
            match self.consume(&stop).await {
                Ok(()) => return,
                Err(error) => {
                    self.runtime.set_write_durable_verified(false);
                    tracing::warn!(%error, "the service is not ready");
                }
            }
            tokio::select! {
                biased;
                () = stop.cancelled() => return,
                () = tokio::time::sleep(VERIFY_INTERVAL) => {}
            }
        }
    }

    /// Consume the durable until `stop` (`Ok`) or until the bus or the durable fails.
    async fn consume(&self, stop: &CancellationToken) -> Result<(), DurableError> {
        let context = self.context().await.ok_or(DurableError)?;
        let consumer = write_durable(&context).await?;
        self.runtime.set_write_durable_verified(true);
        let mut messages = consumer
            .stream()
            .max_messages_per_batch(BATCH)
            .messages()
            .await
            .map_err(|_| DurableError)?;
        let mut verify = tokio::time::interval(VERIFY_INTERVAL);
        verify.tick().await;
        loop {
            tokio::select! {
                biased;
                () = stop.cancelled() => return Ok(()),
                _ = verify.tick() => {
                    write_durable(&context).await?;
                }
                message = messages.next() => match message {
                    Some(Ok(message)) => self.take(&message).await,
                    Some(Err(_)) | None => return Err(DurableError),
                },
            }
        }
    }

    /// Hand one message to the write path and acknowledge it.
    async fn take(&self, message: &jetstream::Message) {
        let Ok(command) = CommandEnvelope::from_json(&message.payload) else {
            tracing::warn!("a message of the write durable is not a command; it is dropped");
            acknowledge(message, AckKind::Term).await;
            return;
        };
        match self.writes.handle(&command).await {
            Ok(_) => acknowledge(message, AckKind::Ack).await,
            Err(error) => {
                tracing::warn!(%error, "a write command is delivered again later");
                acknowledge(message, AckKind::Nak(Some(RETRY_DELAY))).await;
            }
        }
    }
}

/// Send `kind` for `message`. A failure is logged: the server delivers the message again after
/// the ack wait, and the write path takes a repeat as a repeat.
async fn acknowledge(message: &jetstream::Message, kind: AckKind) {
    if let Err(error) = message.ack_with(kind).await {
        tracing::warn!(%error, "a message of the write durable could not be acknowledged");
    }
}
