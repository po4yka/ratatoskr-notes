//! The consumer of the write commands.

use std::sync::Arc;

use crate::admin::RuntimeState;
use crate::outbox::BusSettings;

/// The command stream that Edge owns.
pub const COMMAND_STREAM: &str = "ratatoskr_commands";

/// The durable of the write commands. Edge creates it; notes only looks it up.
pub const WRITE_DURABLE: &str = "ratatoskr_notes_write_requests";

/// The one subject that the durable delivers.
pub const WRITE_FILTER: &str = "cmd.notes.file.write_requested.v1";

/// The durable is missing, cannot be reached, or differs from what Edge provisions.
#[derive(Debug, thiserror::Error)]
#[error("the durable {WRITE_DURABLE} is missing or differs from its specification")]
pub struct DurableError;

/// Reads the write commands through [`WRITE_DURABLE`].
#[derive(Debug)]
pub struct WriteConsumer {
    settings: BusSettings,
    runtime: Arc<RuntimeState>,
}

impl WriteConsumer {
    /// A consumer of the server of `settings`, which reports its readiness to `runtime`.
    #[must_use]
    pub const fn new(settings: BusSettings, runtime: Arc<RuntimeState>) -> Self {
        Self { settings, runtime }
    }

    /// Look the durable up and verify it. Checks nothing yet.
    ///
    /// # Errors
    ///
    /// [`DurableError`] when the durable is missing or differs.
    pub async fn verify(&self) -> Result<(), DurableError> {
        let _ = (&self.settings, &self.runtime);
        Ok(())
    }
}
