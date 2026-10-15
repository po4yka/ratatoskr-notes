//! The write path of a connected vault (workspace change `add-notes-write-back`, OBS-024).
//!
//! Edge publishes each write as `notes.file.write_requested.v1` on the command stream. Notes reads
//! the commands through the durable that Edge creates for it.

mod consumer;

use ratatoskr_event_envelope::CommandEnvelope;
use uuid::Uuid;

use crate::files::FileRecorder;

pub use consumer::{COMMAND_STREAM, DurableError, WRITE_DURABLE, WRITE_FILTER, WriteConsumer};

/// What [`Writes::handle`] did with one command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Handled {
    /// The write waits in the queue of its file.
    Queued {
        /// The pending write.
        write_id: Uuid,
    },
    /// The command repeats an earlier write; nothing is queued again.
    Repeated {
        /// The earlier write.
        write_id: Uuid,
    },
    /// The command is refused with the code, and nothing changed.
    Refused {
        /// The error code of the failure.
        code: &'static str,
    },
}

/// One write that a channel delivers now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery {
    /// The pending write.
    pub write_id: Uuid,
    /// The operation that Edge minted for it.
    pub operation_id: Uuid,
    /// The wire word of the operation: `create`, `update`, `rename`, `move`, `delete`,
    /// `set_property` or `resolve_conflict`.
    pub kind: String,
    /// The path that the write changes.
    pub path: String,
    /// The new path of a `rename` or a `move`.
    pub target_path: Option<String>,
    /// The revision that the write is based on.
    pub base_hash: Option<String>,
    /// The SHA-256 of the content that the write puts at the path.
    pub content_hash: Option<String>,
}

/// Why a channel did not apply a delivered write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    /// The file in the vault does not have the base revision of the write.
    BaseMismatch {
        /// The SHA-256 of the file in the vault, when the channel knows it.
        local_hash: Option<String>,
    },
    /// The channel failed the write with its typed error code.
    Failed {
        /// The error code.
        code: String,
    },
}

/// The outcome of the write of one operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The write waits in its queue or for its acknowledgement.
    Pending,
    /// The channel acknowledged the write.
    Succeeded {
        /// The SHA-256 of the file after the write; `None` after a delete.
        revision: Option<String>,
    },
    /// The write failed.
    Failed {
        /// The error code.
        code: String,
        /// The conflict that the write raised.
        conflict_id: Option<Uuid>,
    },
}

/// A write could not be processed now. The command is delivered again.
#[derive(Debug, thiserror::Error)]
#[error("a write could not be processed: {0}")]
pub struct WriteError(&'static str);

/// The pending writes of every connection.
#[derive(Debug, Clone)]
pub struct Writes {
    recorder: FileRecorder,
}

impl Writes {
    /// The writes of the database and the blob store of `recorder`.
    #[must_use]
    pub const fn new(recorder: FileRecorder) -> Self {
        Self { recorder }
    }

    /// Take one write command. Queues nothing yet.
    ///
    /// # Errors
    ///
    /// [`WriteError`] when the database fails; the command is delivered again.
    pub async fn handle(&self, command: &CommandEnvelope) -> Result<Handled, WriteError> {
        let _ = (&self.recorder, command);
        Ok(Handled::Queued {
            write_id: Uuid::now_v7(),
        })
    }

    /// The writes of `connection_id` that its channel delivers now, in the order of acceptance.
    ///
    /// # Errors
    ///
    /// [`WriteError`] when the database fails.
    pub async fn deliveries(&self, connection_id: Uuid) -> Result<Vec<Delivery>, WriteError> {
        let _ = connection_id;
        Ok(Vec::new())
    }

    /// The channel acknowledged the delivered write `write_id` of `connection_id`; the file has
    /// the SHA-256 `revision` now, or no file is at the path after a delete. Returns whether a
    /// delivered write was acknowledged.
    ///
    /// # Errors
    ///
    /// [`WriteError`] when the database fails.
    pub async fn acknowledge(
        &self,
        connection_id: Uuid,
        write_id: Uuid,
        revision: Option<&str>,
    ) -> Result<bool, WriteError> {
        let _ = (connection_id, write_id, revision);
        Ok(false)
    }

    /// The channel refused the delivered write `write_id` of `connection_id`. Returns whether a
    /// delivered write was refused.
    ///
    /// # Errors
    ///
    /// [`WriteError`] when the database fails.
    pub async fn refuse(
        &self,
        connection_id: Uuid,
        write_id: Uuid,
        refusal: Refusal,
    ) -> Result<bool, WriteError> {
        let _ = (connection_id, write_id, refusal);
        Ok(false)
    }

    /// The outcome of the write of `operation_id`, or `None` when no write has it.
    ///
    /// # Errors
    ///
    /// [`WriteError`] when the database fails.
    pub async fn outcome(&self, operation_id: Uuid) -> Result<Option<Outcome>, WriteError> {
        let _ = operation_id;
        Ok(None)
    }
}
