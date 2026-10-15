//! The write path of a connected vault (workspace change `add-notes-write-back`, OBS-024).
//!
//! Edge publishes each write as `notes.file.write_requested.v1` on the command stream. Notes reads
//! the commands through the durable that Edge creates for it.

mod consumer;
mod queue;
mod request;

use ratatoskr_event_envelope::CommandEnvelope;
use uuid::Uuid;

use crate::files::{FileRecorder, RecordError};
use queue::Locked;
use request::Request;

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

impl From<ratatoskr_notes_persistence::PersistenceError> for WriteError {
    fn from(_: ratatoskr_notes_persistence::PersistenceError) -> Self {
        Self("the database")
    }
}

impl From<RecordError> for WriteError {
    fn from(_: RecordError) -> Self {
        Self("a file change")
    }
}

/// How the channel ended a delivered write.
#[derive(Debug, Clone, Copy)]
enum End<'a> {
    /// The file has this SHA-256 now, or no file is at the path.
    Acknowledged(Option<&'a str>),
    /// The channel did not apply the write.
    Refused(&'a Refusal),
}

/// The code of a command that is not a valid write command.
const INVALID: &str = "notes.request.invalid";

/// The code of a connection that the tenant does not hold.
const CONNECTION_NOT_FOUND: &str = "notes.connection.not_found";

/// The code of a write whose content notes does not hold for its connection.
const CONTENT_MISSING: &str = "notes.write.content_missing";

/// The code of a write that met a revision that its writer did not see.
const CONFLICT: &str = "notes.write.conflict";

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

    /// Take one write command: queue it at the end of the queue of its file, and deliver it when
    /// it is the head. The content that the write names must be bytes that the connection
    /// uploaded or holds as a revision, with the declared length; otherwise the command is
    /// refused with `notes.write.content_missing`.
    ///
    /// # Errors
    ///
    /// [`WriteError`] when the database fails; the command is delivered again.
    pub async fn handle(&self, command: &CommandEnvelope) -> Result<Handled, WriteError> {
        let Some(request) = Request::parse(command) else {
            return Ok(Handled::Refused { code: INVALID });
        };
        let database = self.recorder.database();
        let Some((mut transaction, connection)) =
            database.begin_file_change(request.connection_id).await?
        else {
            return Ok(Handled::Refused {
                code: CONNECTION_NOT_FOUND,
            });
        };
        if let Some(digest) = &request.content_hash {
            let usable = self
                .recorder
                .usable_length(&mut transaction, digest)
                .await?;
            if usable != request.content_length {
                return Ok(Handled::Refused {
                    code: CONTENT_MISSING,
                });
            }
        }
        let write_id = Uuid::now_v7();
        transaction
            .insert_write(&request.new_write(write_id))
            .await?;
        Locked {
            recorder: &self.recorder,
            transaction: &mut transaction,
            connection: &connection,
            connection_id: request.connection_id,
        }
        .advance(&request.path_key)
        .await?;
        transaction.commit().await?;
        Ok(Handled::Queued { write_id })
    }

    /// The writes of `connection_id` that its channel delivers now, in the order of acceptance.
    ///
    /// # Errors
    ///
    /// [`WriteError`] when the database fails.
    pub async fn deliveries(&self, connection_id: Uuid) -> Result<Vec<Delivery>, WriteError> {
        let rows = self
            .recorder
            .database()
            .delivering_writes(connection_id)
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| Delivery {
                write_id: row.write_id,
                operation_id: row.operation_id,
                kind: row.kind,
                path: row.path,
                target_path: row.target_path,
                base_hash: row.base_hash,
                content_hash: row.content_hash,
            })
            .collect())
    }

    /// The channel acknowledged the delivered write `write_id` of `connection_id`; the file has
    /// the SHA-256 `revision` now, or no file is at the path after a delete. Only now does the
    /// revision become current and `notes.file.changed.v1` go to the outbox. The next write of
    /// the queue of the file is delivered. Returns whether a delivered write was acknowledged.
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
        self.end(connection_id, write_id, End::Acknowledged(revision))
            .await
    }

    /// The channel refused the delivered write `write_id` of `connection_id`: a base mismatch
    /// fails it as a conflict, any other refusal with the code of the channel. The next write of
    /// the queue of the file is delivered. Returns whether a delivered write was refused.
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
        self.end(connection_id, write_id, End::Refused(&refusal))
            .await
    }

    /// End the delivered write `write_id`, and deliver the next write of its queue.
    async fn end(
        &self,
        connection_id: Uuid,
        write_id: Uuid,
        end: End<'_>,
    ) -> Result<bool, WriteError> {
        let database = self.recorder.database();
        let Some((mut transaction, connection)) = database.begin_file_change(connection_id).await?
        else {
            return Ok(false);
        };
        let Some(write) = transaction.write(write_id).await? else {
            return Ok(false);
        };
        if write.state != "delivering" {
            return Ok(false);
        }
        let mut locked = Locked {
            recorder: &self.recorder,
            transaction: &mut transaction,
            connection: &connection,
            connection_id,
        };
        match end {
            End::Acknowledged(revision) => {
                let revision = if write.kind == "delete" {
                    None
                } else {
                    revision
                };
                locked.record(&write, revision).await?;
                locked.transaction.succeed_write(write_id, revision).await?;
            }
            End::Refused(Refusal::BaseMismatch { .. }) => {
                locked.transaction.fail_write(write_id, CONFLICT).await?;
            }
            End::Refused(Refusal::Failed { code }) => {
                locked.transaction.fail_write(write_id, code).await?;
            }
        }
        locked.advance(&write.path_key).await?;
        transaction.commit().await?;
        Ok(true)
    }

    /// The outcome of the write of `operation_id`, or `None` when no write has it.
    ///
    /// # Errors
    ///
    /// [`WriteError`] when the database fails.
    pub async fn outcome(&self, operation_id: Uuid) -> Result<Option<Outcome>, WriteError> {
        let Some(write) = self
            .recorder
            .database()
            .write_of_operation(operation_id)
            .await?
        else {
            return Ok(None);
        };
        Ok(Some(match write.state.as_str() {
            "succeeded" => Outcome::Succeeded {
                revision: write.revision,
            },
            "failed" => Outcome::Failed {
                code: write.error_code.unwrap_or_default(),
                conflict_id: None,
            },
            _ => Outcome::Pending,
        }))
    }
}
