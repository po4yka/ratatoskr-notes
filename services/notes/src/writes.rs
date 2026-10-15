//! The write path of a connected vault (workspace change `add-notes-write-back`, OBS-024).
//!
//! Edge publishes each write as `notes.file.write_requested.v1` on the command stream. Notes reads
//! the commands through the durable that Edge creates for it.

mod conflict;
mod consumer;
mod group;
mod merge;
mod mode;
mod queue;
mod report;
mod request;

use std::sync::Arc;

use ratatoskr_event_envelope::CommandEnvelope;
use ratatoskr_notes_persistence::{FileTransaction, OutboxEvent};
use tokio::sync::Semaphore;
use uuid::Uuid;

use crate::events::EnvelopeBuildError;
use crate::files::{FileRecorder, RecordError};
use conflict::{Raised, Sides};
use queue::Locked;
use report::{ReportRow, Step};
use request::Request;

pub(crate) use conflict::MAX_SIDE_BYTES;
pub use consumer::{COMMAND_STREAM, DurableError, WRITE_DURABLE, WRITE_FILTER, WriteConsumer};
pub use mode::{ModeChange, RECONFIGURE_FAILED, WRITE_MODE_OPERATION_KIND};

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
    /// The SHA-256 of the content that the write puts at the path, or of the JSON value of a
    /// `set_property`.
    pub content_hash: Option<String>,
    /// The frontmatter property of a `set_property`.
    pub property_key: Option<String>,
    /// `set` or `remove` for a `set_property`.
    pub property_change: Option<String>,
    /// The write group of a rename and its link rewrites: the `write_id` of the rename. A rewrite
    /// is an `update` of a linking note.
    pub group_id: Option<Uuid>,
    /// Whether the sync writer landed the write in the working tree.
    pub landed: bool,
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

/// The SHA-256 that the file at the path of `write` has after the vault applied it as written:
/// the content of a `create`, an `update` or a resolution, and of a `rename` or a `move` the new
/// bytes of its own link rewrites, or else its base.
/// `None` for a `delete` and a `set_property`, whose bytes notes does not compute.
pub(crate) fn expected_hash(write: &ratatoskr_notes_persistence::WriteRow) -> Option<&str> {
    match write.kind.as_str() {
        "create" | "update" | "resolve_conflict" => write.content_hash.as_deref(),
        "rename" | "move" => write.content_hash.as_deref().or(write.base_hash.as_deref()),
        _ => None,
    }
}

/// Whether the change of `path` to the SHA-256 `digest` (`None`: the file left the tree) is the
/// echo of the delivered write at the head of its queue, and if so acknowledge that write. A
/// `create`, an `update` or a resolution is echoed by its path with its content hash, or with any
/// hash once the sync writer landed it in the tree, and a `delete` by the removal of its path. Returns the write that the echo acknowledged.
///
/// # Errors
///
/// [`RecordError`] when the database fails.
pub(crate) async fn echo(
    recorder: &FileRecorder,
    connection_id: Uuid,
    path: &str,
    digest: Option<&str>,
) -> Result<Option<Uuid>, RecordError> {
    let Some((mut transaction, _)) = recorder.database().begin_file_change(connection_id).await?
    else {
        return Ok(None);
    };
    let head = transaction
        .queue_head(&ratatoskr_notes_index::paths::path_key(path))
        .await?;
    drop(transaction);
    let Some(head) = head.filter(|head| head.state == "delivering" && head.path == path) else {
        return Ok(None);
    };
    // After the write landed in the tree, the next change of its path is its acknowledgement,
    // also with other bytes: the vault rewrote the file, for example Obsidian formatted it
    // (design D6). Before it landed, only the exact bytes are.
    let echoes = match digest {
        Some(digest) => {
            matches!(head.kind.as_str(), "create" | "update" | "resolve_conflict")
                && (head.landed || head.content_hash.as_deref() == Some(digest))
        }
        None => head.kind == "delete",
    };
    if !echoes {
        return Ok(None);
    }
    let acknowledged = Writes::new(recorder.clone())
        .end(connection_id, head.write_id, End::Acknowledged(digest))
        .await
        .map_err(|_| RecordError::WRITE)?;
    Ok(acknowledged.then_some(head.write_id))
}

/// The outcome of the rename `leader` whose write group is `group`: pending while a write of the
/// group is open; failed when one failed, with `notes.write.partially_applied` when another
/// succeeded, else with the code of the rename (or of the first failed write) and the conflict of
/// the group when there is one; else succeeded with the file of the rename.
fn group_outcome(
    leader: &ratatoskr_notes_persistence::WriteRow,
    group: &[ratatoskr_notes_persistence::WriteRow],
) -> Outcome {
    if group
        .iter()
        .any(|write| matches!(write.state.as_str(), "queued" | "delivering"))
    {
        return Outcome::Pending;
    }
    if !group.iter().any(|write| write.state == "failed") {
        return Outcome::Succeeded {
            revision: leader.revision.clone(),
        };
    }
    let code = if group.iter().any(|write| write.state == "succeeded") {
        PARTIALLY_APPLIED
    } else {
        group::failure_code(group, leader.write_id).unwrap_or_default()
    };
    Outcome::Failed {
        code: code.to_owned(),
        conflict_id: group.iter().find_map(|write| write.conflict_id),
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

/// The event type of a raised conflict.
const RAISED: &str = "notes.conflict.raised.v1";

/// How many unannounced conflicts one pass of [`Writes::announce_pending`] reads.
const ANNOUNCE_BATCH: i64 = 256;

/// How many merge proposals run at the same time. One merge of sides at the caps of
/// `writes::conflict` peaks at about 12 MiB, so two stay within 25 MiB.
const MERGES: usize = 2;

/// The code of a command that is not a valid write command.
const INVALID: &str = "notes.request.invalid";

/// The code of a connection that the tenant does not hold.
pub(crate) const CONNECTION_NOT_FOUND: &str = "notes.connection.not_found";

/// The code of a write to a connection whose write mode is off.
const DISABLED: &str = "notes.write.disabled";

/// The code of a write whose content notes does not hold for its connection.
const CONTENT_MISSING: &str = "notes.write.content_missing";

/// The code of a resolution whose conflict the connection does not hold open.
const CONFLICT_NOT_FOUND: &str = "notes.conflict.not_found";

/// The code of a write that met a revision that its writer did not see.
const CONFLICT: &str = "notes.write.conflict";

/// The code of a write that stopped the write path by a defect.
const INTERNAL: &str = "notes.write.internal";

/// The code of a rename whose link rewrites the channel applied only in part.
const PARTIALLY_APPLIED: &str = "notes.write.partially_applied";

/// The pending writes of every connection.
#[derive(Debug, Clone)]
pub struct Writes {
    recorder: FileRecorder,
    /// At most [`MERGES`] merge proposals are computed at the same time.
    merges: Arc<Semaphore>,
    /// A path whose rename panics while its link rewrites are planned. For tests.
    panic_in_plan: Option<Arc<str>>,
    /// A path whose write panics after it was committed. For tests.
    panic_after_commit: Option<Arc<str>>,
}

impl Writes {
    /// The writes of the database and the blob store of `recorder`.
    #[must_use]
    pub fn new(recorder: FileRecorder) -> Self {
        Self {
            recorder,
            merges: Arc::new(Semaphore::new(MERGES)),
            panic_in_plan: None,
            panic_after_commit: None,
        }
    }

    /// Panic while the link rewrites of a rename of `path` are planned. For tests: a defect of the
    /// write path must not stop the consumer.
    #[doc(hidden)]
    #[must_use]
    pub fn with_panic_in_plan(mut self, path: &str) -> Self {
        self.panic_in_plan = Some(Arc::from(path));
        self
    }

    /// Panic after a write of `path` was committed, before its conflicts are announced. For tests:
    /// a defect after the commit must not give the operation a second end.
    #[doc(hidden)]
    #[must_use]
    pub fn with_panic_after_commit(mut self, path: &str) -> Self {
        self.panic_after_commit = Some(Arc::from(path));
        self
    }

    /// Take one write command: queue it at the end of the queue of its file, and deliver it when
    /// it is the head. The tenant must hold the connection (else `notes.connection.not_found`,
    /// the code of a missing connection too), and its write mode must be on (else
    /// `notes.write.disabled`). The content that the write names must be bytes that the connection
    /// uploaded or holds as a revision, with the declared length; otherwise the command is
    /// refused with `notes.write.content_missing`.
    ///
    /// A `resolve_conflict` names an open conflict of the connection (else
    /// `notes.conflict.not_found`). It writes the file of the conflict on the revision that the
    /// conflict recorded as current: `keep_current` that revision, `keep_writer` the writer's
    /// content, `accept_merge` the merge proposal, and `custom` uploaded bytes. A choice without
    /// its side is refused with `notes.request.invalid`.
    ///
    /// A command whose idempotency key names an earlier write of the connection queues nothing:
    /// with the same origin and operation its operation reports the outcome of that write, and
    /// with others it is refused with `notes.request.invalid`.
    ///
    /// # Errors
    ///
    /// [`WriteError`] when the database fails; the command is delivered again.
    ///
    /// # Panics
    ///
    /// Only with the test hooks [`Self::with_panic_in_plan`], at a rename of its path, and
    /// [`Self::with_panic_after_commit`], after a write of its path was committed.
    pub async fn handle(&self, command: &CommandEnvelope) -> Result<Handled, WriteError> {
        let Some(mut request) = Request::parse(command) else {
            if let Some((owner_id, operation_id)) = Request::operation_of(command) {
                self.refused(owner_id, operation_id, INVALID).await?;
            }
            return Ok(Handled::Refused { code: INVALID });
        };
        let database = self.recorder.database();
        let Some((mut transaction, connection)) =
            database.begin_file_change(request.connection_id).await?
        else {
            return self.refuse_request(&request, CONNECTION_NOT_FOUND).await;
        };
        // One code for a missing and a foreign connection, so that no other connection is
        // disclosed.
        if connection.owner_id != request.owner_id {
            drop(transaction);
            return self.refuse_request(&request, CONNECTION_NOT_FOUND).await;
        }
        if !connection.write_mode_on {
            drop(transaction);
            return self.refuse_request(&request, DISABLED).await;
        }
        let mut locked = Locked {
            recorder: &self.recorder,
            transaction: &mut transaction,
            connection: &connection,
            connection_id: request.connection_id,
            raised: Vec::new(),
        };
        if let Some((write_id, digest)) = locked
            .transaction
            .write_of_key(&request.idempotency_key)
            .await?
        {
            if digest != request.digest {
                drop(transaction);
                return self.refuse_request(&request, INVALID).await;
            }
            locked
                .transaction
                .attach_operation(request.operation_id, write_id)
                .await?;
            if let Some(write) = locked.transaction.write(write_id).await? {
                locked.replay(request.operation_id, &write).await?;
            }
            transaction.commit().await?;
            return Ok(Handled::Repeated { write_id });
        }
        // The content of a resolution other than `custom` is a side of its conflict, which notes
        // holds; the content of any other write is bytes that the client uploaded.
        let uploaded = request
            .resolution
            .is_none_or(|(_, choice)| choice == "custom");
        if let Err(code) = self
            .resolution_sides(locked.transaction, &mut request)
            .await?
        {
            drop(transaction);
            return self.refuse_request(&request, code).await;
        }
        if let Some(digest) = request.content_hash.as_ref().filter(|_| uploaded) {
            let usable = self
                .recorder
                .usable_length(locked.transaction, digest)
                .await?;
            if usable != request.content_length {
                drop(transaction);
                return self.refuse_request(&request, CONTENT_MISSING).await;
            }
        }
        let write_id = Uuid::now_v7();
        // A rename rewrites the links that named the old path, as one write group (design D7).
        #[expect(clippy::panic, reason = "the test hook of a defect in the write path")]
        if request.target_path.is_some()
            && self.panic_in_plan.as_deref() == Some(request.path.as_str())
        {
            panic!("the test hook panics in the plan of a rename");
        }
        let plan = group::plan(&self.recorder, locked.transaction, &request).await?;
        if let Some((content_hash, content_length)) = &plan.own {
            request.content_hash = Some(content_hash.clone());
            request.content_length = Some(*content_length);
        }
        let mut write = request.new_write(write_id);
        if !plan.members.is_empty() {
            write.group_id = Some(write_id);
        }
        locked.transaction.insert_write(&write).await?;
        group::insert_members(locked.transaction, &write, &plan).await?;
        locked.report(write_id, Step::Queued).await?;
        locked.advance(&request.path_key).await?;
        if let Some(target_key) = request.target_path_key.as_deref() {
            locked.advance(target_key).await?;
        }
        let raised = std::mem::take(&mut locked.raised);
        transaction.commit().await?;
        // The test hook of a defect after the commit.
        assert!(
            self.panic_after_commit.as_deref() != Some(request.path.as_str()),
            "the test hook panics after the commit of a write"
        );
        self.announce(&raised).await;
        Ok(Handled::Queued { write_id })
    }

    /// Fail the operation of `command`, which stopped the write path by a defect, with
    /// `notes.write.internal`. Nothing of the command was committed. A failure is logged.
    pub(crate) async fn fail_defect(&self, command: &CommandEnvelope) {
        let Some((owner_id, operation_id)) = Request::operation_of(command) else {
            return;
        };
        if let Err(error) = self.refused(owner_id, operation_id, INTERNAL).await {
            tracing::error!(%error, %operation_id, "the failure of a write could not be reported");
        }
    }

    /// Give the resolution `request` the path of its conflict, the revision that the conflict
    /// recorded as current as its base, and the content of its choice. A request that is no
    /// resolution changes nothing. `Err` holds the code of a refusal: no open conflict, or a
    /// choice without its side (`keep_writer` without content, `accept_merge` without a proposal,
    /// `keep_current` of a deleted file, whose resolved event has no revision).
    async fn resolution_sides(
        &self,
        transaction: &mut FileTransaction,
        request: &mut Request,
    ) -> Result<Result<(), &'static str>, WriteError> {
        let Some((conflict_id, choice)) = request.resolution else {
            return Ok(Ok(()));
        };
        let Some(conflict) = transaction.open_conflict(conflict_id).await? else {
            return Ok(Err(CONFLICT_NOT_FOUND));
        };
        let content = match choice {
            "keep_writer" => conflict.writer_hash,
            "accept_merge" => conflict.merged_hash,
            _ => request.content_hash.clone(),
        };
        let usable = match choice {
            "keep_current" => conflict.current_hash.is_some(),
            _ => content.is_some(),
        };
        if !usable {
            return Ok(Err(INVALID));
        }
        if choice != "custom" {
            request.content_length = match &content {
                Some(digest) => self.recorder.blob_length(digest).await,
                None => None,
            };
        }
        request.content_hash = content;
        request.path_key = conflict.path_key;
        request.path = conflict.path;
        request.base_hash = conflict.current_hash;
        Ok(Ok(()))
    }

    /// Fail the operation of `request` with `code`, and answer that the command is refused.
    async fn refuse_request(
        &self,
        request: &Request,
        code: &'static str,
    ) -> Result<Handled, WriteError> {
        self.refused(request.owner_id, request.operation_id, code)
            .await?;
        Ok(Handled::Refused { code })
    }

    /// Write the report `failed` with `code` of the operation `operation_id` of `owner_id`, for a
    /// command that queued nothing.
    async fn refused(
        &self,
        owner_id: Uuid,
        operation_id: Uuid,
        code: &str,
    ) -> Result<(), WriteError> {
        match ReportRow::new(owner_id, operation_id, Step::Failed(code, None)) {
            Ok(row) => self.recorder.database().insert_event(&row.event()).await?,
            Err(error) => {
                tracing::error!(%error, %operation_id, "a report of a write could not be built");
            }
        }
        Ok(())
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
                property_key: row.property_key,
                property_change: row.property_change,
                group_id: row.group_id,
                landed: row.landed,
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
            raised: Vec::new(),
        };
        match (end, write.group_id) {
            // A write of a group ends alone; the operation of the rename ends with the group.
            (End::Acknowledged(revision), Some(group_id)) => {
                locked.record(&write, revision).await?;
                locked
                    .transaction
                    .succeed_write(write.write_id, revision)
                    .await?;
                locked.settle_group(group_id).await?;
            }
            (End::Refused(refusal), Some(group_id)) => {
                let code = match refusal {
                    Refusal::BaseMismatch { local_hash } => {
                        locked
                            .transaction
                            .set_local_hash(write.write_id, local_hash.as_deref())
                            .await?;
                        CONFLICT
                    }
                    Refusal::Failed { code } => code.as_str(),
                };
                locked.transaction.fail_write(write.write_id, code).await?;
                if write.write_id == group_id {
                    locked.fail_unlanded_rewrites(group_id, code).await?;
                }
                locked.settle_group(group_id).await?;
            }
            (End::Acknowledged(revision), None) => {
                let revision = if write.kind == "delete" {
                    None
                } else {
                    revision
                };
                locked.record(&write, revision).await?;
                locked.succeed(&write, revision).await?;
                locked.resolved(&write, revision).await?;
            }
            (End::Refused(Refusal::BaseMismatch { local_hash }), None) => {
                locked.conflict(&write, local_hash.as_deref()).await?;
            }
            (End::Refused(Refusal::Failed { code }), None) => {
                locked.fail(&write, code).await?;
            }
        }
        let ended = match write.group_id {
            Some(group_id) => locked.transaction.group_writes(group_id).await?,
            None => vec![write],
        };
        for ended in &ended {
            for key in queue::keys_of(ended) {
                locked.advance(key).await?;
            }
        }
        let raised = std::mem::take(&mut locked.raised);
        transaction.commit().await?;
        self.announce(&raised).await;
        Ok(true)
    }

    /// The `sync` connections that are `connecting` or `active` and have a delivered write.
    ///
    /// # Errors
    ///
    /// [`WriteError`] when the database fails.
    pub async fn sync_connections_delivering(&self) -> Result<Vec<Uuid>, WriteError> {
        Ok(self
            .recorder
            .database()
            .sync_connections_delivering()
            .await?)
    }

    /// Mark the delivering write `write_id` of `connection_id` as landed in the working tree of its
    /// `sync` connection, or no longer landed when its write did not happen. Returns `false` when the
    /// write is no longer delivering: it must not land then.
    ///
    /// # Errors
    ///
    /// [`WriteError`] when the database fails.
    pub async fn set_landed(
        &self,
        connection_id: Uuid,
        write_id: Uuid,
        landed: bool,
    ) -> Result<bool, WriteError> {
        Ok(self
            .recorder
            .database()
            .set_landed(connection_id, write_id, landed)
            .await?)
    }

    /// Whether a change of `connection_id` may name the bytes with the SHA-256 `digest`: the
    /// connection uploaded them or holds them as a revision, and the blob store holds them.
    ///
    /// # Errors
    ///
    /// [`WriteError`] when the database fails.
    pub async fn may_name(&self, connection_id: Uuid, digest: &str) -> Result<bool, WriteError> {
        let Some((mut transaction, _)) = self
            .recorder
            .database()
            .begin_file_change(connection_id)
            .await?
        else {
            return Ok(false);
        };
        let usable = self
            .recorder
            .usable_length(&mut transaction, digest)
            .await?;
        Ok(usable.is_some())
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
        if let Some(group_id) = write.group_id {
            let group = self
                .recorder
                .database()
                .group_writes(write.connection_id, group_id)
                .await?;
            return Ok(Some(group_outcome(&write, &group)));
        }
        Ok(Some(match write.state.as_str() {
            "succeeded" => Outcome::Succeeded {
                revision: write.revision,
            },
            "failed" => Outcome::Failed {
                code: write.error_code.unwrap_or_default(),
                conflict_id: write.conflict_id,
            },
            _ => Outcome::Pending,
        }))
    }

    /// Announce every conflict that is recorded and not announced yet, as a stop of the process
    /// between the commit of a conflict and its announcement leaves it: compute its merge proposal
    /// and write `notes.conflict.raised.v1`. It reads them in batches of [`ANNOUNCE_BATCH`] until
    /// none is left; a conflict that fails is logged and the others continue. A conflict that is
    /// no longer open is not announced. The service calls it at start. Returns how many it
    /// announced.
    ///
    /// # Errors
    ///
    /// [`WriteError`] when the database fails.
    pub async fn announce_pending(&self) -> Result<usize, WriteError> {
        let database = self.recorder.database();
        let mut announced = 0;
        let mut after = None;
        loop {
            let pending = database
                .unannounced_conflicts(after, ANNOUNCE_BATCH)
                .await?;
            let Some(last) = pending.last() else {
                return Ok(announced);
            };
            after = Some(*last);
            for conflict_id in pending {
                match self.announce_one(conflict_id).await {
                    Ok(true) => announced += 1,
                    Ok(false) => {}
                    Err(error) => {
                        tracing::warn!(%error, %conflict_id, "a conflict could not be announced; the others continue");
                    }
                }
            }
        }
    }

    /// Announce the conflicts `raised` of a transaction that committed. A failure is logged: the
    /// next start of the service announces them.
    async fn announce(&self, raised: &[Uuid]) {
        for conflict_id in raised {
            if let Err(error) = self.announce_one(*conflict_id).await {
                tracing::warn!(%error, %conflict_id, "a conflict is announced at the next start");
            }
        }
    }

    /// Compute the merge proposal of the conflict `conflict_id` outside any lock, then store it
    /// and write `notes.conflict.raised.v1` in one short transaction under the lock of its
    /// connection. Returns whether this call announced it.
    async fn announce_one(&self, conflict_id: Uuid) -> Result<bool, WriteError> {
        let database = self.recorder.database();
        let Some((owner_id, conflict)) = database.conflict(conflict_id).await? else {
            return Ok(false);
        };
        // A resolved conflict needs no proposal and no raised event.
        if conflict.state != "open" {
            return Ok(false);
        }
        let sides = Sides {
            base: conflict.base_hash.as_deref(),
            current: conflict.current_hash.as_deref(),
            writer: conflict.writer_hash.as_deref(),
        };
        let proposal = {
            let _merge = self
                .merges
                .acquire()
                .await
                .map_err(|_| WriteError("the merge"))?;
            conflict::propose(&self.recorder, &conflict.media_type, sides).await
        };
        let Some((mut transaction, _)) = database.begin_file_change(conflict.connection_id).await?
        else {
            return Ok(false);
        };
        let stored = proposal
            .as_ref()
            .map(|(merged_hash, overlapping)| (merged_hash.as_str(), *overlapping));
        if !transaction.announce_conflict(conflict_id, stored).await? {
            return Ok(false);
        }
        let event_id = Uuid::now_v7();
        let envelope = conflict::raised_envelope(
            event_id,
            &Raised {
                conflict_id,
                owner_id,
                operation_id: conflict.operation_id,
                connection_id: conflict.connection_id,
                path: &conflict.path,
                origin: &conflict.origin,
                media_type: &conflict.media_type,
                sides,
                proposal: proposal.as_ref(),
            },
        )
        .and_then(|envelope| {
            serde_json::to_value(envelope).map_err(|_| EnvelopeBuildError::SERIALIZE)
        })
        .map_err(|_| WriteError("the conflict event"))?;
        let aggregate_id = conflict::conflict_aggregate(conflict_id);
        transaction
            .write_event(&OutboxEvent {
                event_id,
                event_type: RAISED,
                aggregate_type: conflict::CONFLICT_AGGREGATE,
                aggregate_id: &aggregate_id,
                envelope: &envelope,
            })
            .await?;
        transaction.commit().await?;
        Ok(true)
    }
}
