//! The queue of each file and its head (workspace design D1).
//!
//! The open writes of one connection with one path key form the queue of that file, in the order
//! of acceptance. Only the head is delivered. The next write reaches the head only after the
//! channel acknowledged or refused the write before it. The base of a write is compared with the
//! current revision when the write reaches the head, not when it is accepted, so that a chained
//! edit, whose base is the revision that the write before it produces, passes.
//!
//! A delivered write is pending: its bytes are a blob that the write names, not a revision. Only
//! the acknowledgement of the channel makes a revision current and publishes
//! `notes.file.changed.v1`, so no consumer of note events sees bytes that the vault refused.

use ratatoskr_notes_index::paths::path_key;
use ratatoskr_notes_persistence::{
    FileTransaction, LockedConnection, NewConflict, OutboxEvent, WriteRow,
};
use uuid::Uuid;

use super::conflict::{self, Resolved};
use super::report::{FileResult, ReportRow, Step};
use super::{CONFLICT, CONFLICT_NOT_FOUND};
use crate::events::EnvelopeBuildError;
use crate::files::{FileRecorder, RecordError, Revision, media_type};

/// The event type of a resolved conflict.
const RESOLVED: &str = "notes.conflict.resolved.v1";

/// What the head of a queue does now.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Check {
    /// The base is current: deliver the write.
    Deliver,
    /// The base is not current: a conflict with this current revision of the file.
    Conflict(Option<String>),
    /// The write cannot be delivered: fail it with this code.
    Fail(&'static str),
}

/// The changes of the files of one connection, under the lock of its row.
pub(super) struct Locked<'a> {
    pub(super) recorder: &'a FileRecorder,
    pub(super) transaction: &'a mut FileTransaction,
    pub(super) connection: &'a LockedConnection,
    pub(super) connection_id: Uuid,
    /// The conflicts that this transaction raised; they are announced after its commit.
    pub(super) raised: Vec<Uuid>,
}

impl Locked<'_> {
    /// Deliver the head of the queue of `path_key` when it waits and its base is current. A head
    /// whose base is not current fails as a conflict, and the next write becomes the head. A
    /// resolution `keep_current` whose base is current needs no delivery: it ends at once. A
    /// `rename` or a `move` is in the queue of its path and of its target, and is delivered only
    /// when it is the head of both; when it ends here, the other queue moves on too.
    pub(super) async fn advance(&mut self, path_key: &str) -> Result<(), RecordError> {
        let mut keys = vec![path_key.to_owned()];
        while let Some(key) = keys.pop() {
            while let Some(head) = self.transaction.queue_head(&key).await? {
                if head.state != "queued" || !self.heads_every_queue(&head).await? {
                    break;
                }
                let other = keys_of(&head).into_iter().find(|other| *other != key);
                match self.check_base(&head).await? {
                    Check::Deliver if head.choice.as_deref() == Some("keep_current") => {
                        let revision = head.base_hash.as_deref();
                        self.succeed(&head, revision).await?;
                        self.resolved(&head, revision).await?;
                    }
                    Check::Deliver => {
                        self.transaction.deliver_write(head.write_id).await?;
                        let wait = self.wait();
                        self.report(head.write_id, Step::Awaiting(wait)).await?;
                        break;
                    }
                    Check::Conflict(current) => self.conflict(&head, current.as_deref()).await?,
                    Check::Fail(code) => self.fail(&head, code).await?,
                }
                keys.extend(other.map(str::to_owned));
            }
        }
        Ok(())
    }

    /// The stage of a delivered write of this connection: what its channel waits for.
    fn wait(&self) -> &'static str {
        if self.connection.channel == "sync" {
            "awaiting_obsidian"
        } else {
            "awaiting_device"
        }
    }

    /// Whether `write` is the head of every queue it is in: the queue of its path, and for a
    /// `rename` or a `move` the queue of its target too.
    async fn heads_every_queue(&mut self, write: &WriteRow) -> Result<bool, RecordError> {
        for key in keys_of(write) {
            let head = self.transaction.queue_head(key).await?;
            if head.is_none_or(|head| head.write_id != write.write_id) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Whether `write` may be delivered now. A `create` passes only when no present file has its
    /// path key, which is case-folded and in NFC; every other write only when the present file
    /// with its path key has its base revision, and a resolution only while its conflict is open
    /// and the file still has the revision that the conflict recorded. A `rename` or a `move` also
    /// needs a target path key that no other present file holds.
    async fn check_base(&mut self, write: &WriteRow) -> Result<Check, RecordError> {
        let current = self
            .transaction
            .file_by_key(&write.path_key)
            .await?
            .filter(|file| file.present)
            .map(|file| file.revision);
        let base_is_current = match (write.kind.as_str(), write.resolves) {
            ("create", _) => current.is_none(),
            ("resolve_conflict", Some(conflict_id)) => {
                if self.transaction.open_conflict(conflict_id).await?.is_none() {
                    return Ok(Check::Fail(CONFLICT_NOT_FOUND));
                }
                write.base_hash == current
            }
            _ => current.is_some() && write.base_hash == current,
        };
        if !base_is_current {
            return Ok(Check::Conflict(current));
        }
        let Some(target) = write.target_path.as_deref() else {
            return Ok(Check::Deliver);
        };
        let target_key = path_key(target);
        if target_key == write.path_key {
            return Ok(Check::Deliver);
        }
        let taken = self
            .transaction
            .file_by_key(&target_key)
            .await?
            .is_some_and(|file| file.present);
        Ok(if taken {
            Check::Conflict(current)
        } else {
            Check::Deliver
        })
    }

    /// Mark the conflict that the resolution `write` ends `resolved`, and write
    /// `notes.conflict.resolved.v1` with the file at `revision`. A write that ends no conflict
    /// changes nothing.
    pub(super) async fn resolved(
        &mut self,
        write: &WriteRow,
        revision: Option<&str>,
    ) -> Result<(), RecordError> {
        let Some(conflict_id) = write.resolves else {
            return Ok(());
        };
        self.transaction.resolve_conflict(conflict_id).await?;
        let Some(revision) = revision else {
            return Ok(());
        };
        let event_id = Uuid::now_v7();
        let envelope = conflict::resolved_envelope(
            event_id,
            &Resolved {
                conflict_id,
                owner_id: self.connection.owner_id,
                operation_id: write.operation_id,
                connection_id: self.connection_id,
                path: &write.path,
                origin: &write.origin,
                revision,
            },
        )?;
        let envelope = serde_json::to_value(envelope).map_err(|_| EnvelopeBuildError::SERIALIZE)?;
        let aggregate_id = conflict::conflict_aggregate(conflict_id);
        self.transaction
            .write_event(&OutboxEvent {
                event_id,
                event_type: RESOLVED,
                aggregate_type: conflict::CONFLICT_AGGREGATE,
                aggregate_id: &aggregate_id,
                envelope: &envelope,
            })
            .await?;
        Ok(())
    }

    /// Record a conflict of `write` with the revision `current` of the vault, not announced yet,
    /// end the write as `failed` with `notes.write.conflict`, and report it with the conflict id.
    /// The caller announces the conflict after the commit, outside the lock of the connection row
    /// ([`Self::raised`] lists it).
    pub(super) async fn conflict(
        &mut self,
        write: &WriteRow,
        current: Option<&str>,
    ) -> Result<(), RecordError> {
        let conflict_id = Uuid::now_v7();
        self.transaction
            .raise_conflict(&NewConflict {
                conflict_id,
                write,
                media_type: media_type(&write.path),
                base_hash: write.base_hash.as_deref(),
                current_hash: current,
                writer_hash: write.content_hash.as_deref(),
            })
            .await?;
        self.raised.push(conflict_id);
        self.report(write.write_id, Step::Failed(CONFLICT, Some(conflict_id)))
            .await
    }

    /// End `write` as `failed` with `code`, and report it.
    pub(super) async fn fail(&mut self, write: &WriteRow, code: &str) -> Result<(), RecordError> {
        self.transaction.fail_write(write.write_id, code).await?;
        self.report(write.write_id, Step::Failed(code, None)).await
    }

    /// End `write` as `succeeded` with the file at `revision`, and report it with its file.
    pub(super) async fn succeed(
        &mut self,
        write: &WriteRow,
        revision: Option<&str>,
    ) -> Result<(), RecordError> {
        self.transaction
            .succeed_write(write.write_id, revision)
            .await?;
        let Some(ended) = self.transaction.write(write.write_id).await? else {
            return Ok(());
        };
        for operation_id in self.transaction.operations_of(write.write_id).await? {
            self.replay(operation_id, &ended).await?;
        }
        Ok(())
    }

    /// Report the state of `write` to the operation `operation_id`, which waits for it: the
    /// stage of an open write, or the end of an ended one.
    pub(super) async fn replay(
        &mut self,
        operation_id: Uuid,
        write: &WriteRow,
    ) -> Result<(), RecordError> {
        match write.state.as_str() {
            "queued" => self.report_one(operation_id, Step::Queued).await,
            "delivering" => {
                let wait = self.wait();
                self.report_one(operation_id, Step::Awaiting(wait)).await
            }
            "succeeded" => {
                let path = write.target_path.as_deref().unwrap_or(&write.path);
                let file = self.transaction.file_by_key(&path_key(path)).await?;
                let Some(file) = file else {
                    return self.report_one(operation_id, Step::Succeeded(None)).await;
                };
                let length = match &write.revision {
                    Some(revision) => self.recorder.blob_length(revision).await.unwrap_or(0),
                    None => 0,
                };
                let transformed = super::expected_hash(write)
                    .is_some_and(|expected| write.revision.as_deref() != Some(expected));
                let result = FileResult {
                    file_id: file.file_id,
                    revision: write
                        .revision
                        .as_deref()
                        .map(|revision| (revision, media_type(path), length)),
                    transformed,
                };
                self.report_one(operation_id, Step::Succeeded(Some(result)))
                    .await
            }
            _ => {
                let code = write.error_code.as_deref().unwrap_or(CONFLICT);
                self.report_one(operation_id, Step::Failed(code, write.conflict_id))
                    .await
            }
        }
    }

    /// Write `step` for every operation that waits for the write `write_id`.
    pub(super) async fn report(
        &mut self,
        write_id: Uuid,
        step: Step<'_>,
    ) -> Result<(), RecordError> {
        for operation_id in self.transaction.operations_of(write_id).await? {
            self.report_one(operation_id, step).await?;
        }
        Ok(())
    }

    /// Write `step` of the operation `operation_id`. A report that cannot be built is logged and
    /// left out: Platform's stale-operation reaper ends the operation.
    async fn report_one(&mut self, operation_id: Uuid, step: Step<'_>) -> Result<(), RecordError> {
        match ReportRow::new(self.connection.owner_id, operation_id, step) {
            Ok(row) => self.transaction.write_event(&row.event()).await?,
            Err(error) => {
                tracing::error!(%error, %operation_id, "a report of a write could not be built");
            }
        }
        Ok(())
    }

    /// Make the result of `write`, which the channel acknowledged with the file at the SHA-256
    /// `acknowledged`, the current state of its files, and publish it. The bytes of the
    /// acknowledged revision are in the blob root.
    pub(super) async fn record(
        &mut self,
        write: &WriteRow,
        acknowledged: Option<&str>,
    ) -> Result<(), RecordError> {
        if write.kind == "delete" {
            self.recorder
                .apply_removal(
                    self.transaction,
                    self.connection,
                    self.connection_id,
                    &write.path,
                )
                .await?;
            return Ok(());
        }
        let Some(digest) = acknowledged else {
            return Err(RecordError::ACKNOWLEDGED_BYTES);
        };
        let path = write.target_path.as_deref().unwrap_or(&write.path);
        let Some(length) = self.recorder.blob_length(digest).await else {
            return Err(RecordError::ACKNOWLEDGED_BYTES);
        };
        let note_index = self.recorder.note_index(path, digest, length).await;
        let revision = Revision {
            path,
            digest,
            length,
            note_index: note_index.as_ref(),
        };
        if write.target_path.is_some() {
            self.recorder
                .apply_rename(
                    self.transaction,
                    self.connection,
                    self.connection_id,
                    &write.path,
                    &revision,
                )
                .await?;
        } else {
            self.recorder
                .apply_revision(
                    self.transaction,
                    self.connection,
                    self.connection_id,
                    &revision,
                )
                .await?;
        }
        Ok(())
    }
}

/// The path keys of the queues that `write` is in: its path, and the target of a `rename` or a
/// `move` when that is another key.
pub(super) fn keys_of(write: &WriteRow) -> Vec<&str> {
    let mut keys = vec![write.path_key.as_str()];
    if let Some(target) = write.target_path_key.as_deref()
        && target != write.path_key
    {
        keys.push(target);
    }
    keys
}
