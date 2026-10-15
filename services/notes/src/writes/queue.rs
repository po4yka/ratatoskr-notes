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
use ratatoskr_notes_persistence::{FileTransaction, LockedConnection, NewConflict, WriteRow};
use uuid::Uuid;

use super::CONFLICT;
use super::report::{FileResult, ReportRow, Step};
use crate::files::{FileRecorder, RecordError, Revision, media_type};

/// The changes of the files of one connection, under the lock of its row.
pub(super) struct Locked<'a> {
    pub(super) recorder: &'a FileRecorder,
    pub(super) transaction: &'a mut FileTransaction,
    pub(super) connection: &'a LockedConnection,
    pub(super) connection_id: Uuid,
}

impl Locked<'_> {
    /// Deliver the head of the queue of `path_key` when it waits and its base is current. A head
    /// whose base is not current fails as a conflict, and the next write becomes the head.
    pub(super) async fn advance(&mut self, path_key: &str) -> Result<(), RecordError> {
        while let Some(head) = self.transaction.queue_head(path_key).await? {
            if head.state != "queued" {
                return Ok(());
            }
            match self.check_base(&head).await? {
                Ok(()) => {
                    self.transaction.deliver_write(head.write_id).await?;
                    self.report(head.write_id, Step::Delivering).await?;
                    return Ok(());
                }
                Err(current) => self.conflict(&head, current.as_deref()).await?,
            }
        }
        Ok(())
    }

    /// `Ok` when `write` may be delivered now; otherwise the current revision of its file, or
    /// `None` when no present file holds its path key. A `create` passes only when no present
    /// file has its path key, which is case-folded and in NFC; every other write only when the
    /// present file with its path key has its base revision. A `rename` or a `move` also needs a
    /// target path key that no other present file holds.
    async fn check_base(
        &mut self,
        write: &WriteRow,
    ) -> Result<Result<(), Option<String>>, RecordError> {
        let current = self
            .transaction
            .file_by_key(&write.path_key)
            .await?
            .filter(|file| file.present)
            .map(|file| file.revision);
        let base_is_current = match write.kind.as_str() {
            "create" => current.is_none(),
            "resolve_conflict" => true,
            _ => current.is_some() && write.base_hash == current,
        };
        if !base_is_current {
            return Ok(Err(current));
        }
        let Some(target) = write.target_path.as_deref() else {
            return Ok(Ok(()));
        };
        let target_key = path_key(target);
        if target_key == write.path_key {
            return Ok(Ok(()));
        }
        let taken = self
            .transaction
            .file_by_key(&target_key)
            .await?
            .is_some_and(|file| file.present);
        Ok(if taken { Err(current) } else { Ok(()) })
    }

    /// Record a conflict of `write` with the revision `current` of the vault, end the write as
    /// `failed` with `notes.write.conflict`, and report it with the conflict id.
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
            "delivering" => self.report_one(operation_id, Step::Delivering).await,
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
                let result = FileResult {
                    file_id: file.file_id,
                    revision: write
                        .revision
                        .as_deref()
                        .map(|revision| (revision, media_type(path), length)),
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
