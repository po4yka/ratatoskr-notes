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
use ratatoskr_notes_persistence::{FileTransaction, LockedConnection, WriteRow};
use uuid::Uuid;

use super::CONFLICT;
use crate::files::{FileRecorder, RecordError, Revision};

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
            if self.base_is_current(&head).await? {
                self.transaction.deliver_write(head.write_id).await?;
                return Ok(());
            }
            self.transaction.fail_write(head.write_id, CONFLICT).await?;
        }
        Ok(())
    }

    /// Whether `write` may be delivered now: a `create` only when no present file has its path
    /// key, which is case-folded and in NFC; every other write only when the present file with
    /// its path key has its base revision. A `rename` or a `move` also needs a target path key
    /// that no other present file holds.
    async fn base_is_current(&mut self, write: &WriteRow) -> Result<bool, RecordError> {
        let current = self
            .transaction
            .file_by_key(&write.path_key)
            .await?
            .filter(|file| file.present);
        let base_is_current = match write.kind.as_str() {
            "create" => current.is_none(),
            "resolve_conflict" => true,
            _ => current.is_some_and(|file| write.base_hash.as_deref() == Some(&file.revision)),
        };
        let Some(target) = write.target_path.as_deref() else {
            return Ok(base_is_current);
        };
        let target_key = path_key(target);
        if !base_is_current || target_key == write.path_key {
            return Ok(base_is_current);
        }
        let taken = self
            .transaction
            .file_by_key(&target_key)
            .await?
            .is_some_and(|file| file.present);
        Ok(!taken)
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
