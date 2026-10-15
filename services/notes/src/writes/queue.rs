//! The queue of each file and its head (workspace design D1).
//!
//! The open writes of one connection with one path key form the queue of that file, in the order
//! of acceptance. Only the head is delivered. The next write reaches the head only after the
//! channel acknowledged or refused the write before it.

use ratatoskr_notes_persistence::{FileTransaction, LockedConnection, WriteRow};
use uuid::Uuid;

use crate::files::{FileRecorder, RecordError, Revision};

/// The changes of the files of one connection, under the lock of its row.
pub(super) struct Locked<'a> {
    pub(super) recorder: &'a FileRecorder,
    pub(super) transaction: &'a mut FileTransaction,
    pub(super) connection: &'a LockedConnection,
    pub(super) connection_id: Uuid,
}

impl Locked<'_> {
    /// Deliver the head of the queue of `path_key` when it waits.
    pub(super) async fn advance(&mut self, path_key: &str) -> Result<(), RecordError> {
        let Some(head) = self.transaction.queue_head(path_key).await? else {
            return Ok(());
        };
        if head.state != "queued" {
            return Ok(());
        }
        self.record(&head).await?;
        self.transaction.deliver_write(head.write_id).await?;
        Ok(())
    }

    /// Make the result of `write` the current state of its files.
    async fn record(&mut self, write: &WriteRow) -> Result<(), RecordError> {
        match (write.kind.as_str(), &write.content_hash, &write.base_hash) {
            ("create" | "update", Some(digest), _) => {
                let length = write.content_length.unwrap_or_default();
                let note_index = self.recorder.note_index(&write.path, digest, length).await;
                let revision = Revision {
                    path: &write.path,
                    digest,
                    length,
                    note_index: note_index.as_ref(),
                };
                self.recorder
                    .apply_revision(
                        self.transaction,
                        self.connection,
                        self.connection_id,
                        &revision,
                    )
                    .await?;
            }
            ("delete", _, _) => {
                self.recorder
                    .apply_removal(
                        self.transaction,
                        self.connection,
                        self.connection_id,
                        &write.path,
                    )
                    .await?;
            }
            ("rename" | "move", _, Some(digest)) => {
                let Some(target) = write.target_path.as_deref() else {
                    return Ok(());
                };
                let length = self.recorder.blob_length(digest).await.unwrap_or_default();
                let note_index = self.recorder.note_index(target, digest, length).await;
                let revision = Revision {
                    path: target,
                    digest,
                    length,
                    note_index: note_index.as_ref(),
                };
                self.recorder
                    .apply_rename(
                        self.transaction,
                        self.connection,
                        self.connection_id,
                        &write.path,
                        &revision,
                    )
                    .await?;
            }
            _ => {}
        }
        Ok(())
    }
}
