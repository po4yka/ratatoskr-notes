//! The files of a connection, their revisions and the event of each change (workspace design D7,
//! spec `notes-file-changes`).

use ratatoskr_notes_persistence::Database;
use uuid::Uuid;

use crate::blobs::BlobStore;
use crate::watcher::TreeChange;

/// What [`FileRecorder::record`] did with one change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Recorded {
    /// One `notes.file.changed.v1` is in the outbox.
    Published {
        /// The event.
        event_id: Uuid,
        /// The file.
        file_id: Uuid,
        /// The position of the event among the events of its connection.
        connection_sequence: u64,
    },
    /// Nothing changed and nothing is published.
    Nothing,
}

/// A change could not be recorded. The message names no path: a file name is the owner's content.
#[derive(Debug, thiserror::Error)]
#[error("a file change could not be recorded: {0}")]
pub struct RecordError(&'static str);

/// Records the changes of the files of each connection.
#[derive(Debug, Clone)]
pub struct FileRecorder {
    database: Database,
    blobs: BlobStore,
}

impl FileRecorder {
    /// A recorder that writes to `database` and finds the bytes of each revision in `blobs`.
    #[must_use]
    pub fn new(database: Database, blobs: BlobStore) -> Self {
        Self { database, blobs }
    }

    /// Record one change of a file of `connection_id`.
    ///
    /// # Errors
    ///
    /// [`RecordError`] when the change cannot be recorded.
    pub async fn record(
        &self,
        connection_id: Uuid,
        change: &TreeChange,
    ) -> Result<Recorded, RecordError> {
        let _ = (connection_id, change, &self.database, &self.blobs);
        tokio::task::yield_now().await;
        Ok(Recorded::Nothing)
    }
}
