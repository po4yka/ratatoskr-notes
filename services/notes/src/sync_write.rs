//! The delivery of the writes of a `sync` connection into its working tree (workspace design D5,
//! spec `notes-write-delivery`, requirement "The sync channel writes the working tree
//! atomically").

use std::path::PathBuf;

use uuid::Uuid;

use crate::files::FileRecorder;
use crate::writes::{WriteError, Writes};

/// Delivers the writes of `sync` connections into their working trees.
#[derive(Debug, Clone)]
pub struct SyncWriter {
    writes: Writes,
    trees_root: PathBuf,
}

impl SyncWriter {
    /// A writer that records through `recorder` and writes the trees under `trees_root`.
    #[must_use]
    pub fn new(recorder: FileRecorder, trees_root: impl Into<PathBuf>) -> Self {
        Self {
            writes: Writes::new(recorder),
            trees_root: trees_root.into(),
        }
    }

    /// Deliver the delivering writes of `connection_id` into its working tree. Returns how many it
    /// ended or wrote. The delivery is not built yet (task 7.6): it does nothing.
    ///
    /// # Errors
    ///
    /// [`WriteError`] when the database fails.
    pub async fn deliver(&self, connection_id: Uuid) -> Result<usize, WriteError> {
        let _ = (&self.writes, &self.trees_root, connection_id);
        tokio::task::yield_now().await;
        Ok(0)
    }
}
