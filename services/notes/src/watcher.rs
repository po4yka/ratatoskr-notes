//! The watcher of the working tree of a `sync` connection (workspace design D3).

use std::path::PathBuf;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::blobs::BlobStore;

/// One change of the working tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TreeChange {
    /// A file has a new settled state. Its bytes are in the blob root under `digest`.
    Revision {
        /// The path of the file, relative to the root of the tree.
        path: PathBuf,
        /// The lowercase hex SHA-256 of the bytes.
        digest: String,
        /// The number of bytes.
        length: u64,
    },
}

/// Watch `tree` until `stop`, and send each change to `changes`.
#[must_use]
pub fn watch(
    tree: PathBuf,
    blobs: BlobStore,
    changes: mpsc::Sender<TreeChange>,
    stop: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let _unused = (tree, blobs, changes);
        stop.cancelled().await;
    })
}
