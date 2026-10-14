//! The files of a `sync` connection: the watcher of its working tree feeds the recorder
//! (workspace design D3 and D7), and the first full scan completes the connect operation
//! (workspace design D10).
//!
//! The working tree appears when `ob` writes the vault, so the pipeline waits for it. When the
//! first full read of the tree ended and each file that it found settled, the pipeline reports the
//! phase `indexing`, records each change that the watcher sent before that, and then makes the
//! connection `active` with its `notes.vault.connected.v1` and the last report.

use std::path::PathBuf;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::blobs::BlobStore;
use crate::files::FileRecorder;
use crate::operation::Completion;
use crate::watcher::{self, TreeChange};

/// How many changes wait between the watcher and the recorder.
const QUEUE: usize = 256;

/// How often the pipeline looks for a working tree that does not exist yet.
const TREE_POLL: Duration = Duration::from_secs(1);

/// The parts that one pipeline needs.
#[derive(Debug, Clone)]
pub(crate) struct Pipeline {
    pub(crate) recorder: FileRecorder,
    pub(crate) blobs: BlobStore,
    pub(crate) trees_root: PathBuf,
}

impl Pipeline {
    /// Watch the working tree of `connection_id` and record each change until `stop`. When
    /// `completion` is given, complete the connect operation after the first full scan.
    pub(crate) async fn run(
        self,
        connection_id: Uuid,
        stop: CancellationToken,
        completion: Option<Completion>,
    ) {
        let tree = self.trees_root.join(connection_id.to_string());
        while !tokio::fs::try_exists(&tree).await.unwrap_or(false) {
            tokio::select! {
                biased;
                () = stop.cancelled() => return,
                () = tokio::time::sleep(TREE_POLL) => {}
            }
        }
        let (sender, mut changes) = mpsc::channel(QUEUE);
        let (first_sender, mut first_scan) = oneshot::channel();
        let watching = watcher::watch_reporting(
            tree,
            self.blobs.clone(),
            sender,
            stop.child_token(),
            Some(first_sender),
        );
        let mut completion = completion;
        loop {
            tokio::select! {
                biased;
                () = stop.cancelled() => break,
                first = &mut first_scan, if completion.is_some() => {
                    if let (Ok(()), Some(completion)) = (first, completion.take()) {
                        completion.indexing(self.recorder.database()).await;
                        while let Ok(change) = changes.try_recv() {
                            self.record(connection_id, &change).await;
                        }
                        completion.complete(self.recorder.database()).await;
                    }
                }
                change = changes.recv() => match change {
                    Some(change) => self.record(connection_id, &change).await,
                    None => break,
                },
            }
        }
        drop(changes);
        let _ = watching.await;
    }

    async fn record(&self, connection_id: Uuid, change: &TreeChange) {
        if let Err(error) = self.recorder.record(connection_id, change).await {
            tracing::error!(%error, %connection_id, "a change of a working tree was not recorded");
        }
    }
}
