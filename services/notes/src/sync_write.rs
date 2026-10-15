//! The delivery of the writes of a `sync` connection into its working tree (workspace design D5,
//! spec `notes-write-delivery`, requirement "The sync channel writes the working tree
//! atomically").
//!
//! For each delivered write of a connection, under a lock of its path, the writer reads the hash
//! of the file in the working tree:
//!
//! - the file has the hash that the write produces: the vault holds the write, which is
//!   acknowledged (the watcher acknowledges it too when it sees the file first, see
//!   `writes::echo`);
//! - the file has the base of the write (no file for a `create`): the writer applies the write. New
//!   bytes go to a temporary file in the same folder, which is flushed to disk and then renamed over
//!   the file, so a reader sees the old or the new bytes and never a mix. A `delete` removes the
//!   file, and a `rename` or a `move` renames it. The Headless client then sends the change to
//!   Obsidian Sync, and the watcher reports it back as the acknowledgement;
//! - otherwise the file changed after the queue check: nothing is written, and the write fails
//!   with a conflict whose current side is the bytes of the tree.
//!
//! A `set_property` needs a change of the frontmatter that notes does not compute on this channel
//! yet; it fails with `notes.write.unsupported`. Paths are not logged: a file name is the owner's
//! content.

use std::collections::HashMap;
use std::io::{Read as _, Write as _};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ratatoskr_notes_index::paths::path_key;
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::blobs::BlobStore;
use crate::files::FileRecorder;
use crate::writes::{Delivery, Refusal, WriteError, Writes};

/// How often the writer looks for delivered writes of the `sync` connections.
const POLL: Duration = Duration::from_secs(2);

/// The code of a write that this channel cannot apply.
const UNSUPPORTED: &str = "notes.write.unsupported";

/// The locks of the files that a delivery changes now, by connection and path key.
type FileLocks = Arc<Mutex<HashMap<(Uuid, String), Arc<tokio::sync::Mutex<()>>>>>;

/// Delivers the writes of `sync` connections into their working trees.
#[derive(Debug, Clone)]
pub struct SyncWriter {
    writes: Writes,
    blobs: BlobStore,
    trees_root: PathBuf,
    locks: FileLocks,
}

/// What a delivery found in the tree and did.
enum Step {
    /// The tree holds the write: acknowledge it with this hash (none after a delete).
    Acknowledge(Option<String>),
    /// The tree has the base: the write was applied, and its echo acknowledges it.
    Applied,
    /// The tree has other bytes, with this hash (none: no file).
    Mismatch(Option<String>),
    /// The channel cannot apply the write.
    Unsupported,
}

impl SyncWriter {
    /// A writer that records through `recorder` and writes the trees under `trees_root`.
    #[must_use]
    pub fn new(recorder: FileRecorder, trees_root: impl Into<PathBuf>) -> Self {
        Self {
            blobs: recorder.blobs().clone(),
            writes: Writes::new(recorder),
            trees_root: trees_root.into(),
            locks: Arc::default(),
        }
    }

    /// Deliver the writes of every `sync` connection every two seconds, until `stop`.
    pub async fn run(self, stop: CancellationToken) {
        loop {
            let connections = self
                .writes
                .sync_connections_delivering()
                .await
                .unwrap_or_else(|error| {
                    tracing::error!(%error, "the sync connections with writes could not be read");
                    Vec::new()
                });
            for connection_id in connections {
                if let Err(error) = self.deliver(connection_id).await {
                    tracing::error!(%error, %connection_id, "the writes of a sync connection could not be delivered");
                }
            }
            tokio::select! {
                () = stop.cancelled() => return,
                () = tokio::time::sleep(POLL) => {}
            }
        }
    }

    /// Deliver the delivered writes of `connection_id` into its working tree. Returns how many it
    /// applied or ended.
    ///
    /// # Errors
    ///
    /// [`WriteError`] when the database fails.
    pub async fn deliver(&self, connection_id: Uuid) -> Result<usize, WriteError> {
        let tree = self.trees_root.join(connection_id.to_string());
        let mut done = 0;
        for delivery in self.writes.deliveries(connection_id).await? {
            let lock = self.lock(connection_id, &delivery.path);
            let _held = lock.lock().await;
            let step = self.apply(&tree, &delivery).await;
            done += 1;
            match step {
                Step::Applied => {}
                Step::Acknowledge(revision) => {
                    self.writes
                        .acknowledge(connection_id, delivery.write_id, revision.as_deref())
                        .await?;
                }
                Step::Mismatch(local_hash) => {
                    self.writes
                        .refuse(
                            connection_id,
                            delivery.write_id,
                            Refusal::BaseMismatch { local_hash },
                        )
                        .await?;
                }
                Step::Unsupported => {
                    self.writes
                        .refuse(
                            connection_id,
                            delivery.write_id,
                            Refusal::Failed {
                                code: UNSUPPORTED.to_owned(),
                            },
                        )
                        .await?;
                }
            }
        }
        Ok(done)
    }

    /// The lock of the file at `path` of `connection_id`.
    fn lock(&self, connection_id: Uuid, path: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self
            .locks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // A lock that nobody else holds now is not kept.
        locks.retain(|_, lock| Arc::strong_count(lock) > 1);
        Arc::clone(locks.entry((connection_id, path_key(path))).or_default())
    }

    /// Compare the tree with the write and apply it when the tree has its base.
    async fn apply(&self, tree: &Path, delivery: &Delivery) -> Step {
        let Some(file) = inside(tree, &delivery.path) else {
            return Step::Unsupported;
        };
        let blobs = self.blobs.clone();
        let delivery = delivery.clone();
        let target = delivery
            .target_path
            .as_deref()
            .and_then(|target| inside(tree, target));
        let applied = tokio::task::spawn_blocking(move || {
            apply_blocking(&blobs, &file, target.as_deref(), &delivery)
        })
        .await;
        match applied {
            Ok(Ok(step)) => step,
            Ok(Err(error)) => {
                tracing::error!(%error, "a write of a working tree failed; the next pass tries again");
                Step::Applied
            }
            Err(error) => {
                tracing::error!(%error, "a write of a working tree stopped");
                Step::Applied
            }
        }
    }
}

/// The step of `delivery` against the tree, in blocking file operations.
fn apply_blocking(
    blobs: &BlobStore,
    file: &Path,
    target: Option<&Path>,
    delivery: &Delivery,
) -> std::io::Result<Step> {
    let current = hash_of(file)?;
    let base = delivery.base_hash.clone();
    match delivery.kind.as_str() {
        "create" | "update" | "resolve_conflict" => {
            let Some(content) = delivery.content_hash.clone() else {
                return Ok(Step::Unsupported);
            };
            if current.as_deref() == Some(content.as_str()) {
                return Ok(Step::Acknowledge(Some(content)));
            }
            if current != base {
                return mismatch(blobs, file, current.as_deref());
            }
            let Some(source) = blobs.path_of(&content) else {
                return Ok(Step::Unsupported);
            };
            write_atomically(file, &std::fs::read(source)?)?;
            Ok(Step::Applied)
        }
        "delete" => match current {
            None => Ok(Step::Acknowledge(None)),
            Some(_) if current == base => {
                std::fs::remove_file(file)?;
                Ok(Step::Applied)
            }
            Some(_) => mismatch(blobs, file, current.as_deref()),
        },
        "rename" | "move" => {
            let Some(target) = target else {
                return Ok(Step::Unsupported);
            };
            let moved = hash_of(target)?;
            if current.is_none() && moved.is_some() && moved == base {
                return Ok(Step::Acknowledge(moved));
            }
            if current.is_none() || current != base || moved.is_some() {
                return mismatch(blobs, file, current.as_deref());
            }
            if let Some(parent) = target.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::rename(file, target)?;
            Ok(Step::Applied)
        }
        _ => Ok(Step::Unsupported),
    }
}

/// The tree has the bytes `current` at `file`: keep them in the blob store, so that the conflict
/// can show them, and refuse the write.
fn mismatch(blobs: &BlobStore, file: &Path, current: Option<&str>) -> std::io::Result<Step> {
    if current.is_some() {
        let stored = blobs.store_file(file)?;
        return Ok(Step::Mismatch(Some(stored.digest)));
    }
    Ok(Step::Mismatch(None))
}

/// The path of `relative` in `tree`, or `None` when it is not a plain relative path.
fn inside(tree: &Path, relative: &str) -> Option<PathBuf> {
    let relative = Path::new(relative);
    relative
        .components()
        .all(|component| matches!(component, Component::Normal(_)))
        .then(|| tree.join(relative))
}

/// The lowercase hex SHA-256 of the file at `path`, or `None` when no file is there.
fn hash_of(path: &Path) -> std::io::Result<Option<String>> {
    let mut file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(buffer.get(..read).unwrap_or_default());
    }
    Ok(Some(format!("{:x}", hasher.finalize())))
}

/// Replace the file at `path` with `bytes` so that a reader sees the old or the new bytes and never
/// a mix: write a temporary file in the same folder, flush it to disk, and rename it over the file.
/// The temporary name starts with a dot, which Obsidian Sync does not sync.
///
/// # Errors
///
/// The error of a file operation; the temporary file is removed then.
pub fn write_atomically(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let folder = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(folder)?;
    let part = folder.join(format!(".ratatoskr-write-{}.part", Uuid::now_v7().simple()));
    let written = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&part)
        .and_then(|mut file| {
            file.write_all(bytes)?;
            file.sync_all()
        })
        .and_then(|()| std::fs::rename(&part, path));
    if let Err(error) = written {
        let _ = std::fs::remove_file(&part);
        return Err(error);
    }
    if let Ok(directory) = std::fs::File::open(folder) {
        // The rename is durable once the folder is flushed.
        let _ = directory.sync_all();
    }
    Ok(())
}
