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
//! A `set_property` on a file that has its base: notes changes the one frontmatter key itself
//! (`property::change`, every other byte stays), stores the new bytes, writes them atomically, and
//! acknowledges the write with their hash at once, because the write named no file hash for the
//! echo to match. A file that is not UTF-8 or whose frontmatter has no end fails with
//! `notes.write.unsupported`. Paths are not logged: a file name is the owner's content.
//!
//! The tree belongs to the user of `ob`, who can put a symbolic link anywhere in it. Every file
//! operation therefore runs relative to a directory descriptor that was opened from the root of the
//! tree one name at a time with `O_NOFOLLOW`, as the watcher reads the tree: a link in any component
//! refuses the write with `notes.write.unsafe_path`, so no write, delete, rename or read leaves the
//! tree, and no byte outside it reaches the blob store.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::File;
use std::io::{Read as _, Seek as _, Write as _};
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ratatoskr_notes_index::paths::path_key;
use rustix::fs::{AtFlags, FileType, Mode, OFlags};
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

/// The code of a write whose path in the tree passes through a symbolic link, or a file where a
/// folder must be.
const UNSAFE_PATH: &str = "notes.write.unsafe_path";

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
    /// The path passes through a symbolic link or a file: the write would leave the tree.
    Unsafe,
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
                Step::Unsupported | Step::Unsafe => {
                    let code = if matches!(step, Step::Unsafe) {
                        UNSAFE_PATH
                    } else {
                        UNSUPPORTED
                    };
                    self.writes
                        .refuse(
                            connection_id,
                            delivery.write_id,
                            Refusal::Failed {
                                code: code.to_owned(),
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
        let blobs = self.blobs.clone();
        let delivery = delivery.clone();
        let tree = tree.to_path_buf();
        let applied = tokio::task::spawn_blocking(move || {
            let root = crate::watcher::beneath::open_root(&tree)?;
            apply_blocking(&blobs, root.as_fd(), &delivery)
        })
        .await;
        match applied {
            Ok(Ok(step)) => step,
            Ok(Err(error)) if is_unsafe(&error) => Step::Unsafe,
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

/// The step of `delivery` against the tree whose root is `root`, in blocking file operations.
fn apply_blocking(
    blobs: &BlobStore,
    root: BorrowedFd<'_>,
    delivery: &Delivery,
) -> std::io::Result<Step> {
    let Some((folder, name)) = split(&delivery.path) else {
        return Ok(Step::Unsafe);
    };
    let parent = open_folder(root, folder, false)?;
    let (current, opened) = match &parent {
        Some(parent) => hash_at(parent.as_fd(), name)?,
        None => (None, None),
    };
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
                return mismatch(blobs, opened);
            }
            let Some(source) = blobs.path_of(&content) else {
                return Ok(Step::Unsupported);
            };
            let parent = match parent {
                Some(parent) => parent,
                None => open_folder(root, folder, true)?.ok_or_else(missing)?,
            };
            write_at(parent.as_fd(), name, &std::fs::read(source)?)?;
            Ok(Step::Applied)
        }
        "delete" => match (&current, &parent) {
            (None, _) => Ok(Step::Acknowledge(None)),
            (Some(_), Some(parent)) if current == base => {
                rustix::fs::unlinkat(parent.as_fd(), name, AtFlags::empty())?;
                Ok(Step::Applied)
            }
            _ => mismatch(blobs, opened),
        },
        "rename" | "move" => {
            let Some((target_folder, target_name)) =
                delivery.target_path.as_deref().and_then(split)
            else {
                return Ok(Step::Unsafe);
            };
            let target_parent = open_folder(root, target_folder, false)?;
            let (moved, _) = match &target_parent {
                Some(target_parent) => hash_at(target_parent.as_fd(), target_name)?,
                None => (None, None),
            };
            if current.is_none() && moved.is_some() && moved == base {
                return Ok(Step::Acknowledge(moved));
            }
            let Some(parent) = parent.filter(|_| current.is_some() && current == base) else {
                return mismatch(blobs, opened);
            };
            if moved.is_some() {
                return mismatch(blobs, opened);
            }
            let target_parent = match target_parent {
                Some(target_parent) => target_parent,
                None => open_folder(root, target_folder, true)?.ok_or_else(missing)?,
            };
            rustix::fs::renameat(parent.as_fd(), name, target_parent.as_fd(), target_name)?;
            Ok(Step::Applied)
        }
        "set_property" => {
            let (Some(parent), Some(file)) = (parent, opened) else {
                return Ok(Step::Mismatch(None));
            };
            if current != base {
                return mismatch(blobs, Some(file));
            }
            set_property(blobs, parent.as_fd(), name, file, delivery)
        }
        _ => Ok(Step::Unsupported),
    }
}

/// Change the frontmatter property of `delivery` in `file`, the open file `name` of `folder`,
/// which has the base of the write.
fn set_property(
    blobs: &BlobStore,
    folder: BorrowedFd<'_>,
    name: &OsStr,
    mut file: File,
    delivery: &Delivery,
) -> std::io::Result<Step> {
    let Some(key) = delivery.property_key.as_deref() else {
        return Ok(Step::Unsupported);
    };
    let value = match delivery.property_change.as_deref() {
        Some("set") => {
            let Some(source) = delivery
                .content_hash
                .as_deref()
                .and_then(|digest| blobs.path_of(digest))
            else {
                return Ok(Step::Unsupported);
            };
            let Ok(value) = serde_json::from_slice(&std::fs::read(source)?) else {
                return Ok(Step::Unsupported);
            };
            Some(value)
        }
        Some("remove") => None,
        _ => return Ok(Step::Unsupported),
    };
    file.rewind()?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    let Some(changed) = crate::property::change(&bytes, key, value.as_ref()) else {
        return Ok(Step::Unsupported);
    };
    let digest = format!("{:x}", Sha256::digest(&changed));
    if changed != bytes {
        let part = blobs.create_part()?;
        std::fs::write(&part, &changed)?;
        blobs.seal_part(&part, &digest)?;
        write_at(folder, name, &changed)?;
    }
    Ok(Step::Acknowledge(Some(digest)))
}

/// The tree has other bytes than the base, in the open file `opened` (none: no file): keep them in
/// the blob store, so that the conflict can show them, and refuse the write.
fn mismatch(blobs: &BlobStore, opened: Option<File>) -> std::io::Result<Step> {
    let Some(mut file) = opened else {
        return Ok(Step::Mismatch(None));
    };
    file.rewind()?;
    let stored = blobs.store_open_file(&file)?;
    Ok(Step::Mismatch(Some(stored.digest)))
}

/// The folder and the name of the relative path `relative`, or `None` when it is not a plain
/// relative path of names.
fn split(relative: &str) -> Option<(&Path, &OsStr)> {
    let path = Path::new(relative);
    let plain = path
        .components()
        .all(|component| matches!(component, Component::Normal(_)));
    let name = path.file_name()?;
    plain.then(|| (path.parent().unwrap_or_else(|| Path::new("")), name))
}

/// The flags of each folder that the writer opens.
const FOLDER: OFlags = OFlags::RDONLY
    .union(OFlags::DIRECTORY)
    .union(OFlags::NOFOLLOW)
    .union(OFlags::CLOEXEC);

/// Open `folder`, relative to `root`, one name at a time without following a link. A missing
/// folder is `None`, or is created when `create` is true.
fn open_folder(
    root: BorrowedFd<'_>,
    folder: &Path,
    create: bool,
) -> std::io::Result<Option<OwnedFd>> {
    let mut current = rustix::fs::openat(root, ".", FOLDER, Mode::empty())?;
    for component in folder.components() {
        let Component::Normal(name) = component else {
            return Err(std::io::Error::from_raw_os_error(libc_eloop()));
        };
        current = match rustix::fs::openat(&current, name, FOLDER, Mode::empty()) {
            Ok(next) => next,
            Err(rustix::io::Errno::NOENT) if create => {
                match rustix::fs::mkdirat(&current, name, Mode::from_raw_mode(0o2770)) {
                    Ok(()) | Err(rustix::io::Errno::EXIST) => {}
                    Err(error) => return Err(error.into()),
                }
                rustix::fs::openat(&current, name, FOLDER, Mode::empty())?
            }
            Err(rustix::io::Errno::NOENT) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
    }
    Ok(Some(current))
}

/// The lowercase hex SHA-256 of the regular file `name` in `folder`, and the open file; `None` when
/// no file is there. A link or another kind of file is an unsafe path.
fn hash_at(
    folder: BorrowedFd<'_>,
    name: &OsStr,
) -> std::io::Result<(Option<String>, Option<File>)> {
    let opened = match rustix::fs::openat(
        folder,
        name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
        Mode::empty(),
    ) {
        Ok(opened) => opened,
        Err(rustix::io::Errno::NOENT) => return Ok((None, None)),
        Err(error) => return Err(error.into()),
    };
    if FileType::from_raw_mode(rustix::fs::fstat(&opened)?.st_mode) != FileType::RegularFile {
        return Err(std::io::Error::from_raw_os_error(libc_eloop()));
    }
    let mut file = File::from(opened);
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(buffer.get(..read).unwrap_or_default());
    }
    Ok((Some(format!("{:x}", hasher.finalize())), Some(file)))
}

/// The error number of a link where none may be.
fn libc_eloop() -> i32 {
    rustix::io::Errno::LOOP.raw_os_error()
}

/// Whether `error` says that a path passes through a link or a file: `ELOOP` from `O_NOFOLLOW`,
/// `ENOTDIR` from `O_DIRECTORY`, and `EMLINK`, which FreeBSD returns for `O_NOFOLLOW`.
fn is_unsafe(error: &std::io::Error) -> bool {
    [
        rustix::io::Errno::LOOP,
        rustix::io::Errno::NOTDIR,
        rustix::io::Errno::MLINK,
    ]
    .iter()
    .any(|errno| error.raw_os_error() == Some(errno.raw_os_error()))
}

/// The error of a folder that is gone after it was created.
fn missing() -> std::io::Error {
    std::io::Error::from(std::io::ErrorKind::NotFound)
}

/// Replace the file at `path` with `bytes` so that a reader sees the old or the new bytes and never
/// a mix (see [`write_at`]).
///
/// # Errors
///
/// The error of a file operation; the temporary file is removed then.
pub fn write_atomically(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let folder = path.parent().unwrap_or_else(|| Path::new("."));
    let name = path.file_name().ok_or_else(missing)?;
    std::fs::create_dir_all(folder)?;
    let folder = rustix::fs::open(folder, FOLDER, Mode::empty())?;
    write_at(folder.as_fd(), name, bytes)
}

/// Replace the file `name` of `folder` with `bytes`: write a temporary file in the same folder,
/// created new without following a link, flush it to disk, and rename it over the file, all
/// relative to `folder`. The rename replaces a link at `name` itself, never its target. The
/// temporary name starts with a dot, which Obsidian Sync does not sync.
fn write_at(folder: BorrowedFd<'_>, name: &OsStr, bytes: &[u8]) -> std::io::Result<()> {
    let part = format!(".ratatoskr-write-{}.part", Uuid::now_v7().simple());
    let written = rustix::fs::openat(
        folder,
        part.as_str(),
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::from_raw_mode(0o660),
    )
    .map_err(std::io::Error::from)
    .and_then(|opened| {
        let mut file = File::from(opened);
        file.write_all(bytes)?;
        file.sync_all()
    })
    .and_then(|()| Ok(rustix::fs::renameat(folder, part.as_str(), folder, name)?));
    if let Err(error) = written {
        let _ = rustix::fs::unlinkat(folder, part.as_str(), AtFlags::empty());
        return Err(error);
    }
    // The rename is durable once the folder is flushed.
    let _ = rustix::fs::fsync(folder);
    Ok(())
}
