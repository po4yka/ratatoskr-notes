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
//! - the file has the base of the write (no file for a `create`): the writer marks the write
//!   landed and applies it. New
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
    /// The tree has the base, and the write may land: a check that changed nothing.
    Ready,
    /// A file operation failed: the next pass tries again.
    Retry,
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
            let mut step = self.apply(&tree, &delivery, false).await;
            if matches!(step, Step::Ready) {
                // The write lands now: from here, the next change of its path that the watcher
                // reports is its acknowledgement (design D6).
                // A write that ended meanwhile, for example because write mode was turned off,
                // does not land.
                step = if self
                    .writes
                    .set_landed(connection_id, delivery.write_id, true)
                    .await?
                {
                    self.apply(&tree, &delivery, true).await
                } else {
                    Step::Retry
                };
                if !matches!(step, Step::Applied | Step::Acknowledge(_)) {
                    self.writes
                        .set_landed(connection_id, delivery.write_id, false)
                        .await?;
                }
            }
            done += 1;
            match step {
                Step::Applied | Step::Ready | Step::Retry => {}
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

    /// Compare the tree with the write, and apply it when the tree has its base and `land` is true;
    /// when `land` is false, a write that would be applied is [`Step::Ready`] and nothing changes.
    async fn apply(&self, tree: &Path, delivery: &Delivery, land: bool) -> Step {
        let blobs = self.blobs.clone();
        let delivery = delivery.clone();
        let tree = tree.to_path_buf();
        let applied = tokio::task::spawn_blocking(move || {
            let root = crate::watcher::beneath::open_root(&tree)?;
            apply_blocking(&blobs, root.as_fd(), &delivery, land)
        })
        .await;
        match applied {
            Ok(Ok(step)) => step,
            Ok(Err(error)) if is_unsafe(&error) => Step::Unsafe,
            Ok(Err(error)) => {
                tracing::error!(%error, "a write of a working tree failed; the next pass tries again");
                Step::Retry
            }
            Err(error) => {
                tracing::error!(%error, "a write of a working tree stopped");
                Step::Retry
            }
        }
    }
}

/// The step of `delivery` against the tree whose root is `root`, in blocking file operations.
fn apply_blocking(
    blobs: &BlobStore,
    root: BorrowedFd<'_>,
    delivery: &Delivery,
    land: bool,
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
            if !land {
                return Ok(Step::Ready);
            }
            let parent = match parent {
                Some(parent) => parent,
                None => open_folder(root, folder, true)?.ok_or_else(missing)?,
            };
            write_at(parent.as_fd(), name, &std::fs::read(source)?)?;
            Ok(Step::Applied)
        }
        "delete" => match (&current, &parent) {
            (None, _) => Ok(Step::Acknowledge(None)),
            (Some(_), Some(_)) if current == base && !land => Ok(Step::Ready),
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
            if !land {
                return Ok(Step::Ready);
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
            if !land {
                return Ok(Step::Ready);
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

/// The start of the name of the temporary file of a write.
const PART_PREFIX: &str = ".ratatoskr-write-";

/// The end of the name of the temporary file of a write.
const PART_SUFFIX: &str = ".part";

/// Whether the last name of `path` is the temporary file of a write, which the watcher and the
/// index leave out: it lives only until its rename, and it is never a note.
#[must_use]
pub fn is_write_part(path: &Path) -> bool {
    path.file_name()
        .and_then(OsStr::to_str)
        .is_some_and(|name| name.starts_with(PART_PREFIX) && name.ends_with(PART_SUFFIX))
}

/// The mode of a folder that the writer creates in a tree: the group of the trees, of which the
/// service and the user of `ob` are members, reads and writes it, and the setgid bit keeps that
/// group on what is created in it.
const GROUP_FOLDER: rustix::fs::RawMode = 0o2770;

/// The mode of a file that the writer writes in a tree, for the same group.
const GROUP_FILE: rustix::fs::RawMode = 0o660;

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
                match rustix::fs::mkdirat(&current, name, Mode::from_raw_mode(GROUP_FOLDER)) {
                    Ok(()) | Err(rustix::io::Errno::EXIST) => {}
                    Err(error) => return Err(error.into()),
                }
                let created = rustix::fs::openat(&current, name, FOLDER, Mode::empty())?;
                // The umask of the service (0027, for the blob store) would leave the group
                // without write; ob, another member of the group, writes into the folder too.
                rustix::fs::fchmod(&created, Mode::from_raw_mode(GROUP_FOLDER))?;
                created
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

/// Replace the file at `path` with `bytes` through [`write_at`]. For the test of the atomic write
/// only: the service writes a tree only relative to folder descriptors.
#[cfg(test)]
fn write_atomically(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
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
    let part = format!("{PART_PREFIX}{}{PART_SUFFIX}", Uuid::now_v7().simple());
    let written = rustix::fs::openat(
        folder,
        part.as_str(),
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::from_raw_mode(GROUP_FILE),
    )
    .map_err(std::io::Error::from)
    .and_then(|opened| {
        // The umask of the service would leave the group without write.
        rustix::fs::fchmod(&opened, Mode::from_raw_mode(GROUP_FILE))?;
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

#[cfg(test)]
mod tests {
    //! The atomic write of a working tree (OBS-024 tasks 7.7, 7.8 and 7.28).

    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use sha2::{Digest as _, Sha256};

    use super::write_atomically;

    /// How many times the writer replaces the file while the reader reads it.
    const WRITES: usize = 1_000;

    fn sha256(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
    }

    /// The hashes that a reader saw while `write` replaced the file `WRITES` times with old and
    /// new bytes in turn, other than the hashes of the old and the new bytes.
    fn torn_reads(write: fn(&Path, &[u8]) -> std::io::Result<()>) -> Vec<String> {
        let folder = tempfile::tempdir().expect("a working tree");
        let path = folder.path().join("Plan.md");
        let old = vec![b'o'; 256 * 1024];
        let new = vec![b'n'; 256 * 1024];
        std::fs::write(&path, &old).expect("the old bytes are in the tree");
        let whole = [sha256(&old), sha256(&new)];
        let done = Arc::new(AtomicBool::new(false));
        let reader = {
            let (path, done) = (path.clone(), Arc::clone(&done));
            std::thread::spawn(move || read_until(&path, &done))
        };
        for round in 0..WRITES {
            let bytes = if round % 2 == 0 { &new } else { &old };
            write(&path, bytes).expect("the file is written");
        }
        done.store(true, Ordering::Release);
        let seen = reader.join().expect("the reader ends");
        assert!(!seen.is_empty(), "the reader read the file");
        seen.into_iter()
            .filter(|hash| !whole.contains(hash))
            .collect()
    }

    /// The hash of the file at `path` each time a reader reads it, until `done`.
    fn read_until(path: &Path, done: &AtomicBool) -> Vec<String> {
        let mut seen = Vec::new();
        while !done.load(Ordering::Acquire) {
            if let Ok(bytes) = std::fs::read(path) {
                seen.push(sha256(&bytes));
            }
        }
        seen
    }

    /// The fault mode of this test: a direct write, which truncates the file and then fills it.
    fn direct_write(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
        std::fs::write(path, bytes)
    }

    #[test]
    fn reader_never_sees_partial_file() {
        // The reader can see a torn file: against the fault mode it sees one.
        assert!(
            !torn_reads(direct_write).is_empty(),
            "the reader sees a torn file of a direct write"
        );
        let torn = torn_reads(write_atomically);
        assert!(
            torn.is_empty(),
            "every hash that the reader saw is the old or the new one; {} were not",
            torn.len()
        );
    }
}
