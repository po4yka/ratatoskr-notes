//! The notes blob root: the bytes of each revision, named by their SHA-256 (workspace design D7,
//! fleet spec `blob-references`).
//!
//! A blob is at `<root>/sha256/<first two hex digits>/<other 62 hex digits>`, as in the other
//! content-addressed stores of the fleet. The bytes are first copied to `<root>/staging/` and
//! hashed in the same pass, then linked to their content path, so that a reader never sees a part
//! of a blob under its content name. A blob that exists already is not written again.
//!
//! The read group (workspace design D12): the deploy role gives the root the group
//! `ratatoskr-notes-blobs` and the mode 2750. Each blob is mode 0640 and each directory is mode
//! 2750, whatever the umask, and the setgid bit gives each new file and directory the group of the
//! root. So the read group can read a blob, and no other user can. On Linux a new directory in a
//! setgid directory gets the bit from the kernel, and the store changes its mode only when it
//! differs: a `chmod` by a user that is not in the group would remove the bit. The service user is
//! not in the group, so [`BlobStore::open`] refuses a umask stricter than 0027, which would make the
//! store run that `chmod` on each new directory. It also deletes the staged copies that a stop left
//! behind.

use std::fs::{DirBuilder, File, OpenOptions};
use std::io::{Read as _, Write as _};
use std::os::fd::OwnedFd;
use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};

use rustix::fs::{Mode, OFlags};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

/// The size of one read while a file is copied, so that memory stays bounded for any file size.
const CHUNK_BYTES: usize = 64 * 1024;

/// The mode of each blob: the owner writes, the read group reads.
const BLOB_MODE: u32 = 0o640;

/// The mode of each directory: the setgid bit keeps the group of the root.
const DIRECTORY_MODE: u32 = 0o2750;

/// The strictest umask that keeps [`BLOB_MODE`] and [`DIRECTORY_MODE`] at the create, so that the
/// store never runs `chmod` on a new directory on Linux.
#[cfg(target_os = "linux")]
const STRICTEST_UMASK: u32 = 0o027;

/// The extension of a staged copy.
const PART: &str = "part";

/// The content-addressed store under one blob root.
#[derive(Debug, Clone)]
pub struct BlobStore {
    root: PathBuf,
}

/// A blob that is in the store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredBlob {
    /// The lowercase hex SHA-256 of the bytes.
    pub digest: String,
    /// The number of bytes.
    pub length: u64,
}

impl BlobStore {
    /// The store under `root`. The deploy role creates the root.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// The store under `root`, for the service, which calls it at its start. The deploy role
    /// creates the root. It refuses a umask stricter than 0027: with it, the store would run
    /// `chmod` on each new directory, and a `chmod` by an owner that is not in the group of the
    /// root removes the setgid bit. It deletes each staged copy that a stop of the service left in
    /// `<root>/staging/`. It blocks.
    ///
    /// # Errors
    ///
    /// The reason when the umask is stricter than 0027 or cannot be read, or when a staged copy
    /// cannot be deleted.
    pub fn open(root: impl Into<PathBuf>) -> std::io::Result<Self> {
        check_umask()?;
        let store = Self::new(root);
        store.remove_stale_parts()?;
        Ok(store)
    }

    /// The blob root.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The path of the blob with this digest, or `None` when the digest is too short.
    #[must_use]
    pub fn path_of(&self, digest: &str) -> Option<PathBuf> {
        let (prefix, rest) = digest.split_at_checked(2)?;
        Some(self.root.join("sha256").join(prefix).join(rest))
    }

    /// Copy the regular file at `source` into the store. It blocks, so an async caller runs it on
    /// the blocking pool.
    ///
    /// The file is opened without following a symbolic link, and a file that is not a regular file
    /// is refused, so that a link in a working tree cannot make the service copy another file.
    ///
    /// # Errors
    ///
    /// The I/O error when the source cannot be read or the store cannot be written. A part that
    /// was copied is deleted.
    pub fn store_file(&self, source: &Path) -> std::io::Result<StoredBlob> {
        self.store_open_file(&open_regular_file(source)?)
    }

    /// Copy the bytes of an open file into the store, from its current offset. It blocks, so an
    /// async caller runs it on the blocking pool. The watcher opens the file itself, so that the
    /// bytes come from the file whose state it checked.
    ///
    /// # Errors
    ///
    /// The I/O error when the file cannot be read or the store cannot be written. A part that was
    /// copied is deleted.
    pub fn store_open_file(&self, input: &File) -> std::io::Result<StoredBlob> {
        let staging_dir = self.root.join("staging");
        self.create_directories(&staging_dir)?;
        let staging = staging_dir.join(format!("{}.{PART}", Uuid::now_v7()));
        let copied = copy_and_hash(input, &staging);
        let (digest, length) = match copied {
            Ok(copied) => copied,
            Err(error) => {
                let _ = std::fs::remove_file(&staging);
                return Err(error);
            }
        };
        let linked = self.link(&staging, &digest);
        let _ = std::fs::remove_file(&staging);
        linked?;
        Ok(StoredBlob { digest, length })
    }

    /// Delete each staged copy in `<root>/staging/`. Only one process writes the store.
    fn remove_stale_parts(&self) -> std::io::Result<()> {
        let entries = match std::fs::read_dir(self.root.join("staging")) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };
        for entry in entries {
            let path = entry?.path();
            if path.extension().is_some_and(|extension| extension == PART) {
                std::fs::remove_file(&path)?;
            }
        }
        Ok(())
    }

    /// Give the staged copy its content name, unless a blob with that name exists already.
    fn link(&self, staging: &Path, digest: &str) -> std::io::Result<()> {
        let target = self
            .path_of(digest)
            .ok_or_else(|| std::io::Error::other("the digest is too short"))?;
        if target.exists() {
            return Ok(());
        }
        if let Some(parent) = target.parent() {
            self.create_directories(parent)?;
        }
        match std::fs::hard_link(staging, &target) {
            // The new name is on disk when this returns.
            Ok(()) => sync_parent(&target),
            // Two writers of the same bytes: the first link wins, and the bytes are equal.
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
            Err(error) => Err(error),
        }
    }

    /// Create each missing directory from the root down to `directory`, with [`DIRECTORY_MODE`].
    fn create_directories(&self, directory: &Path) -> std::io::Result<()> {
        let below_root = directory
            .strip_prefix(&self.root)
            .map_err(std::io::Error::other)?;
        let mut current = self.root.clone();
        for component in below_root.components() {
            current.push(component);
            match DirBuilder::new()
                .mode(DIRECTORY_MODE & 0o777)
                .create(&current)
            {
                Ok(()) => set_mode_if_different(&current, DIRECTORY_MODE)?,
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
}

/// Write the directory that holds `path` to disk, so that a new name in it stays after a crash.
fn sync_parent(path: &Path) -> std::io::Result<()> {
    match path.parent() {
        Some(parent) => File::open(parent)?.sync_all(),
        None => Ok(()),
    }
}

/// Refuse a umask stricter than [`STRICTEST_UMASK`]. Linux gives the umask in `/proc/self/status`
/// without changing it. Other platforms are not a host, and there the check does nothing.
#[cfg(target_os = "linux")]
fn check_umask() -> std::io::Result<()> {
    let status = std::fs::read_to_string("/proc/self/status")?;
    let umask = status
        .lines()
        .find_map(|line| line.strip_prefix("Umask:"))
        .and_then(|value| u32::from_str_radix(value.trim(), 8).ok())
        .ok_or_else(|| std::io::Error::other("the umask of the process cannot be read"))?;
    if umask & !STRICTEST_UMASK & 0o777 != 0 {
        return Err(std::io::Error::other(format!(
            "the umask {umask:04o} is stricter than 0027: the blob store would remove the setgid \
             bit of its directories; give the service UMask=0027"
        )));
    }
    Ok(())
}

/// Other platforms are not a host: the umask cannot be read there without changing it.
#[cfg(not(target_os = "linux"))]
#[expect(
    clippy::unnecessary_wraps,
    reason = "the same signature as the Linux implementation"
)]
fn check_umask() -> std::io::Result<()> {
    Ok(())
}

/// Give `path` the mode `mode` unless it has it already. The kernel gives a new directory in a
/// setgid directory the setgid bit; a `chmod` by an owner outside the group would remove it.
fn set_mode_if_different(path: &Path, mode: u32) -> std::io::Result<()> {
    let current = std::fs::metadata(path)?.permissions().mode() & 0o7777;
    if current != mode {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))?;
    }
    Ok(())
}

/// Open a regular file for reading without following a symbolic link. `O_NONBLOCK` keeps the open
/// of a FIFO from waiting for a writer; the type check then refuses it.
fn open_regular_file(source: &Path) -> std::io::Result<File> {
    let descriptor: OwnedFd = rustix::fs::open(
        source,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    )?;
    let file = File::from(descriptor);
    if !file.metadata()?.is_file() {
        return Err(std::io::Error::other("the source is not a regular file"));
    }
    Ok(file)
}

/// Copy `input` to a new file at `staging` and hash it in the same pass. Returns the lowercase hex
/// SHA-256 and the length. The copy is on disk when this returns.
fn copy_and_hash(mut input: &File, staging: &Path) -> std::io::Result<(String, u64)> {
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(BLOB_MODE)
        .open(staging)?;
    // A stricter umask than 0027 removes the group bit at the create.
    set_mode_if_different(staging, BLOB_MODE)?;
    let mut hasher = Sha256::new();
    let mut length: u64 = 0;
    let mut chunk = vec![0_u8; CHUNK_BYTES];
    loop {
        let read = input.read(&mut chunk)?;
        let Some(bytes) = chunk.get(..read) else {
            return Err(std::io::Error::other("a read exceeded its buffer"));
        };
        if bytes.is_empty() {
            break;
        }
        hasher.update(bytes);
        output.write_all(bytes)?;
        length += u64::try_from(read).map_err(std::io::Error::other)?;
    }
    output.sync_all()?;
    Ok((format!("{:x}", hasher.finalize()), length))
}

#[cfg(test)]
mod tests {
    use super::BlobStore;

    #[test]
    fn open_deletes_stale_parts_only() {
        let root = tempfile::tempdir().expect("a blob root");
        let staging = root.path().join("staging");
        std::fs::create_dir(&staging).expect("the staging directory");
        std::fs::write(staging.join("0192.part"), b"a copy that a stop left").expect("a part");
        std::fs::write(staging.join("keep.txt"), b"not a part").expect("another file");

        BlobStore::open(root.path()).expect("the store opens");

        assert!(
            !staging.join("0192.part").exists(),
            "the stale part is gone"
        );
        assert!(staging.join("keep.txt").exists(), "another file stays");
    }
}
