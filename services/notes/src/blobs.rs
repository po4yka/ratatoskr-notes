//! The notes blob root: the bytes of each revision, named by their SHA-256 (workspace design D7,
//! fleet spec `blob-references`).
//!
//! A blob is at `<root>/sha256/<first two hex digits>/<other 62 hex digits>`, as in the other
//! content-addressed stores of the fleet. The bytes are first copied to `<root>/staging/` and
//! hashed in the same pass, then linked to their content path, so that a reader never sees a part
//! of a blob under its content name. A blob that exists already is not written again.

use std::fs::File;
use std::io::{Read as _, Write as _};
use std::os::fd::OwnedFd;
use std::path::{Path, PathBuf};

use rustix::fs::{Mode, OFlags};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

/// The size of one read while a file is copied, so that memory stays bounded for any file size.
const CHUNK_BYTES: usize = 64 * 1024;

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
        let input = open_regular_file(source)?;
        let staging_dir = self.root.join("staging");
        std::fs::create_dir_all(&staging_dir)?;
        let staging = staging_dir.join(format!("{}.part", Uuid::now_v7()));
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

    /// Give the staged copy its content name, unless a blob with that name exists already.
    fn link(&self, staging: &Path, digest: &str) -> std::io::Result<()> {
        let target = self
            .path_of(digest)
            .ok_or_else(|| std::io::Error::other("the digest is too short"))?;
        if target.exists() {
            return Ok(());
        }
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)?;
        }
        match std::fs::hard_link(staging, &target) {
            Ok(()) => Ok(()),
            // Two writers of the same bytes: the first link wins, and the bytes are equal.
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
            Err(error) => Err(error),
        }
    }
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
fn copy_and_hash(mut input: File, staging: &Path) -> std::io::Result<(String, u64)> {
    let mut output = File::create_new(staging)?;
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
