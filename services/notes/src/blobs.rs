//! The notes blob root: the bytes of each revision, named by their SHA-256 (workspace design D7,
//! fleet spec `blob-references`).

use std::path::{Path, PathBuf};

/// The content-addressed store under one blob root.
#[derive(Debug, Clone)]
pub struct BlobStore {
    root: PathBuf,
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
}
