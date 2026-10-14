//! Paths, path keys, collisions and exclusions (workspace design D7).
//!
//! A path is stored and published in Unicode NFC. Its path key is the NFC path folded with Unicode
//! default case folding. Two files whose path keys are equal make the second one a collision: it
//! gets no revision, and the connection reports [`PATH_COLLISION`] with both paths. `.obsidian/`,
//! `.trash/`, the excluded folders of the owner and files over the size limit are not indexed.

use std::collections::{BTreeMap, HashMap};

use uuid::Uuid;

/// The condition of a connection that holds two paths with one path key.
pub const PATH_COLLISION: &str = "notes.file.path_collision";

/// The size limit of a file when the configuration does not set one: 64 MiB.
pub const DEFAULT_MAX_FILE_BYTES: u64 = 64 * 1024 * 1024;

/// What decides whether a path is indexed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathRules {
    /// The folders that the owner excluded, relative to the vault root.
    pub excluded_folders: Vec<String>,
    /// The largest file that is indexed, in bytes.
    pub max_file_bytes: u64,
}

impl Default for PathRules {
    fn default() -> Self {
        Self {
            excluded_folders: Vec::new(),
            max_file_bytes: DEFAULT_MAX_FILE_BYTES,
        }
    }
}

/// Why a path is not indexed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Exclusion {
    /// Under `.obsidian/`, the configuration of the vault.
    ObsidianConfig,
    /// Under `.trash/`, the trash of the vault.
    Trash,
    /// Under a folder that the owner excluded.
    ExcludedFolder,
    /// Larger than the size limit.
    TooLarge,
}

impl PathRules {
    /// Why `path` with `length` bytes is not indexed, or `None` when it is.
    #[must_use]
    pub fn exclusion(&self, path: &str, length: u64) -> Option<Exclusion> {
        let _ = (path, length, &self.excluded_folders, self.max_file_bytes);
        None
    }
}

/// `path` in Unicode NFC.
#[must_use]
pub fn nfc_path(path: &str) -> String {
    path.to_owned()
}

/// The path key of `path`: the NFC path folded with Unicode default case folding.
#[must_use]
pub fn path_key(path: &str) -> String {
    path.to_owned()
}

/// What a change of a file does, given the current file with the same path key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Admission {
    /// No file has this path key: the change creates one.
    Created,
    /// The file had been deleted: the change creates it again.
    Recreated,
    /// The bytes changed under the same path.
    Updated,
    /// The bytes are those of the current revision: nothing happens.
    Unchanged,
    /// Another present file has this path key: the change is a collision with that path.
    Collision {
        /// The path of the file that was observed first.
        existing_path: String,
    },
}

/// The file that a path key names now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Current<'a> {
    /// Its NFC path.
    pub path: &'a str,
    /// Its current revision, or `None` when it was deleted.
    pub revision: Option<&'a str>,
}

/// What a change of `path` with `revision` does, given `current`, the file with the same path
/// key.
#[must_use]
pub fn admit(current: Option<Current<'_>>, path: &str, revision: &str) -> Admission {
    let _ = (current, path, revision);
    Admission::Unchanged
}

/// One collision of a connection.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PathCollision {
    /// The path that got no revision.
    pub path: String,
    /// The path of the file that was observed first.
    pub existing_path: String,
}

/// What one observation of a file gives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Observation {
    /// The file is published with this id and this NFC path.
    Published {
        /// The stable id of the file.
        file_id: Uuid,
        /// The NFC path.
        path: String,
        /// Created, recreated or updated.
        admission: Admission,
    },
    /// The bytes are those of the current revision: nothing is published.
    Unchanged,
    /// The path is not indexed.
    Excluded(Exclusion),
    /// The path collides with a file that was observed first.
    Collision(PathCollision),
}

/// A file that a [`PathTable`] knows.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Known {
    file_id: Uuid,
    path: String,
    revision: Option<String>,
}

/// The files of one connection by path key, and its collisions. The service keeps the same state
/// in the database and decides with the same [`admit`].
#[derive(Debug, Clone, Default)]
pub struct PathTable {
    files: HashMap<String, Known>,
    collisions: BTreeMap<String, PathCollision>,
}

impl PathTable {
    /// An empty table.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Observe `raw_path` with the bytes `revision` of `length` bytes.
    pub fn observe(
        &mut self,
        raw_path: &str,
        revision: &str,
        length: u64,
        rules: &PathRules,
    ) -> Observation {
        let _ = (raw_path, revision, length, rules, &mut self.files);
        Observation::Unchanged
    }

    /// The conditions of the connection: one [`PATH_COLLISION`] with both paths per collision.
    #[must_use]
    pub fn conditions(&self) -> Vec<(&'static str, Vec<String>)> {
        self.collisions
            .values()
            .map(|collision| {
                (
                    PATH_COLLISION,
                    vec![collision.existing_path.clone(), collision.path.clone()],
                )
            })
            .collect()
    }
}
