//! Paths, path keys, collisions and exclusions (workspace design D7).
//!
//! A path is stored and published in Unicode NFC. Its path key is the NFC path folded with Unicode
//! default case folding. Two files whose path keys are equal make the second one a collision: it
//! gets no revision, and the connection reports [`PATH_COLLISION`] with both paths. `.obsidian/`,
//! `.trash/`, the excluded folders of the owner and files over the size limit are not indexed.

use std::collections::{BTreeMap, HashMap};

use uuid::Uuid;

use crate::text::nfc;

/// The folder of the vault configuration, as a path key.
const OBSIDIAN_FOLDER: &str = ".obsidian";

/// The folder of the vault trash, as a path key.
const TRASH_FOLDER: &str = ".trash";

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
    ///
    /// `path` is the NFC path. `.obsidian/`, `.trash/` and an excluded folder match with the
    /// comparison of path keys, so the case of their names does not matter.
    pub fn exclusion(&self, path: &str, length: u64) -> Option<Exclusion> {
        let key = path_key(path);
        match key.split('/').next() {
            Some(OBSIDIAN_FOLDER) => return Some(Exclusion::ObsidianConfig),
            Some(TRASH_FOLDER) => return Some(Exclusion::Trash),
            _ => {}
        }
        let excluded = self.excluded_folders.iter().any(|folder| {
            let folder = path_key(folder.trim_matches('/'));
            !folder.is_empty()
                && key
                    .strip_prefix(&folder)
                    .is_some_and(|rest| rest.starts_with('/'))
        });
        if excluded {
            return Some(Exclusion::ExcludedFolder);
        }
        (length > self.max_file_bytes).then_some(Exclusion::TooLarge)
    }
}

/// `path` in Unicode NFC.
#[must_use]
pub fn nfc_path(path: &str) -> String {
    nfc(path)
}

/// The path key of `path`: the NFC path folded with Unicode default case folding.
#[must_use]
pub fn path_key(path: &str) -> String {
    caseless::default_case_fold_str(&nfc(path))
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
///
/// A deleted file is created again, also under a path that differs only in case. A present file
/// under another path is a collision. Equal bytes change nothing.
pub fn admit(current: Option<Current<'_>>, path: &str, revision: &str) -> Admission {
    match current {
        None => Admission::Created,
        Some(Current { revision: None, .. }) => Admission::Recreated,
        Some(current) if current.path != path => Admission::Collision {
            existing_path: current.path.to_owned(),
        },
        Some(current) if current.revision == Some(revision) => Admission::Unchanged,
        Some(_) => Admission::Updated,
    }
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
    collisions: BTreeMap<String, Waiting>,
    observed: u64,
}

/// A collision with the revision of its path, in the order of observation.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Waiting {
    collision: PathCollision,
    revision: String,
    order: u64,
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
        let path = nfc_path(raw_path);
        if let Some(exclusion) = rules.exclusion(&path, length) {
            return Observation::Excluded(exclusion);
        }
        let key = path_key(&path);
        let current = self.files.get(&key).map(|known| Current {
            path: &known.path,
            revision: known.revision.as_deref(),
        });
        match admit(current, &path, revision) {
            Admission::Unchanged => Observation::Unchanged,
            Admission::Collision { existing_path } => {
                let collision = PathCollision {
                    path: path.clone(),
                    existing_path,
                };
                self.observed += 1;
                let waiting = Waiting {
                    collision: collision.clone(),
                    revision: revision.to_owned(),
                    order: self.observed,
                };
                self.collisions.insert(path, waiting);
                Observation::Collision(collision)
            }
            admission => {
                let known = self.files.entry(key).or_insert_with(|| Known {
                    file_id: Uuid::now_v7(),
                    path: path.clone(),
                    revision: None,
                });
                known.path.clone_from(&path);
                known.revision = Some(revision.to_owned());
                Observation::Published {
                    file_id: known.file_id,
                    path,
                    admission,
                }
            }
        }
    }

    /// Observe that `raw_path` left the vault. Returns the id of the file when a published file
    /// was deleted. A path that was a collision is no longer one. The collision that waits longest
    /// for the freed path key is published under that file id, as after a rename that changes
    /// only the case of a name.
    pub fn remove(&mut self, raw_path: &str) -> Option<Uuid> {
        let path = nfc_path(raw_path);
        if self.collisions.remove(&path).is_some() {
            return None;
        }
        let key = path_key(&path);
        let known = self.files.get_mut(&key)?;
        if known.path != path || known.revision.is_none() {
            return None;
        }
        known.revision = None;
        let file_id = known.file_id;
        let next = self
            .collisions
            .iter()
            .filter(|(waiting, _)| path_key(waiting) == key)
            .min_by_key(|(_, waiting)| waiting.order)
            .map(|(waiting, _)| waiting.clone());
        if let Some(waiting) = next.and_then(|waiting| self.collisions.remove(&waiting)) {
            known.path = waiting.collision.path;
            known.revision = Some(waiting.revision);
        }
        Some(file_id)
    }

    /// The conditions of the connection: one [`PATH_COLLISION`] with both paths per collision.
    #[must_use]
    pub fn conditions(&self) -> Vec<(&'static str, Vec<String>)> {
        self.collisions
            .values()
            .map(|Waiting { collision, .. }| {
                (
                    PATH_COLLISION,
                    vec![collision.existing_path.clone(), collision.path.clone()],
                )
            })
            .collect()
    }
}
