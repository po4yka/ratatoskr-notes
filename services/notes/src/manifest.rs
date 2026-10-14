//! The manifest of a device channel (spec `notes-device-channels`, requirement "A device
//! reconciles by manifest after a gap"): every path of the vault with the SHA-256 and the length
//! of its bytes.
//!
//! When the blob store does not hold the bytes of an indexed path, nothing is recorded and the
//! answer lists those hashes; the device uploads them and sends the manifest again. Otherwise one
//! transaction records exactly the changes that make the files of the connection equal to the
//! manifest: `deleted` for a present path that the manifest does not name, `renamed` when exactly
//! one missing path and exactly one new path have the same hash, `updated` for a path whose hash
//! differs, and `created` for a new path. The committed device sequence does not change.
//!
//! Paths are not logged: a file name is the owner's content.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use ratatoskr_identifiers::DigestHex;
use ratatoskr_notes_persistence::{FileRow, FileTransaction, LockedConnection};
use uuid::Uuid;

use crate::channel::{BatchOutcome, Sender, admits, vault_path};
use crate::files::{FileRecorder, RecordError, Revision};

/// The most entries of one manifest.
pub const MAX_MANIFEST_ENTRIES: usize = 100_000;

/// One file of a manifest, as a device sends it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestEntry {
    /// The path relative to the vault root.
    pub path: String,
    /// The lowercase hex SHA-256 of its bytes.
    pub sha256: String,
    /// The number of its bytes.
    pub length_bytes: u64,
}

/// What [`apply_manifest`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestOutcome {
    /// The blob store does not hold these hashes; nothing is recorded.
    Needed {
        /// The hashes, sorted.
        needed: Vec<String>,
        /// The committed device sequence.
        committed: u64,
    },
    /// The files of the connection are equal to the manifest now.
    Applied {
        /// The committed device sequence.
        committed: u64,
    },
    /// The manifest breaks a rule of its format, or the connection refuses the sender.
    Refused(BatchOutcome),
}

/// One entry after its checks.
#[derive(Debug, Clone)]
struct Entry {
    path: String,
    digest: String,
    length: u64,
}

/// Reconcile the files of `connection_id` with the manifest `entries` of its device.
///
/// # Errors
///
/// [`RecordError`] when the database fails or an event cannot be built.
pub async fn apply_manifest(
    recorder: &FileRecorder,
    sender: &Sender<'_>,
    connection_id: Uuid,
    entries: &[ManifestEntry],
) -> Result<ManifestOutcome, RecordError> {
    let Some(entries) = check(entries) else {
        return Ok(ManifestOutcome::Refused(BatchOutcome::Invalid));
    };
    let Some((mut transaction, connection)) =
        recorder.database().begin_file_change(connection_id).await?
    else {
        return Ok(ManifestOutcome::Refused(BatchOutcome::NotFound));
    };
    if let Some(refused) = admits(sender, &connection) {
        return Ok(ManifestOutcome::Refused(refused));
    }
    let committed = connection.device_sequence;
    let rules = recorder.rules(&connection);
    let indexed: Vec<&Entry> = entries
        .iter()
        .filter(|entry| rules.exclusion(&entry.path, entry.length).is_none())
        .collect();
    let mut needed = BTreeSet::new();
    for entry in &indexed {
        match recorder
            .usable_length(&mut transaction, &entry.digest)
            .await?
        {
            None => {
                needed.insert(entry.digest.clone());
            }
            Some(stored) if stored != entry.length => {
                return Ok(ManifestOutcome::Refused(BatchOutcome::Invalid));
            }
            Some(_) => {}
        }
    }
    if !needed.is_empty() {
        return Ok(ManifestOutcome::Needed {
            needed: needed.into_iter().collect(),
            committed,
        });
    }
    let named: HashSet<&str> = entries.iter().map(|entry| entry.path.as_str()).collect();
    for path in transaction.collision_paths().await? {
        if !named.contains(path.as_str()) {
            transaction.forget_collision(&path).await?;
        }
    }
    let present = transaction.present_files().await?;
    let plan = Plan::of(&present, &indexed, &named);
    let reconcile = Reconcile {
        recorder,
        connection: &connection,
        connection_id,
    };
    reconcile.apply(&mut transaction, &plan).await?;
    transaction.commit().await?;
    Ok(ManifestOutcome::Applied { committed })
}

/// The changes that make the present files equal to the manifest.
#[derive(Debug, Default)]
struct Plan<'a> {
    deleted: Vec<&'a str>,
    renamed: Vec<(&'a str, &'a Entry)>,
    revised: Vec<&'a Entry>,
}

impl<'a> Plan<'a> {
    /// Compare the present files with the indexed entries. `named` holds every path of the
    /// manifest, also an excluded one, so that a file over the size limit is not deleted.
    fn of(present: &'a [FileRow], indexed: &[&'a Entry], named: &HashSet<&str>) -> Self {
        let current: HashMap<&str, &FileRow> = present
            .iter()
            .map(|file| (file.path.as_str(), file))
            .collect();
        let mut plan = Self::default();
        let mut missing: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
        for file in present {
            if !named.contains(file.path.as_str()) {
                missing
                    .entry(file.revision.as_str())
                    .or_default()
                    .push(&file.path);
            }
        }
        let mut new: BTreeMap<&str, Vec<&'a Entry>> = BTreeMap::new();
        for &entry in indexed {
            match current.get(entry.path.as_str()) {
                Some(file) if file.revision == entry.digest => {}
                Some(_) => plan.revised.push(entry),
                None => new.entry(entry.digest.as_str()).or_default().push(entry),
            }
        }
        for (digest, paths) in missing {
            match (paths.as_slice(), new.get(digest).map(Vec::as_slice)) {
                ([from], Some([to])) => {
                    plan.renamed.push((from, to));
                    new.remove(digest);
                }
                _ => plan.deleted.extend(paths),
            }
        }
        plan.revised.extend(new.into_values().flatten());
        plan
    }
}

/// The recorder and the locked connection of one reconciliation.
struct Reconcile<'a> {
    recorder: &'a FileRecorder,
    connection: &'a LockedConnection,
    connection_id: Uuid,
}

impl Reconcile<'_> {
    /// Record the deletions first, so that a freed path key can take a new path, then the renames,
    /// then the updates and the creates.
    async fn apply(
        &self,
        transaction: &mut FileTransaction,
        plan: &Plan<'_>,
    ) -> Result<(), RecordError> {
        let (recorder, connection, id) = (self.recorder, self.connection, self.connection_id);
        for path in &plan.deleted {
            recorder
                .apply_removal(transaction, connection, id, path)
                .await?;
        }
        for (from, entry) in &plan.renamed {
            let note_index = self.index(entry).await;
            let revision = revision_of(entry, note_index.as_ref());
            recorder
                .apply_rename(transaction, connection, id, from, &revision)
                .await?;
        }
        for entry in &plan.revised {
            let note_index = self.index(entry).await;
            let revision = revision_of(entry, note_index.as_ref());
            recorder
                .apply_revision(transaction, connection, id, &revision)
                .await?;
        }
        Ok(())
    }

    async fn index(&self, entry: &Entry) -> Option<serde_json::Value> {
        self.recorder
            .note_index(&entry.path, &entry.digest, entry.length)
            .await
    }
}

fn revision_of<'a>(entry: &'a Entry, note_index: Option<&'a serde_json::Value>) -> Revision<'a> {
    Revision {
        path: &entry.path,
        digest: &entry.digest,
        length: entry.length,
        note_index,
    }
}

/// The entries after their checks, or `None` when there are too many, a path is not a vault path
/// or appears twice, or a hash is malformed.
fn check(entries: &[ManifestEntry]) -> Option<Vec<Entry>> {
    if entries.len() > MAX_MANIFEST_ENTRIES {
        return None;
    }
    let mut seen = HashSet::with_capacity(entries.len());
    let mut checked = Vec::with_capacity(entries.len());
    for entry in entries {
        let path = vault_path(&entry.path)?;
        DigestHex::parse(&entry.sha256).ok()?;
        if !seen.insert(path.clone()) {
            return None;
        }
        checked.push(Entry {
            path,
            digest: entry.sha256.clone(),
            length: entry.length_bytes,
        });
    }
    Some(checked)
}
