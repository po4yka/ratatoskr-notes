//! The files of a connection, their revisions and the event of each change (workspace design D7,
//! spec `notes-file-changes`).
//!
//! Each change runs in one transaction that holds the lock of the connection row. The change
//! decides with [`admit`] of `ratatoskr-notes-index`, against the file with the same path key:
//! a new revision of a file writes the file, the revision, the next connection sequence and one
//! `notes.file.changed.v1` to the outbox; equal bytes write nothing; a second path with the same
//! key is recorded as a collision and publishes nothing. The bytes are in the blob root before
//! the change is recorded: the watcher copies and syncs them first, and the recorder refuses a
//! revision whose blob it cannot find. So the relay never publishes an event whose bytes are not
//! on disk.
//!
//! Paths are not logged: a file name is the owner's content.

use std::path::Path;

use ratatoskr_notes_contracts::{FileChangeKind, VaultPath};
use ratatoskr_notes_index::paths::{
    Admission, Current, DEFAULT_MAX_FILE_BYTES, PathRules, admit, nfc_path, path_key,
};
use ratatoskr_notes_persistence::{Database, FileRow, FileTransaction, NewRevision, OutboxEvent};
use uuid::Uuid;

use crate::blobs::BlobStore;
use crate::events::{self, FileChange};
use crate::watcher::TreeChange;

/// The most excluded folders of one connection; `schema.sql` holds the same bound.
pub const MAX_EXCLUDED_FOLDERS: usize = 64;

/// The event type of a file change.
const FILE_CHANGED: &str = "notes.file.changed.v1";

/// What [`FileRecorder::record`] did with one change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Recorded {
    /// One `notes.file.changed.v1` is in the outbox.
    Published {
        /// The event.
        event_id: Uuid,
        /// The file.
        file_id: Uuid,
        /// The position of the event among the events of its connection.
        connection_sequence: u64,
    },
    /// Nothing changed and nothing is published.
    Nothing,
}

/// A change could not be recorded. The message names no path: a file name is the owner's content.
#[derive(Debug, thiserror::Error)]
#[error("a file change could not be recorded: {0}")]
pub struct RecordError(&'static str);

impl From<ratatoskr_notes_persistence::PersistenceError> for RecordError {
    fn from(_: ratatoskr_notes_persistence::PersistenceError) -> Self {
        Self("the database")
    }
}

impl From<events::EnvelopeBuildError> for RecordError {
    fn from(_: events::EnvelopeBuildError) -> Self {
        Self("the event")
    }
}

/// The media type of a file by its extension. A `.md` file is `text/markdown`.
#[must_use]
pub fn media_type(path: &str) -> &'static str {
    let extension = Path::new(path)
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase)
        .unwrap_or_default();
    match extension.as_str() {
        "md" | "markdown" => "text/markdown",
        "txt" => "text/plain",
        "canvas" | "json" => "application/json",
        "pdf" => "application/pdf",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "svg" => "image/svg+xml",
        _ => "application/octet-stream",
    }
}

/// Records the changes of the files of each connection.
#[derive(Debug, Clone)]
pub struct FileRecorder {
    database: Database,
    blobs: BlobStore,
    max_file_bytes: u64,
}

impl FileRecorder {
    /// A recorder that writes to `database` and finds the bytes of each revision in `blobs`.
    #[must_use]
    pub fn new(database: Database, blobs: BlobStore) -> Self {
        Self {
            database,
            blobs,
            max_file_bytes: DEFAULT_MAX_FILE_BYTES,
        }
    }

    /// Index no file larger than `max_file_bytes` instead of [`DEFAULT_MAX_FILE_BYTES`].
    #[must_use]
    pub const fn with_max_file_bytes(mut self, max_file_bytes: u64) -> Self {
        self.max_file_bytes = max_file_bytes;
        self
    }

    /// Record one change of a file of `connection_id`. A connection that is not `connecting` or
    /// `active`, an excluded path, a path that the contract refuses, equal bytes and a collision
    /// record [`Recorded::Nothing`].
    ///
    /// # Errors
    ///
    /// [`RecordError`] when the blob of a revision is missing, or the database fails.
    pub async fn record(
        &self,
        connection_id: Uuid,
        change: &TreeChange,
    ) -> Result<Recorded, RecordError> {
        let raw = match change {
            TreeChange::Revision { path, .. } | TreeChange::Removed { path } => path,
        };
        let Some(path) = raw.to_str().map(nfc_path) else {
            tracing::warn!(%connection_id, "a file whose name is not UTF-8 is not indexed");
            return Ok(Recorded::Nothing);
        };
        if VaultPath::parse(&path).is_err() {
            tracing::warn!(%connection_id, "a file whose path the contract refuses is not indexed");
            return Ok(Recorded::Nothing);
        }
        match change {
            TreeChange::Revision { digest, length, .. } => {
                self.record_revision(connection_id, &path, digest, *length)
                    .await
            }
            TreeChange::Removed { .. } => self.record_removal(connection_id, &path).await,
        }
    }

    async fn record_revision(
        &self,
        connection_id: Uuid,
        path: &str,
        digest: &str,
        length: u64,
    ) -> Result<Recorded, RecordError> {
        let blob = self
            .blobs
            .path_of(digest)
            .ok_or(RecordError("the digest"))?;
        if !tokio::fs::try_exists(&blob).await.unwrap_or(false) {
            return Err(RecordError(
                "the blob of the revision is not in the blob root",
            ));
        }
        let media_type = media_type(path);
        let note_index = if media_type == "text/markdown" && length <= self.max_file_bytes {
            index_of(&blob).await
        } else {
            None
        };

        let Some((mut transaction, connection)) =
            self.database.begin_file_change(connection_id).await?
        else {
            return Ok(Recorded::Nothing);
        };
        let rules = PathRules {
            excluded_folders: connection.excluded_folders,
            max_file_bytes: self.max_file_bytes,
        };
        if rules.exclusion(path, length).is_some() {
            return Ok(Recorded::Nothing);
        }
        let key = path_key(path);
        let current = transaction.file_by_key(&key).await?;
        let admission = admit(current.as_ref().map(current_of), path, digest);
        let (file_id, kind) = match (admission, &current) {
            (Admission::Unchanged, _) => return Ok(Recorded::Nothing),
            (Admission::Collision { .. }, _) => {
                transaction.record_collision(path, &key).await?;
                transaction.commit().await?;
                tracing::warn!(%connection_id, code = ratatoskr_notes_index::paths::PATH_COLLISION, "two paths of a connection differ only in case or normalisation");
                return Ok(Recorded::Nothing);
            }
            (Admission::Updated, Some(file)) => (file.file_id, FileChangeKind::Updated),
            (Admission::Recreated, Some(file)) => (file.file_id, FileChangeKind::Created),
            _ => (Uuid::now_v7(), FileChangeKind::Created),
        };
        transaction
            .put_revision(&NewRevision {
                file_id,
                path,
                path_key: &key,
                revision: digest,
                media_type,
                length_bytes: length,
                note_index: note_index.as_ref(),
            })
            .await?;
        let change = FileChange {
            owner_id: connection.owner_id,
            connection_id,
            file_id,
            connection_sequence: 0,
            kind,
            path,
            revision: digest,
            media_type,
            blob_length: Some(length),
        };
        publish(transaction, change).await
    }

    async fn record_removal(
        &self,
        connection_id: Uuid,
        path: &str,
    ) -> Result<Recorded, RecordError> {
        let Some((mut transaction, connection)) =
            self.database.begin_file_change(connection_id).await?
        else {
            return Ok(Recorded::Nothing);
        };
        if transaction.forget_collision(path).await? {
            transaction.commit().await?;
            return Ok(Recorded::Nothing);
        }
        let Some(file) = transaction.file_by_key(&path_key(path)).await? else {
            return Ok(Recorded::Nothing);
        };
        if !file.present || file.path != path {
            return Ok(Recorded::Nothing);
        }
        delete(transaction, connection.owner_id, connection_id, &file).await
    }
}

/// The admission view of a stored file.
fn current_of(file: &FileRow) -> Current<'_> {
    Current {
        path: &file.path,
        revision: file.present.then_some(file.revision.as_str()),
    }
}

/// The index of the Markdown note in `blob`, as JSON. `None` when the blob cannot be read or is not
/// UTF-8: the file is still recorded, without an index.
async fn index_of(blob: &Path) -> Option<serde_json::Value> {
    let bytes = tokio::fs::read(blob).await.ok()?;
    let text = String::from_utf8(bytes).ok()?;
    let index = tokio::task::spawn_blocking(move || ratatoskr_notes_index::parse(&text))
        .await
        .ok()?;
    serde_json::to_value(index).ok()
}

/// Mark `file` deleted, and publish its `deleted` event, which names its last revision and no blob.
async fn delete(
    mut transaction: FileTransaction,
    owner_id: Uuid,
    connection_id: Uuid,
    file: &FileRow,
) -> Result<Recorded, RecordError> {
    transaction.delete_file(file.file_id).await?;
    let change = FileChange {
        owner_id,
        connection_id,
        file_id: file.file_id,
        connection_sequence: 0,
        kind: FileChangeKind::Deleted,
        path: &file.path,
        revision: &file.revision,
        media_type: &file.media_type,
        blob_length: None,
    };
    publish(transaction, change).await
}

/// Take the next connection sequence, write the event of `change` to the outbox and commit.
async fn publish(
    mut transaction: FileTransaction,
    mut change: FileChange<'_>,
) -> Result<Recorded, RecordError> {
    change.connection_sequence = transaction.next_sequence().await?;
    let event_id = Uuid::now_v7();
    let envelope = events::file_changed(event_id, &change)?;
    let envelope = serde_json::to_value(&envelope).map_err(|_| RecordError("the event"))?;
    let aggregate_id = events::file_aggregate(change.file_id);
    transaction
        .write_event(&OutboxEvent {
            event_id,
            event_type: FILE_CHANGED,
            aggregate_type: events::FILE_AGGREGATE,
            aggregate_id: &aggregate_id,
            envelope: &envelope,
        })
        .await?;
    transaction.commit().await?;
    Ok(Recorded::Published {
        event_id,
        file_id: change.file_id,
        connection_sequence: change.connection_sequence,
    })
}

/// What [`set_excluded_folders`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExcludedFoldersSet {
    /// The folders are set, and this many present files under them were published `deleted`.
    Set {
        /// The folders now, in NFC and without a leading or trailing `/`.
        folders: Vec<String>,
        /// The number of `deleted` events.
        deleted: usize,
    },
    /// The owner holds no such connection, or it is not `connecting` or `active`.
    NotFound,
    /// A folder is empty, too long, holds a control character, or there are too many.
    Invalid,
}

/// Set the excluded folders of a connection of `owner_id`, and publish each present file under
/// them once as `deleted`, in one transaction. A file under an excluded folder publishes nothing
/// later.
///
/// # Errors
///
/// [`RecordError`] when the database fails.
pub async fn set_excluded_folders(
    database: &Database,
    owner_id: Uuid,
    connection_id: Uuid,
    folders: &[String],
) -> Result<ExcludedFoldersSet, RecordError> {
    let Some(folders) = normalised_folders(folders) else {
        return Ok(ExcludedFoldersSet::Invalid);
    };
    let Some((mut transaction, connection)) = database.begin_file_change(connection_id).await?
    else {
        return Ok(ExcludedFoldersSet::NotFound);
    };
    if connection.owner_id != owner_id {
        return Ok(ExcludedFoldersSet::NotFound);
    }
    transaction.set_excluded_folders(&folders).await?;
    let rules = PathRules {
        excluded_folders: folders.clone(),
        max_file_bytes: u64::MAX,
    };
    let excluded: Vec<FileRow> = transaction
        .present_files()
        .await?
        .into_iter()
        .filter(|file| rules.exclusion(&file.path, 0).is_some())
        .collect();
    let mut events = Vec::with_capacity(excluded.len());
    for file in &excluded {
        transaction.delete_file(file.file_id).await?;
        let connection_sequence = transaction.next_sequence().await?;
        let change = FileChange {
            owner_id,
            connection_id,
            file_id: file.file_id,
            connection_sequence,
            kind: FileChangeKind::Deleted,
            path: &file.path,
            revision: &file.revision,
            media_type: &file.media_type,
            blob_length: None,
        };
        let event_id = Uuid::now_v7();
        let envelope = serde_json::to_value(events::file_changed(event_id, &change)?)
            .map_err(|_| RecordError("the event"))?;
        events.push((event_id, events::file_aggregate(file.file_id), envelope));
    }
    for (event_id, aggregate_id, envelope) in &events {
        transaction
            .write_event(&OutboxEvent {
                event_id: *event_id,
                event_type: FILE_CHANGED,
                aggregate_type: events::FILE_AGGREGATE,
                aggregate_id,
                envelope,
            })
            .await?;
    }
    transaction.commit().await?;
    Ok(ExcludedFoldersSet::Set {
        folders,
        deleted: events.len(),
    })
}

/// The folders in NFC without a leading or trailing `/`, each once, or `None` when one is empty,
/// longer than 1024 bytes or holds a control character, or there are more than
/// [`MAX_EXCLUDED_FOLDERS`].
fn normalised_folders(folders: &[String]) -> Option<Vec<String>> {
    if folders.len() > MAX_EXCLUDED_FOLDERS {
        return None;
    }
    let mut normalised: Vec<String> = Vec::with_capacity(folders.len());
    for folder in folders {
        let folder = nfc_path(folder.trim_matches('/'));
        if folder.is_empty() || folder.len() > 1024 || folder.chars().any(char::is_control) {
            return None;
        }
        if !normalised.contains(&folder) {
            normalised.push(folder);
        }
    }
    Some(normalised)
}
