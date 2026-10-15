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
use ratatoskr_notes_persistence::{
    Database, FileRow, FileTransaction, LockedConnection, NewCollision, NewRevision, OutboxEvent,
};
use uuid::Uuid;

use crate::blobs::BlobStore;
use crate::events::{self, FileChange};
use crate::watcher::TreeChange;

/// The most excluded folders of one connection; `schema.sql` holds the same bound.
pub const MAX_EXCLUDED_FOLDERS: usize = 64;

/// The SHA-256 of no bytes.
pub(crate) const EMPTY_SHA256: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

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
    /// The change is the echo of a write that notes delivered through the `sync` channel: it
    /// acknowledged that write, which made its revision current and published it once.
    Acknowledged {
        /// The write.
        write_id: Uuid,
    },
}

/// A change could not be recorded. The message names no path: a file name is the owner's content.
#[derive(Debug, thiserror::Error)]
#[error("a file change could not be recorded: {0}")]
pub struct RecordError(&'static str);

impl RecordError {
    /// The channel acknowledged a write with bytes that the blob root does not hold.
    pub(crate) const ACKNOWLEDGED_BYTES: Self =
        Self("the acknowledged bytes are not in the blob root");

    /// The write that a change acknowledged could not be ended.
    pub(crate) const WRITE: Self = Self("the write that the change acknowledged");

    /// The new bytes of a link rewrite could not be stored.
    pub(crate) const REWRITE: Self = Self("the bytes of a link rewrite");
}

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

    /// The database that the recorder writes.
    #[must_use]
    pub fn database(&self) -> &Database {
        &self.database
    }

    /// Index no file larger than `max_file_bytes` instead of [`DEFAULT_MAX_FILE_BYTES`].
    #[must_use]
    pub const fn with_max_file_bytes(mut self, max_file_bytes: u64) -> Self {
        self.max_file_bytes = max_file_bytes;
        self
    }

    /// The blob store that holds the bytes of each revision.
    #[must_use]
    pub fn blobs(&self) -> &BlobStore {
        &self.blobs
    }

    /// The largest file that is indexed, in bytes.
    #[must_use]
    pub const fn max_file_bytes(&self) -> u64 {
        self.max_file_bytes
    }

    /// Record one change of a file of `connection_id`. A connection that is not `connecting` or
    /// `active`, an excluded path, a path that the contract refuses, equal bytes and a collision
    /// record [`Recorded::Nothing`]. A change with the path and the hash of a write that notes
    /// delivered and that waits for its acknowledgement is that acknowledgement
    /// ([`Recorded::Acknowledged`]): the echo of Ratatoskr's own write (OBS-024 design D6).
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
        if crate::sync_write::is_write_part(raw) {
            return Ok(Recorded::Nothing);
        }
        let Some(path) = raw.to_str().map(nfc_path) else {
            tracing::warn!(%connection_id, "a file whose name is not UTF-8 is not indexed");
            return Ok(Recorded::Nothing);
        };
        if VaultPath::parse(&path).is_err() {
            tracing::warn!(%connection_id, "a file whose path the contract refuses is not indexed");
            return Ok(Recorded::Nothing);
        }
        let digest = match change {
            TreeChange::Revision { digest, .. } => Some(digest.as_str()),
            TreeChange::Removed { .. } => None,
        };
        if let Some(write_id) = crate::writes::echo(self, connection_id, &path, digest).await? {
            return Ok(Recorded::Acknowledged { write_id });
        }
        let note_index = match change {
            TreeChange::Revision { digest, length, .. } => {
                if !self.holds(digest).await {
                    return Err(RecordError(
                        "the blob of the revision is not in the blob root",
                    ));
                }
                self.note_index(&path, digest, *length).await
            }
            TreeChange::Removed { .. } => None,
        };
        let Some((mut transaction, connection)) =
            self.database.begin_file_change(connection_id).await?
        else {
            return Ok(Recorded::Nothing);
        };
        let recorded = match change {
            TreeChange::Revision { digest, length, .. } => {
                let revision = Revision {
                    path: &path,
                    digest,
                    length: *length,
                    note_index: note_index.as_ref(),
                };
                self.apply_revision(&mut transaction, &connection, connection_id, &revision)
                    .await?
            }
            TreeChange::Removed { .. } => {
                self.apply_removal(&mut transaction, &connection, connection_id, &path)
                    .await?
            }
        };
        transaction.commit().await?;
        Ok(recorded)
    }

    /// Whether the blob store holds the bytes with the SHA-256 `digest`.
    pub(crate) async fn holds(&self, digest: &str) -> bool {
        self.blob_length(digest).await.is_some()
    }

    /// The number of bytes of the blob with the SHA-256 `digest` when a change of the connection of
    /// `transaction` may name it, or `None` when it is missing for that connection: the connection
    /// did not upload it and holds no revision with it, or the store does not hold it. Any
    /// connection may name the blob of no bytes.
    pub(crate) async fn usable_length(
        &self,
        transaction: &mut FileTransaction,
        digest: &str,
    ) -> Result<Option<u64>, RecordError> {
        if digest != EMPTY_SHA256 && !transaction.may_use(digest).await? {
            return Ok(None);
        }
        Ok(self.blob_length(digest).await)
    }

    /// The number of bytes of the blob with the SHA-256 `digest`, or `None` when the store does
    /// not hold it. The blob of no bytes is put in the store when it is not there: the transfer
    /// contract refuses an upload of 0 bytes, so a device cannot send it.
    pub(crate) async fn blob_length(&self, digest: &str) -> Option<u64> {
        let blob = self.blobs.path_of(digest)?;
        if let Ok(metadata) = tokio::fs::metadata(&blob).await {
            return metadata.is_file().then_some(metadata.len());
        }
        if digest != EMPTY_SHA256 {
            return None;
        }
        let store = self.blobs.clone();
        let stored = tokio::task::spawn_blocking(move || {
            let part = store.create_part()?;
            store.seal_part(&part, EMPTY_SHA256)
        })
        .await
        .ok()?;
        match stored {
            Ok(stored) if stored.digest == EMPTY_SHA256 => Some(0),
            Ok(_) => None,
            Err(error) => {
                tracing::error!(%error, "the blob of no bytes could not be stored");
                None
            }
        }
    }

    /// The index of the revision `digest` of `path` when it is a Markdown note that is not too
    /// large, read from the blob store. `None` for any other file.
    pub(crate) async fn note_index(
        &self,
        path: &str,
        digest: &str,
        length: u64,
    ) -> Option<serde_json::Value> {
        if media_type(path) != "text/markdown" || length > self.max_file_bytes {
            return None;
        }
        index_of(&self.blobs.path_of(digest)?).await
    }

    /// The rules that decide whether a path of `connection` is indexed.
    pub(crate) fn rules(&self, connection: &LockedConnection) -> PathRules {
        PathRules {
            excluded_folders: connection.excluded_folders.clone(),
            max_file_bytes: self.max_file_bytes,
        }
    }

    /// Record a revision of `revision.path` in `transaction`, without a commit. The blob of the
    /// revision is in the store.
    pub(crate) async fn apply_revision(
        &self,
        transaction: &mut FileTransaction,
        connection: &LockedConnection,
        connection_id: Uuid,
        revision: &Revision<'_>,
    ) -> Result<Recorded, RecordError> {
        let path = revision.path;
        if self
            .rules(connection)
            .exclusion(path, revision.length)
            .is_some()
        {
            return Ok(Recorded::Nothing);
        }
        let key = path_key(path);
        let current = transaction.file_by_key(&key).await?;
        let admission = admit(current.as_ref().map(current_of), path, revision.digest);
        let (file_id, kind) = match (admission, &current) {
            (Admission::Unchanged, _) => return Ok(Recorded::Nothing),
            (Admission::Collision { .. }, _) => {
                transaction
                    .record_collision(&NewCollision {
                        path,
                        path_key: &key,
                        revision: revision.digest,
                        media_type: media_type(path),
                        length_bytes: revision.length,
                    })
                    .await?;
                tracing::warn!(%connection_id, code = ratatoskr_notes_index::paths::PATH_COLLISION, "two paths of a connection differ only in case or normalisation");
                return Ok(Recorded::Nothing);
            }
            (Admission::Updated, Some(file)) => (file.file_id, FileChangeKind::Updated),
            (Admission::Recreated, Some(file)) => (file.file_id, FileChangeKind::Created),
            _ => (Uuid::now_v7(), FileChangeKind::Created),
        };
        self.put(
            transaction,
            connection,
            connection_id,
            file_id,
            kind,
            None,
            revision,
        )
        .await
    }

    /// Make `revision` the current revision of the file `file_id` under its path, and publish the
    /// change `kind`, which names `previous_path` for a rename.
    #[expect(
        clippy::too_many_arguments,
        reason = "the parts of one change; a struct would only rename them"
    )]
    async fn put(
        &self,
        transaction: &mut FileTransaction,
        connection: &LockedConnection,
        connection_id: Uuid,
        file_id: Uuid,
        kind: FileChangeKind,
        previous_path: Option<&str>,
        revision: &Revision<'_>,
    ) -> Result<Recorded, RecordError> {
        let media_type = media_type(revision.path);
        transaction
            .put_revision(&NewRevision {
                file_id,
                path: revision.path,
                path_key: &path_key(revision.path),
                revision: revision.digest,
                media_type,
                length_bytes: revision.length,
                note_index: revision.note_index,
            })
            .await?;
        let change = FileChange {
            owner_id: connection.owner_id,
            connection_id,
            file_id,
            connection_sequence: 0,
            kind,
            path: revision.path,
            previous_path,
            revision: revision.digest,
            media_type,
            blob_length: Some(revision.length),
        };
        publish(transaction, change).await
    }

    /// Record that `path` left the vault, in `transaction`, without a commit.
    pub(crate) async fn apply_removal(
        &self,
        transaction: &mut FileTransaction,
        connection: &LockedConnection,
        connection_id: Uuid,
        path: &str,
    ) -> Result<Recorded, RecordError> {
        if transaction.forget_collision(path).await? {
            return Ok(Recorded::Nothing);
        }
        let Some(file) = transaction.file_by_key(&path_key(path)).await? else {
            return Ok(Recorded::Nothing);
        };
        if !file.present || file.path != path {
            return Ok(Recorded::Nothing);
        }
        self.remove(transaction, connection, connection_id, &file)
            .await
    }

    /// Delete `file`, and give its path key to the collision that waits longest for it, as after a
    /// rename that changes only the case of a name.
    async fn remove(
        &self,
        transaction: &mut FileTransaction,
        connection: &LockedConnection,
        connection_id: Uuid,
        file: &FileRow,
    ) -> Result<Recorded, RecordError> {
        let deleted = delete(transaction, connection.owner_id, connection_id, file).await?;
        self.free_key(
            transaction,
            connection,
            connection_id,
            &path_key(&file.path),
        )
        .await?;
        Ok(deleted)
    }

    /// Record the collision that waits longest for `key` as a revision of its path, now that no
    /// present file holds the key.
    async fn free_key(
        &self,
        transaction: &mut FileTransaction,
        connection: &LockedConnection,
        connection_id: Uuid,
        key: &str,
    ) -> Result<(), RecordError> {
        let Some((path, digest, length)) = transaction.oldest_collision(key).await? else {
            return Ok(());
        };
        transaction.forget_collision(&path).await?;
        let note_index = self.note_index(&path, &digest, length).await;
        let revision = Revision {
            path: &path,
            digest: &digest,
            length,
            note_index: note_index.as_ref(),
        };
        self.apply_revision(transaction, connection, connection_id, &revision)
            .await?;
        Ok(())
    }

    /// Record that the file at `from` moved to `revision.path`, in `transaction`, without a
    /// commit. A present file at `from` keeps its id and publishes one `renamed` change. When
    /// `from` names no present file, the change records the new path as a revision. When the new
    /// path is excluded, the file is deleted. When another file holds the new path key, or a
    /// deleted file does, the file at `from` is deleted and the new path is recorded as a
    /// revision, so that one path key keeps one file.
    pub(crate) async fn apply_rename(
        &self,
        transaction: &mut FileTransaction,
        connection: &LockedConnection,
        connection_id: Uuid,
        from: &str,
        revision: &Revision<'_>,
    ) -> Result<Recorded, RecordError> {
        let old = transaction
            .file_by_key(&path_key(from))
            .await?
            .filter(|file| file.present && file.path == from);
        let Some(old) = old else {
            transaction.forget_collision(from).await?;
            return self
                .apply_revision(transaction, connection, connection_id, revision)
                .await;
        };
        if self
            .rules(connection)
            .exclusion(revision.path, revision.length)
            .is_some()
        {
            return self
                .remove(transaction, connection, connection_id, &old)
                .await;
        }
        let key = path_key(revision.path);
        let target_is_free =
            key == path_key(from) || transaction.file_by_key(&key).await?.is_none();
        if !target_is_free {
            self.remove(transaction, connection, connection_id, &old)
                .await?;
            return self
                .apply_revision(transaction, connection, connection_id, revision)
                .await;
        }
        let kind = FileChangeKind::Renamed;
        let renamed = self
            .put(
                transaction,
                connection,
                connection_id,
                old.file_id,
                kind,
                Some(from),
                revision,
            )
            .await?;
        if key != path_key(from) {
            self.free_key(transaction, connection, connection_id, &path_key(from))
                .await?;
        }
        Ok(renamed)
    }
}

/// One revision to record: a path and the facts of its bytes.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Revision<'a> {
    /// The NFC path.
    pub(crate) path: &'a str,
    /// The lowercase hex SHA-256 of the bytes.
    pub(crate) digest: &'a str,
    /// The number of bytes.
    pub(crate) length: u64,
    /// The index of a Markdown note.
    pub(crate) note_index: Option<&'a serde_json::Value>,
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
    transaction: &mut FileTransaction,
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
        previous_path: None,
        revision: &file.revision,
        media_type: &file.media_type,
        blob_length: None,
    };
    publish(transaction, change).await
}

/// Take the next connection sequence and write the event of `change` to the outbox, in
/// `transaction`.
async fn publish(
    transaction: &mut FileTransaction,
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
            previous_path: None,
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
