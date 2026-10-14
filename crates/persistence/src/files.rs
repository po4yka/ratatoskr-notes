//! Files, revisions, path collisions and the connection sequence.
//!
//! Every change of the files of one connection runs in a [`FileTransaction`], which holds the lock
//! of the connection row from its start to its commit. So the changes of one connection are in
//! one order, the connection sequence has no gap, and the outbox rows of one connection are in the
//! order of their sequence.

use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::connections::OutboxEvent;
use crate::{Database, PersistenceError};

/// The connection whose files a [`FileTransaction`] changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockedConnection {
    /// The owner from the claim that Edge minted.
    pub owner_id: Uuid,
    /// The wire word of its channel.
    pub channel: String,
    /// The folders that the owner excluded.
    pub excluded_folders: Vec<String>,
    /// The highest device change sequence that is committed; 0 for a `sync` connection.
    pub device_sequence: u64,
}

/// One file of a connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileRow {
    /// The file.
    pub file_id: Uuid,
    /// Its NFC path.
    pub path: String,
    /// Whether it is in the vault now.
    pub present: bool,
    /// Its current revision, or its last one when it was deleted.
    pub revision: String,
    /// The media type of that revision.
    pub media_type: String,
}

/// One revision to record.
#[derive(Debug, Clone, Copy)]
pub struct NewRevision<'a> {
    /// The file.
    pub file_id: Uuid,
    /// The NFC path of the file now.
    pub path: &'a str,
    /// The path key of `path`.
    pub path_key: &'a str,
    /// The lowercase hex SHA-256 of the bytes.
    pub revision: &'a str,
    /// The media type of the bytes.
    pub media_type: &'a str,
    /// The number of bytes.
    pub length_bytes: u64,
    /// The index of a Markdown revision.
    pub note_index: Option<&'a serde_json::Value>,
}

/// The changes of the files of one connection, under the lock of its row.
#[derive(Debug)]
pub struct FileTransaction {
    transaction: Transaction<'static, Postgres>,
    connection_id: Uuid,
}

fn query_error(error: sqlx::Error) -> PersistenceError {
    PersistenceError::Query(error)
}

impl Database {
    /// Begin the changes of the files of `connection_id`, and lock its row. Returns `None` when the
    /// connection is not `connecting` or `active`: a removed, failed or expired connection records
    /// no change.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when a statement fails.
    pub async fn begin_file_change(
        &self,
        connection_id: Uuid,
    ) -> Result<Option<(FileTransaction, LockedConnection)>, PersistenceError> {
        let mut transaction = self.pool.begin().await.map_err(query_error)?;
        let row: Option<(Uuid, String, Vec<String>, i64)> = sqlx::query_as(
            "select owner_id, channel, excluded_folders, device_sequence from notes.connections
             where connection_id = $1 and state in ('connecting', 'active')
             for update",
        )
        .bind(connection_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(query_error)?;
        Ok(
            row.map(|(owner_id, channel, excluded_folders, device_sequence)| {
                (
                    FileTransaction {
                        transaction,
                        connection_id,
                    },
                    LockedConnection {
                        owner_id,
                        channel,
                        excluded_folders,
                        // A CHECK keeps the sequence at 0 or more.
                        device_sequence: u64::try_from(device_sequence).unwrap_or_default(),
                    },
                )
            }),
        )
    }
}

impl FileTransaction {
    /// The file of this connection with the path key `path_key`, present or deleted.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn file_by_key(
        &mut self,
        path_key: &str,
    ) -> Result<Option<FileRow>, PersistenceError> {
        let row: Option<(Uuid, String, String, String, String)> = sqlx::query_as(
            "select file_id, path, state, revision, media_type from notes.files
             where connection_id = $1 and path_key = $2",
        )
        .bind(self.connection_id)
        .bind(path_key)
        .fetch_optional(&mut *self.transaction)
        .await
        .map_err(query_error)?;
        Ok(row.map(file_row))
    }

    /// Every present file of this connection.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn present_files(&mut self) -> Result<Vec<FileRow>, PersistenceError> {
        let rows: Vec<(Uuid, String, String, String, String)> = sqlx::query_as(
            "select file_id, path, state, revision, media_type from notes.files
             where connection_id = $1 and state = 'present'
             order by path",
        )
        .bind(self.connection_id)
        .fetch_all(&mut *self.transaction)
        .await
        .map_err(query_error)?;
        Ok(rows.into_iter().map(file_row).collect())
    }

    /// Make `revision` the current revision of its file, which is created when it does not exist,
    /// and record the revision.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when a statement fails.
    pub async fn put_revision(&mut self, new: &NewRevision<'_>) -> Result<(), PersistenceError> {
        // A file over the size limit of the service is never recorded, so the length fits.
        let length = i64::try_from(new.length_bytes).unwrap_or(i64::MAX);
        sqlx::query(
            "insert into notes.files
                 (file_id, connection_id, path, path_key, state, revision, media_type,
                  length_bytes, created_at, updated_at)
             values ($1, $2, $3, $4, 'present', $5, $6, $7, now(), now())
             on conflict (file_id) do update
                 set path = excluded.path, path_key = excluded.path_key, state = 'present',
                     revision = excluded.revision, media_type = excluded.media_type,
                     length_bytes = excluded.length_bytes, updated_at = now()",
        )
        .bind(new.file_id)
        .bind(self.connection_id)
        .bind(new.path)
        .bind(new.path_key)
        .bind(new.revision)
        .bind(new.media_type)
        .bind(length)
        .execute(&mut *self.transaction)
        .await
        .map_err(query_error)?;
        sqlx::query(
            "insert into notes.revisions
                 (file_id, revision, media_type, length_bytes, note_index, created_at)
             values ($1, $2, $3, $4, $5, now())
             on conflict (file_id, revision) do nothing",
        )
        .bind(new.file_id)
        .bind(new.revision)
        .bind(new.media_type)
        .bind(length)
        .bind(new.note_index)
        .execute(&mut *self.transaction)
        .await
        .map_err(query_error)?;
        Ok(())
    }

    /// Mark a file deleted. It keeps its last revision.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn delete_file(&mut self, file_id: Uuid) -> Result<(), PersistenceError> {
        sqlx::query(
            "update notes.files set state = 'deleted', updated_at = now()
             where file_id = $1 and connection_id = $2",
        )
        .bind(file_id)
        .bind(self.connection_id)
        .execute(&mut *self.transaction)
        .await
        .map_err(query_error)?;
        Ok(())
    }

    /// Record that `path` collides with the present file that holds `path_key`.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn record_collision(
        &mut self,
        path: &str,
        path_key: &str,
    ) -> Result<(), PersistenceError> {
        sqlx::query(
            "insert into notes.path_collisions (connection_id, path, path_key, observed_at)
             values ($1, $2, $3, now())
             on conflict (connection_id, path) do nothing",
        )
        .bind(self.connection_id)
        .bind(path)
        .bind(path_key)
        .execute(&mut *self.transaction)
        .await
        .map_err(query_error)?;
        Ok(())
    }

    /// Forget the collision of `path`. Returns whether there was one.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn forget_collision(&mut self, path: &str) -> Result<bool, PersistenceError> {
        let forgotten =
            sqlx::query("delete from notes.path_collisions where connection_id = $1 and path = $2")
                .bind(self.connection_id)
                .bind(path)
                .execute(&mut *self.transaction)
                .await
                .map_err(query_error)?;
        Ok(forgotten.rows_affected() > 0)
    }

    /// Set the folders that the owner excluded.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn set_excluded_folders(
        &mut self,
        folders: &[String],
    ) -> Result<(), PersistenceError> {
        sqlx::query(
            "update notes.connections set excluded_folders = $2, updated_at = now()
             where connection_id = $1",
        )
        .bind(self.connection_id)
        .bind(folders)
        .execute(&mut *self.transaction)
        .await
        .map_err(query_error)?;
        Ok(())
    }

    /// Record `sequence` as the highest device change sequence that is committed.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn set_device_sequence(&mut self, sequence: u64) -> Result<(), PersistenceError> {
        sqlx::query(
            "update notes.connections set device_sequence = $2, updated_at = now()
             where connection_id = $1",
        )
        .bind(self.connection_id)
        .bind(i64::try_from(sequence).unwrap_or(i64::MAX))
        .execute(&mut *self.transaction)
        .await
        .map_err(query_error)?;
        Ok(())
    }

    /// The next connection sequence: one more than the last one, starting at 1.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn next_sequence(&mut self) -> Result<u64, PersistenceError> {
        let next: i64 = sqlx::query_scalar(
            "update notes.connections set last_sequence = last_sequence + 1
             where connection_id = $1
             returning last_sequence",
        )
        .bind(self.connection_id)
        .fetch_one(&mut *self.transaction)
        .await
        .map_err(query_error)?;
        // A CHECK keeps the counter at 0 or more.
        Ok(u64::try_from(next).unwrap_or_default())
    }

    /// Write `event` to the outbox.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn write_event(&mut self, event: &OutboxEvent<'_>) -> Result<(), PersistenceError> {
        crate::connections::insert_outbox(&mut self.transaction, event).await
    }

    /// Commit every change.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the commit fails; nothing is changed then.
    pub async fn commit(self) -> Result<(), PersistenceError> {
        self.transaction.commit().await.map_err(query_error)
    }
}

fn file_row(
    (file_id, path, state, revision, media_type): (Uuid, String, String, String, String),
) -> FileRow {
    FileRow {
        file_id,
        path,
        present: state == "present",
        revision,
        media_type,
    }
}
