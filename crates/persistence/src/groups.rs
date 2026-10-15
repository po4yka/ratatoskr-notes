//! The write group of a rename and the link rewrites it needs (OBS-024, spec
//! `notes-link-rewrite`).

use uuid::Uuid;

use crate::files::{FileTransaction, query_error};
use crate::writes::{WRITE_COLUMNS, WriteRow};
use crate::{Database, PersistenceError};

/// One present Markdown note of a connection, and the index of its current revision.
#[derive(Debug, Clone, PartialEq)]
pub struct NoteRow {
    /// The NFC path of the note.
    pub path: String,
    /// The SHA-256 of its current revision.
    pub revision: String,
    /// The index of that revision, as `notes.revisions.note_index` holds it.
    pub note_index: Option<serde_json::Value>,
}

impl FileTransaction {
    /// Every write of the group `group_id` of this connection, in the order of acceptance.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn group_writes(
        &mut self,
        group_id: Uuid,
    ) -> Result<Vec<WriteRow>, PersistenceError> {
        sqlx::query_as(&format!(
            "select {WRITE_COLUMNS} from notes.writes
             where connection_id = $1 and group_id = $2
             order by position"
        ))
        .bind(self.connection_id)
        .bind(group_id)
        .fetch_all(&mut *self.transaction)
        .await
        .map_err(query_error)
    }

    /// Every present Markdown note of this connection with the index of its current revision.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn note_rows(&mut self) -> Result<Vec<NoteRow>, PersistenceError> {
        let rows: Vec<(String, String, Option<serde_json::Value>)> = sqlx::query_as(
            "select f.path, f.revision, r.note_index
             from notes.files f
             join notes.revisions r on r.file_id = f.file_id and r.revision = f.revision
             where f.connection_id = $1 and f.state = 'present' and f.media_type = 'text/markdown'
             order by f.path",
        )
        .bind(self.connection_id)
        .fetch_all(&mut *self.transaction)
        .await
        .map_err(query_error)?;
        Ok(rows
            .into_iter()
            .map(|(path, revision, note_index)| NoteRow {
                path,
                revision,
                note_index,
            })
            .collect())
    }

    /// Let this connection name the blob `revision` of `length_bytes` bytes, as an upload does,
    /// in the transaction that holds the lock of the connection row.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn record_blob(
        &mut self,
        revision: &str,
        length_bytes: u64,
    ) -> Result<(), PersistenceError> {
        sqlx::query(
            "insert into notes.connection_blobs (connection_id, revision, length_bytes, received_at)
             values ($1, $2, $3, now())
             on conflict (connection_id, revision) do nothing",
        )
        .bind(self.connection_id)
        .bind(revision)
        .bind(i64::try_from(length_bytes).unwrap_or(i64::MAX))
        .execute(&mut *self.transaction)
        .await
        .map_err(query_error)?;
        Ok(())
    }

    /// Record `local_hash`, the hash of the file that the channel reported with a base mismatch,
    /// on the write `write_id`.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn set_local_hash(
        &mut self,
        write_id: Uuid,
        local_hash: Option<&str>,
    ) -> Result<(), PersistenceError> {
        sqlx::query(
            "update notes.writes set local_hash = $3, updated_at = now()
              where connection_id = $1 and write_id = $2",
        )
        .bind(self.connection_id)
        .bind(write_id)
        .bind(local_hash)
        .execute(&mut *self.transaction)
        .await
        .map_err(query_error)?;
        Ok(())
    }

    /// Name the conflict `conflict_id` on the write `write_id`, which failed with it.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn join_conflict(
        &mut self,
        write_id: Uuid,
        conflict_id: Uuid,
    ) -> Result<(), PersistenceError> {
        sqlx::query(
            "update notes.writes set conflict_id = $3, updated_at = now()
              where connection_id = $1 and write_id = $2",
        )
        .bind(self.connection_id)
        .bind(write_id)
        .bind(conflict_id)
        .execute(&mut *self.transaction)
        .await
        .map_err(query_error)?;
        Ok(())
    }

    /// Record `paths`, sorted, as the stale or unapplied files of the group that the conflict
    /// `conflict_id` ends.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn set_stale_paths(
        &mut self,
        conflict_id: Uuid,
        paths: &[String],
    ) -> Result<(), PersistenceError> {
        sqlx::query(
            "update notes.conflicts set stale_paths = $3
              where connection_id = $1 and conflict_id = $2",
        )
        .bind(self.connection_id)
        .bind(conflict_id)
        .bind(paths)
        .execute(&mut *self.transaction)
        .await
        .map_err(query_error)?;
        Ok(())
    }
}

impl Database {
    /// Every write of the group `group_id` of `connection_id`, in the order of acceptance.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn group_writes(
        &self,
        connection_id: Uuid,
        group_id: Uuid,
    ) -> Result<Vec<WriteRow>, PersistenceError> {
        sqlx::query_as(&format!(
            "select {WRITE_COLUMNS} from notes.writes
             where connection_id = $1 and group_id = $2
             order by position"
        ))
        .bind(connection_id)
        .bind(group_id)
        .fetch_all(self.pool())
        .await
        .map_err(query_error)
    }
}
