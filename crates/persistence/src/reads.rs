//! The reads of the read routes: connections, files, revisions and indexes of one owner.
//!
//! Every read names the owner. A connection of another owner, a removed one and a failed one read
//! as absent, so a caller cannot tell them from one that does not exist.

use uuid::Uuid;

use crate::{Database, PersistenceError};

/// One connection of an owner and its conditions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionStatus {
    /// The connection.
    pub connection_id: Uuid,
    /// The wire word of its channel.
    pub channel: String,
    /// The display name of the vault.
    pub vault_display_name: String,
    /// `connecting`, `active` or `login_expired`.
    pub state: String,
    /// The folders that the owner excluded.
    pub excluded_folders: Vec<String>,
    /// Each collision: the path that got no revision, and the path of the file that holds its key.
    pub collisions: Vec<(String, String)>,
}

/// One present file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileSummary {
    /// The file.
    pub file_id: Uuid,
    /// Its NFC path.
    pub path: String,
    /// Its current revision.
    pub revision: String,
    /// The media type of that revision.
    pub media_type: String,
    /// The number of bytes of that revision.
    pub length_bytes: i64,
}

/// The states of a connection that an owner reads. The text matches `HOLDS_VAULT_KEY`.
const READABLE: &str = "state not in ('failed', 'removed')";

impl Database {
    /// The connections of `owner_id` that are not removed and did not fail, oldest first.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when a statement fails.
    pub async fn connections_of(
        &self,
        owner_id: Uuid,
    ) -> Result<Vec<ConnectionStatus>, PersistenceError> {
        let rows: Vec<(Uuid, String, String, String, Vec<String>)> = sqlx::query_as(&format!(
            "select connection_id, channel, vault_display_name, state, excluded_folders
             from notes.connections where owner_id = $1 and {READABLE}
             order by created_at"
        ))
        .bind(owner_id)
        .fetch_all(&self.pool)
        .await
        .map_err(PersistenceError::Query)?;
        let collisions: Vec<(Uuid, String, String)> = sqlx::query_as(&format!(
            "select c.connection_id, c.path, f.path
             from notes.path_collisions c
             join notes.files f on f.connection_id = c.connection_id and f.path_key = c.path_key
             join notes.connections n on n.connection_id = c.connection_id
             where n.owner_id = $1 and n.{READABLE}
             order by c.path"
        ))
        .bind(owner_id)
        .fetch_all(&self.pool)
        .await
        .map_err(PersistenceError::Query)?;
        Ok(rows
            .into_iter()
            .map(
                |(connection_id, channel, vault_display_name, state, excluded_folders)| {
                    ConnectionStatus {
                        connection_id,
                        channel,
                        vault_display_name,
                        state,
                        excluded_folders,
                        collisions: collisions
                            .iter()
                            .filter(|(id, _, _)| *id == connection_id)
                            .map(|(_, path, existing)| (path.clone(), existing.clone()))
                            .collect(),
                    }
                },
            )
            .collect())
    }

    /// Whether `owner_id` reads the connection `connection_id`.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn owner_reads(
        &self,
        owner_id: Uuid,
        connection_id: Uuid,
    ) -> Result<bool, PersistenceError> {
        sqlx::query_scalar(&format!(
            "select exists (select 1 from notes.connections
                            where connection_id = $1 and owner_id = $2 and {READABLE})"
        ))
        .bind(connection_id)
        .bind(owner_id)
        .fetch_one(&self.pool)
        .await
        .map_err(PersistenceError::Query)
    }

    /// The present files of a connection, by path.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn present_files_of(
        &self,
        connection_id: Uuid,
    ) -> Result<Vec<FileSummary>, PersistenceError> {
        let rows: Vec<(Uuid, String, String, String, i64)> = sqlx::query_as(
            "select file_id, path, revision, media_type, length_bytes from notes.files
             where connection_id = $1 and state = 'present'
             order by path",
        )
        .bind(connection_id)
        .fetch_all(&self.pool)
        .await
        .map_err(PersistenceError::Query)?;
        Ok(rows
            .into_iter()
            .map(
                |(file_id, path, revision, media_type, length_bytes)| FileSummary {
                    file_id,
                    path,
                    revision,
                    media_type,
                    length_bytes,
                },
            )
            .collect())
    }

    /// A revision of the file with the path key `path_key`: the current one of a present file when
    /// `revision` is `None`, or the named one of the file, present or deleted.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn revision_of(
        &self,
        connection_id: Uuid,
        path_key: &str,
        revision: Option<&str>,
    ) -> Result<Option<FileSummary>, PersistenceError> {
        let row: Option<(Uuid, String, String, String, i64)> = sqlx::query_as(
            "select f.file_id, f.path, r.revision, r.media_type, r.length_bytes
             from notes.files f
             join notes.revisions r on r.file_id = f.file_id
             where f.connection_id = $1 and f.path_key = $2
               and (($3::text is null and f.state = 'present' and r.revision = f.revision)
                    or r.revision = $3)",
        )
        .bind(connection_id)
        .bind(path_key)
        .bind(revision)
        .fetch_optional(&self.pool)
        .await
        .map_err(PersistenceError::Query)?;
        Ok(row.map(
            |(file_id, path, revision, media_type, length_bytes)| FileSummary {
                file_id,
                path,
                revision,
                media_type,
                length_bytes,
            },
        ))
    }

    /// The path of each present file of a connection, with the index of its current revision when
    /// it is a Markdown note.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn current_indexes(
        &self,
        connection_id: Uuid,
    ) -> Result<Vec<(String, Option<serde_json::Value>)>, PersistenceError> {
        sqlx::query_as(
            "select f.path, r.note_index
             from notes.files f
             join notes.revisions r on r.file_id = f.file_id and r.revision = f.revision
             where f.connection_id = $1 and f.state = 'present'
             order by f.path",
        )
        .bind(connection_id)
        .fetch_all(&self.pool)
        .await
        .map_err(PersistenceError::Query)
    }
}
