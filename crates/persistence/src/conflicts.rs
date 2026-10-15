//! The conflicts of the write path, as the routes and the announcement read them (OBS-024).

use uuid::Uuid;

use crate::files::query_error;
use crate::writes::ConflictRow;
use crate::{Database, PersistenceError};

impl Database {
    /// The conflict `conflict_id` of a connection of `owner_id`. A conflict of another owner reads
    /// as absent.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn conflict_of_owner(
        &self,
        owner_id: Uuid,
        conflict_id: Uuid,
    ) -> Result<Option<ConflictRow>, PersistenceError> {
        Ok(self
            .conflict(conflict_id)
            .await?
            .filter(|(owner, _)| *owner == owner_id)
            .map(|(_, conflict)| conflict))
    }

    /// The conflict `conflict_id` and the owner of its connection.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn conflict(
        &self,
        conflict_id: Uuid,
    ) -> Result<Option<(Uuid, ConflictRow)>, PersistenceError> {
        type Row = (
            Uuid,
            Uuid,
            Uuid,
            Uuid,
            String,
            String,
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<bool>,
            String,
            Option<Vec<String>>,
        );
        let row: Option<Row> = sqlx::query_as(
            "select n.owner_id, c.conflict_id, c.connection_id, c.operation_id, c.path, c.origin,
                    c.media_type, c.base_hash, c.current_hash, c.writer_hash, c.merged_hash,
                    c.has_overlapping_hunks, c.state, c.stale_paths
             from notes.conflicts c
             join notes.connections n on n.connection_id = c.connection_id
             where c.conflict_id = $1",
        )
        .bind(conflict_id)
        .fetch_optional(self.pool())
        .await
        .map_err(query_error)?;
        Ok(row.map(
            |(
                owner_id,
                conflict_id,
                connection_id,
                operation_id,
                path,
                origin,
                media_type,
                base_hash,
                current_hash,
                writer_hash,
                merged_hash,
                has_overlapping_hunks,
                state,
                stale_paths,
            )| {
                (
                    owner_id,
                    ConflictRow {
                        conflict_id,
                        connection_id,
                        operation_id,
                        path,
                        origin,
                        media_type,
                        base_hash,
                        current_hash,
                        writer_hash,
                        merged_hash,
                        has_overlapping_hunks,
                        state,
                        stale_paths,
                    },
                )
            },
        ))
    }

    /// The open conflicts that are not announced, oldest first, at most `limit`, after the
    /// conflict `after` (the ids are `UUIDv7`, in the order of their creation).
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn unannounced_conflicts(
        &self,
        after: Option<Uuid>,
        limit: i64,
    ) -> Result<Vec<Uuid>, PersistenceError> {
        sqlx::query_scalar(
            "select conflict_id from notes.conflicts
             where not announced and state = 'open'
               and ($1::uuid is null or conflict_id > $1)
             order by conflict_id
             limit $2",
        )
        .bind(after)
        .bind(limit)
        .fetch_all(self.pool())
        .await
        .map_err(query_error)
    }
}
