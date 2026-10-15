//! The writes that Ratatoskr asked for, and the queue of each file (OBS-024).
//!
//! Every change of a write runs in the [`FileTransaction`] of its connection, which holds the lock
//! of the connection row. So the queue of a file changes in one order, and a write that reaches
//! the head of its queue is checked against revisions that no other change can move meanwhile.

use uuid::Uuid;

use crate::files::{FileTransaction, query_error};
use crate::{Database, PersistenceError};

/// One write to record.
#[derive(Debug, Clone, Copy)]
pub struct NewWrite<'a> {
    /// The write.
    pub write_id: Uuid,
    /// The operation that Edge minted for it.
    pub operation_id: Uuid,
    /// The hex SHA-256 idempotency key of the writer.
    pub idempotency_key: &'a str,
    /// The hex SHA-256 of the origin and the operation of the command.
    pub request_digest: &'a str,
    /// `platform` or `site:<site id>`.
    pub origin: &'a str,
    /// The wire word of the operation.
    pub kind: &'a str,
    /// The NFC path that the write changes.
    pub path: &'a str,
    /// The path key of `path`, which names the queue.
    pub path_key: &'a str,
    /// The new path of a `rename` or a `move`.
    pub target_path: Option<&'a str>,
    /// The revision that the writer saw.
    pub base_hash: Option<&'a str>,
    /// The SHA-256 of the uploaded bytes that the write puts at the path.
    pub content_hash: Option<&'a str>,
    /// The number of those bytes.
    pub content_length: Option<u64>,
    /// The conflict that a `resolve_conflict` ends, and its choice.
    pub resolves: Option<(Uuid, &'a str)>,
}

/// One recorded write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteRow {
    /// The write.
    pub write_id: Uuid,
    /// Its operation.
    pub operation_id: Uuid,
    /// Its origin.
    pub origin: String,
    /// The wire word of its operation.
    pub kind: String,
    /// The NFC path that it changes.
    pub path: String,
    /// The path key of `path`.
    pub path_key: String,
    /// The new path of a `rename` or a `move`.
    pub target_path: Option<String>,
    /// The revision that the writer saw.
    pub base_hash: Option<String>,
    /// The SHA-256 of the bytes that it puts at the path.
    pub content_hash: Option<String>,
    /// The number of those bytes.
    pub content_length: Option<u64>,
    /// `queued`, `delivering`, `succeeded` or `failed`.
    pub state: String,
    /// The SHA-256 of the file after a write that succeeded.
    pub revision: Option<String>,
    /// The error code of a write that failed.
    pub error_code: Option<String>,
    /// The conflict that a write which failed with a conflict raised.
    pub conflict_id: Option<Uuid>,
    /// The conflict that a `resolve_conflict` ends.
    pub resolves: Option<Uuid>,
    /// The choice of a `resolve_conflict`.
    pub choice: Option<String>,
}

/// An open conflict of a connection, as a resolution reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenConflict {
    /// The NFC path of the file.
    pub path: String,
    /// The path key of `path`.
    pub path_key: String,
    /// The revision of the vault when the conflict was raised.
    pub current_hash: Option<String>,
    /// The writer's content.
    pub writer_hash: Option<String>,
    /// The merged bytes of the merge proposal.
    pub merged_hash: Option<String>,
}

/// One conflict to record.
#[derive(Debug, Clone, Copy)]
pub struct NewConflict<'a> {
    /// The conflict.
    pub conflict_id: Uuid,
    /// The write that raised it.
    pub write: &'a WriteRow,
    /// The media type of the file.
    pub media_type: &'a str,
    /// The revision that the writer saw.
    pub base_hash: Option<&'a str>,
    /// The revision of the vault.
    pub current_hash: Option<&'a str>,
    /// The writer's content.
    pub writer_hash: Option<&'a str>,
}

/// One recorded conflict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConflictRow {
    /// The conflict.
    pub conflict_id: Uuid,
    /// The connection of its file.
    pub connection_id: Uuid,
    /// The operation of the write that raised it.
    pub operation_id: Uuid,
    /// The NFC path of the file.
    pub path: String,
    /// The origin of that write.
    pub origin: String,
    /// The media type of the file.
    pub media_type: String,
    /// The revision that the writer saw.
    pub base_hash: Option<String>,
    /// The revision of the vault when the conflict was raised.
    pub current_hash: Option<String>,
    /// The writer's content.
    pub writer_hash: Option<String>,
    /// The hash of the merged bytes of the merge proposal.
    pub merged_hash: Option<String>,
    /// Whether the merge proposal has overlapping hunks.
    pub has_overlapping_hunks: Option<bool>,
    /// `open` or `resolved`.
    pub state: String,
}

/// The columns of a [`WriteRow`], in its order.
const WRITE_COLUMNS: &str = "write_id, operation_id, origin, kind, path, path_key, target_path,
     base_hash, content_hash, content_length, state, revision, error_code, conflict_id, resolves,
     choice";

/// The row of a [`WriteRow`].
type WriteTuple = (
    Uuid,
    Uuid,
    String,
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i64>,
    String,
    Option<String>,
    Option<String>,
    Option<Uuid>,
    Option<Uuid>,
    Option<String>,
);

fn write_row(
    (
        write_id,
        operation_id,
        origin,
        kind,
        path,
        path_key,
        target_path,
        base_hash,
        content_hash,
        content_length,
        state,
        revision,
        error_code,
        conflict_id,
        resolves,
        choice,
    ): WriteTuple,
) -> WriteRow {
    WriteRow {
        write_id,
        operation_id,
        origin,
        kind,
        path,
        path_key,
        target_path,
        base_hash,
        content_hash,
        // A CHECK keeps the length at 0 or more.
        content_length: content_length.map(|length| u64::try_from(length).unwrap_or_default()),
        state,
        revision,
        error_code,
        conflict_id,
        resolves,
        choice,
    }
}

impl FileTransaction {
    /// Record `write` as `queued` at the end of the queue of its file.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn insert_write(&mut self, write: &NewWrite<'_>) -> Result<(), PersistenceError> {
        sqlx::query(
            "insert into notes.writes
                 (write_id, connection_id, operation_id, idempotency_key, request_digest, origin,
                  kind, path, path_key, target_path, base_hash, content_hash, content_length,
                  resolves, choice, state, created_at, updated_at)
             values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, 'queued',
                     now(), now())",
        )
        .bind(write.write_id)
        .bind(self.connection_id)
        .bind(write.operation_id)
        .bind(write.idempotency_key)
        .bind(write.request_digest)
        .bind(write.origin)
        .bind(write.kind)
        .bind(write.path)
        .bind(write.path_key)
        .bind(write.target_path)
        .bind(write.base_hash)
        .bind(write.content_hash)
        .bind(
            write
                .content_length
                .map(|length| i64::try_from(length).unwrap_or(i64::MAX)),
        )
        .bind(write.resolves.map(|(conflict_id, _)| conflict_id))
        .bind(write.resolves.map(|(_, choice)| choice))
        .execute(&mut *self.transaction)
        .await
        .map_err(query_error)?;
        self.attach_operation(write.operation_id, write.write_id)
            .await
    }

    /// The write of this connection with the idempotency key `key`, and the digest of its
    /// request.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn write_of_key(
        &mut self,
        key: &str,
    ) -> Result<Option<(Uuid, String)>, PersistenceError> {
        sqlx::query_as(
            "select write_id, request_digest from notes.writes
             where connection_id = $1 and idempotency_key = $2",
        )
        .bind(self.connection_id)
        .bind(key)
        .fetch_optional(&mut *self.transaction)
        .await
        .map_err(query_error)
    }

    /// Let the operation `operation_id` report the outcome of the write `write_id`. An operation
    /// that already waits for it changes nothing.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn attach_operation(
        &mut self,
        operation_id: Uuid,
        write_id: Uuid,
    ) -> Result<(), PersistenceError> {
        sqlx::query(
            "insert into notes.write_operations (operation_id, write_id, created_at)
             values ($1, $2, now())
             on conflict (operation_id) do nothing",
        )
        .bind(operation_id)
        .bind(write_id)
        .execute(&mut *self.transaction)
        .await
        .map_err(query_error)?;
        Ok(())
    }

    /// The head of the queue of the path key `path_key`: its oldest open write.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn queue_head(
        &mut self,
        path_key: &str,
    ) -> Result<Option<WriteRow>, PersistenceError> {
        let row: Option<WriteTuple> = sqlx::query_as(&format!(
            "select {WRITE_COLUMNS} from notes.writes
             where connection_id = $1 and path_key = $2 and state in ('queued', 'delivering')
             order by position limit 1"
        ))
        .bind(self.connection_id)
        .bind(path_key)
        .fetch_optional(&mut *self.transaction)
        .await
        .map_err(query_error)?;
        Ok(row.map(write_row))
    }

    /// The write `write_id` of this connection.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn write(&mut self, write_id: Uuid) -> Result<Option<WriteRow>, PersistenceError> {
        let row: Option<WriteTuple> = sqlx::query_as(&format!(
            "select {WRITE_COLUMNS} from notes.writes
             where connection_id = $1 and write_id = $2"
        ))
        .bind(self.connection_id)
        .bind(write_id)
        .fetch_optional(&mut *self.transaction)
        .await
        .map_err(query_error)?;
        Ok(row.map(write_row))
    }

    /// Mark the write `write_id` `delivering`.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn deliver_write(&mut self, write_id: Uuid) -> Result<(), PersistenceError> {
        self.set_write_state(write_id, "delivering", None, None)
            .await
    }

    /// End the write `write_id` as `succeeded` with the file at `revision`, or no file after a
    /// delete.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn succeed_write(
        &mut self,
        write_id: Uuid,
        revision: Option<&str>,
    ) -> Result<(), PersistenceError> {
        self.set_write_state(write_id, "succeeded", revision, None)
            .await
    }

    /// End the write `write_id` as `failed` with the error code `code`.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn fail_write(&mut self, write_id: Uuid, code: &str) -> Result<(), PersistenceError> {
        self.set_write_state(write_id, "failed", None, Some(code))
            .await
    }

    /// Record `conflict` as open and not announced, and end its write as `failed` with
    /// `notes.write.conflict`.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when a statement fails.
    pub async fn raise_conflict(
        &mut self,
        conflict: &NewConflict<'_>,
    ) -> Result<(), PersistenceError> {
        let write = conflict.write;
        sqlx::query(
            "insert into notes.conflicts
                 (conflict_id, connection_id, write_id, operation_id, path, path_key, origin,
                  media_type, base_hash, current_hash, writer_hash, announced, state,
                  created_at)
             values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, false, 'open', now())",
        )
        .bind(conflict.conflict_id)
        .bind(self.connection_id)
        .bind(write.write_id)
        .bind(write.operation_id)
        .bind(&write.path)
        .bind(&write.path_key)
        .bind(&write.origin)
        .bind(conflict.media_type)
        .bind(conflict.base_hash)
        .bind(conflict.current_hash)
        .bind(conflict.writer_hash)
        .execute(&mut *self.transaction)
        .await
        .map_err(query_error)?;
        self.set_write_state(write.write_id, "failed", None, Some("notes.write.conflict"))
            .await?;
        sqlx::query(
            "update notes.writes set conflict_id = $3
              where connection_id = $1 and write_id = $2",
        )
        .bind(self.connection_id)
        .bind(write.write_id)
        .bind(conflict.conflict_id)
        .execute(&mut *self.transaction)
        .await
        .map_err(query_error)?;
        Ok(())
    }

    /// Store the merge `proposal` of the conflict `conflict_id` of this connection and mark it
    /// announced. Returns `false` and changes nothing when it is announced already or no longer
    /// open.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn announce_conflict(
        &mut self,
        conflict_id: Uuid,
        proposal: Option<(&str, bool)>,
    ) -> Result<bool, PersistenceError> {
        let announced = sqlx::query(
            "update notes.conflicts
                set announced = true, merged_hash = $3, has_overlapping_hunks = $4
              where connection_id = $1 and conflict_id = $2 and not announced
                and state = 'open'",
        )
        .bind(self.connection_id)
        .bind(conflict_id)
        .bind(proposal.map(|(merged_hash, _)| merged_hash))
        .bind(proposal.map(|(_, overlapping)| overlapping))
        .execute(&mut *self.transaction)
        .await
        .map_err(query_error)?;
        Ok(announced.rows_affected() == 1)
    }

    /// The open conflict `conflict_id` of this connection.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn open_conflict(
        &mut self,
        conflict_id: Uuid,
    ) -> Result<Option<OpenConflict>, PersistenceError> {
        type Row = (
            String,
            String,
            Option<String>,
            Option<String>,
            Option<String>,
        );
        let row: Option<Row> = sqlx::query_as(
            "select path, path_key, current_hash, writer_hash, merged_hash
                 from notes.conflicts
                 where connection_id = $1 and conflict_id = $2 and state = 'open'",
        )
        .bind(self.connection_id)
        .bind(conflict_id)
        .fetch_optional(&mut *self.transaction)
        .await
        .map_err(query_error)?;
        Ok(row.map(
            |(path, path_key, current_hash, writer_hash, merged_hash)| OpenConflict {
                path,
                path_key,
                current_hash,
                writer_hash,
                merged_hash,
            },
        ))
    }

    /// Mark the open conflict `conflict_id` of this connection `resolved`, and announced: a
    /// conflict resolved before its announcement gets no `notes.conflict.raised.v1`.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn resolve_conflict(&mut self, conflict_id: Uuid) -> Result<(), PersistenceError> {
        sqlx::query(
            "update notes.conflicts
                set state = 'resolved', resolved_at = now(), announced = true
              where connection_id = $1 and conflict_id = $2 and state = 'open'",
        )
        .bind(self.connection_id)
        .bind(conflict_id)
        .execute(&mut *self.transaction)
        .await
        .map_err(query_error)?;
        Ok(())
    }

    /// The operations that report the outcome of the write `write_id`, the first one first.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn operations_of(&mut self, write_id: Uuid) -> Result<Vec<Uuid>, PersistenceError> {
        sqlx::query_scalar(
            "select operation_id from notes.write_operations
             where write_id = $1
             order by created_at, operation_id",
        )
        .bind(write_id)
        .fetch_all(&mut *self.transaction)
        .await
        .map_err(query_error)
    }

    async fn set_write_state(
        &mut self,
        write_id: Uuid,
        state: &str,
        revision: Option<&str>,
        error_code: Option<&str>,
    ) -> Result<(), PersistenceError> {
        sqlx::query(
            "update notes.writes
                set state = $3, revision = $4, error_code = $5, updated_at = now()
              where connection_id = $1 and write_id = $2",
        )
        .bind(self.connection_id)
        .bind(write_id)
        .bind(state)
        .bind(revision)
        .bind(error_code)
        .execute(&mut *self.transaction)
        .await
        .map_err(query_error)?;
        Ok(())
    }
}

impl Database {
    /// The writes of `connection_id` that are `delivering`, in the order of acceptance.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn delivering_writes(
        &self,
        connection_id: Uuid,
    ) -> Result<Vec<WriteRow>, PersistenceError> {
        let rows: Vec<WriteTuple> = sqlx::query_as(&format!(
            "select {WRITE_COLUMNS} from notes.writes
             where connection_id = $1 and state = 'delivering'
             order by position"
        ))
        .bind(connection_id)
        .fetch_all(self.pool())
        .await
        .map_err(query_error)?;
        Ok(rows.into_iter().map(write_row).collect())
    }

    /// The write that the operation `operation_id` reports: the write it asked for, or the
    /// earlier write whose idempotency key it repeated.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn write_of_operation(
        &self,
        operation_id: Uuid,
    ) -> Result<Option<WriteRow>, PersistenceError> {
        let row: Option<WriteTuple> = sqlx::query_as(&format!(
            "select {WRITE_COLUMNS} from notes.writes
             where write_id = (select write_id from notes.write_operations
                               where operation_id = $1)"
        ))
        .bind(operation_id)
        .fetch_optional(self.pool())
        .await
        .map_err(query_error)?;
        Ok(row.map(write_row))
    }

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
        );
        let row: Option<Row> = sqlx::query_as(
            "select n.owner_id, c.conflict_id, c.connection_id, c.operation_id, c.path, c.origin,
                    c.media_type, c.base_hash, c.current_hash, c.writer_hash, c.merged_hash,
                    c.has_overlapping_hunks, c.state
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
