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
    /// The path key of `target_path`.
    pub target_path_key: Option<&'a str>,
    /// The revision that the writer saw.
    pub base_hash: Option<&'a str>,
    /// The SHA-256 of the uploaded bytes that the write puts at the path.
    pub content_hash: Option<&'a str>,
    /// The number of those bytes.
    pub content_length: Option<u64>,
    /// The conflict that a `resolve_conflict` ends, and its choice.
    pub resolves: Option<(Uuid, &'a str)>,
    /// The property of a `set_property`, and `set` or `remove`.
    pub property: Option<(&'a str, &'a str)>,
    /// The write group of a rename and its link rewrites: the `write_id` of the rename.
    pub group_id: Option<Uuid>,
}

/// One recorded write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteRow {
    /// The write.
    pub write_id: Uuid,
    /// Its connection.
    pub connection_id: Uuid,
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
    /// The path key of `target_path`.
    pub target_path_key: Option<String>,
    /// The property of a `set_property`.
    pub property_key: Option<String>,
    /// `set` or `remove` for a `set_property`.
    pub property_change: Option<String>,
    /// The order of acceptance in the connection.
    pub position: i64,
    /// Whether the write landed in the working tree of its `sync` connection.
    pub landed: bool,
    /// The number of the delivery of the write in its connection: the cursor of the queue of a
    /// device. `None` until the write is delivering.
    pub delivery_sequence: Option<i64>,
    /// The write group of a rename and its link rewrites: the `write_id` of the rename.
    pub group_id: Option<Uuid>,
    /// The SHA-256 of the file that the channel reported with a base mismatch.
    pub local_hash: Option<String>,
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
    /// Every stale or unapplied file of the write group that the conflict ends, sorted.
    pub stale_paths: Option<Vec<String>>,
}

/// The columns of a [`WriteRow`].
pub(crate) const WRITE_COLUMNS: &str =
    "write_id, connection_id, operation_id, origin, kind, path, path_key, target_path,
     base_hash, content_hash, content_length, state, revision, error_code, conflict_id, resolves,
     choice, target_path_key, property_key, property_change, position, delivery_sequence, landed,
     group_id, local_hash";

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for WriteRow {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        use sqlx::Row as _;
        let content_length: Option<i64> = row.try_get("content_length")?;
        Ok(Self {
            write_id: row.try_get("write_id")?,
            connection_id: row.try_get("connection_id")?,
            operation_id: row.try_get("operation_id")?,
            origin: row.try_get("origin")?,
            kind: row.try_get("kind")?,
            path: row.try_get("path")?,
            path_key: row.try_get("path_key")?,
            target_path: row.try_get("target_path")?,
            base_hash: row.try_get("base_hash")?,
            content_hash: row.try_get("content_hash")?,
            // A CHECK keeps the length at 0 or more.
            content_length: content_length.map(|length| u64::try_from(length).unwrap_or_default()),
            state: row.try_get("state")?,
            revision: row.try_get("revision")?,
            error_code: row.try_get("error_code")?,
            conflict_id: row.try_get("conflict_id")?,
            resolves: row.try_get("resolves")?,
            choice: row.try_get("choice")?,
            target_path_key: row.try_get("target_path_key")?,
            property_key: row.try_get("property_key")?,
            property_change: row.try_get("property_change")?,
            position: row.try_get("position")?,
            landed: row.try_get("landed")?,
            delivery_sequence: row.try_get("delivery_sequence")?,
            group_id: row.try_get("group_id")?,
            local_hash: row.try_get("local_hash")?,
        })
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
                  resolves, choice, target_path_key, property_key, property_change, group_id,
                  state, created_at, updated_at)
             values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,
                     $18, $19, 'queued', now(), now())",
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
        .bind(write.target_path_key)
        .bind(write.property.map(|(key, _)| key))
        .bind(write.property.map(|(_, change)| change))
        .bind(write.group_id)
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

    /// The head of the queue of the path key `path_key`: its oldest open write, of that path key or
    /// a `rename` or `move` to it.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn queue_head(
        &mut self,
        path_key: &str,
    ) -> Result<Option<WriteRow>, PersistenceError> {
        let row: Option<WriteRow> = sqlx::query_as(&format!(
            "select {WRITE_COLUMNS} from notes.writes
             where connection_id = $1 and (path_key = $2 or target_path_key = $2)
               and state in ('queued', 'delivering')
             order by position limit 1"
        ))
        .bind(self.connection_id)
        .bind(path_key)
        .fetch_optional(&mut *self.transaction)
        .await
        .map_err(query_error)?;
        Ok(row)
    }

    /// The write `write_id` of this connection.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn write(&mut self, write_id: Uuid) -> Result<Option<WriteRow>, PersistenceError> {
        let row: Option<WriteRow> = sqlx::query_as(&format!(
            "select {WRITE_COLUMNS} from notes.writes
             where connection_id = $1 and write_id = $2"
        ))
        .bind(self.connection_id)
        .bind(write_id)
        .fetch_optional(&mut *self.transaction)
        .await
        .map_err(query_error)?;
        Ok(row)
    }

    /// Mark the write `write_id` `delivering`, with the next delivery sequence of its connection.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn deliver_write(&mut self, write_id: Uuid) -> Result<(), PersistenceError> {
        // The lock of the connection row orders the deliveries of one connection, so the numbers
        // grow in the order in which the deliveries commit.
        sqlx::query(
            "with next as (
                 update notes.connections set last_delivery = last_delivery + 1
                 where connection_id = $1
                 returning last_delivery)
             update notes.writes
                set state = 'delivering', delivery_sequence = (select last_delivery from next),
                    updated_at = now()
              where connection_id = $1 and write_id = $2",
        )
        .bind(self.connection_id)
        .bind(write_id)
        .execute(&mut *self.transaction)
        .await
        .map_err(query_error)?;
        Ok(())
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

    /// Set the write mode of this connection: `on` when `on`, `off` otherwise.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn set_write_mode(&mut self, on: bool) -> Result<(), PersistenceError> {
        sqlx::query(
            "update notes.connections set write_mode = $2, updated_at = now()
             where connection_id = $1",
        )
        .bind(self.connection_id)
        .bind(if on { "on" } else { "off" })
        .execute(&mut *self.transaction)
        .await
        .map_err(query_error)?;
        Ok(())
    }

    /// The delivering writes of this connection that did not land in its working tree, in the order
    /// of acceptance: on the `sync` channel these are not delivered to the vault yet.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn unlanded_writes(&mut self) -> Result<Vec<WriteRow>, PersistenceError> {
        sqlx::query_as(&format!(
            "select {WRITE_COLUMNS} from notes.writes
             where connection_id = $1 and state = 'delivering' and not landed
             order by position"
        ))
        .bind(self.connection_id)
        .fetch_all(&mut *self.transaction)
        .await
        .map_err(query_error)
    }

    /// The writes of this connection that wait in a queue and were not delivered, in the order of
    /// acceptance.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn queued_writes(&mut self) -> Result<Vec<WriteRow>, PersistenceError> {
        let rows: Vec<WriteRow> = sqlx::query_as(&format!(
            "select {WRITE_COLUMNS} from notes.writes
             where connection_id = $1 and state = 'queued'
             order by position"
        ))
        .bind(self.connection_id)
        .fetch_all(&mut *self.transaction)
        .await
        .map_err(query_error)?;
        Ok(rows)
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
        let rows: Vec<WriteRow> = sqlx::query_as(&format!(
            "select {WRITE_COLUMNS} from notes.writes
             where connection_id = $1 and state = 'delivering'
             order by position"
        ))
        .bind(connection_id)
        .fetch_all(self.pool())
        .await
        .map_err(query_error)?;
        Ok(rows)
    }

    /// Mark the delivering write `write_id` of `connection_id` landed or not. Returns `false` when
    /// the write is no longer delivering, for example because write mode was turned off.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn set_landed(
        &self,
        connection_id: Uuid,
        write_id: Uuid,
        landed: bool,
    ) -> Result<bool, PersistenceError> {
        let marked = sqlx::query(
            "update notes.writes set landed = $3, updated_at = now()
             where connection_id = $1 and write_id = $2 and state = 'delivering'",
        )
        .bind(connection_id)
        .bind(write_id)
        .bind(landed)
        .execute(self.pool())
        .await
        .map_err(query_error)?;
        Ok(marked.rows_affected() == 1)
    }

    /// The `sync` connections that are `connecting` or `active` and have a delivering write.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn sync_connections_delivering(&self) -> Result<Vec<Uuid>, PersistenceError> {
        sqlx::query_scalar(
            "select c.connection_id from notes.connections c
             where c.channel = 'sync' and c.state in ('connecting', 'active')
               and exists (select 1 from notes.writes w
                           where w.connection_id = c.connection_id and w.state = 'delivering')
             order by c.created_at",
        )
        .fetch_all(self.pool())
        .await
        .map_err(query_error)
    }

    /// The delivering writes of `connection_id` after the delivery sequence `after`, at most
    /// `limit`, in the order of delivery: the queue that the device of a `plugin` or `cli`
    /// connection reads. A link rewrite of a write group is not in it: the device applies the
    /// rename, and Obsidian rewrites the links. The rename of a group stays in it while any write
    /// of the group is delivering, so that a device sends its result again.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn device_queue(
        &self,
        connection_id: Uuid,
        after: i64,
        limit: i64,
    ) -> Result<Vec<WriteRow>, PersistenceError> {
        sqlx::query_as(&format!(
            // Both branches read only delivering writes, through `writes_delivering_idx`; the
            // delivered history is never scanned.
            "select {WRITE_COLUMNS} from notes.writes
             where connection_id = $1 and delivery_sequence > $2
               and write_id in (
                   select write_id from notes.writes
                    where connection_id = $1 and state = 'delivering'
                      and (group_id is null or group_id = write_id)
                   union
                   select group_id from notes.writes
                    where connection_id = $1 and state = 'delivering'
                      and group_id is not null and group_id <> write_id)
             order by delivery_sequence limit $3"
        ))
        .bind(connection_id)
        .bind(after)
        .bind(limit)
        .fetch_all(self.pool())
        .await
        .map_err(query_error)
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
        let row: Option<WriteRow> = sqlx::query_as(&format!(
            "select {WRITE_COLUMNS} from notes.writes
             where write_id = (select write_id from notes.write_operations
                               where operation_id = $1)"
        ))
        .bind(operation_id)
        .fetch_optional(self.pool())
        .await
        .map_err(query_error)?;
        Ok(row)
    }
}
