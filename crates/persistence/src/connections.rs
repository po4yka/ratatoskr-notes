//! Vault connections: create one with its encrypted secrets, refuse a second one for the same
//! owner and vault key, record a finished or a failed login, and remove one.

use uuid::Uuid;

use crate::{Database, PersistenceError};

/// A connection to create.
#[derive(Debug, Clone, Copy)]
pub struct NewConnection<'a> {
    /// The id that the service minted.
    pub connection_id: Uuid,
    /// The owner from the claim that Edge mints.
    pub owner_id: Uuid,
    /// The wire word of the channel: `sync`, `plugin` or `cli`.
    pub channel: &'a str,
    /// The key that identifies the vault for this owner.
    pub vault_key: &'a str,
    /// The display name of the vault.
    pub vault_display_name: &'a str,
    /// The encrypted secrets of a `sync` connection; empty for the other channels.
    pub secrets: &'a [NewSecret<'a>],
    /// The connect operation that Edge minted.
    pub operation_id: Uuid,
    /// The device of a `plugin` or `cli` connection.
    pub device_id: Option<Uuid>,
}

/// One encrypted secret of a connection. The service encrypts; this crate never sees plaintext.
#[derive(Debug, Clone, Copy)]
pub struct NewSecret<'a> {
    /// `account_password` or `e2e_password`.
    pub kind: &'a str,
    /// The 24-byte `XChaCha20` nonce.
    pub nonce: &'a [u8],
    /// The ciphertext with its Poly1305 tag.
    pub ciphertext: &'a [u8],
}

/// The states in which a connection holds its vault key. A `failed` or `removed` connection does
/// not, so the owner can connect the vault again. The text matches the partial unique index.
const HOLDS_VAULT_KEY: &str = "state not in ('failed', 'removed')";

/// What [`Database::create_connection`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateOutcome {
    /// The connection exists now, in the state `connecting`.
    Created,
    /// The owner already has a connection for this vault key that is not removed.
    VaultAlreadyConnected {
        /// The existing connection.
        connection_id: Uuid,
        /// The wire word of its channel.
        channel: String,
        /// The device of a `plugin` or `cli` connection.
        device_id: Option<Uuid>,
    },
}

impl Database {
    /// Create a connection in the state `connecting` with its encrypted secrets, in one
    /// transaction, unless the owner already has one for the same vault key that is not removed
    /// and did not fail.
    ///
    /// The partial unique index decides, so two concurrent requests cannot both create one.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when a statement fails; [`PersistenceError::Contended`] when
    /// concurrent writers keep the connection from being created or found.
    pub async fn create_connection(
        &self,
        new: &NewConnection<'_>,
    ) -> Result<CreateOutcome, PersistenceError> {
        // Two passes at most: the existing connection can be removed between the refused insert
        // and the read, and the second insert then succeeds.
        for _ in 0..2 {
            let mut transaction = self.pool.begin().await.map_err(PersistenceError::Query)?;
            let inserted = sqlx::query(&format!(
                "insert into notes.connections
                     (connection_id, owner_id, channel, vault_key, vault_display_name, state,
                      operation_id, device_id, created_at, updated_at)
                 values ($1, $2, $3, $4, $5, 'connecting', $6, $7, now(), now())
                 on conflict (owner_id, vault_key) where {HOLDS_VAULT_KEY} do nothing"
            ))
            .bind(new.connection_id)
            .bind(new.owner_id)
            .bind(new.channel)
            .bind(new.vault_key)
            .bind(new.vault_display_name)
            .bind(new.operation_id)
            .bind(new.device_id)
            .execute(&mut *transaction)
            .await
            .map_err(PersistenceError::Query)?;
            if inserted.rows_affected() == 1 {
                for secret in new.secrets {
                    sqlx::query(
                        "insert into notes.connection_secrets
                             (connection_id, secret_kind, nonce, ciphertext, created_at)
                         values ($1, $2, $3, $4, now())",
                    )
                    .bind(new.connection_id)
                    .bind(secret.kind)
                    .bind(secret.nonce)
                    .bind(secret.ciphertext)
                    .execute(&mut *transaction)
                    .await
                    .map_err(PersistenceError::Query)?;
                }
                transaction
                    .commit()
                    .await
                    .map_err(PersistenceError::Query)?;
                return Ok(CreateOutcome::Created);
            }

            let existing: Option<(Uuid, String, Option<Uuid>)> = sqlx::query_as(&format!(
                "select connection_id, channel, device_id from notes.connections
                 where owner_id = $1 and vault_key = $2 and {HOLDS_VAULT_KEY}"
            ))
            .bind(new.owner_id)
            .bind(new.vault_key)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(PersistenceError::Query)?;
            transaction
                .rollback()
                .await
                .map_err(PersistenceError::Query)?;
            if let Some((connection_id, channel, device_id)) = existing {
                return Ok(CreateOutcome::VaultAlreadyConnected {
                    connection_id,
                    channel,
                    device_id,
                });
            }
        }
        Err(PersistenceError::Contended)
    }
}

/// A connection of one owner that is not removed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionRecord {
    /// The connection.
    pub connection_id: Uuid,
    /// The wire word of its channel.
    pub channel: String,
    /// The display name of the vault.
    pub vault_display_name: String,
    /// `connecting`, `active` or `login_expired`.
    pub state: String,
    /// Whether the first login of a `sync` connection finished.
    pub logged_in: bool,
    /// The connect operation.
    pub operation_id: Option<Uuid>,
}

/// The connect operation of one connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectOperation {
    /// The connection.
    pub connection_id: Uuid,
    /// Its owner.
    pub owner_id: Uuid,
    /// The operation that Edge minted, when the connection has one.
    pub operation_id: Option<Uuid>,
    /// The wire word of its channel.
    pub channel: String,
    /// The display name of its vault.
    pub vault_display_name: String,
    /// Its state.
    pub state: String,
}

/// One event for the outbox.
#[derive(Debug, Clone, Copy)]
pub struct OutboxEvent<'a> {
    /// The envelope's event id.
    pub event_id: Uuid,
    /// The envelope's event type, for example `notes.vault.disconnected.v1`.
    pub event_type: &'a str,
    /// The kind of the aggregate, for example `notes_connection`.
    pub aggregate_type: &'a str,
    /// The envelope's aggregate reference, for example `notes_connection:<uuid>`.
    pub aggregate_id: &'a str,
    /// The whole envelope.
    pub envelope: &'a serde_json::Value,
}

impl Database {
    /// The connection with this id if this owner holds it, and it is not removed and did not fail.
    ///
    /// A connection of another owner reads as absent, so a caller cannot tell it from one that
    /// does not exist.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn connection_of_owner(
        &self,
        owner_id: Uuid,
        connection_id: Uuid,
    ) -> Result<Option<ConnectionRecord>, PersistenceError> {
        let row: Option<(Uuid, String, String, String, bool, Option<Uuid>)> =
            sqlx::query_as(&format!(
                "select connection_id, channel, vault_display_name, state,
                        logged_in_at is not null, operation_id
                 from notes.connections
                 where connection_id = $1 and owner_id = $2 and {HOLDS_VAULT_KEY}"
            ))
            .bind(connection_id)
            .bind(owner_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(PersistenceError::Query)?;
        Ok(row.map(
            |(connection_id, channel, vault_display_name, state, logged_in, operation_id)| {
                ConnectionRecord {
                    connection_id,
                    channel,
                    vault_display_name,
                    state,
                    logged_in,
                    operation_id,
                }
            },
        ))
    }

    /// Remove a connection in one transaction: mark it `removed`, delete its stored ciphertext,
    /// and write `event` to the outbox.
    ///
    /// Returns `false` and writes nothing when the connection is already removed or is not this
    /// owner's, so two concurrent removals write one event.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when a statement fails; nothing is committed then.
    pub async fn remove_connection(
        &self,
        owner_id: Uuid,
        connection_id: Uuid,
        event: &OutboxEvent<'_>,
    ) -> Result<bool, PersistenceError> {
        let mut transaction = self.pool.begin().await.map_err(PersistenceError::Query)?;
        let removed = sqlx::query(&format!(
            "update notes.connections set state = 'removed', updated_at = now()
             where connection_id = $1 and owner_id = $2 and {HOLDS_VAULT_KEY}"
        ))
        .bind(connection_id)
        .bind(owner_id)
        .execute(&mut *transaction)
        .await
        .map_err(PersistenceError::Query)?;
        if removed.rows_affected() == 0 {
            return Ok(false);
        }
        sqlx::query("delete from notes.connection_secrets where connection_id = $1")
            .bind(connection_id)
            .execute(&mut *transaction)
            .await
            .map_err(PersistenceError::Query)?;
        insert_outbox(&mut transaction, event).await?;
        transaction
            .commit()
            .await
            .map_err(PersistenceError::Query)?;
        Ok(true)
    }
}

/// Write `event` to the outbox in `transaction`.
pub(crate) async fn insert_outbox(
    transaction: &mut sqlx::Transaction<'static, sqlx::Postgres>,
    event: &OutboxEvent<'_>,
) -> Result<(), PersistenceError> {
    sqlx::query(
        "insert into notes.outbox
             (event_id, event_type, aggregate_type, aggregate_id, payload, created_at)
         values ($1, $2, $3, $4, $5, now())",
    )
    .bind(event.event_id)
    .bind(event.event_type)
    .bind(event.aggregate_type)
    .bind(event.aggregate_id)
    .bind(event.envelope)
    .execute(&mut **transaction)
    .await
    .map_err(PersistenceError::Query)?;
    Ok(())
}

impl Database {
    /// Mark a `connecting` connection `failed` after its first login failed or a stop of the
    /// service interrupted it, and delete its stored ciphertext, in one transaction. The vault key
    /// is then free again.
    ///
    /// Returns `false` and changes nothing when the connection is not `connecting`.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when a statement fails; nothing is committed then.
    pub async fn fail_connection(&self, connection_id: Uuid) -> Result<bool, PersistenceError> {
        let mut transaction = self.pool.begin().await.map_err(PersistenceError::Query)?;
        let failed = sqlx::query(
            "update notes.connections set state = 'failed', updated_at = now()
             where connection_id = $1 and state = 'connecting'",
        )
        .bind(connection_id)
        .execute(&mut *transaction)
        .await
        .map_err(PersistenceError::Query)?;
        if failed.rows_affected() == 0 {
            return Ok(false);
        }
        sqlx::query("delete from notes.connection_secrets where connection_id = $1")
            .bind(connection_id)
            .execute(&mut *transaction)
            .await
            .map_err(PersistenceError::Query)?;
        transaction
            .commit()
            .await
            .map_err(PersistenceError::Query)?;
        Ok(true)
    }
}

impl Database {
    /// Whether the connection is `connecting`. A connect reads it before the login starts, so
    /// that a connection that the owner removed meanwhile gets no login.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn is_connecting(&self, connection_id: Uuid) -> Result<bool, PersistenceError> {
        sqlx::query_scalar(
            "select exists (select 1 from notes.connections
                            where connection_id = $1 and state = 'connecting')",
        )
        .bind(connection_id)
        .fetch_one(&self.pool)
        .await
        .map_err(PersistenceError::Query)
    }

    /// Record that the first login of a connection succeeded. The connection stays `connecting`
    /// until its first scan, which can end before this record: the record also takes an `active`
    /// connection.
    ///
    /// Returns `false` and changes nothing when the connection is neither `connecting` nor
    /// `active`, for example because the owner removed it while the login ran.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn record_login(&self, connection_id: Uuid) -> Result<bool, PersistenceError> {
        let recorded = sqlx::query(
            "update notes.connections set logged_in_at = now(), updated_at = now()
             where connection_id = $1 and state in ('connecting', 'active')",
        )
        .bind(connection_id)
        .execute(&self.pool)
        .await
        .map_err(PersistenceError::Query)?;
        Ok(recorded.rows_affected() == 1)
    }

    /// The `sync` connections that are `connecting` and whose first login did not finish. When no
    /// connect runs, as at the start of the service, a stop of the service interrupted each one.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn interrupted_logins(&self) -> Result<Vec<ConnectOperation>, PersistenceError> {
        self.connect_operations(
            "channel = 'sync' and state = 'connecting' and logged_in_at is null",
            None,
        )
        .await
    }

    /// The connect operation of a connection, with its owner, channel, display name and state.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn connect_operation(
        &self,
        connection_id: Uuid,
    ) -> Result<Option<ConnectOperation>, PersistenceError> {
        Ok(self
            .connect_operations("connection_id = $1", Some(connection_id))
            .await?
            .into_iter()
            .next())
    }

    async fn connect_operations(
        &self,
        filter: &str,
        connection_id: Option<Uuid>,
    ) -> Result<Vec<ConnectOperation>, PersistenceError> {
        let query = format!(
            "select connection_id, owner_id, operation_id, channel, vault_display_name, state
             from notes.connections where {filter}
             order by created_at"
        );
        let mut query =
            sqlx::query_as::<_, (Uuid, Uuid, Option<Uuid>, String, String, String)>(&query);
        if let Some(connection_id) = connection_id {
            query = query.bind(connection_id);
        }
        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(PersistenceError::Query)?;
        Ok(rows
            .into_iter()
            .map(
                |(connection_id, owner_id, operation_id, channel, vault_display_name, state)| {
                    ConnectOperation {
                        connection_id,
                        owner_id,
                        operation_id,
                        channel,
                        vault_display_name,
                        state,
                    }
                },
            )
            .collect())
    }

    /// Write one event to the outbox.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn insert_event(&self, event: &OutboxEvent<'_>) -> Result<(), PersistenceError> {
        let mut transaction = self.pool.begin().await.map_err(PersistenceError::Query)?;
        insert_outbox(&mut transaction, event).await?;
        transaction.commit().await.map_err(PersistenceError::Query)
    }

    /// Make a `connecting` connection `active` and write `events` to the outbox, in one
    /// transaction. Returns `false` and changes nothing when the connection is not `connecting`.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when a statement fails; nothing is committed then.
    pub async fn activate_connection(
        &self,
        connection_id: Uuid,
        events: &[OutboxEvent<'_>],
    ) -> Result<bool, PersistenceError> {
        let mut transaction = self.pool.begin().await.map_err(PersistenceError::Query)?;
        let activated = sqlx::query(
            "update notes.connections set state = 'active', updated_at = now()
             where connection_id = $1 and state = 'connecting'",
        )
        .bind(connection_id)
        .execute(&mut *transaction)
        .await
        .map_err(PersistenceError::Query)?;
        if activated.rows_affected() == 0 {
            return Ok(false);
        }
        for event in events {
            insert_outbox(&mut transaction, event).await?;
        }
        transaction
            .commit()
            .await
            .map_err(PersistenceError::Query)?;
        Ok(true)
    }
}

impl Database {
    /// The `sync` connections whose sync unit must run: their first login finished, and they are
    /// `connecting` or `active`. The service starts the unit of each one at its start.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn sync_connections_to_run(&self) -> Result<Vec<Uuid>, PersistenceError> {
        sqlx::query_scalar(
            "select connection_id from notes.connections
             where channel = 'sync' and state in ('connecting', 'active')
               and logged_in_at is not null
             order by created_at",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(PersistenceError::Query)
    }
}

impl Database {
    /// Mark a `sync` connection `login_expired` when it is `connecting` or `active`: its Sync login
    /// no longer works, and the owner must connect again.
    ///
    /// Returns `false` and changes nothing when the connection is in another state, for example
    /// because the owner removed it.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn expire_login(&self, connection_id: Uuid) -> Result<bool, PersistenceError> {
        let expired = sqlx::query(
            "update notes.connections set state = 'login_expired', updated_at = now()
             where connection_id = $1 and channel = 'sync' and state in ('connecting', 'active')",
        )
        .bind(connection_id)
        .execute(&self.pool)
        .await
        .map_err(PersistenceError::Query)?;
        Ok(expired.rows_affected() == 1)
    }
}
