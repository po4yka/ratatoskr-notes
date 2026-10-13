//! Vault connections: create one, and refuse a second one for the same owner and vault key.

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
}

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
    },
}

impl Database {
    /// Create a connection in the state `connecting`, unless the owner already has one for the
    /// same vault key that is not removed.
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
            let inserted = sqlx::query(
                "insert into notes.connections
                     (connection_id, owner_id, channel, vault_key, vault_display_name, state,
                      created_at, updated_at)
                 values ($1, $2, $3, $4, $5, 'connecting', now(), now())
                 on conflict (owner_id, vault_key) where state <> 'removed' do nothing",
            )
            .bind(new.connection_id)
            .bind(new.owner_id)
            .bind(new.channel)
            .bind(new.vault_key)
            .bind(new.vault_display_name)
            .execute(&self.pool)
            .await
            .map_err(PersistenceError::Query)?;
            if inserted.rows_affected() == 1 {
                return Ok(CreateOutcome::Created);
            }

            let existing: Option<(Uuid, String)> = sqlx::query_as(
                "select connection_id, channel from notes.connections
                 where owner_id = $1 and vault_key = $2 and state <> 'removed'",
            )
            .bind(new.owner_id)
            .bind(new.vault_key)
            .fetch_optional(&self.pool)
            .await
            .map_err(PersistenceError::Query)?;
            if let Some((connection_id, channel)) = existing {
                return Ok(CreateOutcome::VaultAlreadyConnected {
                    connection_id,
                    channel,
                });
            }
        }
        Err(PersistenceError::Contended)
    }
}
