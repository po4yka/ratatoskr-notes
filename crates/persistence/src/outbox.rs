//! The reads and writes of the outbox relay.

use uuid::Uuid;

use crate::{Database, PersistenceError};

/// One event that the relay still owes the bus.
#[derive(Debug, Clone, PartialEq)]
pub struct PendingEvent {
    /// The envelope's event id, which the relay sends as `Nats-Msg-Id`.
    pub event_id: Uuid,
    /// The envelope's event type, which names its subject.
    pub event_type: String,
    /// The whole envelope.
    pub envelope: serde_json::Value,
}

impl Database {
    /// The oldest `limit` events that are not published, in the order of the outbox.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn pending_events(&self, limit: i64) -> Result<Vec<PendingEvent>, PersistenceError> {
        let rows: Vec<(Uuid, String, serde_json::Value)> = sqlx::query_as(
            "select event_id, event_type, payload from notes.outbox
             where published_at is null
             order by position
             limit $1",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(PersistenceError::Query)?;
        Ok(rows
            .into_iter()
            .map(|(event_id, event_type, envelope)| PendingEvent {
                event_id,
                event_type,
                envelope,
            })
            .collect())
    }

    /// Mark an event published after the bus acknowledged it.
    ///
    /// # Errors
    ///
    /// [`PersistenceError::Query`] when the statement fails.
    pub async fn mark_published(&self, event_id: Uuid) -> Result<(), PersistenceError> {
        sqlx::query(
            "update notes.outbox set published_at = now()
             where event_id = $1 and published_at is null",
        )
        .bind(event_id)
        .execute(&self.pool)
        .await
        .map_err(PersistenceError::Query)?;
        Ok(())
    }
}
