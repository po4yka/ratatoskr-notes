//! The event envelopes that Notes writes to its outbox.

use ratatoskr_event_envelope::{EnvelopeSchemaVersion, EventEnvelope, EventPayload, ProducerName};
use ratatoskr_identifiers::{
    EntityRef, EventId, Extensions, NotesConnectionId, TenantRef, WireTimestamp,
};
use ratatoskr_notes_contracts::{VaultChannel, VaultDisconnected, VaultDisplayName};
use uuid::Uuid;

/// The producer name on every envelope of this service.
const PRODUCER: &str = "ratatoskr-notes";

/// The aggregate kind of the two vault events.
pub const CONNECTION_AGGREGATE: &str = "notes_connection";

/// An envelope could not be built from the stored values. The message names the field, never a
/// value.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("the event envelope could not be built: {0}")]
pub struct EnvelopeBuildError(&'static str);

impl EnvelopeBuildError {
    /// The envelope could not be serialized for the outbox.
    pub const SERIALIZE: Self = Self("serialization");
}

/// The aggregate reference of a connection, `notes_connection:<uuid>`.
#[must_use]
pub fn connection_aggregate(connection_id: Uuid) -> String {
    format!("{CONNECTION_AGGREGATE}:{connection_id}")
}

/// `notes.vault.disconnected.v1` for one removed connection.
///
/// The owner is the envelope's `tenant_id`. No operation owns a disconnect, so the event
/// correlates to itself as `event:<event id>`.
///
/// # Errors
///
/// [`EnvelopeBuildError`] when a stored value does not satisfy the contract.
pub fn vault_disconnected(
    event_id: Uuid,
    owner_id: Uuid,
    connection_id: Uuid,
    channel: &str,
    vault_display_name: &str,
) -> Result<EventEnvelope, EnvelopeBuildError> {
    let channel: VaultChannel =
        serde_json::from_value(serde_json::Value::String(channel.to_owned()))
            .map_err(|_| EnvelopeBuildError("channel"))?;
    let payload = VaultDisconnected {
        connection_id: NotesConnectionId(connection_id),
        channel,
        vault_display_name: VaultDisplayName::parse(vault_display_name)
            .map_err(|_| EnvelopeBuildError("vault_display_name"))?,
    };
    let mut envelope = EventEnvelope {
        event_id: EventId(event_id),
        event_type: VaultDisconnected::event_type(),
        occurred_at: WireTimestamp::now(),
        producer: ProducerName::parse(PRODUCER).map_err(|_| EnvelopeBuildError("producer"))?,
        aggregate_id: EntityRef::parse(&connection_aggregate(connection_id))
            .map_err(|_| EnvelopeBuildError("aggregate_id"))?,
        correlation_id: EntityRef::parse(&format!("event:{event_id}"))
            .map_err(|_| EnvelopeBuildError("correlation_id"))?,
        causation_id: None,
        tenant_id: Some(
            TenantRef::parse(&format!("user:{owner_id}"))
                .map_err(|_| EnvelopeBuildError("tenant_id"))?,
        ),
        schema_version: EnvelopeSchemaVersion::CURRENT,
        payload: serde_json::Map::new(),
        extensions: Extensions::new(),
    };
    envelope
        .set_payload(&payload)
        .map_err(|_| EnvelopeBuildError("payload"))?;
    Ok(envelope)
}
