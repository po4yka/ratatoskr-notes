//! The event envelopes that Notes writes to its outbox.

use ratatoskr_event_envelope::{EnvelopeSchemaVersion, EventEnvelope, EventPayload, ProducerName};
use ratatoskr_identifiers::{
    BlobOwner, BlobRef, ContentDigest, DigestAlgorithm, DigestHex, EntityRef, EventId, Extensions,
    MediaType, NotesConnectionId, NotesFileId, TenantRef, WireTimestamp,
};
use ratatoskr_notes_contracts::{
    FileChangeKind, FileChanged, VaultChannel, VaultDisconnected, VaultDisplayName, VaultPath,
};
use uuid::Uuid;

/// The producer name on every envelope of this service, and the owner of every blob it names.
pub const PRODUCER: &str = "ratatoskr-notes";

/// The aggregate kind of `notes.file.changed.v1`.
pub const FILE_AGGREGATE: &str = "notes_file";

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

/// The aggregate reference of a file, `notes_file:<uuid>`.
#[must_use]
pub fn file_aggregate(file_id: Uuid) -> String {
    format!("{FILE_AGGREGATE}:{file_id}")
}

/// One file change, as the recorder knows it.
#[derive(Debug, Clone, Copy)]
pub struct FileChange<'a> {
    /// The owner of the connection.
    pub owner_id: Uuid,
    /// The connection.
    pub connection_id: Uuid,
    /// The file.
    pub file_id: Uuid,
    /// The position of the event among the events of its connection.
    pub connection_sequence: u64,
    /// What happened.
    pub kind: FileChangeKind,
    /// The NFC path after the change.
    pub path: &'a str,
    /// The lowercase hex SHA-256 of the bytes.
    pub revision: &'a str,
    /// The media type of the bytes.
    pub media_type: &'a str,
    /// The number of bytes, for every kind except `deleted`, which names no blob.
    pub blob_length: Option<u64>,
}

/// `notes.file.changed.v1` for one change. The owner is the envelope's `tenant_id`, and the event
/// correlates to itself. The payload passes [`FileChanged::validate`].
///
/// # Errors
///
/// [`EnvelopeBuildError`] when a value does not satisfy the contract.
pub fn file_changed(
    event_id: Uuid,
    change: &FileChange<'_>,
) -> Result<EventEnvelope, EnvelopeBuildError> {
    let revision = DigestHex::parse(change.revision).map_err(|_| EnvelopeBuildError("revision"))?;
    let media_type =
        MediaType::parse(change.media_type).map_err(|_| EnvelopeBuildError("media_type"))?;
    let blob = match change.blob_length {
        Some(length_bytes) => Some(BlobRef {
            owner_service: BlobOwner::parse(PRODUCER).map_err(|_| EnvelopeBuildError("blob"))?,
            digest: ContentDigest {
                algorithm: DigestAlgorithm::Sha256,
                hex: revision.clone(),
            },
            media_type: media_type.clone(),
            length_bytes,
        }),
        None => None,
    };
    let payload = FileChanged {
        connection_id: NotesConnectionId(change.connection_id),
        file_id: NotesFileId(change.file_id),
        connection_sequence: change.connection_sequence,
        change_kind: change.kind,
        path: VaultPath::parse(change.path).map_err(|_| EnvelopeBuildError("path"))?,
        previous_path: None,
        revision,
        media_type,
        blob,
    };
    payload
        .validate()
        .map_err(|_| EnvelopeBuildError("payload"))?;
    envelope(
        event_id,
        FileChanged::event_type(),
        &file_aggregate(change.file_id),
        change.owner_id,
        &payload,
    )
}

/// An envelope of this service with `payload`, owned by `owner_id`, that correlates to itself.
fn envelope<P: EventPayload + serde::Serialize>(
    event_id: Uuid,
    event_type: ratatoskr_event_envelope::EventType,
    aggregate_id: &str,
    owner_id: Uuid,
    payload: &P,
) -> Result<EventEnvelope, EnvelopeBuildError> {
    let mut envelope = EventEnvelope {
        event_id: EventId(event_id),
        event_type,
        occurred_at: WireTimestamp::now(),
        producer: ProducerName::parse(PRODUCER).map_err(|_| EnvelopeBuildError("producer"))?,
        aggregate_id: EntityRef::parse(aggregate_id)
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
        .set_payload(payload)
        .map_err(|_| EnvelopeBuildError("payload"))?;
    Ok(envelope)
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
    envelope(
        event_id,
        VaultDisconnected::event_type(),
        &connection_aggregate(connection_id),
        owner_id,
        &payload,
    )
}
