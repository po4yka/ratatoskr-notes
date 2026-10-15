//! One write command, read into the facts that notes records.

use ratatoskr_event_envelope::CommandEnvelope;
use ratatoskr_notes_contracts::{FileWriteRequested, PropertyChange, WriteOperation};
use ratatoskr_notes_index::paths::{nfc_path, path_key};
use ratatoskr_notes_persistence::NewWrite;
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

/// The facts of one write command.
#[derive(Debug, Clone)]
pub(super) struct Request {
    pub(super) owner_id: Uuid,
    pub(super) operation_id: Uuid,
    pub(super) idempotency_key: String,
    pub(super) digest: String,
    pub(super) connection_id: Uuid,
    pub(super) origin: String,
    pub(super) kind: &'static str,
    pub(super) path: String,
    pub(super) path_key: String,
    pub(super) target_path: Option<String>,
    pub(super) base_hash: Option<String>,
    pub(super) content_hash: Option<String>,
    pub(super) content_length: Option<u64>,
    /// The conflict that a `resolve_conflict` ends, and the wire word of its choice. Its path,
    /// its base and, except for `custom`, its content come from the conflict.
    pub(super) resolution: Option<(Uuid, &'static str)>,
}

impl Request {
    /// The facts of `command`, or `None` when it is not a valid write command of a user: another
    /// command type, a payload that `notes/write-request.v1` refuses, no `user:` tenant, or an
    /// aggregate that is not its operation. A `resolve_conflict` names a conflict and not a path:
    /// its path is empty here.
    pub(super) fn parse(command: &CommandEnvelope) -> Option<Self> {
        let payload: FileWriteRequested = command.payload_as().ok()?;
        let owner_id = command.tenant_id?.user_id().0;
        let operation_id = payload.operation_id.0;
        if command.aggregate_id.to_wire() != format!("operation:{operation_id}") {
            return None;
        }
        let mut resolution = None;
        let (kind, path, target_path, base_hash, content) = match &payload.operation {
            WriteOperation::Create(create) => (
                "create",
                create.path.as_str(),
                None,
                None,
                Some(&create.blob),
            ),
            WriteOperation::Update(update) => (
                "update",
                update.path.as_str(),
                None,
                Some(update.base_hash.as_str()),
                Some(&update.blob),
            ),
            WriteOperation::Rename(relocate) | WriteOperation::Move(relocate) => (
                payload.operation.kind(),
                relocate.path.as_str(),
                Some(relocate.target_path.as_str()),
                Some(relocate.base_hash.as_str()),
                None,
            ),
            WriteOperation::Delete(delete) => (
                "delete",
                delete.path.as_str(),
                None,
                Some(delete.base_hash.as_str()),
                None,
            ),
            WriteOperation::SetProperty(property) => (
                "set_property",
                property.path.as_str(),
                None,
                Some(property.base_hash.as_str()),
                match &property.property_change {
                    PropertyChange::Set(blob) => Some(blob),
                    _ => None,
                },
            ),
            WriteOperation::ResolveConflict(resolve) => {
                resolution = Some((resolve.conflict_id.0, resolve.choice.as_str()));
                ("resolve_conflict", "", None, None, resolve.blob.as_ref())
            }
            _ => return None,
        };
        let path = nfc_path(path);
        // The digest of what the writer asked for, to tell a retry from a reused key.
        let request = serde_json::to_vec(&(payload.origin.as_str(), &payload.operation)).ok()?;
        Some(Self {
            owner_id,
            idempotency_key: payload.idempotency_key.hex.as_str().to_owned(),
            digest: format!("{:x}", Sha256::digest(&request)),
            operation_id,
            connection_id: payload.connection_id.0,
            origin: payload.origin.as_str().to_owned(),
            kind,
            path_key: path_key(&path),
            path,
            target_path: target_path.map(nfc_path),
            base_hash: base_hash.map(str::to_owned),
            content_hash: content.map(|blob| blob.digest.hex.as_str().to_owned()),
            content_length: content.map(|blob| blob.length_bytes),
            resolution,
        })
    }

    /// The owner and the operation of `command` when it names them, for the report of a command
    /// that [`Self::parse`] refuses.
    pub(super) fn operation_of(command: &CommandEnvelope) -> Option<(Uuid, Uuid)> {
        let owner_id = command.tenant_id?.user_id().0;
        let operation_id = command
            .payload
            .get("operation_id")
            .and_then(serde_json::Value::as_str)
            .and_then(|operation_id| Uuid::parse_str(operation_id).ok())?;
        Some((owner_id, operation_id))
    }

    /// The row of this write.
    pub(super) fn new_write(&self, write_id: Uuid) -> NewWrite<'_> {
        NewWrite {
            write_id,
            operation_id: self.operation_id,
            idempotency_key: &self.idempotency_key,
            request_digest: &self.digest,
            origin: &self.origin,
            kind: self.kind,
            path: &self.path,
            path_key: &self.path_key,
            target_path: self.target_path.as_deref(),
            base_hash: self.base_hash.as_deref(),
            content_hash: self.content_hash.as_deref(),
            content_length: self.content_length,
            resolves: self.resolution,
        }
    }
}
