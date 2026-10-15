//! The write path helpers: an active `plugin` connection with write mode on, the commands that
//! Edge would publish, and a fake channel that sees the deliveries and acknowledges them.

use ratatoskr_event_envelope::{
    CommandEnvelope, CommandPayload as _, EnvelopeSchemaVersion, ProducerName,
};
use ratatoskr_identifiers::{CommandId, EntityRef, Extensions, TenantRef, WireTimestamp};
use ratatoskr_notes::files::FileRecorder;
use ratatoskr_notes::writes::{Delivery, Handled, Outcome, Refusal, Writes};
use ratatoskr_notes_contracts::FileWriteRequested;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::support::channel::{Channel, OWNER, entry, sha256};

/// A domain router, its blob store and the pending writes of one test database.
pub(crate) struct Writer {
    pub(crate) channel: Channel,
    pub(crate) writes: Writes,
}

/// One write command and the operation that Edge minted for it.
pub(crate) struct Command {
    pub(crate) operation_id: Uuid,
    pub(crate) envelope: CommandEnvelope,
}

impl Writer {
    pub(crate) async fn new() -> Self {
        let channel = Channel::new().await;
        let writes = Writes::new(FileRecorder::new(
            channel.fixture.database.clone(),
            channel.blobs.clone(),
        ));
        Self { channel, writes }
    }

    /// An active `plugin` connection of `owner` whose vault holds `files`, with write mode on.
    pub(crate) async fn vault_of(&self, owner: Uuid, files: &[(&str, &[u8])]) -> Uuid {
        let connection = self
            .channel
            .plugin_connection(owner, &format!("vault-{}", Uuid::now_v7().simple()))
            .await;
        for (_, bytes) in files {
            if !bytes.is_empty() {
                self.channel.hold(owner, connection, bytes).await;
            }
        }
        let entries: Vec<Value> = files
            .iter()
            .map(|(path, bytes)| entry(path, bytes))
            .collect();
        let applied = self
            .channel
            .device_call_as(owner, connection, "manifest", json!({ "entries": entries }))
            .await;
        assert_eq!(
            applied.body["needed_sha256"],
            json!([]),
            "{:?}",
            applied.body
        );
        self.set_write_mode(connection, "on").await;
        connection
    }

    /// [`Self::vault_of`] for [`OWNER`].
    pub(crate) async fn vault(&self, files: &[(&str, &[u8])]) -> Uuid {
        self.vault_of(OWNER, files).await
    }

    /// Set the write mode of a connection directly in its row.
    pub(crate) async fn set_write_mode(&self, connection_id: Uuid, mode: &str) {
        sqlx::query("update notes.connections set write_mode = $2 where connection_id = $1")
            .bind(connection_id)
            .bind(mode)
            .execute(self.channel.fixture.pool())
            .await
            .expect("the write mode is set");
    }

    /// Upload `bytes` for `connection_id` of [`OWNER`] and return their notes-owned `BlobRef`.
    pub(crate) async fn stage(&self, connection_id: Uuid, bytes: &[u8]) -> Value {
        self.stage_for(OWNER, connection_id, bytes, "text/markdown")
            .await
    }

    /// Upload `bytes` of `media_type` for `connection_id` of `owner`.
    pub(crate) async fn stage_for(
        &self,
        owner: Uuid,
        connection_id: Uuid,
        bytes: &[u8],
        media_type: &str,
    ) -> Value {
        self.channel.hold(owner, connection_id, bytes).await;
        blob_ref(bytes, media_type)
    }

    /// Hand `command` to the write path.
    pub(crate) async fn send(&self, command: &Command) -> Handled {
        self.writes
            .handle(&command.envelope)
            .await
            .expect("the command is handled")
    }

    /// The writes that the channel of `connection_id` delivers now.
    pub(crate) async fn deliveries(&self, connection_id: Uuid) -> Vec<Delivery> {
        self.writes
            .deliveries(connection_id)
            .await
            .expect("the deliveries can be read")
    }

    /// The channel acknowledges `delivery` with the hash of its content.
    pub(crate) async fn acknowledge(&self, connection_id: Uuid, delivery: &Delivery) {
        let revision = delivery.content_hash.clone().or(delivery.base_hash.clone());
        let acknowledged = self
            .writes
            .acknowledge(connection_id, delivery.write_id, revision.as_deref())
            .await
            .expect("the acknowledgement is recorded");
        assert!(acknowledged, "a delivered write is acknowledged");
    }

    /// The channel refuses `delivery`.
    pub(crate) async fn refuse(&self, connection_id: Uuid, delivery: &Delivery, refusal: Refusal) {
        let refused = self
            .writes
            .refuse(connection_id, delivery.write_id, refusal)
            .await
            .expect("the refusal is recorded");
        assert!(refused, "a delivered write is refused");
    }

    /// The outcome of the write of `command`.
    pub(crate) async fn outcome(&self, command: &Command) -> Option<Outcome> {
        self.writes
            .outcome(command.operation_id)
            .await
            .expect("the outcome can be read")
    }

    /// The current revision of the present file at `path` of `connection_id`.
    pub(crate) async fn current(&self, connection_id: Uuid, path: &str) -> Option<String> {
        sqlx::query_scalar(
            "select revision from notes.files
             where connection_id = $1 and path = $2 and state = 'present'",
        )
        .bind(connection_id)
        .bind(path)
        .fetch_optional(self.channel.fixture.pool())
        .await
        .expect("the files can be read")
    }

    /// The revisions that the `notes.file.changed.v1` events of `connection_id` name, in order.
    pub(crate) async fn changed(&self, connection_id: Uuid) -> Vec<String> {
        self.channel
            .changes(connection_id)
            .await
            .iter()
            .map(|change| change["revision"].as_str().unwrap_or_default().to_owned())
            .collect()
    }

    pub(crate) async fn cleanup(self) {
        self.channel.cleanup().await;
    }
}

/// The notes-owned `BlobRef` of `bytes`.
pub(crate) fn blob_ref(bytes: &[u8], media_type: &str) -> Value {
    json!({
        "owner_service": "ratatoskr-notes",
        "digest": { "algorithm": "sha256", "hex": sha256(bytes) },
        "media_type": media_type,
        "length_bytes": bytes.len(),
    })
}

/// The command of `operation` for `connection_id` of `owner`, from `origin`, with the
/// idempotency key `key`.
pub(crate) fn command_with(
    owner: Uuid,
    connection_id: Uuid,
    origin: &str,
    key: &str,
    operation: &Value,
) -> Command {
    let operation_id = Uuid::now_v7();
    let reference = format!("operation:{operation_id}");
    let payload = json!({
        "operation_id": operation_id,
        "idempotency_key": { "algorithm": "sha256", "hex": sha256(key.as_bytes()) },
        "connection_id": connection_id,
        "origin": origin,
        "operation": operation,
    });
    let payload = payload
        .as_object()
        .cloned()
        .expect("the payload is a JSON object");
    let envelope = CommandEnvelope {
        command_id: CommandId(Uuid::now_v7()),
        command_type: FileWriteRequested::command_type(),
        issued_at: WireTimestamp::now(),
        producer: ProducerName::parse("ratatoskr-platform").expect("a producer"),
        aggregate_id: EntityRef::parse(&reference).expect("an aggregate"),
        correlation_id: EntityRef::parse(&reference).expect("a correlation"),
        causation_id: None,
        tenant_id: Some(TenantRef::parse(&format!("user:{owner}")).expect("a tenant")),
        schema_version: EnvelopeSchemaVersion::CURRENT,
        payload,
        extensions: Extensions::new(),
    };
    Command {
        operation_id,
        envelope,
    }
}

/// The command of `operation` for `connection_id` of [`OWNER`] from `platform`, with a key of its
/// own.
pub(crate) fn command(connection_id: Uuid, operation: &Value) -> Command {
    command_with(
        OWNER,
        connection_id,
        "platform",
        &Uuid::now_v7().to_string(),
        operation,
    )
}

/// `update` of `path` on `base` with the content `blob`.
pub(crate) fn update(path: &str, base: &str, blob: &Value) -> Value {
    json!({ "update": { "path": path, "base_hash": base, "blob": blob } })
}

/// `create` of `path` with the content `blob`.
pub(crate) fn create(path: &str, blob: &Value) -> Value {
    json!({ "create": { "path": path, "blob": blob } })
}
