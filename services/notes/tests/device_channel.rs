//! The device channel routes (workspace spec `notes-device-channels`, group 8): change batches with
//! the device sequence, the blob receipt of the shared transfer contract, and the manifest that
//! reconciles a vault after a gap.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions read the JSON answers they build"
)]

mod support;

use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use http_body_util::BodyExt as _;
use ratatoskr_notes::api::{ApiState, domain_router};
use ratatoskr_notes::blobs::BlobStore;
use ratatoskr_notes_persistence::test_support::TestDatabase;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use crate::support::systemd::FakeSystemd;
use crate::support::{
    Answer, DEVICE_ID_HEADER, DEVICE_KIND_HEADER, USER_HEADER, connect, connection_id, device_of,
    send_with, terminal_report,
};
use tower::ServiceExt as _;

const OWNER: Uuid = Uuid::from_u128(0x0192_0000_0000_7000_8000_0000_0000_0dc1);

/// The domain router of one test database, with its blob store.
struct Channel {
    fixture: TestDatabase,
    systemd: FakeSystemd,
    router: Router,
    blobs: BlobStore,
    scratch: tempfile::TempDir,
}

impl Channel {
    async fn new() -> Self {
        Self::with(|state| state).await
    }

    /// A router whose state `settings` adjusts.
    async fn with(settings: impl FnOnce(ApiState<FakeSystemd>) -> ApiState<FakeSystemd>) -> Self {
        let fixture = TestDatabase::create().await.expect("a test database");
        let systemd = FakeSystemd::new();
        let root = systemd.blob_root();
        std::fs::create_dir_all(&root).expect("the blob root");
        let blobs = BlobStore::new(root);
        let mut channel = Self {
            fixture,
            systemd,
            router: Router::new(),
            blobs,
            scratch: tempfile::tempdir().expect("a scratch directory"),
        };
        channel.router = domain_router(settings(channel.state()));
        channel
    }

    /// The state of the domain routes of this test database, as the service builds it at start.
    fn state(&self) -> ApiState<FakeSystemd> {
        ApiState::new(self.fixture.database.clone(), self.systemd.clone())
            .with_key_file(self.systemd.key_file())
            .with_credentials_dir(self.systemd.credentials_root())
            .with_blobs(self.blobs.clone())
    }

    /// A `plugin` connection of `owner`, from the device of `owner`.
    async fn plugin_connection(&self, owner: Uuid, vault_key: &str) -> Uuid {
        connection_id(&connect(&self.router, owner, "plugin", vault_key).await)
    }

    /// Put `bytes` in the blob store, as a finished upload does.
    fn hold(&self, bytes: &[u8]) {
        let source = self.scratch.path().join(Uuid::now_v7().to_string());
        std::fs::write(&source, bytes).expect("the source file");
        self.blobs.store_file(&source).expect("the blob is stored");
    }

    /// `POST /v1/notes/connections/{id}/changes` from `device` of the kind `kind`.
    async fn batch_from(
        &self,
        owner: Uuid,
        device: Uuid,
        kind: &str,
        connection_id: Uuid,
        changes: Vec<Value>,
    ) -> Answer {
        let claims = [
            (DEVICE_ID_HEADER, device.to_string()),
            (DEVICE_KIND_HEADER, kind.to_owned()),
        ];
        send_with(
            &self.router,
            Method::POST,
            &format!("/v1/notes/connections/{connection_id}/changes"),
            owner,
            &claims,
            Some(json!({ "changes": changes })),
        )
        .await
    }

    /// A batch from the plugin of `owner`.
    async fn batch(&self, owner: Uuid, connection_id: Uuid, changes: Vec<Value>) -> Answer {
        self.batch_from(
            owner,
            device_of(owner),
            "obsidian_plugin",
            connection_id,
            changes,
        )
        .await
    }

    /// The number of `notes.file.changed.v1` events of a connection.
    async fn events(&self, connection_id: Uuid) -> i64 {
        sqlx::query_scalar(
            "select count(*) from notes.outbox
             where event_type = 'notes.file.changed.v1'
               and payload -> 'payload' ->> 'connection_id' = $1",
        )
        .bind(connection_id.to_string())
        .fetch_one(self.fixture.pool())
        .await
        .expect("the outbox can be read")
    }

    /// The number of revisions of the files of a connection.
    async fn revisions(&self, connection_id: Uuid) -> i64 {
        sqlx::query_scalar(
            "select count(*) from notes.revisions r
             join notes.files f on f.file_id = r.file_id
             where f.connection_id = $1",
        )
        .bind(connection_id)
        .fetch_one(self.fixture.pool())
        .await
        .expect("the revisions can be read")
    }

    async fn cleanup(self) {
        self.fixture
            .cleanup()
            .await
            .expect("the test database is dropped");
    }
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// One `created` or `updated` change with the facts of `bytes`.
fn change(sequence: u64, kind: &str, path: &str, bytes: &[u8]) -> Value {
    json!({
        "sequence": sequence,
        "kind": kind,
        "path": path,
        "sha256": sha256(bytes),
        "length_bytes": bytes.len(),
    })
}

#[tokio::test]
async fn duplicate_batch_is_idempotent() {
    let channel = Channel::new().await;
    let connection = channel
        .plugin_connection(OWNER, "vault-key-duplicate")
        .await;
    for bytes in [&b"alpha v1\n"[..], b"beta v1\n", b"alpha v2\n"] {
        channel.hold(bytes);
    }
    let changes = vec![
        change(1, "created", "Alpha.md", b"alpha v1\n"),
        change(2, "created", "Beta.md", b"beta v1\n"),
        change(3, "updated", "Alpha.md", b"alpha v2\n"),
    ];

    let first = channel.batch(OWNER, connection, changes.clone()).await;
    let second = channel.batch(OWNER, connection, changes).await;

    assert_eq!(first.status, StatusCode::OK, "{:?}", first.body);
    assert_eq!(second.status, StatusCode::OK, "{:?}", second.body);
    assert_eq!(first.body["committed_sequence"], 3);
    assert_eq!(
        second.body["committed_sequence"], 3,
        "both answers name the same committed sequence"
    );
    assert_eq!(
        channel.revisions(connection).await,
        3,
        "one revision per change"
    );
    assert_eq!(channel.events(connection).await, 3, "one event per change");

    channel.cleanup().await;
}

#[tokio::test]
async fn sequence_gap_commits_nothing() {
    let channel = Channel::new().await;
    let connection = channel.plugin_connection(OWNER, "vault-key-gap").await;
    let mut first = Vec::new();
    for sequence in 1..=10_u64 {
        let bytes = format!("note {sequence}\n");
        channel.hold(bytes.as_bytes());
        first.push(change(
            sequence,
            "created",
            &format!("n{sequence}.md"),
            bytes.as_bytes(),
        ));
    }
    let committed = channel.batch(OWNER, connection, first).await;
    channel.hold(b"late\n");

    let gap = channel
        .batch(
            OWNER,
            connection,
            vec![
                change(12, "created", "late.md", b"late\n"),
                change(13, "updated", "n1.md", b"late\n"),
            ],
        )
        .await;

    assert_eq!(committed.status, StatusCode::OK, "{:?}", committed.body);
    assert_eq!(gap.status, StatusCode::CONFLICT, "{:?}", gap.body);
    assert_eq!(gap.body["code"], "notes.channel.sequence_gap");
    assert_eq!(
        gap.body["committed_sequence"], 10,
        "the answer names the committed sequence"
    );
    assert_eq!(
        channel.events(connection).await,
        10,
        "nothing of the batch is committed"
    );

    channel.cleanup().await;
}

#[tokio::test]
async fn oversized_batch_commits_nothing() {
    let channel = Channel::new().await;
    let connection = channel
        .plugin_connection(OWNER, "vault-key-oversized")
        .await;
    channel.hold(b"one body\n");
    let changes: Vec<Value> = (1..=257_u64)
        .map(|sequence| {
            change(
                sequence,
                "created",
                &format!("f{sequence}.md"),
                b"one body\n",
            )
        })
        .collect();

    let answer = channel.batch(OWNER, connection, changes).await;

    assert_eq!(
        answer.status,
        StatusCode::PAYLOAD_TOO_LARGE,
        "{:?}",
        answer.body
    );
    assert_eq!(answer.body["code"], "notes.channel.batch_too_large");
    assert_eq!(channel.events(connection).await, 0, "nothing is committed");
    assert_eq!(channel.revisions(connection).await, 0, "no revision");

    channel.cleanup().await;
}

const OTHER_OWNER: Uuid = Uuid::from_u128(0x0192_0000_0000_7000_8000_0000_0000_0dc2);

/// A second device of [`OWNER`], for example a phone that holds a copy of the vault folder.
const SECOND_DEVICE: Uuid = Uuid::from_u128(0x0192_0000_0000_7000_8000_0000_0000_0dc3);

/// The words of the hint that points the owner to Obsidian Sync.
const SYNC_HINT: &str = "Obsidian Sync";

impl Channel {
    /// `POST /v1/notes/connections` with `channel = plugin` from `device`.
    async fn connect_from(&self, owner: Uuid, device: Uuid, vault_key: &str) -> Answer {
        let claims = [
            (DEVICE_ID_HEADER, device.to_string()),
            (DEVICE_KIND_HEADER, "obsidian_plugin".to_owned()),
        ];
        let body = json!({
            "channel": "plugin",
            "vault_key": vault_key,
            "vault_display_name": "Personal",
        });
        send_with(
            &self.router,
            Method::POST,
            "/v1/notes/connections",
            owner,
            &claims,
            Some(body),
        )
        .await
    }

    /// The device that a connection records.
    async fn device(&self, connection_id: Uuid) -> Option<Uuid> {
        sqlx::query_scalar("select device_id from notes.connections where connection_id = $1")
            .bind(connection_id)
            .fetch_one(self.fixture.pool())
            .await
            .expect("the connection exists")
    }
}

#[tokio::test]
async fn foreign_connection_changes_are_not_found() {
    let channel = Channel::new().await;
    let foreign = channel
        .plugin_connection(OTHER_OWNER, "vault-key-foreign")
        .await;
    channel.hold(b"intruder\n");

    let answer = channel
        .batch(
            OWNER,
            foreign,
            vec![change(1, "created", "Intruder.md", b"intruder\n")],
        )
        .await;

    assert_eq!(answer.status, StatusCode::NOT_FOUND, "{:?}", answer.body);
    assert_eq!(answer.body["code"], "notes.connection.not_found");
    assert_eq!(
        channel.events(foreign).await,
        0,
        "no change is recorded for the connection of the other owner"
    );

    channel.cleanup().await;
}

#[tokio::test]
async fn export_agent_on_plugin_connection_is_channel_mismatch() {
    let channel = Channel::new().await;
    let connection = channel.plugin_connection(OWNER, "vault-key-kind").await;
    channel.hold(b"from the agent\n");

    let answer = channel
        .batch_from(
            OWNER,
            device_of(OWNER),
            "export_agent",
            connection,
            vec![change(1, "created", "Agent.md", b"from the agent\n")],
        )
        .await;

    assert_eq!(answer.status, StatusCode::CONFLICT, "{:?}", answer.body);
    assert_eq!(answer.body["code"], "notes.channel_mismatch");
    assert_eq!(channel.events(connection).await, 0, "no change is recorded");

    channel.cleanup().await;
}

#[tokio::test]
async fn second_device_for_one_connection_is_refused() {
    let channel = Channel::new().await;
    let connection = channel.plugin_connection(OWNER, "vault-key-copied").await;
    channel.hold(b"from the copy\n");

    let connect = channel
        .connect_from(OWNER, SECOND_DEVICE, "vault-key-copied")
        .await;
    let batch = channel
        .batch_from(
            OWNER,
            SECOND_DEVICE,
            "obsidian_plugin",
            connection,
            vec![change(1, "created", "Copy.md", b"from the copy\n")],
        )
        .await;

    for (step, answer) in [("connect", &connect), ("batch", &batch)] {
        assert_eq!(
            answer.status,
            StatusCode::CONFLICT,
            "{step}: {:?}",
            answer.body
        );
        assert_eq!(answer.body["code"], "notes.channel_mismatch", "{step}");
        assert!(
            answer.body["message"]
                .as_str()
                .is_some_and(|message| message.contains(SYNC_HINT)),
            "{step}: the message points the owner to Obsidian Sync: {:?}",
            answer.body
        );
    }
    assert_eq!(
        channel.device(connection).await,
        Some(device_of(OWNER)),
        "the connection keeps its first device"
    );
    assert_eq!(channel.events(connection).await, 0, "no change is recorded");

    channel.cleanup().await;
}

impl Channel {
    /// A JSON request from the plugin of [`OWNER`] to a route of `connection_id`.
    async fn device_call(&self, connection_id: Uuid, route: &str, body: Value) -> Answer {
        let claims = [
            (DEVICE_ID_HEADER, device_of(OWNER).to_string()),
            (DEVICE_KIND_HEADER, "obsidian_plugin".to_owned()),
        ];
        send_with(
            &self.router,
            Method::POST,
            &format!("/v1/notes/connections/{connection_id}/{route}"),
            OWNER,
            &claims,
            Some(body),
        )
        .await
    }

    /// `PUT .../blobs/{token}/chunks/{index}` with the raw bytes of one chunk.
    async fn put_chunk(
        &self,
        connection_id: Uuid,
        token: &str,
        index: u32,
        bytes: &[u8],
    ) -> Answer {
        let request = Request::builder()
            .method(Method::PUT)
            .uri(format!(
                "/v1/notes/connections/{connection_id}/blobs/{token}/chunks/{index}"
            ))
            .header(USER_HEADER, OWNER.to_string())
            .header(DEVICE_ID_HEADER, device_of(OWNER).to_string())
            .header(DEVICE_KIND_HEADER, "obsidian_plugin")
            .header("content-type", "application/octet-stream")
            .body(Body::from(bytes.to_vec()))
            .expect("a request");
        let response = self
            .router
            .clone()
            .oneshot(request)
            .await
            .expect("the router answers");
        let status = response.status();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("a body")
            .to_bytes();
        Answer {
            status,
            body: serde_json::from_slice(&body).unwrap_or(Value::Null),
        }
    }

    /// Upload `bytes` under the declared SHA-256 `declared` through the transfer routes, in one
    /// chunk. Returns the answers of the open, the chunk and the finalize.
    async fn upload(&self, connection_id: Uuid, declared: &str, bytes: &[u8]) -> [Answer; 3] {
        let opened = self
            .device_call(
                connection_id,
                "blobs",
                json!({
                    "declared_size_bytes": bytes.len(),
                    "media_type": "text/markdown",
                    "digest": { "algorithm": "sha256", "hex": declared },
                    "chunk_size_bytes": 65_536,
                }),
            )
            .await;
        let token = opened.body["resumption_token"]
            .as_str()
            .unwrap_or("rst_00000000000000000000000000")
            .to_owned();
        let chunk = self.put_chunk(connection_id, &token, 0, bytes).await;
        let finalized = self
            .device_call(
                connection_id,
                &format!("blobs/{token}/finalize"),
                json!({ "resumption_token": token }),
            )
            .await;
        [opened, chunk, finalized]
    }
}

#[tokio::test]
async fn upload_with_wrong_digest_is_refused() {
    let channel = Channel::new().await;
    let connection = channel.plugin_connection(OWNER, "vault-key-upload").await;
    let declared_bytes = b"the declared bytes\n";
    let uploaded_bytes = b"the uploaded bytes\n";
    let declared = sha256(declared_bytes);

    let [opened, chunk, finalized] = channel.upload(connection, &declared, uploaded_bytes).await;
    let batch = channel
        .batch(
            OWNER,
            connection,
            vec![change(1, "created", "Declared.md", declared_bytes)],
        )
        .await;

    assert_eq!(opened.status, StatusCode::CREATED, "{:?}", opened.body);
    assert_eq!(chunk.status, StatusCode::OK, "{:?}", chunk.body);
    assert_eq!(finalized.status, StatusCode::OK, "{:?}", finalized.body);
    assert_eq!(
        finalized.body["outcome"], "digest_mismatch",
        "the transfer contract answers the mismatch as its outcome"
    );
    assert_eq!(finalized.body["declared_sha256_hex"], declared.as_str());
    assert_eq!(
        finalized.body["computed_sha256_hex"],
        sha256(uploaded_bytes).as_str()
    );
    for digest in [declared.clone(), sha256(uploaded_bytes)] {
        let blob = channel.blobs.path_of(&digest).expect("a blob path");
        assert!(!blob.exists(), "no blob is registered for {digest}");
    }
    assert_eq!(batch.status, StatusCode::CONFLICT, "{:?}", batch.body);
    assert_eq!(batch.body["code"], "notes.channel.bytes_missing");

    channel.cleanup().await;
}

impl Channel {
    /// The payloads of the `notes.file.changed.v1` events of a connection, in order.
    async fn changes(&self, connection_id: Uuid) -> Vec<Value> {
        sqlx::query_scalar(
            "select payload -> 'payload' from notes.outbox
             where event_type = 'notes.file.changed.v1'
               and payload -> 'payload' ->> 'connection_id' = $1
             order by position",
        )
        .bind(connection_id.to_string())
        .fetch_all(self.fixture.pool())
        .await
        .expect("the outbox can be read")
    }
}

/// One manifest entry with the facts of `bytes`.
fn entry(path: &str, bytes: &[u8]) -> Value {
    json!({ "path": path, "sha256": sha256(bytes), "length_bytes": bytes.len() })
}

#[tokio::test]
async fn manifest_reconciles_offline_changes() {
    let channel = Channel::new().await;
    let connection = channel.plugin_connection(OWNER, "vault-key-manifest").await;
    let original = |n: u32| format!("file {n:02} as it was\n");
    let mut first = Vec::new();
    for n in 1..=20_u32 {
        channel.hold(original(n).as_bytes());
        first.push(change(
            u64::from(n),
            "created",
            &format!("f{n:02}.md"),
            original(n).as_bytes(),
        ));
    }
    let committed = channel.batch(OWNER, connection, first).await;
    // While the plugin was disabled: f01 is edited, f02 is deleted, f03 is renamed and new.md is
    // created. The new bytes are only on the device.
    let edited = b"file 01 edited offline\n";
    let created = b"a note made offline\n";
    let mut entries: Vec<Value> = (4..=20_u32)
        .map(|n| entry(&format!("f{n:02}.md"), original(n).as_bytes()))
        .collect();
    entries.push(entry("f01.md", edited));
    entries.push(entry("Moved/f03.md", original(3).as_bytes()));
    entries.push(entry("new.md", created));
    let manifest = json!({ "entries": entries });

    let asked = channel
        .device_call(connection, "manifest", manifest.clone())
        .await;
    let uploads = [
        channel.upload(connection, &sha256(edited), edited).await,
        channel.upload(connection, &sha256(created), created).await,
    ];
    let applied = channel.device_call(connection, "manifest", manifest).await;

    assert_eq!(committed.status, StatusCode::OK, "{:?}", committed.body);
    assert_eq!(asked.status, StatusCode::OK, "{:?}", asked.body);
    let mut needed = vec![sha256(edited), sha256(created)];
    needed.sort();
    assert_eq!(
        asked.body["needed_sha256"],
        json!(needed),
        "the first answer names the bytes that the service needs"
    );
    assert_eq!(asked.body["committed_sequence"], 20);
    for [_, _, finalized] in &uploads {
        assert_eq!(finalized.body["outcome"], "stored", "{:?}", finalized.body);
    }
    assert_eq!(applied.status, StatusCode::OK, "{:?}", applied.body);
    assert_eq!(applied.body["needed_sha256"], json!([]));
    let changes = channel.changes(connection).await;
    let offline: Vec<(String, String, Value)> = changes
        .iter()
        .skip(20)
        .map(|change| {
            (
                change["change_kind"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned(),
                change["path"].as_str().unwrap_or_default().to_owned(),
                change["previous_path"].clone(),
            )
        })
        .collect();
    let mut kinds: Vec<&str> = offline.iter().map(|(kind, _, _)| kind.as_str()).collect();
    kinds.sort_unstable();
    assert_eq!(
        kinds,
        ["created", "deleted", "renamed", "updated"],
        "exactly one change of each kind: {offline:#?}"
    );
    assert!(
        offline.contains(&("updated".to_owned(), "f01.md".to_owned(), Value::Null)),
        "{offline:#?}"
    );
    assert!(
        offline.contains(&("deleted".to_owned(), "f02.md".to_owned(), Value::Null)),
        "{offline:#?}"
    );
    assert!(
        offline.contains(&(
            "renamed".to_owned(),
            "Moved/f03.md".to_owned(),
            json!("f03.md")
        )),
        "{offline:#?}"
    );
    assert!(
        offline.contains(&("created".to_owned(), "new.md".to_owned(), Value::Null)),
        "{offline:#?}"
    );

    channel.cleanup().await;
}

impl Channel {
    /// The state of a connection.
    async fn connection_state(&self, connection_id: Uuid) -> String {
        sqlx::query_scalar("select state from notes.connections where connection_id = $1")
            .bind(connection_id)
            .fetch_one(self.fixture.pool())
            .await
            .expect("the connection exists")
    }

    /// The number of `notes.vault.connected.v1` events of a connection.
    async fn connected_events(&self, connection_id: Uuid) -> i64 {
        sqlx::query_scalar(
            "select count(*) from notes.outbox
             where event_type = 'notes.vault.connected.v1'
               and payload -> 'payload' ->> 'connection_id' = $1",
        )
        .bind(connection_id.to_string())
        .fetch_one(self.fixture.pool())
        .await
        .expect("the outbox can be read")
    }
}

/// A short bound for the first manifest, so that a test sees its end.
const SHORT_DEADLINE: Duration = Duration::from_millis(200);

#[tokio::test]
async fn first_manifest_completes_the_device_connect() {
    let channel = Channel::new().await;
    let connection = channel
        .plugin_connection(OWNER, "vault-key-first-manifest")
        .await;
    channel.hold(b"the first note\n");

    let applied = channel
        .device_call(
            connection,
            "manifest",
            json!({ "entries": [entry("First.md", b"the first note\n")] }),
        )
        .await;
    let report = terminal_report(&channel.fixture, connection).await;

    assert_eq!(applied.status, StatusCode::OK, "{:?}", applied.body);
    assert_eq!(applied.body["needed_sha256"], json!([]));
    assert_eq!(
        report["status"], "succeeded",
        "the first manifest ends the connect operation: {report:?}"
    );
    assert_eq!(channel.connection_state(connection).await, "active");
    assert_eq!(channel.connected_events(connection).await, 1);

    channel.cleanup().await;
}

#[tokio::test]
async fn device_connect_without_manifest_fails_in_its_bound() {
    let channel = Channel::with(|state| state.with_manifest_deadline(SHORT_DEADLINE)).await;
    let connection = channel
        .plugin_connection(OWNER, "vault-key-no-manifest")
        .await;

    let report = terminal_report(&channel.fixture, connection).await;

    assert_eq!(report["status"], "failed", "{report:?}");
    assert_eq!(report["error"]["code"], "notes.channel.manifest_missing");
    assert_eq!(channel.connection_state(connection).await, "failed");
    assert_eq!(channel.connected_events(connection).await, 0);

    channel.cleanup().await;
}

#[tokio::test]
async fn manifest_deadline_holds_after_a_restart() {
    let channel = Channel::new().await;
    let connection = channel.plugin_connection(OWNER, "vault-key-restart").await;

    // The next start of the service finds the connection that still waits for its manifest.
    let restarted = channel.state().with_manifest_deadline(SHORT_DEADLINE);
    restarted.resume_device_connects().await;
    let report = terminal_report(&channel.fixture, connection).await;

    assert_eq!(report["status"], "failed", "{report:?}");
    assert_eq!(report["error"]["code"], "notes.channel.manifest_missing");
    assert_eq!(channel.connection_state(connection).await, "failed");

    channel.cleanup().await;
}
