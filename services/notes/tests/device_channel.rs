//! The device channel routes (workspace spec `notes-device-channels`, group 8): change batches with
//! the device sequence, the blob receipt of the shared transfer contract, and the manifest that
//! reconciles a vault after a gap.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions read the JSON answers they build"
)]

mod support;

use axum::Router;
use axum::http::{Method, StatusCode};
use ratatoskr_notes::api::{ApiState, domain_router};
use ratatoskr_notes::blobs::BlobStore;
use ratatoskr_notes_persistence::test_support::TestDatabase;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use crate::support::systemd::FakeSystemd;
use crate::support::{
    Answer, DEVICE_ID_HEADER, DEVICE_KIND_HEADER, connect, connection_id, device_of, send_with,
};

const OWNER: Uuid = Uuid::from_u128(0x0192_0000_0000_7000_8000_0000_0000_0dc1);

/// The domain router of one test database, with its blob store.
struct Channel {
    fixture: TestDatabase,
    _systemd: FakeSystemd,
    router: Router,
    blobs: BlobStore,
    scratch: tempfile::TempDir,
}

impl Channel {
    async fn new() -> Self {
        let fixture = TestDatabase::create().await.expect("a test database");
        let systemd = FakeSystemd::new();
        let root = systemd.blob_root();
        std::fs::create_dir_all(&root).expect("the blob root");
        let blobs = BlobStore::new(root);
        let router = domain_router(
            ApiState::new(fixture.database.clone(), systemd.clone())
                .with_key_file(systemd.key_file())
                .with_credentials_dir(systemd.credentials_root())
                .with_blobs(blobs.clone()),
        );
        Self {
            fixture,
            _systemd: systemd,
            router,
            blobs,
            scratch: tempfile::tempdir().expect("a scratch directory"),
        }
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
