//! File changes and their events (workspace spec `notes-file-changes`, task 7.5): one
//! `notes.file.changed.v1` per change with a `BlobRef` to the bytes, nothing for equal bytes, a
//! connection sequence without gaps, and one `deleted` per file of a folder that the owner
//! excludes. The outbox relay publishes each event on its subject.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions read the JSON events they build"
)]

mod support;

use std::path::PathBuf;
use std::time::Duration;

use axum::Router;
use axum::http::{Method, StatusCode};
use futures_util::StreamExt as _;
use ratatoskr_event_envelope::EventEnvelope;
use ratatoskr_notes::api::{ApiState, domain_router};
use ratatoskr_notes::blobs::BlobStore;
use ratatoskr_notes::files::FileRecorder;
use ratatoskr_notes::outbox::{BusSettings, OutboxRelay};
use ratatoskr_notes::watcher::TreeChange;
use ratatoskr_notes_contracts::FileChanged;
use ratatoskr_notes_persistence::test_support::TestDatabase;
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use crate::support::systemd::FakeSystemd;
use crate::support::{connect, connection_id, send};

const OWNER: Uuid = Uuid::from_u128(0x0192_0000_0000_7000_8000_0000_0000_00f1);

/// A connection of [`OWNER`], its router, its blob store and its recorder.
struct Vault {
    fixture: TestDatabase,
    _systemd: FakeSystemd,
    router: Router,
    blobs: BlobStore,
    recorder: FileRecorder,
    connection_id: Uuid,
    scratch: tempfile::TempDir,
}

impl Vault {
    async fn new() -> Self {
        let fixture = TestDatabase::create().await.expect("a test database");
        let systemd = FakeSystemd::new();
        let router = domain_router(
            ApiState::new(fixture.database.clone(), systemd.clone())
                .with_key_file(systemd.key_file())
                .with_credentials_dir(systemd.credentials_root()),
        );
        let root = systemd.blob_root();
        std::fs::create_dir_all(&root).expect("the blob root");
        let blobs = BlobStore::new(root);
        let recorder = FileRecorder::new(fixture.database.clone(), blobs.clone());
        let answer = connect(&router, OWNER, "plugin", "vault-key-file-events").await;
        let connection_id = connection_id(&answer);
        Self {
            fixture,
            _systemd: systemd,
            router,
            blobs,
            recorder,
            connection_id,
            scratch: tempfile::tempdir().expect("a scratch directory"),
        }
    }

    /// Store `bytes` in the blob root and record a revision of `path`.
    async fn save(&self, path: &str, bytes: &[u8]) {
        let source = self.scratch.path().join(Uuid::now_v7().to_string());
        std::fs::write(&source, bytes).expect("the source file");
        let stored = self.blobs.store_file(&source).expect("the blob is stored");
        self.recorder
            .record(
                self.connection_id,
                &TreeChange::Revision {
                    path: PathBuf::from(path),
                    digest: stored.digest,
                    length: stored.length,
                },
            )
            .await
            .expect("the change is recorded");
    }

    async fn remove(&self, path: &str) {
        self.recorder
            .record(
                self.connection_id,
                &TreeChange::Removed {
                    path: PathBuf::from(path),
                },
            )
            .await
            .expect("the removal is recorded");
    }

    /// Every `notes.file.changed.v1` envelope of this connection in the outbox.
    async fn events(&self) -> Vec<Value> {
        sqlx::query_scalar(
            "select payload from notes.outbox
             where event_type = 'notes.file.changed.v1'
               and payload -> 'payload' ->> 'connection_id' = $1
             order by created_at, event_id",
        )
        .bind(self.connection_id.to_string())
        .fetch_all(self.fixture.pool())
        .await
        .expect("the outbox can be read")
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

#[tokio::test]
async fn created_file_publishes_one_event_with_blob_ref() {
    let vault = Vault::new().await;
    let bytes = b"# Plan\n\nThe plan of the project.\n";

    vault.save("Projects/Plan.md", bytes).await;

    let events = vault.events().await;
    assert_eq!(events.len(), 1, "one event: {events:?}");
    let envelope: EventEnvelope =
        serde_json::from_value(events[0].clone()).expect("the event is an envelope");
    assert_eq!(envelope.producer.as_str(), "ratatoskr-notes");
    assert_eq!(
        envelope.tenant_id.map(|tenant| tenant.to_string()),
        Some(format!("user:{OWNER}"))
    );
    let payload: FileChanged = serde_json::from_value(Value::Object(envelope.payload))
        .expect("the payload satisfies the contract");
    assert_eq!(payload.change_kind.as_str(), "created");
    assert_eq!(payload.path.as_str(), "Projects/Plan.md");
    assert_eq!(payload.revision.as_str(), sha256(bytes));
    assert_eq!(payload.media_type.as_str(), "text/markdown");
    assert_eq!(payload.connection_sequence, 1);
    let blob = payload.blob.expect("a created file carries a BlobRef");
    assert_eq!(
        blob.digest.hex.as_str(),
        sha256(bytes),
        "the digest is the revision"
    );
    assert_eq!(blob.owner_service.as_str(), "ratatoskr-notes");
    assert_eq!(blob.length_bytes, bytes.len() as u64);

    vault.cleanup().await;
}

#[tokio::test]
async fn blob_ref_digest_equals_revision() {
    let vault = Vault::new().await;
    vault.save("Daily/2026-10-14.md", b"A day.\n").await;

    let events = vault.events().await;
    assert_eq!(events.len(), 1, "one event: {events:?}");
    let payload = &events[0]["payload"];
    let digest = payload["blob"]["digest"]["hex"]
        .as_str()
        .expect("the event names the digest of its blob");
    // As Knowledge does: read the bytes from the notes blob root and hash them.
    let path = vault.blobs.path_of(digest).expect("a blob path");
    let bytes = std::fs::read(path).expect("the blob can be read");
    assert_eq!(
        sha256(&bytes),
        payload["revision"].as_str().unwrap_or_default()
    );

    vault.cleanup().await;
}

#[tokio::test]
async fn identical_bytes_publish_nothing() {
    let vault = Vault::new().await;

    vault.save("Note.md", b"same bytes\n").await;
    vault.save("Note.md", b"same bytes\n").await;

    let events = vault.events().await;
    assert_eq!(events.len(), 1, "only the first save publishes: {events:?}");

    vault.cleanup().await;
}

#[tokio::test]
async fn connection_sequence_has_no_gaps() {
    let vault = Vault::new().await;
    // 100 changes: 40 creates (20 of them at the same time), 20 updates, 10 saves with equal
    // bytes, 10 deletes, 10 excluded paths and 10 creates of the deleted files again. 80 of them
    // publish an event.
    futures_util::future::join_all(
        (0..20).map(|n| vault.save_owned(format!("f{n}.md"), format!("v1 {n}"))),
    )
    .await;
    for n in 20..40 {
        vault
            .save(&format!("f{n}.md"), format!("v1 {n}").as_bytes())
            .await;
    }
    for n in 0..20 {
        vault
            .save(&format!("f{n}.md"), format!("v2 {n}").as_bytes())
            .await;
    }
    for n in 20..30 {
        vault
            .save(&format!("f{n}.md"), format!("v1 {n}").as_bytes())
            .await;
    }
    for n in 30..40 {
        vault.remove(&format!("f{n}.md")).await;
    }
    for n in 0..10 {
        vault.save(&format!(".obsidian/x{n}.json"), b"{}").await;
    }
    for n in 30..40 {
        vault
            .save(&format!("f{n}.md"), format!("v3 {n}").as_bytes())
            .await;
    }

    let mut sequences: Vec<u64> = vault
        .events()
        .await
        .iter()
        .map(|event| {
            event["payload"]["connection_sequence"]
                .as_u64()
                .unwrap_or(0)
        })
        .collect();
    sequences.sort_unstable();
    assert_eq!(
        sequences,
        (1..=80).collect::<Vec<u64>>(),
        "the sequences are exactly 1 to the number of events"
    );

    vault.cleanup().await;
}

impl Vault {
    async fn save_owned(&self, path: String, bytes: String) {
        self.save(&path, bytes.as_bytes()).await;
    }
}

#[tokio::test]
async fn excluding_a_folder_publishes_each_file_deleted_once() {
    let vault = Vault::new().await;
    for name in ["a", "b", "c"] {
        vault
            .save(&format!("Private/{name}.md"), name.as_bytes())
            .await;
    }
    vault.save("Public.md", b"public").await;
    let path = format!(
        "/v1/notes/connections/{}/excluded-folders",
        vault.connection_id
    );
    let body = serde_json::json!({ "excluded_folders": ["Private"] });

    let answer = send(&vault.router, Method::PUT, &path, OWNER, Some(body.clone())).await;
    let again = send(&vault.router, Method::PUT, &path, OWNER, Some(body)).await;
    vault.save("Private/d.md", b"later").await;
    vault.save("Private/a.md", b"changed later").await;

    let deleted: Vec<String> = vault
        .events()
        .await
        .iter()
        .filter(|event| event["payload"]["change_kind"] == "deleted")
        .map(|event| {
            event["payload"]["path"]
                .as_str()
                .unwrap_or_default()
                .to_owned()
        })
        .collect();
    let mut deleted_sorted = deleted.clone();
    deleted_sorted.sort();
    assert_eq!(
        deleted_sorted,
        ["Private/a.md", "Private/b.md", "Private/c.md"],
        "each file of the folder is published deleted once"
    );
    assert_eq!(
        vault.events().await.len(),
        7,
        "four creates and three deletes, and nothing later under Private/"
    );
    assert_eq!(answer.status, StatusCode::OK, "{:?}", answer.body);
    assert_eq!(again.status, StatusCode::OK, "{:?}", again.body);

    vault.cleanup().await;
}

/// Where the relay test reaches a NATS server with `JetStream`. `NOTES_TEST_NATS_URL` overrides
/// it; CI starts a server there.
#[expect(
    clippy::disallowed_methods,
    reason = "a test-only location of a scratch NATS server, not a setting of the service"
)]
fn nats_url() -> String {
    std::env::var("NOTES_TEST_NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".to_owned())
}

/// The stream that the relay test reads. The production stream `ratatoskr_events` belongs to Edge.
const TEST_STREAM: &str = "notes_test_events";

#[tokio::test]
async fn outbox_relay_publishes_each_event_once_on_its_subject() {
    let vault = Vault::new().await;
    vault.save("Relayed.md", b"relayed bytes\n").await;
    let client = async_nats::connect(nats_url())
        .await
        .expect("the test NATS server can be reached");
    let jetstream = async_nats::jetstream::new(client);
    let stream = jetstream
        .get_or_create_stream(async_nats::jetstream::stream::Config {
            name: TEST_STREAM.to_owned(),
            subjects: vec![
                "evt.notes.>".to_owned(),
                "evt.platform.operation.reported.v1".to_owned(),
            ],
            ..Default::default()
        })
        .await
        .expect("the test stream exists");
    let relay = OutboxRelay::new(
        vault.fixture.database.clone(),
        BusSettings {
            url: nats_url(),
            nkey_seed_path: None,
        },
    );

    let published = relay.publish_pending().await.expect("a pass of the relay");
    let again = relay.publish_pending().await.expect("a second pass");

    let rows: i64 = sqlx::query_scalar("select count(*) from notes.outbox")
        .fetch_one(vault.fixture.pool())
        .await
        .expect("the outbox can be counted");
    // The file event, and the report of the connect operation of the plugin connection.
    assert_eq!(published, 2, "each event of the outbox is published");
    assert_eq!(rows, 2, "the outbox holds the file event and one report");
    assert_eq!(again, 0, "a published event is not published again");
    let stored = vault.events().await;
    let event_id = stored[0]["event_id"]
        .as_str()
        .expect("an event id")
        .to_owned();
    let consumer = stream
        .create_consumer(async_nats::jetstream::consumer::pull::Config {
            filter_subject: "evt.notes.file.changed.v1".to_owned(),
            ..Default::default()
        })
        .await
        .expect("a consumer of the test stream");
    let mut messages = consumer
        .stream()
        .expires(Duration::from_secs(2))
        .messages()
        .await
        .expect("the messages of the stream");
    let mut found = None;
    while let Ok(Some(Ok(message))) =
        tokio::time::timeout(Duration::from_secs(2), messages.next()).await
    {
        let id = message
            .headers
            .as_ref()
            .and_then(|headers| headers.get("Nats-Msg-Id"))
            .map(|value| value.as_str().to_owned());
        if id.as_deref() == Some(event_id.as_str()) {
            found = Some(message);
            break;
        }
    }
    let message = found.expect("the event is in the stream with its event id as Nats-Msg-Id");
    let body: Value = serde_json::from_slice(&message.payload).expect("the message is JSON");
    assert_eq!(
        body, stored[0],
        "the stored envelope is published unchanged"
    );
    assert_eq!(message.subject.as_str(), "evt.notes.file.changed.v1");

    vault.cleanup().await;
}

#[tokio::test]
async fn case_only_rename_indexes_the_new_path() {
    let vault = Vault::new().await;
    let bytes = b"one note, renamed in Finder\n";

    // A rename that changes only the case arrives as the new path, a collision while the old path
    // is present, and then the removal of the old path.
    vault.save("Note.md", bytes).await;
    vault.save("note.md", bytes).await;
    vault.remove("Note.md").await;

    let present: Vec<(String, String)> = sqlx::query_as(
        "select path, revision from notes.files
         where connection_id = $1 and state = 'present'",
    )
    .bind(vault.connection_id)
    .fetch_all(vault.fixture.pool())
    .await
    .expect("the files can be read");
    let collisions: i64 =
        sqlx::query_scalar("select count(*) from notes.path_collisions where connection_id = $1")
            .bind(vault.connection_id)
            .fetch_one(vault.fixture.pool())
            .await
            .expect("the collisions can be counted");
    let events: Vec<(String, String)> = vault
        .events()
        .await
        .iter()
        .map(|event| {
            (
                event["payload"]["change_kind"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned(),
                event["payload"]["path"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned(),
            )
        })
        .collect();
    assert_eq!(
        present,
        [("note.md".to_owned(), sha256(bytes))],
        "the new path is indexed with its revision"
    );
    assert_eq!(collisions, 0, "no collision is left");
    assert_eq!(
        events,
        [
            ("created".to_owned(), "Note.md".to_owned()),
            ("deleted".to_owned(), "Note.md".to_owned()),
            ("created".to_owned(), "note.md".to_owned()),
        ]
    );

    vault.cleanup().await;
}
