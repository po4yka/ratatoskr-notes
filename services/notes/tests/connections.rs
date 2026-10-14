//! Vault connections (workspace spec `notes-vault-connections`): the one-channel rule
//! (requirement "A vault connection has exactly one channel") and disconnect (requirement
//! "Connection events name the connection and never its secrets").

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions read the JSON answers they build"
)]

mod support;

use axum::Router;
use axum::http::{Method, StatusCode};
use ratatoskr_event_envelope::{EventEnvelope, EventPayload as _};
use ratatoskr_notes::api::{ApiState, domain_router};
use ratatoskr_notes_contracts::{VaultChannel, VaultDisconnected};
use ratatoskr_notes_persistence::test_support::TestDatabase;
use uuid::Uuid;

use crate::support::systemd::{Call, FakeSystemd};
use crate::support::{connect, connection_id, send};

const OWNER_A: Uuid = Uuid::from_u128(0x0192_0000_0000_7000_8000_0000_0000_000a);
const OWNER_B: Uuid = Uuid::from_u128(0x0192_0000_0000_7000_8000_0000_0000_000b);
const REMOTE_VAULT_ID: &str = "remote-vault-0001";

fn router(fixture: &TestDatabase, systemd: &FakeSystemd) -> Router {
    domain_router(
        ApiState::new(fixture.database.clone(), systemd.clone()).with_key_file(systemd.key_file()),
    )
}

async fn connections_of(fixture: &TestDatabase, owner: Uuid) -> i64 {
    sqlx::query_scalar("select count(*) from notes.connections where owner_id = $1")
        .bind(owner)
        .fetch_one(fixture.pool())
        .await
        .expect("the connections can be counted")
}

#[tokio::test]
async fn second_channel_for_one_vault_is_refused() {
    let fixture = TestDatabase::create().await.expect("a test database");
    let router = router(&fixture, &FakeSystemd::new());

    let sync = connect(&router, OWNER_A, "sync", REMOTE_VAULT_ID).await;
    let sync_id = connection_id(&sync);

    let plugin = connect(&router, OWNER_A, "plugin", REMOTE_VAULT_ID).await;
    assert_eq!(plugin.status, StatusCode::CONFLICT, "{:?}", plugin.body);
    assert_eq!(
        plugin.body["code"], "notes.connection.vault_already_connected",
        "{:?}",
        plugin.body
    );
    assert_eq!(plugin.body["connection_id"], sync_id.to_string());
    assert_eq!(plugin.body["channel"], "sync");
    assert_eq!(connections_of(&fixture, OWNER_A).await, 1);

    fixture
        .cleanup()
        .await
        .expect("the test database is dropped");
}

#[tokio::test]
async fn two_owners_may_connect_one_remote_vault() {
    let fixture = TestDatabase::create().await.expect("a test database");
    let router = router(&fixture, &FakeSystemd::new());

    let first = connection_id(&connect(&router, OWNER_A, "sync", REMOTE_VAULT_ID).await);
    let second = connection_id(&connect(&router, OWNER_B, "sync", REMOTE_VAULT_ID).await);

    assert_ne!(first, second);
    assert_eq!(connections_of(&fixture, OWNER_A).await, 1);
    assert_eq!(connections_of(&fixture, OWNER_B).await, 1);

    fixture
        .cleanup()
        .await
        .expect("the test database is dropped");
}

/// Writes what a finished login and a running sync leave on the host (workspace design D3): the
/// working tree with a note, and `ob-home` with the token and the vault configuration.
fn leave_sync_state(
    systemd: &FakeSystemd,
    connection_id: Uuid,
) -> (std::path::PathBuf, std::path::PathBuf) {
    let tree = systemd.connection_dir(connection_id).join("tree");
    let ob_home = systemd.connection_dir(connection_id).join("ob-home");
    let state = ob_home.join("obsidian-headless");
    std::fs::create_dir_all(tree.join("Daily")).expect("the tree is created");
    std::fs::write(tree.join("Daily/2026-10-13.md"), "# A note\n").expect("a note is written");
    std::fs::create_dir_all(state.join("sync").join(REMOTE_VAULT_ID)).expect("ob-home is created");
    std::fs::write(state.join("auth_token"), "FAKE-TOKEN-MARKER").expect("a token is written");
    std::fs::write(
        state.join("sync").join(REMOTE_VAULT_ID).join("config.json"),
        r#"{"encryptionKey":"FAKE-KEY-MARKER"}"#,
    )
    .expect("a vault configuration is written");
    (tree, ob_home)
}

/// The number of stored secrets of a connection.
async fn secrets_of(fixture: &TestDatabase, connection_id: Uuid) -> i64 {
    sqlx::query_scalar("select count(*) from notes.connection_secrets where connection_id = $1")
        .bind(connection_id)
        .fetch_one(fixture.pool())
        .await
        .expect("the secrets can be counted")
}

#[tokio::test]
async fn disconnect_removes_unit_tree_state_and_secrets() {
    let fixture = TestDatabase::create().await.expect("a test database");
    let systemd = FakeSystemd::new();
    let router = router(&fixture, &systemd);
    let id = connection_id(&connect(&router, OWNER_A, "sync", REMOTE_VAULT_ID).await);
    let calls_of_connect = systemd.calls().len();
    let (tree, ob_home) = leave_sync_state(&systemd, id);
    assert_eq!(
        secrets_of(&fixture, id).await,
        2,
        "the connect stored both passwords"
    );

    let answer = send(
        &router,
        Method::DELETE,
        &format!("/v1/notes/connections/{id}"),
        OWNER_A,
        None,
    )
    .await;
    assert_eq!(answer.status, StatusCode::NO_CONTENT, "{:?}", answer.body);

    // The sync unit is stopped, then the cleanup unit runs, through the bus and nothing else.
    assert_eq!(
        systemd.calls()[calls_of_connect..],
        [
            Call::Stop(format!("ratatoskr-notes-sync@{id}.service")),
            Call::Start(format!("ratatoskr-notes-cleanup@{id}.service")),
        ]
    );
    assert_eq!(
        systemd.ob_invocations(),
        vec![format!("sync-unlink --path {}", tree.display())]
    );
    assert!(!tree.exists(), "the working tree is gone");
    assert!(!ob_home.exists(), "the client state is gone");

    assert_eq!(secrets_of(&fixture, id).await, 0, "the ciphertext is gone");

    let events: Vec<(String, serde_json::Value)> =
        sqlx::query_as("select event_type, payload from notes.outbox where aggregate_id = $1")
            .bind(format!("notes_connection:{id}"))
            .fetch_all(fixture.pool())
            .await
            .expect("the outbox can be read");
    assert_eq!(events.len(), 1, "exactly one event: {events:?}");
    let (event_type, stored) = &events[0];
    assert_eq!(event_type, "notes.vault.disconnected.v1");
    let envelope = EventEnvelope::from_json(stored.to_string().as_bytes())
        .expect("the outbox holds a contract envelope");
    assert_eq!(envelope.event_type, VaultDisconnected::event_type());
    assert_eq!(
        envelope.tenant_id.map(|tenant| tenant.to_string()),
        Some(format!("user:{OWNER_A}"))
    );
    let payload: VaultDisconnected = envelope.payload_as().expect("a VaultDisconnected payload");
    assert_eq!(payload.connection_id.0, id);
    assert_eq!(payload.channel, VaultChannel::Sync);
    assert_eq!(payload.vault_display_name.as_str(), "Personal");

    fixture
        .cleanup()
        .await
        .expect("the test database is dropped");
}
