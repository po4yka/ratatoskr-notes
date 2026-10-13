//! Vault connections: the one-channel rule (workspace spec `notes-vault-connections`, requirement
//! "A vault connection has exactly one channel").

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions read the JSON answers they build"
)]

mod support;

use axum::http::StatusCode;
use ratatoskr_notes::api::{ApiState, domain_router};
use ratatoskr_notes_persistence::test_support::TestDatabase;
use uuid::Uuid;

use crate::support::{connect, connection_id};

const OWNER_A: Uuid = Uuid::from_u128(0x0192_0000_0000_7000_8000_0000_0000_000a);
const OWNER_B: Uuid = Uuid::from_u128(0x0192_0000_0000_7000_8000_0000_0000_000b);
const REMOTE_VAULT_ID: &str = "remote-vault-0001";

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
    let router = domain_router(ApiState::new(fixture.database.clone()));

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
    let router = domain_router(ApiState::new(fixture.database.clone()));

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
