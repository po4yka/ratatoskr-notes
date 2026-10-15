//! The terminal report of a connect operation is written with the state that ends it (workspace
//! design D10): a stop of the process right after a connection was failed, removed or its login
//! expired must not leave the operation without its end. In each test, the hook
//! `ApiState::with_after_terminal_state` never ends, and stands for that stop.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions read the JSON events they build"
)]

mod support;

use std::future::Future;
use std::time::Duration;

use axum::http::Method;
use ratatoskr_notes::api::{ApiState, domain_router};
use ratatoskr_notes_persistence::test_support::TestDatabase;
use ratatoskr_notes_persistence::{NewConnection, NewSecret};
use serde_json::Value;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::support::systemd::FakeSystemd;
use crate::support::{MARKER_SECRETS, connect, connect_sync, connection_id, send};

const OWNER: Uuid = Uuid::from_u128(0x0192_0000_0000_7000_8000_0000_0000_0e01);

/// How long a test waits for the hook to run.
const HOOK_DEADLINE: Duration = Duration::from_mins(1);

/// The state of the domain routes, with a hook after a terminal state that never ends and sends
/// the connection id to the returned receiver.
fn stopping_state(
    fixture: &TestDatabase,
    systemd: &FakeSystemd,
) -> (ApiState<FakeSystemd>, mpsc::UnboundedReceiver<Uuid>) {
    let (stopped, receiver) = mpsc::unbounded_channel();
    let state = ApiState::new(fixture.database.clone(), systemd.clone())
        .with_key_file(systemd.key_file())
        .with_credentials_dir(systemd.credentials_root())
        .with_trees_root(systemd.trees_root())
        .with_after_terminal_state(move |connection_id| {
            let stopped = stopped.clone();
            async move {
                let _ = stopped.send(connection_id);
                std::future::pending::<()>().await;
            }
        });
    (state, receiver)
}

/// Run `step` until the hook stops the process, and return the connection id that it names.
async fn until_stopped(step: impl Future, stops: &mut mpsc::UnboundedReceiver<Uuid>) -> Uuid {
    let waited = tokio::time::timeout(HOOK_DEADLINE, async {
        tokio::select! {
            _ = step => stops.recv().await,
            stopped = stops.recv() => stopped,
        }
    })
    .await;
    waited
        .ok()
        .flatten()
        .expect("the process stops after the terminal state")
}

/// The terminal reports of the connect operation of a connection, written by now.
async fn terminal_reports(fixture: &TestDatabase, connection_id: Uuid) -> Vec<Value> {
    sqlx::query_scalar(
        "select o.payload -> 'payload' from notes.outbox o
         join notes.connections c
           on o.payload -> 'payload' ->> 'operation_id' = c.operation_id::text
         where c.connection_id = $1
           and o.event_type = 'platform.operation.reported.v1'
           and o.payload -> 'payload' ->> 'status'
               in ('succeeded', 'partially_succeeded', 'failed')",
    )
    .bind(connection_id)
    .fetch_all(fixture.pool())
    .await
    .expect("the outbox can be read")
}

async fn state_of(fixture: &TestDatabase, connection_id: Uuid) -> String {
    sqlx::query_scalar("select state from notes.connections where connection_id = $1")
        .bind(connection_id)
        .fetch_one(fixture.pool())
        .await
        .expect("the connection can be read")
}

/// Wait until the connection is in `wanted`, or the deadline passes.
async fn wait_for_state(pool: &sqlx::PgPool, connection_id: Uuid, wanted: &str) {
    let deadline = tokio::time::Instant::now() + HOOK_DEADLINE;
    while tokio::time::Instant::now() < deadline {
        let state: String =
            sqlx::query_scalar("select state from notes.connections where connection_id = $1")
                .bind(connection_id)
                .fetch_one(pool)
                .await
                .expect("the connection can be read");
        if state == wanted {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Assert one terminal report `failed` with `code` for the connection.
async fn assert_one_failure(fixture: &TestDatabase, connection_id: Uuid, code: &str) {
    let reports = terminal_reports(fixture, connection_id).await;
    assert_eq!(
        reports.len(),
        1,
        "the operation has its end when the process stops: {reports:?}"
    );
    assert_eq!(reports[0]["status"], "failed", "{reports:?}");
    assert_eq!(reports[0]["error"]["code"], code, "{reports:?}");
}

#[tokio::test]
async fn failed_login_has_its_report_at_once() {
    let fixture = TestDatabase::create().await.expect("a test database");
    let systemd = FakeSystemd::new().refuse_e2e_password();
    let (state, mut stops) = stopping_state(&fixture, &systemd);
    let router = domain_router(state);

    let accepted = connect_sync(&router, OWNER, "remote-vault-0e01", MARKER_SECRETS).await;
    let connection = connection_id(&accepted);
    let stopped = until_stopped(std::future::pending::<()>(), &mut stops).await;

    assert_eq!(stopped, connection);
    assert_eq!(state_of(&fixture, connection).await, "failed");
    assert_one_failure(&fixture, connection, "notes.sync.e2e_password_wrong").await;

    fixture
        .cleanup()
        .await
        .expect("the test database is dropped");
}

#[tokio::test]
async fn interrupted_login_has_its_report_at_once() {
    let fixture = TestDatabase::create().await.expect("a test database");
    let systemd = FakeSystemd::new();
    let interrupted = Uuid::now_v7();
    let nonce = [7_u8; 24];
    let ciphertext = [9_u8; 32];
    fixture
        .database
        .create_connection(&NewConnection {
            connection_id: interrupted,
            owner_id: OWNER,
            channel: "sync",
            vault_key: "remote-vault-0e02",
            vault_display_name: "Personal",
            secrets: &[NewSecret {
                kind: "account_password",
                nonce: &nonce,
                ciphertext: &ciphertext,
            }],
            operation_id: Uuid::now_v7(),
            device_id: None,
        })
        .await
        .expect("the interrupted connection is inserted");

    // The next start of the service.
    let (state, mut stops) = stopping_state(&fixture, &systemd);
    let stopped = until_stopped(state.reconcile_interrupted_logins(), &mut stops).await;

    assert_eq!(stopped, interrupted);
    assert_eq!(state_of(&fixture, interrupted).await, "failed");
    assert_one_failure(&fixture, interrupted, "notes.service.unavailable").await;

    fixture
        .cleanup()
        .await
        .expect("the test database is dropped");
}

#[tokio::test]
async fn removed_waiting_connect_has_its_report_at_once() {
    let fixture = TestDatabase::create().await.expect("a test database");
    let systemd = FakeSystemd::new();
    let (state, mut stops) = stopping_state(&fixture, &systemd);
    let router = domain_router(state);
    let connection = connection_id(&connect(&router, OWNER, "plugin", "vault-key-0e03").await);

    let path = format!("/v1/notes/connections/{connection}");
    let stopped = until_stopped(
        send(&router, Method::DELETE, &path, OWNER, None),
        &mut stops,
    )
    .await;

    assert_eq!(stopped, connection);
    assert_eq!(state_of(&fixture, connection).await, "removed");
    assert_one_failure(&fixture, connection, "notes.connection.not_found").await;

    fixture
        .cleanup()
        .await
        .expect("the test database is dropped");
}

#[tokio::test]
async fn expired_login_has_its_report_at_once() {
    let fixture = TestDatabase::create().await.expect("a test database");
    let systemd = FakeSystemd::new();
    // A `sync` connection whose login finished and whose first scan did not end yet.
    let connection = Uuid::now_v7();
    sqlx::query(
        "insert into notes.connections
             (connection_id, owner_id, channel, vault_key, vault_display_name, state,
              logged_in_at, operation_id, created_at, updated_at)
         values ($1, $2, 'sync', 'remote-vault-0e04', 'Personal', 'connecting', now(), $3,
                 now(), now())",
    )
    .bind(connection)
    .bind(OWNER)
    .bind(Uuid::now_v7())
    .execute(fixture.pool())
    .await
    .expect("a connecting connection is inserted");
    let unit = format!("ratatoskr-notes-sync@{connection}.service");

    let (state, mut stops) = stopping_state(&fixture, &systemd);
    state.resume_sync_connections().await;
    systemd
        .journals()
        .log_auth_failures(&unit, Duration::from_mins(10));
    let stopped = until_stopped(std::future::pending::<()>(), &mut stops).await;

    assert_eq!(stopped, connection);
    assert_eq!(state_of(&fixture, connection).await, "login_expired");
    assert_one_failure(&fixture, connection, "notes.sync.login_expired").await;

    fixture
        .cleanup()
        .await
        .expect("the test database is dropped");
}

#[tokio::test]
async fn login_expiry_before_its_record_reports_once() {
    let fixture = TestDatabase::create().await.expect("a test database");
    let systemd = FakeSystemd::new();
    // The sync unit runs, and its login expires before the connect records the login.
    let (pool, journals) = (fixture.pool().clone(), systemd.clone());
    let state = ApiState::new(fixture.database.clone(), systemd.clone())
        .with_key_file(systemd.key_file())
        .with_credentials_dir(systemd.credentials_root())
        .with_trees_root(systemd.trees_root())
        .with_after_sync_start(move |connection_id| {
            let (pool, journals) = (pool.clone(), journals.clone());
            async move {
                let unit = format!("ratatoskr-notes-sync@{connection_id}.service");
                journals
                    .journals()
                    .log_auth_failures(&unit, Duration::from_mins(10));
                wait_for_state(&pool, connection_id, "login_expired").await;
            }
        });
    let router = domain_router(state.clone());

    let accepted = connect_sync(&router, OWNER, "remote-vault-0e05", MARKER_SECRETS).await;
    let connection = connection_id(&accepted);
    // The login ran, and the hook saw it expire; the stop then waits for the rest of the task of
    // the connect.
    wait_for_state(fixture.pool(), connection, "login_expired").await;
    state.shut_down().await;

    assert_eq!(state_of(&fixture, connection).await, "login_expired");
    assert_one_failure(&fixture, connection, "notes.sync.login_expired").await;

    fixture
        .cleanup()
        .await
        .expect("the test database is dropped");
}
