//! Vault connections (workspace spec `notes-vault-connections`): the one-channel rule
//! (requirement "A vault connection has exactly one channel"), disconnect (requirement
//! "Connection events name the connection and never its secrets"), a connect request that
//! the client drops while the login runs (workspace task 5.14), and a connect that a stop of the
//! service, a disconnect or the limit of concurrent logins meets (workspace tasks 5.16 and 5.18,
//! the stop and the start also through `serve`).

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions read the JSON answers they build"
)]

mod support;

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Router;
use axum::http::{Method, StatusCode};
use ratatoskr_event_envelope::{EventEnvelope, EventPayload as _};
use ratatoskr_notes::api::{ApiState, domain_router};
use ratatoskr_notes_contracts::{VaultChannel, VaultDisconnected};
use ratatoskr_notes_persistence::test_support::TestDatabase;
use ratatoskr_notes_persistence::{NewConnection, NewSecret};
use tokio::sync::{Notify, mpsc};
use uuid::Uuid;

use crate::support::systemd::{Call, FakeSystemd};
use crate::support::{
    connect, connection_id, login_ended, run_service, send, send_tcp, terminal_report,
};

const OWNER_A: Uuid = Uuid::from_u128(0x0192_0000_0000_7000_8000_0000_0000_000a);
const OWNER_B: Uuid = Uuid::from_u128(0x0192_0000_0000_7000_8000_0000_0000_000b);
const REMOTE_VAULT_ID: &str = "remote-vault-0001";

fn state(fixture: &TestDatabase, systemd: &FakeSystemd) -> ApiState<FakeSystemd> {
    ApiState::new(fixture.database.clone(), systemd.clone())
        .with_key_file(systemd.key_file())
        .with_credentials_dir(systemd.credentials_root())
}

fn router(fixture: &TestDatabase, systemd: &FakeSystemd) -> Router {
    domain_router(state(fixture, systemd))
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
/// working tree with a note, and the client state with the token and the vault configuration.
fn leave_sync_state(
    systemd: &FakeSystemd,
    connection_id: Uuid,
) -> (std::path::PathBuf, std::path::PathBuf) {
    let tree = systemd.tree_dir(connection_id);
    let client_state = systemd.client_dir(connection_id);
    let state = client_state.join("obsidian-headless");
    std::fs::create_dir_all(tree.join("Daily")).expect("the tree is created");
    std::fs::write(tree.join("Daily/2026-10-13.md"), "# A note\n").expect("a note is written");
    std::fs::create_dir_all(state.join("sync").join(REMOTE_VAULT_ID))
        .expect("the client state is created");
    std::fs::write(state.join("auth_token"), "FAKE-TOKEN-MARKER").expect("a token is written");
    std::fs::write(
        state.join("sync").join(REMOTE_VAULT_ID).join("config.json"),
        r#"{"encryptionKey":"FAKE-KEY-MARKER"}"#,
    )
    .expect("a vault configuration is written");
    (tree, client_state)
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
    assert_eq!(
        login_ended(&fixture, id).await,
        "connecting",
        "the login finished"
    );
    let calls_of_connect = systemd.calls().len();
    let ob_of_connect = systemd.ob_invocations().len();
    let (tree, client_state) = leave_sync_state(&systemd, id);
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

    // The sync unit and a login that still runs are stopped, then the cleanup unit runs, through
    // the bus and nothing else.
    assert_eq!(
        systemd.calls()[calls_of_connect..],
        [
            Call::Stop(format!("ratatoskr-notes-sync@{id}.service")),
            Call::Stop(format!("ratatoskr-notes-login@{id}.service")),
            Call::Start(format!("ratatoskr-notes-cleanup@{id}.service")),
        ]
    );
    assert_eq!(
        systemd.ob_invocations()[ob_of_connect..],
        [format!("sync-unlink --path {}", tree.display())]
    );
    assert!(!tree.exists(), "the working tree is gone");
    assert!(!client_state.exists(), "the client state is gone");

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

/// How long a test waits for what a dropped connect request leaves to finish.
const SETTLE: Duration = Duration::from_secs(10);

/// The connection id of the first login unit that the service started, once it started.
async fn first_login_started(systemd: &FakeSystemd) -> Option<Uuid> {
    let deadline = Instant::now() + SETTLE;
    loop {
        let started = systemd.calls().iter().find_map(|call| match call {
            Call::Start(unit) => unit
                .strip_prefix("ratatoskr-notes-login@")
                .and_then(|rest| rest.strip_suffix(".service"))
                .and_then(|id| Uuid::parse_str(id).ok()),
            Call::Stop(_) => None,
        });
        if started.is_some() || Instant::now() >= deadline {
            return started;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The service stopped the login unit of a connection after it started it, and ran the cleanup unit
/// after the stop.
fn assert_login_stopped_then_cleaned_up(systemd: &FakeSystemd, id: Uuid) {
    let calls = systemd.calls();
    let position = |call: Call| calls.iter().position(|made| *made == call);
    let started = position(Call::Start(format!("ratatoskr-notes-login@{id}.service")));
    let stopped = position(Call::Stop(format!("ratatoskr-notes-login@{id}.service")));
    let cleaned = position(Call::Start(format!("ratatoskr-notes-cleanup@{id}.service")));
    assert!(
        matches!(
            (started, stopped, cleaned),
            (Some(started), Some(stopped), Some(cleaned)) if started < stopped && stopped < cleaned
        ),
        "the login starts, then stops, then the cleanup unit runs: {calls:?}"
    );
}

/// A directory that does not exist or is empty.
fn holds_nothing(directory: &Path) -> bool {
    match std::fs::read_dir(directory) {
        Ok(mut entries) => entries.next().is_none(),
        Err(_) => true,
    }
}

/// The state of a connection now.
async fn state_of(fixture: &TestDatabase, connection_id: Uuid) -> String {
    sqlx::query_scalar("select state from notes.connections where connection_id = $1")
        .bind(connection_id)
        .fetch_one(fixture.pool())
        .await
        .expect("the connection can be read")
}

/// The login unit of a connection was started.
fn login_started(systemd: &FakeSystemd, id: Uuid) -> bool {
    systemd
        .calls()
        .contains(&Call::Start(format!("ratatoskr-notes-login@{id}.service")))
}

#[tokio::test]
async fn shutdown_during_login_fails_the_connection() {
    let fixture = TestDatabase::create().await.expect("a test database");
    // The login unit runs until the service stops it.
    let systemd = FakeSystemd::new().hold_logins();
    let state = state(&fixture, &systemd);
    let router = domain_router(state.clone());

    let accepted = connection_id(&connect(&router, OWNER_A, "sync", REMOTE_VAULT_ID).await);
    let id = first_login_started(&systemd)
        .await
        .expect("the connect starts the login unit");
    assert_eq!(id, accepted);

    // The stop signal arrives while the login runs. The service fails the connect in the grace
    // window.
    let drained = tokio::time::timeout(SETTLE, state.shut_down()).await;
    assert!(drained.is_ok(), "the connects end in the grace window");
    assert_eq!(
        state_of(&fixture, id).await,
        "failed",
        "a stop of the service does not leave its connection connecting"
    );
    assert_login_stopped_then_cleaned_up(&systemd, id);
    assert_eq!(secrets_of(&fixture, id).await, 0, "the ciphertext is gone");
    assert!(
        holds_nothing(&systemd.client_dir(id)),
        "no client state of the stopped login stays"
    );
    let report = terminal_report(&fixture, id).await;
    assert_eq!(report["status"], "failed", "{report:?}");
    assert_eq!(report["error"]["code"], "notes.service.unavailable");

    // A connect that arrives after the stop signal starts no login.
    let late = connect(&router, OWNER_B, "sync", REMOTE_VAULT_ID).await;
    assert_eq!(
        late.status,
        StatusCode::SERVICE_UNAVAILABLE,
        "{:?}",
        late.body
    );
    assert_eq!(connections_of(&fixture, OWNER_B).await, 0);

    fixture
        .cleanup()
        .await
        .expect("the test database is dropped");
}

/// Inserts a `sync` connection of [`OWNER_A`] for [`REMOTE_VAULT_ID`] with its ciphertext whose
/// login never finished, as a stop of the service leaves it when the grace window ends first.
async fn insert_interrupted_login(fixture: &TestDatabase) -> Uuid {
    let interrupted = Uuid::now_v7();
    let nonce = [7_u8; 24];
    let ciphertext = [9_u8; 32];
    fixture
        .database
        .create_connection(&NewConnection {
            connection_id: interrupted,
            owner_id: OWNER_A,
            channel: "sync",
            vault_key: REMOTE_VAULT_ID,
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
    interrupted
}

#[tokio::test]
async fn next_start_fails_an_interrupted_login() {
    let fixture = TestDatabase::create().await.expect("a test database");
    let systemd = FakeSystemd::new();
    let router = router(&fixture, &systemd);
    // A `sync` connection whose login finished stays `connecting` until its first scan, and so
    // does a `plugin` connection. The start leaves both alone.
    let logged_in = connection_id(&connect(&router, OWNER_A, "sync", "remote-vault-0002").await);
    assert_eq!(login_ended(&fixture, logged_in).await, "connecting");
    let plugin = connection_id(&connect(&router, OWNER_A, "plugin", "plugin-vault-0001").await);

    // What a stop that the grace window cut short leaves: a `sync` connection with its ciphertext
    // whose login never finished, and what the login wrote on the host.
    let interrupted = insert_interrupted_login(&fixture).await;
    let (tree, client_state) = leave_sync_state(&systemd, interrupted);
    let calls_before = systemd.calls().len();

    // The next start of the service.
    let next = state(&fixture, &systemd);
    next.reconcile_interrupted_logins().await;

    assert_eq!(
        systemd.calls()[calls_before..],
        [
            Call::Stop(format!("ratatoskr-notes-sync@{interrupted}.service")),
            Call::Stop(format!("ratatoskr-notes-login@{interrupted}.service")),
            Call::Start(format!("ratatoskr-notes-cleanup@{interrupted}.service")),
        ],
        "the start stops the interrupted login and runs its cleanup unit, and nothing else"
    );
    assert_eq!(state_of(&fixture, interrupted).await, "failed");
    let report = terminal_report(&fixture, interrupted).await;
    assert_eq!(report["status"], "failed", "{report:?}");
    assert_eq!(report["error"]["code"], "notes.service.unavailable");
    assert_eq!(
        secrets_of(&fixture, interrupted).await,
        0,
        "the ciphertext is gone"
    );
    assert!(!tree.exists(), "the working tree is gone");
    assert!(!client_state.exists(), "the client state is gone");

    assert_eq!(state_of(&fixture, logged_in).await, "connecting");
    assert_eq!(secrets_of(&fixture, logged_in).await, 2);
    assert_eq!(state_of(&fixture, plugin).await, "connecting");

    fixture
        .cleanup()
        .await
        .expect("the test database is dropped");
}

#[tokio::test]
async fn connect_after_delete_starts_no_login() {
    let fixture = TestDatabase::create().await.expect("a test database");
    let systemd = FakeSystemd::new();
    // The connect waits after its insert until the test releases it.
    let (reached_sender, mut reached) = mpsc::unbounded_channel::<Uuid>();
    let release = Arc::new(Notify::new());
    let router = domain_router(state(&fixture, &systemd).with_before_login({
        let release = Arc::clone(&release);
        move |connection_id| {
            let reached = reached_sender.clone();
            let release = Arc::clone(&release);
            async move {
                let _ = reached.send(connection_id);
                release.notified().await;
            }
        }
    }));

    let accepted = connection_id(&connect(&router, OWNER_A, "sync", REMOTE_VAULT_ID).await);
    let id = tokio::time::timeout(SETTLE, reached.recv())
        .await
        .expect("the connect inserts its connection")
        .expect("the connect names its connection");

    // The owner removes the connection between its insert and the start of its login.
    let removed = send(
        &router,
        Method::DELETE,
        &format!("/v1/notes/connections/{id}"),
        OWNER_A,
        None,
    )
    .await;
    assert_eq!(removed.status, StatusCode::NO_CONTENT, "{:?}", removed.body);
    release.notify_one();
    let report = terminal_report(&fixture, id).await;

    assert_eq!(id, accepted);
    assert!(
        !login_started(&systemd, id),
        "no login unit starts for a removed connection: {:?}",
        systemd.calls()
    );
    assert_eq!(report["status"], "failed", "{report:?}");
    assert_eq!(report["error"]["code"], "notes.connection.not_found");
    assert_eq!(state_of(&fixture, id).await, "removed");
    assert_eq!(secrets_of(&fixture, id).await, 0, "the ciphertext is gone");
    assert!(
        !systemd.credentials_root().join(id.to_string()).exists(),
        "no credential files are written"
    );

    fixture
        .cleanup()
        .await
        .expect("the test database is dropped");
}

#[tokio::test]
async fn logins_are_bounded() {
    let fixture = TestDatabase::create().await.expect("a test database");
    let systemd = FakeSystemd::new().hold_logins();
    let router = domain_router(state(&fixture, &systemd).with_max_logins(1));

    let accepted = connection_id(&connect(&router, OWNER_A, "sync", REMOTE_VAULT_ID).await);
    let first_id = first_login_started(&systemd)
        .await
        .expect("the first connect starts its login unit");
    assert_eq!(first_id, accepted);

    // One login runs, and the limit is one: the next `sync` connect is refused at once.
    let extra = tokio::time::timeout(
        Duration::from_secs(5),
        connect(&router, OWNER_B, "sync", REMOTE_VAULT_ID),
    )
    .await;
    assert!(
        extra.is_ok(),
        "a connect over the limit answers at once and does not wait for a login: {:?}",
        systemd.calls()
    );
    let extra = extra.expect("checked above");
    assert_eq!(
        extra.status,
        StatusCode::SERVICE_UNAVAILABLE,
        "{:?}",
        extra.body
    );
    assert_eq!(extra.body["code"], "notes.service.unavailable");
    assert_eq!(extra.body["retryable"], true);
    assert_eq!(
        connections_of(&fixture, OWNER_B).await,
        0,
        "the refused connect creates no connection"
    );

    // A `plugin` connect runs no login, so the limit does not apply to it.
    connection_id(&connect(&router, OWNER_B, "plugin", "plugin-vault-0001").await);

    // When the first connect ends its login, its place is free again.
    systemd.release_logins();
    assert_eq!(login_ended(&fixture, first_id).await, "connecting");
    let deadline = Instant::now() + SETTLE;
    let mut again = connect(&router, OWNER_B, "sync", REMOTE_VAULT_ID).await;
    while again.status == StatusCode::SERVICE_UNAVAILABLE && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
        again = connect(&router, OWNER_B, "sync", REMOTE_VAULT_ID).await;
    }
    connection_id(&again);

    fixture
        .cleanup()
        .await
        .expect("the test database is dropped");
}

#[tokio::test]
async fn serve_shuts_down_logins_on_stop() {
    let fixture = TestDatabase::create().await.expect("a test database");
    // The login unit runs until the service stops it.
    let systemd = FakeSystemd::new().hold_logins();
    let api = support::free_address();

    let steps = async {
        let body = support::sync_body(REMOTE_VAULT_ID, support::MARKER_SECRETS);
        let accepted = send_tcp(
            api,
            Method::POST,
            "/v1/notes/connections",
            OWNER_A,
            Some(body),
        )
        .await;
        let id = first_login_started(&systemd).await;
        assert!(
            id.is_some(),
            "a connect to the listener of serve starts the login unit through the given bus: {:?}",
            systemd.calls()
        );
        assert_eq!(id, Some(connection_id(&accepted)));
        id.expect("checked above")
    };
    // The stop signal arrives while the login runs.
    let (id, stopped) = run_service(&fixture, &systemd, steps, api).await;

    assert!(
        stopped,
        "serve stops in the grace window when its stop signal arrives"
    );
    assert_eq!(
        state_of(&fixture, id).await,
        "failed",
        "a stop of the service does not leave its connection connecting"
    );
    assert_login_stopped_then_cleaned_up(&systemd, id);
    assert_eq!(secrets_of(&fixture, id).await, 0, "the ciphertext is gone");
    let report = terminal_report(&fixture, id).await;
    assert_eq!(report["status"], "failed", "{report:?}");
    assert_eq!(report["error"]["code"], "notes.service.unavailable");

    fixture
        .cleanup()
        .await
        .expect("the test database is dropped");
}

#[tokio::test]
async fn serve_reconciles_before_it_accepts() {
    let fixture = TestDatabase::create().await.expect("a test database");
    let systemd = FakeSystemd::new();
    let interrupted = insert_interrupted_login(&fixture).await;
    let api = support::free_address();

    // The first request connects the same remote vault again. The interrupted connection holds
    // the vault key until the start fails it, so the request is accepted only when the start
    // failed it before the listener accepted the request.
    let body = support::sync_body(REMOTE_VAULT_ID, support::MARKER_SECRETS);
    let first = async {
        let first = send_tcp(
            api,
            Method::POST,
            "/v1/notes/connections",
            OWNER_A,
            Some(body),
        )
        .await;
        if first.status == StatusCode::ACCEPTED {
            // The login runs after the answer; the test stops the service after it.
            login_ended(&fixture, connection_id(&first)).await;
        }
        first
    };
    let (first, stopped) = run_service(&fixture, &systemd, first, api).await;

    assert_eq!(
        first.status,
        StatusCode::ACCEPTED,
        "the start fails the interrupted login before the first request: {:?}",
        first.body
    );
    let id = connection_id(&first);
    assert_eq!(state_of(&fixture, interrupted).await, "failed");
    assert_eq!(
        secrets_of(&fixture, interrupted).await,
        0,
        "the ciphertext is gone"
    );
    assert_eq!(
        systemd.calls(),
        [
            Call::Stop(format!("ratatoskr-notes-sync@{interrupted}.service")),
            Call::Stop(format!("ratatoskr-notes-login@{interrupted}.service")),
            Call::Start(format!("ratatoskr-notes-cleanup@{interrupted}.service")),
            Call::Start(format!("ratatoskr-notes-login@{id}.service")),
            Call::Start(format!("ratatoskr-notes-sync@{id}.service")),
        ],
        "the start reconciles through the given bus before the first request starts its login"
    );
    assert!(stopped, "serve stops without a failure");

    fixture
        .cleanup()
        .await
        .expect("the test database is dropped");
}

#[tokio::test]
async fn delete_during_login_answers_not_found() {
    let fixture = TestDatabase::create().await.expect("a test database");
    // The login unit runs until the disconnect stops it.
    let systemd = FakeSystemd::new().hold_logins();
    let router = router(&fixture, &systemd);

    let accepted = connection_id(&connect(&router, OWNER_A, "sync", REMOTE_VAULT_ID).await);
    let id = first_login_started(&systemd)
        .await
        .expect("the connect starts the login unit");
    assert_eq!(id, accepted);

    // The owner removes the connection while its login runs.
    let removed = send(
        &router,
        Method::DELETE,
        &format!("/v1/notes/connections/{id}"),
        OWNER_A,
        None,
    )
    .await;
    assert_eq!(removed.status, StatusCode::NO_CONTENT, "{:?}", removed.body);

    let report = terminal_report(&fixture, id).await;
    assert_eq!(
        report["error"]["code"], "notes.connection.not_found",
        "the login that the disconnect stopped did not fail; its connection is gone: {report:?}"
    );
    assert_eq!(report["status"], "failed");
    assert_eq!(state_of(&fixture, id).await, "removed");
    assert_eq!(secrets_of(&fixture, id).await, 0, "the ciphertext is gone");

    fixture
        .cleanup()
        .await
        .expect("the test database is dropped");
}
