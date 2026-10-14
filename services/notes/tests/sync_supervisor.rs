//! The Sync supervisor (workspace spec `notes-vault-connections`, requirement "The host is a named
//! Sync device in pull-only mode", workspace design D3).
//!
//! The tests run the real `serve()` with the fake systemd bus and the compose database. The fake
//! runs the `ExecStartPre=` and `ExecStart=` lines of the shipped
//! `deploy/systemd/ratatoskr-notes-sync@.service` with a stub `ob` that records its arguments, so
//! the tests check the shipped unit.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions read the calls and the answers they record"
)]

mod support;

use axum::http::{Method, StatusCode};
use ratatoskr_notes_persistence::test_support::TestDatabase;
use uuid::Uuid;

use crate::support::systemd::{Call, FakeSystemd};
use crate::support::{connection_id, free_address, run_service, send_tcp, sync_body};

const OWNER: Uuid = Uuid::from_u128(0x0192_0000_0000_7000_8000_0000_0000_00d1);
const REMOTE_VAULT_ID: &str = "remote-vault-0061";

/// The calls of `ob` that configure and run the sync of a connection, in order: `sync-config` and
/// `sync`. The login unit's `login` and `sync-setup` are not in it.
fn sync_calls(arguments: &[Vec<String>]) -> Vec<Vec<String>> {
    arguments
        .iter()
        .filter(|call| {
            matches!(
                call.first().map(String::as_str),
                Some("sync-config" | "sync")
            )
        })
        .cloned()
        .collect()
}

#[tokio::test]
async fn connect_starts_one_unit_in_pull_only_mode() {
    let fixture = TestDatabase::create().await.expect("a test database");
    let systemd = FakeSystemd::new();

    // The first start of the service: the owner connects a remote vault through Sync.
    let api = free_address();
    let connect = send_tcp(
        api,
        Method::POST,
        "/v1/notes/connections",
        OWNER,
        Some(sync_body(REMOTE_VAULT_ID, support::MARKER_SECRETS)),
    );
    let (answer, stopped) = run_service(&fixture, &systemd, connect, api).await;
    assert!(stopped, "serve stops without a failure");
    let id = connection_id(&answer);
    let calls_of_first_start = systemd.calls();
    let ob_of_first_start = systemd.ob_arguments();

    // The next start of the service. The first answer of its listener comes after the start.
    let api = free_address();
    let unknown = format!("/v1/notes/connections/{}", Uuid::nil());
    let request = send_tcp(api, Method::DELETE, &unknown, OWNER, None);
    let (answer, stopped) = run_service(&fixture, &systemd, request, api).await;
    assert!(stopped, "serve stops without a failure");
    assert_eq!(answer.status, StatusCode::NOT_FOUND, "{:?}", answer.body);

    let tree = systemd
        .connection_dir(id)
        .join("tree")
        .display()
        .to_string();
    let device_name = format!("ratatoskr-{id}");
    let configure: Vec<String> = [
        "sync-config",
        "--path",
        &tree,
        "--mode",
        "pull-only",
        "--configs",
        "",
        "--device-name",
        &device_name,
    ]
    .map(str::to_owned)
    .to_vec();
    let sync: Vec<String> = ["sync", "--continuous", "--path", &tree]
        .map(str::to_owned)
        .to_vec();
    let sync_unit = Call::Start(format!("ratatoskr-notes-sync@{id}.service"));

    assert_eq!(
        calls_of_first_start,
        [
            Call::Start(format!("ratatoskr-notes-login@{id}.service")),
            sync_unit.clone(),
        ],
        "after the login, the connect starts the one sync unit of the connection"
    );
    assert_eq!(
        sync_calls(&ob_of_first_start),
        [configure.clone(), sync.clone()],
        "the sync unit configures pull-only mode, no configuration sync and the device name, then \
         syncs continuously"
    );
    assert_eq!(
        systemd.calls()[calls_of_first_start.len()..],
        [sync_unit],
        "the next start of the service starts the same unit again, and no other"
    );
    assert_eq!(
        sync_calls(&systemd.ob_arguments()[ob_of_first_start.len()..]),
        [configure, sync],
        "each start of the unit sets pull-only mode again"
    );

    fixture
        .cleanup()
        .await
        .expect("the test database is dropped");
}
