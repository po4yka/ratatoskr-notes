//! The write mode sets the mode of the Headless client of a `sync` connection (workspace change
//! `add-notes-write-back`, spec `notes-write-delivery`, requirement "The write mode sets the
//! Headless client mode", task 6.3).
//!
//! The tests run the real `serve()` with the fake systemd bus, which runs the lines of the shipped
//! `deploy/systemd/ratatoskr-notes-sync-pull@.service` and `-bidi@.service` with a stub `ob` that records its arguments.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions read the calls and the answers they record"
)]

mod support;

use std::time::{Duration, Instant};

use axum::http::{Method, StatusCode};
use ratatoskr_notes_persistence::test_support::TestDatabase;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::support::systemd::{Call, FakeSystemd};
use crate::support::{connection_id, free_address, run_service, send_tcp, send_tcp_as, sync_body};

const OWNER: Uuid = Uuid::from_u128(0x0192_0000_0000_7000_8000_0000_0000_06d3);
const REMOTE_VAULT_ID: &str = "remote-vault-0063";

/// The arguments of each `ob sync-config` call, in order.
fn sync_configs(systemd: &FakeSystemd) -> Vec<Vec<String>> {
    systemd
        .ob_arguments()
        .into_iter()
        .filter(|call| call.first().map(String::as_str) == Some("sync-config"))
        .collect()
}

/// The value that follows `flag` in `call`.
fn flag<'a>(call: &'a [String], flag: &str) -> Option<&'a str> {
    call.iter()
        .position(|word| word == flag)
        .and_then(|at| call.get(at + 1))
        .map(String::as_str)
}

/// The terminal report of the operation `operation_id`, or `Value::Null` after 30 seconds.
async fn terminal(fixture: &TestDatabase, operation_id: Uuid) -> Value {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let found: Option<Value> = sqlx::query_scalar(
            "select payload -> 'payload' from notes.outbox
             where event_type = 'platform.operation.reported.v1'
               and payload -> 'payload' ->> 'operation_id' = $1
               and payload -> 'payload' ->> 'status' in ('succeeded', 'failed')
             order by position limit 1",
        )
        .bind(operation_id.to_string())
        .fetch_optional(fixture.pool())
        .await
        .expect("the outbox can be read");
        if let Some(found) = found {
            return found;
        }
        if Instant::now() >= deadline {
            return Value::Null;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Connect a `sync` vault through the real service, then send each write mode of `modes` in turn
/// and wait for the end of its operation. Returns the connection.
async fn connect_and_switch(fixture: &TestDatabase, systemd: &FakeSystemd, modes: &[&str]) -> Uuid {
    let api = free_address();
    let steps = async {
        let answer = send_tcp(
            api,
            Method::POST,
            "/v1/notes/connections",
            OWNER,
            Some(sync_body(REMOTE_VAULT_ID, support::MARKER_SECRETS)),
        )
        .await;
        let id = connection_id(&answer);
        support::login_ended(fixture, id).await;
        for mode in modes {
            let operation_id = Uuid::now_v7();
            let answer = send_tcp_as(
                api,
                Method::PUT,
                &format!("/v1/notes/connections/{id}/write-mode"),
                OWNER,
                operation_id,
                Some(json!({ "write_mode": mode })),
            )
            .await;
            assert_eq!(answer.status, StatusCode::ACCEPTED, "{:?}", answer.body);
            let ended = terminal(fixture, operation_id).await;
            assert_eq!(ended["status"], "succeeded", "{ended:?}");
        }
        id
    };
    let (id, stopped) = run_service(fixture, systemd, steps, api).await;
    assert!(stopped, "serve stops without a failure");
    id
}

#[tokio::test]
async fn write_mode_on_sets_bidirectional() {
    let fixture = TestDatabase::create().await.expect("a test database");
    let systemd = FakeSystemd::new();

    connect_and_switch(&fixture, &systemd, &["on"]).await;

    let configs = sync_configs(&systemd);
    let modes: Vec<Option<&str>> = configs.iter().map(|call| flag(call, "--mode")).collect();
    assert_eq!(
        modes,
        [Some("pull-only"), Some("bidirectional")],
        "the connect configures pull-only, and write mode on configures bidirectional: \
         {configs:?}"
    );
    assert_eq!(
        flag(&configs[1], "--conflict-strategy"),
        Some("conflict"),
        "write mode on keeps a race inside Obsidian Sync as a conflict file: {configs:?}"
    );

    fixture
        .cleanup()
        .await
        .expect("the test database is dropped");
}

#[tokio::test]
async fn write_mode_off_sets_pull_only() {
    let fixture = TestDatabase::create().await.expect("a test database");
    let systemd = FakeSystemd::new();

    connect_and_switch(&fixture, &systemd, &["on", "off"]).await;

    let configs = sync_configs(&systemd);
    let modes: Vec<Option<&str>> = configs.iter().map(|call| flag(call, "--mode")).collect();
    assert_eq!(
        modes,
        [Some("pull-only"), Some("bidirectional"), Some("pull-only")],
        "write mode off configures pull-only again: {configs:?}"
    );

    fixture
        .cleanup()
        .await
        .expect("the test database is dropped");
}

#[tokio::test]
async fn write_mode_switches_between_two_fixed_units() {
    let fixture = TestDatabase::create().await.expect("a test database");
    let systemd = FakeSystemd::new();

    let id = connect_and_switch(&fixture, &systemd, &["on", "off"]).await;

    let pull = format!("ratatoskr-notes-sync-pull@{id}.service");
    let bidi = format!("ratatoskr-notes-sync-bidi@{id}.service");
    let sync_calls: Vec<Call> = systemd
        .calls()
        .into_iter()
        .filter(|call| match call {
            Call::Start(unit) | Call::Stop(unit) => unit.starts_with("ratatoskr-notes-sync"),
        })
        .collect();
    assert_eq!(
        sync_calls,
        [
            Call::Stop(bidi.clone()),
            Call::Start(pull.clone()),
            Call::Stop(pull.clone()),
            Call::Start(bidi.clone()),
            Call::Stop(bidi),
            Call::Start(pull),
        ],
        "each start of the sync unit of one write mode stops the unit of the other first, so \
         the two never run together"
    );

    fixture
        .cleanup()
        .await
        .expect("the test database is dropped");
}
