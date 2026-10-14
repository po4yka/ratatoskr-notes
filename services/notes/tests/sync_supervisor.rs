//! The Sync supervisor (workspace spec `notes-vault-connections`, requirements "The host is a named
//! Sync device in pull-only mode" and "An expired Sync login stops the connection and waits for the
//! owner", workspace design D3, review items M1 and M2 of group 6).
//!
//! The tests run the real `serve()` with the fake systemd bus and the compose database. The fake
//! runs the `ExecStartPre=` and `ExecStart=` lines of the shipped
//! `deploy/systemd/ratatoskr-notes-sync@.service` with a stub `ob` that records its arguments, so
//! the tests check the shipped unit. The stub `ob` of a sync unit logs to a fake journal under
//! paused time (`support/journal.rs`). The fake can also end the journal follower, as a stop of
//! `journalctl` does, and refuse the journal to the service, as journald refuses a user without
//! read access to the namespace of the sync units (owner decision A of 2026-10-14).

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions read the calls and the answers they record"
)]

mod support;

use std::time::Duration;

use axum::http::{Method, StatusCode};
use ratatoskr_notes_persistence::test_support::TestDatabase;
use uuid::Uuid;

use crate::support::logs::CapturedLogs;
use crate::support::systemd::{Call, FakeSystemd, UNREADABLE_JOURNAL};
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
    let connect = async {
        let answer = send_tcp(
            api,
            Method::POST,
            "/v1/notes/connections",
            OWNER,
            Some(sync_body(REMOTE_VAULT_ID, support::MARKER_SECRETS)),
        )
        .await;
        // The login and the start of the sync unit run after the answer.
        support::login_ended(&fixture, connection_id(&answer)).await;
        answer
    };
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

    let tree = systemd.tree_dir(id).display().to_string();
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

/// Inserts a `sync` connection whose login finished and whose first scan was indexed.
async fn insert_active_connection(fixture: &TestDatabase) -> Uuid {
    let id = Uuid::now_v7();
    sqlx::query(
        "insert into notes.connections
             (connection_id, owner_id, channel, vault_key, vault_display_name, state,
              logged_in_at, created_at, updated_at)
         values ($1, $2, 'sync', $3, 'Personal', 'active', now(), now(), now())",
    )
    .bind(id)
    .bind(OWNER)
    .bind(REMOTE_VAULT_ID)
    .execute(fixture.pool())
    .await
    .expect("an active connection is inserted");
    id
}

async fn state_of(fixture: &TestDatabase, connection_id: Uuid) -> String {
    sqlx::query_scalar("select state from notes.connections where connection_id = $1")
        .bind(connection_id)
        .fetch_one(fixture.pool())
        .await
        .expect("the connection can be read")
}

#[tokio::test]
async fn auth_failure_stops_the_unit_and_waits_for_the_owner() {
    let logs = CapturedLogs::install();
    let fixture = TestDatabase::create().await.expect("a test database");
    let systemd = FakeSystemd::new();
    let id = insert_active_connection(&fixture).await;
    let unit = format!("ratatoskr-notes-sync@{id}.service");

    let api = free_address();
    let unknown = format!("/v1/notes/connections/{}", Uuid::nil());
    let steps = async {
        // The start of the service starts the unit before its listener answers.
        let answer = send_tcp(api, Method::DELETE, &unknown, OWNER, None).await;
        assert_eq!(answer.status, StatusCode::NOT_FOUND, "{:?}", answer.body);
        // The login of the account expired: the stub ob logs the failure every 30 seconds, for 10
        // minutes of paused time.
        systemd
            .journals()
            .log_auth_failures(&unit, Duration::from_mins(10));
        systemd.wait_for_stop(&unit).await
    };
    let (unit_stopped, stopped) = run_service(&fixture, &systemd, steps, api).await;
    assert!(stopped, "serve stops without a failure");

    assert_eq!(
        state_of(&fixture, id).await,
        "login_expired",
        "an authentication failure for 5 minutes expires the login"
    );
    assert!(
        unit_stopped,
        "the sync unit is stopped: {:?}",
        systemd.calls()
    );
    assert_eq!(
        systemd.journals().read_at_stops(&unit),
        [Some(Duration::from_mins(5))],
        "the unit is stopped once, at the first failure 5 minutes after the first one"
    );
    assert!(
        !systemd
            .calls()
            .contains(&Call::Start(format!("ratatoskr-notes-login@{id}.service"))),
        "no login starts: the owner connects again: {:?}",
        systemd.calls()
    );
    assert!(
        logs.text().contains("notes.sync.login_expired"),
        "the service reports the code notes.sync.login_expired"
    );

    fixture
        .cleanup()
        .await
        .expect("the test database is dropped");
}

#[tokio::test]
async fn unreadable_journal_is_reported_at_start() {
    let logs = CapturedLogs::install();
    let fixture = TestDatabase::create().await.expect("a test database");
    let systemd = FakeSystemd::new().unreadable_journal();

    let api = free_address();
    let unknown = format!("/v1/notes/connections/{}", Uuid::nil());
    let request = send_tcp(api, Method::DELETE, &unknown, OWNER, None);
    let (answer, stopped) = run_service(&fixture, &systemd, request, api).await;
    assert!(stopped, "serve stops without a failure");
    assert_eq!(answer.status, StatusCode::NOT_FOUND, "{:?}", answer.body);

    // The buffer holds the lines of every test of this binary, and a follower that ends in
    // another test logs the same code without a reason. So the line is any line with both.
    let text = logs.text();
    let reported: Vec<&str> = text
        .lines()
        .filter(|line| line.contains("notes.sync.journal_unreadable"))
        .collect();
    assert!(
        reported
            .iter()
            .any(|line| line.contains(UNREADABLE_JOURNAL)),
        "the start of the service reports the code notes.sync.journal_unreadable with the reason \
         of journalctl: {reported:?}"
    );

    fixture
        .cleanup()
        .await
        .expect("the test database is dropped");
}

#[tokio::test]
async fn ended_follower_is_started_again() {
    let logs = CapturedLogs::install();
    let fixture = TestDatabase::create().await.expect("a test database");
    let systemd = FakeSystemd::new();
    let id = insert_active_connection(&fixture).await;
    let unit = format!("ratatoskr-notes-sync@{id}.service");

    let api = free_address();
    let unknown = format!("/v1/notes/connections/{}", Uuid::nil());
    let steps = async {
        let answer = send_tcp(api, Method::DELETE, &unknown, OWNER, None).await;
        assert_eq!(answer.status, StatusCode::NOT_FOUND, "{:?}", answer.body);
        let journals = systemd.journals();
        let first = journals.wait_for_followers(1).await;
        // journalctl stops, for example because journald restarted.
        journals.end_followers();
        let again = journals.wait_for_followers(2).await;
        // After that, the login of the account expires.
        journals.log_auth_failures(&unit, Duration::from_mins(10));
        let unit_stopped = systemd.wait_for_stop(&unit).await;
        (first, again, unit_stopped)
    };
    let ((first, again, unit_stopped), stopped) = run_service(&fixture, &systemd, steps, api).await;
    assert!(stopped, "serve stops without a failure");

    assert!(first, "the service follows the journal of the sync units");
    assert!(
        again,
        "the service follows the journal again after the follower ended"
    );
    assert!(
        unit_stopped,
        "an authentication failure for 5 minutes after that stops the unit: {:?}",
        systemd.calls()
    );
    assert_eq!(state_of(&fixture, id).await, "login_expired");
    assert_eq!(
        systemd.journals().read_at_stops(&unit),
        [Some(Duration::from_mins(5))],
        "the unit is stopped once, at the first failure 5 minutes after the first one"
    );
    assert!(
        logs.text().contains("notes.sync.login_expired"),
        "the service reports the code notes.sync.login_expired"
    );

    fixture
        .cleanup()
        .await
        .expect("the test database is dropped");
}

#[tokio::test]
async fn lines_go_to_their_own_connection() {
    let fixture = TestDatabase::create().await.expect("a test database");
    let systemd = FakeSystemd::new();
    let expired = insert_active_connection(&fixture).await;
    let other_vault = Uuid::now_v7();
    sqlx::query(
        "insert into notes.connections
             (connection_id, owner_id, channel, vault_key, vault_display_name, state,
              logged_in_at, created_at, updated_at)
         values ($1, $2, 'sync', 'remote-vault-0062', 'Work', 'active', now(), now(), now())",
    )
    .bind(other_vault)
    .bind(OWNER)
    .execute(fixture.pool())
    .await
    .expect("a second active connection is inserted");
    let expired_unit = format!("ratatoskr-notes-sync@{expired}.service");
    let other_unit = format!("ratatoskr-notes-sync@{other_vault}.service");
    // A sync unit that this process did not start.
    let unknown_unit = format!("ratatoskr-notes-sync@{}.service", Uuid::now_v7());

    let api = free_address();
    let unknown = format!("/v1/notes/connections/{}", Uuid::nil());
    let steps = async {
        let answer = send_tcp(api, Method::DELETE, &unknown, OWNER, None).await;
        assert_eq!(answer.status, StatusCode::NOT_FOUND, "{:?}", answer.body);
        let journals = systemd.journals();
        // In journal order: the other connection fails for 2 minutes only, the unknown unit for
        // 10 minutes, and the expired connection for 10 minutes.
        journals.log_auth_failures(&other_unit, Duration::from_mins(2));
        journals.log_auth_failures(&unknown_unit, Duration::from_mins(10));
        journals.log_auth_failures(&expired_unit, Duration::from_mins(10));
        systemd.wait_for_stop(&expired_unit).await
    };
    let (unit_stopped, stopped) = run_service(&fixture, &systemd, steps, api).await;
    assert!(stopped, "serve stops without a failure");

    assert!(
        unit_stopped,
        "the expired unit is stopped: {:?}",
        systemd.calls()
    );
    assert_eq!(state_of(&fixture, expired).await, "login_expired");
    assert_eq!(
        state_of(&fixture, other_vault).await,
        "active",
        "2 minutes of failures of another unit do not expire this connection"
    );
    let unit_stops: Vec<Call> = systemd
        .calls()
        .into_iter()
        .filter(|call| matches!(call, Call::Stop(_)))
        .collect();
    assert_eq!(
        unit_stops,
        [Call::Stop(expired_unit)],
        "only the unit of the expired login stops; a unit that the service did not start is not counted"
    );

    fixture
        .cleanup()
        .await
        .expect("the test database is dropped");
}
