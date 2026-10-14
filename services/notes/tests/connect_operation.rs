//! The connect operation (workspace design D10, spec `notes-vault-connections`, requirement
//! "Connecting a vault is an operation that only notes reports"): `POST /v1/notes/connections`
//! answers `202` at once with the operation that Edge minted, and notes reports the phases
//! `login`, `setup`, `first_sync` and `indexing` on `platform.operation.reported.v1`, completes the
//! operation when the first full scan is indexed, and fails it with a stable code.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions read the JSON answers and events they build"
)]

mod support;

use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use http_body_util::BodyExt as _;
use ratatoskr_event_envelope::EventEnvelope;
use ratatoskr_notes::api::{ApiState, domain_router};
use ratatoskr_notes::blobs::BlobStore;
use ratatoskr_notes_persistence::test_support::TestDatabase;
use ratatoskr_operation_contracts::OperationReported;
use serde_json::Value;
use tower::ServiceExt as _;
use uuid::Uuid;

use crate::support::systemd::FakeSystemd;
use crate::support::{MARKER_SECRETS, USER_HEADER, sync_body};

const OWNER: Uuid = Uuid::from_u128(0x0192_0000_0000_7000_8000_0000_0000_0d10);

/// The header that carries the operation id that Edge mints.
const OPERATION_HEADER: &str = "x-ratatoskr-operation-id";

/// How long a test waits for the operation to end.
const TERMINAL_DEADLINE: Duration = Duration::from_secs(20);

fn router(fixture: &TestDatabase, systemd: &FakeSystemd) -> Router {
    let root = systemd.blob_root();
    std::fs::create_dir_all(&root).expect("the blob root");
    domain_router(
        ApiState::new(fixture.database.clone(), systemd.clone())
            .with_key_file(systemd.key_file())
            .with_credentials_dir(systemd.credentials_root())
            .with_blobs(BlobStore::new(root))
            .with_trees_root(systemd.trees_root()),
    )
}

/// `POST /v1/notes/connections` as Edge sends it, with the owner and the operation id when given.
async fn connect(router: &Router, body: &Value, operation_id: Option<Uuid>) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method(Method::POST)
        .uri("/v1/notes/connections")
        .header(USER_HEADER, OWNER.to_string())
        .header("content-type", "application/json");
    if let Some(operation_id) = operation_id {
        request = request.header(OPERATION_HEADER, operation_id.to_string());
    }
    let response = router
        .clone()
        .oneshot(
            request
                .body(Body::from(body.to_string()))
                .expect("a request"),
        )
        .await
        .expect("the router answers");
    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("a body")
        .to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

/// Every report of `operation_id` in the outbox, in the order of the outbox.
async fn reports(fixture: &TestDatabase, operation_id: Uuid) -> Vec<Value> {
    sqlx::query_scalar(
        "select payload from notes.outbox
         where event_type = 'platform.operation.reported.v1'
           and payload -> 'payload' ->> 'operation_id' = $1
         order by position",
    )
    .bind(operation_id.to_string())
    .fetch_all(fixture.pool())
    .await
    .expect("the outbox can be read")
}

/// The reports of `operation_id` once one of them is terminal, or what is there at the deadline.
async fn reports_until_terminal(fixture: &TestDatabase, operation_id: Uuid) -> Vec<Value> {
    let deadline = tokio::time::Instant::now() + TERMINAL_DEADLINE;
    loop {
        let found = reports(fixture, operation_id).await;
        let terminal = found.iter().any(|report| {
            matches!(
                report["payload"]["status"].as_str(),
                Some("succeeded" | "failed")
            )
        });
        if terminal || tokio::time::Instant::now() >= deadline {
            return found;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// `(status, stage)` of each report, after checking that each one is a valid report of notes.
fn phases(reports: &[Value]) -> Vec<(String, String)> {
    reports
        .iter()
        .map(|report| {
            let envelope: EventEnvelope =
                serde_json::from_value(report.clone()).expect("the report is an envelope");
            assert_eq!(envelope.producer.as_str(), "ratatoskr-notes");
            assert_eq!(
                envelope.tenant_id.map(|tenant| tenant.to_string()),
                Some(format!("user:{OWNER}"))
            );
            let payload: OperationReported =
                serde_json::from_value(Value::Object(envelope.payload)).expect("a report");
            payload
                .validate()
                .expect("the report keeps the status invariants");
            (
                payload.status.as_str().to_owned(),
                payload
                    .stage
                    .map(|stage| stage.as_str().to_owned())
                    .unwrap_or_default(),
            )
        })
        .collect()
}

async fn events_of(fixture: &TestDatabase, event_type: &str) -> Vec<Value> {
    sqlx::query_scalar("select payload from notes.outbox where event_type = $1 order by position")
        .bind(event_type)
        .fetch_all(fixture.pool())
        .await
        .expect("the outbox can be read")
}

async fn state_of(fixture: &TestDatabase, connection_id: &str) -> String {
    sqlx::query_scalar("select state from notes.connections where connection_id = $1::uuid")
        .bind(connection_id)
        .fetch_one(fixture.pool())
        .await
        .expect("the connection exists")
}

#[tokio::test]
async fn connect_answers_202_at_once_with_the_operation() {
    let fixture = TestDatabase::create().await.expect("a test database");
    let systemd = FakeSystemd::new().hold_logins();
    let router = router(&fixture, &systemd);
    let operation_id = Uuid::now_v7();

    let answer = tokio::time::timeout(
        Duration::from_secs(5),
        connect(
            &router,
            &sync_body("remote-vault-d10", MARKER_SECRETS),
            Some(operation_id),
        ),
    )
    .await;
    systemd.release_logins();

    let (status, body) = answer.expect("the connect answers while the login unit still runs");
    assert_eq!(status, StatusCode::ACCEPTED, "{body:?}");
    assert_eq!(body["operation"]["operation_id"], operation_id.to_string());
    assert_eq!(body["operation"]["kind"], "notes.connection.connect");
    assert_eq!(body["operation"]["lane"], "ratatoskr-notes");
    assert_eq!(body["state"], "connecting");
    assert_eq!(body["channel"], "sync");

    fixture
        .cleanup()
        .await
        .expect("the test database is dropped");
}

#[tokio::test]
async fn connect_reports_each_phase_and_completes_after_the_first_scan() {
    let fixture = TestDatabase::create().await.expect("a test database");
    let systemd = FakeSystemd::new();
    let router = router(&fixture, &systemd);
    let operation_id = Uuid::now_v7();

    let (status, body) = connect(
        &router,
        &sync_body("remote-vault-d10", MARKER_SECRETS),
        Some(operation_id),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body:?}");
    let connection_id = body["connection_id"]
        .as_str()
        .expect("a connection id")
        .to_owned();
    // ob writes the vault into the working tree.
    let tree = systemd.tree_dir(connection_id.parse().expect("a uuid"));
    std::fs::create_dir_all(tree.join("Projects")).expect("the working tree");
    std::fs::write(tree.join("Home.md"), "# Home\n[[Projects/Plan]]\n").expect("a note");
    std::fs::write(tree.join("Projects/Plan.md"), "# Plan\n").expect("a note");

    let found = reports_until_terminal(&fixture, operation_id).await;

    assert_eq!(
        phases(&found),
        [
            ("running".to_owned(), "login".to_owned()),
            ("running".to_owned(), "setup".to_owned()),
            ("running".to_owned(), "first_sync".to_owned()),
            ("running".to_owned(), "indexing".to_owned()),
            ("succeeded".to_owned(), String::new()),
        ],
        "the operation passes through its four phases and completes"
    );
    let files = events_of(&fixture, "notes.file.changed.v1").await;
    assert_eq!(
        files.len(),
        2,
        "both files of the first scan are indexed: {files:?}"
    );
    let connected = events_of(&fixture, "notes.vault.connected.v1").await;
    assert_eq!(connected.len(), 1, "one notes.vault.connected.v1");
    assert_eq!(state_of(&fixture, &connection_id).await, "active");

    fixture
        .cleanup()
        .await
        .expect("the test database is dropped");
}

#[tokio::test]
async fn failed_login_reports_a_terminal_failure_with_its_code() {
    let fixture = TestDatabase::create().await.expect("a test database");
    let systemd = FakeSystemd::new().refuse_e2e_password();
    let router = router(&fixture, &systemd);
    let operation_id = Uuid::now_v7();

    let (status, body) = connect(
        &router,
        &sync_body("remote-vault-d10", MARKER_SECRETS),
        Some(operation_id),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::ACCEPTED,
        "the connect answers at once: {body:?}"
    );
    let found = reports_until_terminal(&fixture, operation_id).await;

    let last = found.last().expect("a report");
    assert_eq!(last["payload"]["status"], "failed", "{found:?}");
    assert_eq!(
        last["payload"]["error"]["code"], "notes.sync.e2e_password_wrong",
        "{found:?}"
    );
    let connection_id = body["connection_id"].as_str().unwrap_or_default();
    assert_eq!(state_of(&fixture, connection_id).await, "failed");
    assert!(
        events_of(&fixture, "notes.vault.connected.v1")
            .await
            .is_empty(),
        "a failed connect publishes no notes.vault.connected.v1"
    );

    fixture
        .cleanup()
        .await
        .expect("the test database is dropped");
}

#[tokio::test]
async fn connect_without_operation_id_is_refused() {
    let fixture = TestDatabase::create().await.expect("a test database");
    let systemd = FakeSystemd::new();
    let router = router(&fixture, &systemd);

    let (status, body) = connect(
        &router,
        &sync_body("remote-vault-d10", MARKER_SECRETS),
        None,
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{body:?}");
    assert_eq!(body["code"], "notes.request.invalid");
    let connections: i64 = sqlx::query_scalar("select count(*) from notes.connections")
        .fetch_one(fixture.pool())
        .await
        .expect("the connections can be counted");
    assert_eq!(connections, 0, "no connection is created");

    fixture
        .cleanup()
        .await
        .expect("the test database is dropped");
}
