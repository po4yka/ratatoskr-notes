//! The device claims that Edge mints on the device routes (workspace design D5): a `plugin` or
//! `cli` connect names its device in `x-ratatoskr-device-id` and the kind of the device in
//! `x-ratatoskr-device-kind`. Notes refuses a device connect without them, refuses a kind that does
//! not match the channel, and records the device of the connection.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions read the JSON answers they build"
)]

mod support;

use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use http_body_util::BodyExt as _;
use ratatoskr_notes::api::{ApiState, domain_router};
use ratatoskr_notes_persistence::test_support::TestDatabase;
use serde_json::Value;
use tower::ServiceExt as _;
use uuid::Uuid;

use crate::support::systemd::FakeSystemd;
use crate::support::{OPERATION_HEADER, USER_HEADER};

const OWNER: Uuid = Uuid::from_u128(0x0192_0000_0000_7000_8000_0000_0000_0de1);
const DEVICE: Uuid = Uuid::from_u128(0x0192_0000_0000_7000_8000_0000_0000_0de2);
const DEVICE_ID_HEADER: &str = "x-ratatoskr-device-id";
const DEVICE_KIND_HEADER: &str = "x-ratatoskr-device-kind";

fn router(fixture: &TestDatabase, systemd: &FakeSystemd) -> Router {
    domain_router(
        ApiState::new(fixture.database.clone(), systemd.clone())
            .with_key_file(systemd.key_file())
            .with_credentials_dir(systemd.credentials_root()),
    )
}

/// A connect of `channel` as Edge sends it, with the device claims that are given.
async fn connect(
    router: &Router,
    channel: &str,
    device_id: Option<&str>,
    device_kind: Option<&str>,
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method(Method::POST)
        .uri("/v1/notes/connections")
        .header(USER_HEADER, OWNER.to_string())
        .header(OPERATION_HEADER, Uuid::now_v7().to_string())
        .header("content-type", "application/json");
    if let Some(device_id) = device_id {
        request = request.header(DEVICE_ID_HEADER, device_id);
    }
    if let Some(device_kind) = device_kind {
        request = request.header(DEVICE_KIND_HEADER, device_kind);
    }
    let body = serde_json::json!({
        "channel": channel,
        "vault_key": "device-vault-0001",
        "vault_display_name": "Personal",
    });
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

async fn connections(fixture: &TestDatabase) -> i64 {
    sqlx::query_scalar("select count(*) from notes.connections")
        .fetch_one(fixture.pool())
        .await
        .expect("the connections can be counted")
}

#[tokio::test]
async fn device_connect_without_device_claims_is_refused() {
    let fixture = TestDatabase::create().await.expect("a test database");
    let systemd = FakeSystemd::new();
    let router = router(&fixture, &systemd);
    let device = DEVICE.to_string();

    let mut wrong = Vec::new();
    for (case, device_id, device_kind) in [
        ("no claims", None, None),
        ("no device kind", Some(device.as_str()), None),
        ("no device id", None, Some("obsidian_plugin")),
        (
            "a device id that is not a UUID",
            Some("device-1"),
            Some("obsidian_plugin"),
        ),
    ] {
        let (status, body) = connect(&router, "plugin", device_id, device_kind).await;
        if status != StatusCode::UNAUTHORIZED || body["code"] != "notes.request.unauthenticated" {
            wrong.push(format!("{case}: {status} {body}"));
        }
    }

    assert!(
        wrong.is_empty(),
        "a device connect without its claims is accepted: {wrong:#?}"
    );
    assert_eq!(connections(&fixture).await, 0, "no connection is created");

    fixture
        .cleanup()
        .await
        .expect("the test database is dropped");
}

#[tokio::test]
async fn device_connect_records_its_device() {
    let fixture = TestDatabase::create().await.expect("a test database");
    let systemd = FakeSystemd::new();
    let router = router(&fixture, &systemd);

    let (status, body) = connect(
        &router,
        "plugin",
        Some(&DEVICE.to_string()),
        Some("obsidian_plugin"),
    )
    .await;

    assert_eq!(status, StatusCode::ACCEPTED, "{body:?}");
    let device: Option<Uuid> = sqlx::query_scalar(
        "select device_id from notes.connections where connection_id = $1::uuid",
    )
    .bind(body["connection_id"].as_str().unwrap_or_default())
    .fetch_one(fixture.pool())
    .await
    .expect("the connection exists");
    assert_eq!(
        device,
        Some(DEVICE),
        "the connection records the device that created it"
    );

    fixture
        .cleanup()
        .await
        .expect("the test database is dropped");
}

#[tokio::test]
async fn export_agent_on_plugin_channel_is_channel_mismatch() {
    let fixture = TestDatabase::create().await.expect("a test database");
    let systemd = FakeSystemd::new();
    let router = router(&fixture, &systemd);
    let device = DEVICE.to_string();

    let (plugin_status, plugin_body) =
        connect(&router, "plugin", Some(&device), Some("export_agent")).await;
    let (cli_status, cli_body) =
        connect(&router, "cli", Some(&device), Some("obsidian_plugin")).await;

    assert_eq!(plugin_status, StatusCode::CONFLICT, "{plugin_body:?}");
    assert_eq!(plugin_body["code"], "notes.channel_mismatch");
    assert_eq!(cli_status, StatusCode::CONFLICT, "{cli_body:?}");
    assert_eq!(cli_body["code"], "notes.channel_mismatch");
    assert_eq!(connections(&fixture).await, 0, "no connection is created");

    fixture
        .cleanup()
        .await
        .expect("the test database is dropped");
}
