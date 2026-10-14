//! Shared scaffolding for the domain API tests: requests with the claim that Edge mints.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    dead_code,
    reason = "test scaffolding; not every test binary uses every helper"
)]

pub(crate) mod logs;
pub(crate) mod systemd;

use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use http_body_util::BodyExt as _;
use serde_json::Value;
use tower::ServiceExt as _;
use uuid::Uuid;

/// The claim header that Edge mints for the owner.
pub(crate) const USER_HEADER: &str = "x-ratatoskr-user-id";

/// One answer of the domain router: the status and the JSON body, `Value::Null` when empty.
#[derive(Debug)]
pub(crate) struct Answer {
    pub(crate) status: StatusCode,
    pub(crate) body: Value,
}

/// Sends one request with the owner claim and an optional JSON body.
pub(crate) async fn send(
    router: &Router,
    method: Method,
    path: &str,
    owner: Uuid,
    body: Option<Value>,
) -> Answer {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header(USER_HEADER, owner.to_string());
    let body = match body {
        Some(json) => {
            request = request.header("content-type", "application/json");
            Body::from(json.to_string())
        }
        None => Body::empty(),
    };
    let response = router
        .clone()
        .oneshot(request.body(body).expect("a request"))
        .await
        .expect("the router answers");
    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("a body")
        .to_bytes();
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    Answer { status, body }
}

/// `POST /v1/notes/connections` for one owner.
pub(crate) async fn connect(
    router: &Router,
    owner: Uuid,
    channel: &str,
    vault_key: &str,
) -> Answer {
    let body = serde_json::json!({
        "channel": channel,
        "vault_key": vault_key,
        "vault_display_name": "Personal",
    });
    send(
        router,
        Method::POST,
        "/v1/notes/connections",
        owner,
        Some(body),
    )
    .await
}

/// The Obsidian secrets that a `sync` connect request carries. Tests use marker values only.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SyncSecrets<'a> {
    pub(crate) password: &'a str,
    pub(crate) mfa_code: &'a str,
    pub(crate) e2e_password: &'a str,
}

/// The account email of every `sync` connect request. The email is not a secret (workspace
/// design D4).
pub(crate) const OBSIDIAN_EMAIL: &str = "owner@example.test";

/// `POST /v1/notes/connections` for a `sync` connection with its Obsidian login.
pub(crate) async fn connect_sync(
    router: &Router,
    owner: Uuid,
    vault_key: &str,
    secrets: SyncSecrets<'_>,
) -> Answer {
    let body = serde_json::json!({
        "channel": "sync",
        "vault_key": vault_key,
        "vault_display_name": "Personal",
        "sync_login": {
            "email": OBSIDIAN_EMAIL,
            "password": secrets.password,
            "mfa_code": secrets.mfa_code,
            "e2e_password": secrets.e2e_password,
        },
    });
    send(
        router,
        Method::POST,
        "/v1/notes/connections",
        owner,
        Some(body),
    )
    .await
}

/// The connection id of a `202` answer.
pub(crate) fn connection_id(answer: &Answer) -> Uuid {
    assert_eq!(
        answer.status,
        StatusCode::ACCEPTED,
        "the connect request must be accepted: {:?}",
        answer.body
    );
    answer.body["connection_id"]
        .as_str()
        .and_then(|raw| Uuid::parse_str(raw).ok())
        .expect("the answer names the connection id")
}
