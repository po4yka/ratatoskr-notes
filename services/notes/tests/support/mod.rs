//! Shared scaffolding for the domain API tests: requests with the claim that Edge mints.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    dead_code,
    reason = "test scaffolding; not every test binary uses every helper"
)]

#[cfg(unix)]
pub(crate) mod cmdline;
pub(crate) mod logs;
pub(crate) mod systemd;

use std::net::{Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use http_body_util::BodyExt as _;
use serde_json::Value;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpStream;
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

/// `POST /v1/notes/connections` for one owner. A `sync` connection carries a login with marker
/// secrets.
pub(crate) async fn connect(
    router: &Router,
    owner: Uuid,
    channel: &str,
    vault_key: &str,
) -> Answer {
    if channel == "sync" {
        return connect_sync(router, owner, vault_key, MARKER_SECRETS).await;
    }
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

/// The marker secrets of a `sync` connect request whose secrets the test does not read.
pub(crate) const MARKER_SECRETS: SyncSecrets<'static> = SyncSecrets {
    password: "MARKER-ACCOUNT-PW-connections",
    mfa_code: "MARKER-MFA-connections",
    e2e_password: "MARKER-E2E-PW-connections",
};

/// The account email of every `sync` connect request. The email is not a secret (workspace
/// design D4).
pub(crate) const OBSIDIAN_EMAIL: &str = "owner@example.test";

/// The body of a `sync` connect request with its Obsidian login.
pub(crate) fn sync_body(vault_key: &str, secrets: SyncSecrets<'_>) -> Value {
    serde_json::json!({
        "channel": "sync",
        "vault_key": vault_key,
        "vault_display_name": "Personal",
        "sync_login": {
            "email": OBSIDIAN_EMAIL,
            "password": secrets.password,
            "mfa_code": secrets.mfa_code,
            "e2e_password": secrets.e2e_password,
        },
    })
}

/// `POST /v1/notes/connections` for a `sync` connection with its Obsidian login.
pub(crate) async fn connect_sync(
    router: &Router,
    owner: Uuid,
    vault_key: &str,
    secrets: SyncSecrets<'_>,
) -> Answer {
    send(
        router,
        Method::POST,
        "/v1/notes/connections",
        owner,
        Some(sync_body(vault_key, secrets)),
    )
    .await
}

/// A loopback address that no listener holds now. The system selects the port, and the listener
/// that got it closes again, so that the service under test can bind it.
pub(crate) fn free_address() -> SocketAddr {
    std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .and_then(|listener| listener.local_addr())
        .expect("a free loopback port")
}

/// How long [`send_tcp`] waits until the listener exists.
const LISTENER_DEADLINE: Duration = Duration::from_secs(10);

/// Sends one HTTP/1.1 request with the owner claim to a listener of the real service, and reads
/// the answer. Until the service binds the listener, the connection is refused, and the request
/// tries again until [`LISTENER_DEADLINE`]. When the listener exists but does not accept yet, the
/// request waits in its backlog.
pub(crate) async fn send_tcp(
    address: SocketAddr,
    method: Method,
    path: &str,
    owner: Uuid,
    body: Option<Value>,
) -> Answer {
    let deadline = Instant::now() + LISTENER_DEADLINE;
    let mut stream = loop {
        match TcpStream::connect(address).await {
            Err(error)
                if error.kind() == std::io::ErrorKind::ConnectionRefused
                    && Instant::now() < deadline =>
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            attempt => break attempt.expect("the listener of the service can be reached"),
        }
    };
    let body = body.map(|json| json.to_string()).unwrap_or_default();
    let request = format!(
        "{method} {path} HTTP/1.1\r\nhost: {address}\r\n{USER_HEADER}: {owner}\r\n\
         content-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(request.as_bytes())
        .await
        .expect("the request is sent");
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .expect("the answer is read");
    let response = String::from_utf8(response).expect("the answer is UTF-8");
    let (head, body) = response
        .split_once("\r\n\r\n")
        .expect("the answer has a head and a body");
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse::<u16>().ok())
        .and_then(|code| StatusCode::from_u16(code).ok())
        .expect("the answer has a status");
    Answer {
        status,
        body: serde_json::from_str(body).unwrap_or(Value::Null),
    }
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
