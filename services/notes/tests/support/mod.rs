//! Shared scaffolding for the domain API tests: requests with the claim that Edge mints.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    dead_code,
    reason = "test scaffolding; not every test binary uses every helper"
)]

#[cfg(unix)]
pub(crate) mod cmdline;
pub(crate) mod journal;
pub(crate) mod logs;
pub(crate) mod systemd;

use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use http_body_util::BodyExt as _;
use metrics_exporter_prometheus::PrometheusBuilder;
use ratatoskr_notes::config::{
    AdminConfig, ApiConfig, BlobsConfig, BusConfig, ConnectionsConfig, DatabaseConfig, FilesConfig,
    NotesConfig, SecretsConfig, ShutdownConfig, TelemetryConfig,
};
use ratatoskr_notes_persistence::test_support::TestDatabase;
use serde_json::Value;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tower::ServiceExt as _;
use uuid::Uuid;

use crate::support::systemd::FakeSystemd;

/// The claim header that Edge mints for the owner.
pub(crate) const USER_HEADER: &str = "x-ratatoskr-user-id";

/// The header that carries the operation id that Edge mints for a connect. The helpers send a
/// fresh one with every request, as Edge does for a connect; other routes ignore it.
pub(crate) const OPERATION_HEADER: &str = "x-ratatoskr-operation-id";

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
        .header(USER_HEADER, owner.to_string())
        .header(OPERATION_HEADER, Uuid::now_v7().to_string());
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
         {OPERATION_HEADER}: {}\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\
         connection: close\r\n\r\n{body}",
        Uuid::now_v7(),
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

/// How long [`run_service`] waits until `serve` ends after its stop signal.
const SERVICE_STOP_DEADLINE: Duration = Duration::from_secs(10);

/// Runs `steps` beside the real service, as `serve` runs it with the fake bus and the test
/// database, then sends the stop signal, as SIGTERM does in production. Returns what the steps
/// returned, and whether `serve` then ended without a failure in [`SERVICE_STOP_DEADLINE`].
///
/// The future of `serve` is not `Send`, so the test polls it beside the steps instead of in a
/// spawned task.
pub(crate) async fn run_service<T>(
    fixture: &TestDatabase,
    systemd: &FakeSystemd,
    steps: impl Future<Output = T>,
    api: SocketAddr,
) -> (T, bool) {
    let config = NotesConfig {
        admin: AdminConfig {
            bind: free_address(),
        },
        api: ApiConfig {
            listen_address: api,
        },
        blobs: BlobsConfig {
            root: systemd.blob_root(),
        },
        bus: BusConfig::default(),
        files: FilesConfig::default(),
        connections: ConnectionsConfig { max_logins: 2 },
        database: DatabaseConfig {
            url: fixture.secret_url(),
            max_connections: 2,
            acquire_timeout_seconds: 5,
        },
        secrets: SecretsConfig {
            key_file: systemd.key_file(),
        },
        shutdown: ShutdownConfig { grace_seconds: 5 },
        telemetry: TelemetryConfig::default(),
    };
    let metrics = PrometheusBuilder::new().build_recorder().handle();
    let (stop, stopped) = oneshot::channel::<()>();
    let serve = ratatoskr_notes::serve(
        &config,
        metrics,
        systemd.clone(),
        systemd.credentials_root(),
        async move {
            let _ = stopped.await;
        },
    );
    tokio::pin!(serve, steps);
    let output = tokio::select! {
        ended = &mut serve => unreachable!("serve ended before its stop signal: {ended:?}"),
        output = &mut steps => output,
    };
    let _ = stop.send(());
    let ended = tokio::time::timeout(SERVICE_STOP_DEADLINE, serve).await;
    (output, matches!(ended, Ok(Ok(()))))
}

/// How long a test waits for the task of a connect, which runs after the `202`.
pub(crate) const CONNECT_DEADLINE: Duration = Duration::from_mins(1);

/// Wait until the task of the connect of `connection_id` ran its login: the login is recorded, or
/// the connection is no longer `connecting`. Returns the state then.
pub(crate) async fn login_ended(fixture: &TestDatabase, connection_id: Uuid) -> String {
    let deadline = Instant::now() + CONNECT_DEADLINE;
    loop {
        let (state, logged_in): (String, bool) = sqlx::query_as(
            "select state, logged_in_at is not null from notes.connections
             where connection_id = $1",
        )
        .bind(connection_id)
        .fetch_one(fixture.pool())
        .await
        .expect("the connection can be read");
        if state != "connecting" || logged_in || Instant::now() >= deadline {
            return state;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The payload of the terminal report of the connect operation of `connection_id`, once there is
/// one, or `Value::Null` at the deadline.
pub(crate) async fn terminal_report(fixture: &TestDatabase, connection_id: Uuid) -> Value {
    let deadline = Instant::now() + CONNECT_DEADLINE;
    loop {
        let found: Option<Value> = sqlx::query_scalar(
            "select o.payload -> 'payload' from notes.outbox o
             join notes.connections c
               on o.payload -> 'payload' ->> 'operation_id' = c.operation_id::text
             where c.connection_id = $1
               and o.event_type = 'platform.operation.reported.v1'
               and o.payload -> 'payload' ->> 'status'
                   in ('succeeded', 'partially_succeeded', 'failed')
             order by o.position limit 1",
        )
        .bind(connection_id)
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
