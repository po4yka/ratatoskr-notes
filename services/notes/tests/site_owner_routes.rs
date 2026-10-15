//! The routes that a site owner session may reach (SITE-027). Notes does not rely only on the route
//! table of Edge: a claim of the session kind `site_owner` reaches the blob routes and the conflict
//! read, the device channel routes refuse it with `notes.channel_mismatch`, and every other route
//! refuses it with `401` `notes.request.unauthenticated`.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions read the JSON answers they build"
)]

mod support;

use axum::http::{Method, StatusCode};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::support::channel::{Channel, OWNER};
use crate::support::send_with;
use crate::support::writes::{ORIGIN_HEADER, SESSION_KIND_HEADER};

/// The claims that Edge mints for a site owner session of the site `site-a`.
fn site_owner() -> [(&'static str, String); 2] {
    [
        (SESSION_KIND_HEADER, "site_owner".to_owned()),
        (ORIGIN_HEADER, "site:site-a".to_owned()),
    ]
}

/// What a route answers to a site owner claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Expected {
    /// The handler of the route serves the claim: the answer carries a code of the handler, which
    /// is neither a refusal of the guard nor `notes.channel_mismatch`. Each such row names an id
    /// that does not exist, so the handler answers with its own code.
    Served,
    /// `409` `notes.channel_mismatch`, as for every session on a device channel route.
    ChannelMismatch,
    /// `401` `notes.request.unauthenticated`.
    Refused,
}

/// One row for each entry of the domain router: the method, the path with `{c}` for the connection,
/// `{t}` for an upload token and `{u}` for an unknown id, and what the route answers.
const ROUTES: [(Method, &str, Expected); 20] = [
    (
        Method::POST,
        "/v1/notes/connections/{u}/blobs",
        Expected::Served,
    ),
    (
        Method::PUT,
        "/v1/notes/connections/{c}/blobs/{t}/chunks/0",
        Expected::Served,
    ),
    (
        Method::POST,
        "/v1/notes/connections/{c}/blobs/{t}/finalize",
        Expected::Served,
    ),
    (
        Method::DELETE,
        "/v1/notes/connections/{c}/blobs/{t}",
        Expected::Served,
    ),
    (Method::GET, "/v1/notes/conflicts/{u}", Expected::Served),
    (
        Method::POST,
        "/v1/notes/connections/{c}/changes",
        Expected::ChannelMismatch,
    ),
    (
        Method::POST,
        "/v1/notes/connections/{c}/manifest",
        Expected::ChannelMismatch,
    ),
    (
        Method::GET,
        "/v1/notes/connections/{c}/blobs/{t}/status",
        Expected::Refused,
    ),
    (Method::POST, "/v1/notes/connections", Expected::Refused),
    (Method::GET, "/v1/notes/connections", Expected::Refused),
    (
        Method::GET,
        "/v1/notes/connections/{c}/files",
        Expected::Refused,
    ),
    (
        Method::GET,
        "/v1/notes/connections/{c}/files/Home.md",
        Expected::Refused,
    ),
    (
        Method::GET,
        "/v1/notes/connections/{c}/index/Home.md",
        Expected::Refused,
    ),
    (
        Method::PUT,
        "/v1/notes/connections/{c}/excluded-folders",
        Expected::Refused,
    ),
    (
        Method::GET,
        "/v1/notes/connections/{c}/device-operations",
        Expected::Refused,
    ),
    (
        Method::GET,
        "/v1/notes/connections/{c}/device-operations/{u}/content",
        Expected::Refused,
    ),
    (
        Method::POST,
        "/v1/notes/connections/{c}/device-operations/{u}/result",
        Expected::Refused,
    ),
    (
        Method::PUT,
        "/v1/notes/connections/{c}/write-mode",
        Expected::Refused,
    ),
    (
        Method::GET,
        "/internal/connections/{c}/files",
        Expected::Refused,
    ),
    (
        Method::DELETE,
        "/v1/notes/connections/{c}",
        Expected::Refused,
    ),
];

/// The token of an upload session that does not exist.
const TOKEN: &str = "rst_00000000000000000000000000";

/// A valid body for a `POST` or `PUT` to `path`, so that a route that serves the claim would act.
fn body(method: &Method, path: &str) -> Option<Value> {
    if method != Method::POST && method != Method::PUT {
        return None;
    }
    let body = if path.ends_with("/blobs") {
        json!({
            "declared_size_bytes": 5,
            "media_type": "text/markdown",
            "digest": { "algorithm": "sha256", "hex": "0".repeat(64) },
            "chunk_size_bytes": 16_777_216,
        })
    } else if path.ends_with("/finalize") {
        json!({ "resumption_token": TOKEN })
    } else if path.ends_with("/changes") {
        json!({ "changes": [] })
    } else if path.ends_with("/manifest") {
        json!({ "entries": [] })
    } else if path.ends_with("/connections") {
        json!({
            "channel": "plugin",
            "vault_key": "vault-key-from-a-site",
            "vault_display_name": "Site",
        })
    } else if path.ends_with("/excluded-folders") {
        json!({ "excluded_folders": ["Private"] })
    } else if path.ends_with("/write-mode") {
        json!({ "write_mode": "on" })
    } else {
        json!({})
    };
    Some(body)
}

#[tokio::test]
async fn site_owner_reaches_only_uploads_and_conflicts() {
    let channel = Channel::new().await;
    let connection = channel.plugin_connection(OWNER, "vault-key-site").await;
    let unknown = Uuid::now_v7();

    let mut answers = Vec::new();
    for (method, template, expected) in ROUTES {
        let path = template
            .replace("{c}", &connection.to_string())
            .replace("{t}", TOKEN)
            .replace("{u}", &unknown.to_string());
        let body = body(&method, &path);
        let answer = send_with(
            &channel.router,
            method.clone(),
            &path,
            OWNER,
            &site_owner(),
            body,
        )
        .await;
        answers.push((format!("{method} {template}"), expected, answer));
    }

    let unauthenticated = (
        StatusCode::UNAUTHORIZED,
        Some("notes.request.unauthenticated"),
    );
    let wrong: Vec<String> = answers
        .iter()
        .filter(|(_, expected, answer)| {
            let refusal = (answer.status, answer.body["code"].as_str());
            match expected {
                Expected::Served => {
                    refusal == unauthenticated
                        || refusal
                            .1
                            .is_none_or(|code| code.is_empty() || code == "notes.channel_mismatch")
                }
                Expected::ChannelMismatch => {
                    refusal != (StatusCode::CONFLICT, Some("notes.channel_mismatch"))
                }
                Expected::Refused => refusal != unauthenticated,
            }
        })
        .map(|(route, expected, answer)| {
            format!(
                "{route}: expected {expected:?}, got {} {}",
                answer.status, answer.body["code"]
            )
        })
        .collect();
    assert!(wrong.is_empty(), "{wrong:#?}");
    let connections: i64 =
        sqlx::query_scalar("select count(*) from notes.connections where owner_id = $1")
            .bind(OWNER)
            .fetch_one(channel.fixture.pool())
            .await
            .expect("the connections can be counted");
    assert_eq!(connections, 1, "no connection was created");
    assert_ne!(channel.connection_state(connection).await, "removed");

    channel.cleanup().await;
}

#[tokio::test]
async fn unknown_session_kind_is_unauthenticated() {
    let channel = Channel::new().await;
    let connection = channel.plugin_connection(OWNER, "vault-key-kinds").await;
    let unknown = Uuid::now_v7();
    let routes = [
        (
            Method::POST,
            format!("/v1/notes/connections/{connection}/blobs"),
        ),
        (
            Method::GET,
            format!("/v1/notes/connections/{connection}/files"),
        ),
        (Method::GET, format!("/v1/notes/conflicts/{unknown}")),
        (
            Method::PUT,
            format!("/v1/notes/connections/{connection}/write-mode"),
        ),
    ];

    let mut wrong = Vec::new();
    for kind in ["Site_Owner", "robot", "anonymous"] {
        let claims = [
            (SESSION_KIND_HEADER, kind.to_owned()),
            (ORIGIN_HEADER, "site:site-a".to_owned()),
        ];
        for (method, path) in &routes {
            let body = body(method, path);
            let answer =
                send_with(&channel.router, method.clone(), path, OWNER, &claims, body).await;
            let refusal = (answer.status, answer.body["code"].as_str());
            if refusal
                != (
                    StatusCode::UNAUTHORIZED,
                    Some("notes.request.unauthenticated"),
                )
            {
                wrong.push(format!(
                    "{kind} {method} {path}: {} {}",
                    answer.status, answer.body
                ));
            }
        }
    }
    // A kind that Platform mints is still served.
    for kind in ["browser", "api_token", "telegram_mini_app", "service"] {
        let answer = send_with(
            &channel.router,
            Method::GET,
            &format!("/v1/notes/connections/{connection}/files"),
            OWNER,
            &[(SESSION_KIND_HEADER, kind.to_owned())],
            None,
        )
        .await;
        if answer.status != StatusCode::OK {
            wrong.push(format!(
                "{kind} GET .../files: {} {}",
                answer.status, answer.body
            ));
        }
    }

    assert!(wrong.is_empty(), "{wrong:#?}");
    assert_ne!(channel.connection_state(connection).await, "removed");

    channel.cleanup().await;
}
