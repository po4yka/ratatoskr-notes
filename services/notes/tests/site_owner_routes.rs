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
    /// The route serves the claim: the answer is not a refusal of the session kind.
    Served,
    /// `409` `notes.channel_mismatch`, as for every session on a device channel route.
    ChannelMismatch,
    /// `401` `notes.request.unauthenticated`.
    Refused,
}

#[tokio::test]
async fn site_owner_reaches_only_uploads_and_conflicts() {
    let channel = Channel::new().await;
    let connection = channel.plugin_connection(OWNER, "vault-key-site").await;
    let base = format!("/v1/notes/connections/{connection}");
    let token = "rst_00000000000000000000000000";
    let unknown = Uuid::now_v7();
    let declaration = json!({
        "declared_size_bytes": 5,
        "media_type": "text/markdown",
        "digest": { "algorithm": "sha256", "hex": "0".repeat(64) },
        "chunk_size_bytes": 16_777_216,
    });
    let connect = json!({
        "channel": "plugin",
        "vault_key": "vault-key-from-a-site",
        "vault_display_name": "Site",
    });
    // One row for each entry of the domain router.
    let table: Vec<(Method, String, Option<Value>, Expected)> = vec![
        (
            Method::POST,
            format!("{base}/blobs"),
            Some(declaration),
            Expected::Served,
        ),
        (
            Method::PUT,
            format!("{base}/blobs/{token}/chunks/0"),
            None,
            Expected::Served,
        ),
        (
            Method::POST,
            format!("{base}/blobs/{token}/finalize"),
            Some(json!({ "resumption_token": token })),
            Expected::Served,
        ),
        (
            Method::DELETE,
            format!("{base}/blobs/{token}"),
            None,
            Expected::Served,
        ),
        (
            Method::GET,
            format!("/v1/notes/conflicts/{unknown}"),
            None,
            Expected::Served,
        ),
        (
            Method::POST,
            format!("{base}/changes"),
            Some(json!({ "changes": [] })),
            Expected::ChannelMismatch,
        ),
        (
            Method::POST,
            format!("{base}/manifest"),
            Some(json!({ "entries": [] })),
            Expected::ChannelMismatch,
        ),
        (
            Method::GET,
            format!("{base}/blobs/{token}/status"),
            None,
            Expected::Refused,
        ),
        (
            Method::POST,
            "/v1/notes/connections".to_owned(),
            Some(connect),
            Expected::Refused,
        ),
        (
            Method::GET,
            "/v1/notes/connections".to_owned(),
            None,
            Expected::Refused,
        ),
        (
            Method::GET,
            format!("{base}/files"),
            None,
            Expected::Refused,
        ),
        (
            Method::GET,
            format!("{base}/files/Home.md"),
            None,
            Expected::Refused,
        ),
        (
            Method::GET,
            format!("{base}/index/Home.md"),
            None,
            Expected::Refused,
        ),
        (
            Method::PUT,
            format!("{base}/excluded-folders"),
            Some(json!({ "excluded_folders": ["Private"] })),
            Expected::Refused,
        ),
        (
            Method::GET,
            format!("{base}/device-operations"),
            None,
            Expected::Refused,
        ),
        (
            Method::GET,
            format!("{base}/device-operations/{unknown}/content"),
            None,
            Expected::Refused,
        ),
        (
            Method::POST,
            format!("{base}/device-operations/{unknown}/result"),
            Some(json!({})),
            Expected::Refused,
        ),
        (
            Method::PUT,
            format!("{base}/write-mode"),
            Some(json!({ "write_mode": "on" })),
            Expected::Refused,
        ),
        (
            Method::GET,
            format!("/internal/connections/{connection}/files"),
            None,
            Expected::Refused,
        ),
        (Method::DELETE, base.clone(), None, Expected::Refused),
    ];

    let mut answers = Vec::new();
    for (method, path, body, expected) in table {
        let answer = send_with(
            &channel.router,
            method.clone(),
            &path,
            OWNER,
            &site_owner(),
            body,
        )
        .await;
        answers.push((format!("{method} {path}"), expected, answer));
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
                Expected::Served => refusal == unauthenticated,
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
