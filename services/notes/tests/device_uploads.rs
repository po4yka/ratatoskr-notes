//! The upload sessions of the device channels (spec `notes-device-channels`, the blob receipt of
//! the shared transfer contract): their bounds, a finalize that runs twice, and the uploads that a
//! removed connection leaves.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions read the JSON answers they build"
)]

mod support;

use axum::http::StatusCode;
use serde_json::{Value, json};

use crate::support::channel::{Channel, OTHER_OWNER, OWNER, sha256};

/// The declaration of an upload of `bytes`.
fn declaration(bytes: &[u8]) -> Value {
    json!({
        "declared_size_bytes": bytes.len(),
        "media_type": "text/markdown",
        "digest": { "algorithm": "sha256", "hex": sha256(bytes) },
        "chunk_size_bytes": 65_536,
    })
}

/// The open sessions of one connection, as the per-connection cap of the service states it.
const SESSIONS_PER_CONNECTION: usize = 4;

#[tokio::test]
async fn upload_sessions_are_bounded_per_connection() {
    let channel = Channel::new().await;
    let busy = channel.plugin_connection(OWNER, "vault-key-busy").await;
    let other = channel
        .plugin_connection(OTHER_OWNER, "vault-key-other")
        .await;

    let mut opened = Vec::new();
    for _ in 0..=SESSIONS_PER_CONNECTION {
        opened.push(
            channel
                .device_call_as(OWNER, busy, "blobs", declaration(b"a note\n"))
                .await,
        );
    }
    let elsewhere = channel
        .device_call_as(OTHER_OWNER, other, "blobs", declaration(b"a note\n"))
        .await;

    for answer in &opened[..SESSIONS_PER_CONNECTION] {
        assert_eq!(answer.status, StatusCode::CREATED, "{:?}", answer.body);
    }
    let over = &opened[SESSIONS_PER_CONNECTION];
    assert_eq!(
        over.status,
        StatusCode::SERVICE_UNAVAILABLE,
        "a connection at its cap: {:?}",
        over.body
    );
    assert_eq!(over.body["code"], "notes.service.unavailable");
    assert_eq!(
        elsewhere.status,
        StatusCode::CREATED,
        "another connection still opens a session: {:?}",
        elsewhere.body
    );

    channel.cleanup().await;
}

#[tokio::test]
async fn burst_of_uploads_never_exceeds_the_cap() {
    let channel = Channel::new().await;
    let connection = channel.plugin_connection(OWNER, "vault-key-burst").await;

    let answers = futures_util::future::join_all(
        (0..10)
            .map(|_| channel.device_call_as(OWNER, connection, "blobs", declaration(b"burst\n"))),
    )
    .await;

    let created = answers
        .iter()
        .filter(|answer| answer.status == StatusCode::CREATED)
        .count();
    assert_eq!(
        created,
        SESSIONS_PER_CONNECTION,
        "{:?}",
        answers
            .iter()
            .map(|answer| answer.status)
            .collect::<Vec<_>>()
    );

    channel.cleanup().await;
}
