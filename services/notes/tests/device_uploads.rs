//! The upload sessions of the device channels (spec `notes-device-channels`, the blob receipt of
//! the shared transfer contract): their bounds, a finalize that runs twice, the uploads that a
//! removed connection leaves, and the close of an open session (SITE-027).

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions read the JSON answers they build"
)]

mod support;

use axum::http::{Method, StatusCode};
use ratatoskr_blob_transfer_contracts::{
    TransferErrorCode, UploadCompletionOutcome, UploadSessionRequest,
};
use ratatoskr_notes::blobs::BlobStore;
use ratatoskr_notes::uploads::{UploadRefusal, Uploads};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::support::channel::{Channel, OTHER_OWNER, OWNER, sha256};
use crate::support::writes::{ORIGIN_HEADER, SESSION_KIND_HEADER, Writer};
use crate::support::{Answer, DEVICE_ID_HEADER, DEVICE_KIND_HEADER, device_of, send_with};

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

#[tokio::test]
async fn second_finalize_of_one_token_is_session_unknown() {
    let root = tempfile::tempdir().expect("a blob root");
    let blobs = BlobStore::new(root.path());
    let uploads = Uploads::default();
    let connection = Uuid::now_v7();
    let bytes = b"finalized once\n";
    let request: UploadSessionRequest =
        serde_json::from_value(declaration(bytes)).expect("a declaration");
    let opened = uploads
        .open(&blobs, connection, &request, 1 << 20)
        .await
        .expect("a session");
    let token = opened.resumption_token.as_str().to_owned();
    uploads
        .chunk(connection, &token, 0, bytes.to_vec())
        .await
        .expect("the chunk is recorded");

    // Both finalize calls take the session before the first one ends it: the second waits for
    // the lock of the session while the first hashes the staged copy.
    let (first, second) = tokio::join!(
        uploads.finalize(&blobs, connection, &token),
        uploads.finalize(&blobs, connection, &token),
    );

    assert!(
        matches!(first, Ok(UploadCompletionOutcome::Stored { .. })),
        "{first:?}"
    );
    assert!(
        matches!(
            second,
            Err(UploadRefusal::Transfer(TransferErrorCode::SessionUnknown))
        ),
        "the second finalize finds no open session: {second:?}"
    );
}

#[tokio::test]
async fn removed_connection_keeps_no_uploads() {
    let channel = Channel::new().await;
    let connection = channel.plugin_connection(OWNER, "vault-key-uploads").await;
    channel.hold(OWNER, connection, b"uploaded once\n").await;
    let uploads = || async {
        sqlx::query_scalar::<_, i64>(
            "select count(*) from notes.connection_blobs where connection_id = $1",
        )
        .bind(connection)
        .fetch_one(channel.fixture.pool())
        .await
        .expect("the uploads can be counted")
    };
    let before = uploads().await;

    let removed = crate::support::send(
        &channel.router,
        axum::http::Method::DELETE,
        &format!("/v1/notes/connections/{connection}"),
        OWNER,
        None,
    )
    .await;

    assert_eq!(before, 1, "the upload is recorded for its connection");
    assert_eq!(removed.status, StatusCode::NO_CONTENT, "{:?}", removed.body);
    assert_eq!(
        uploads().await,
        0,
        "the removal deletes the uploads of the connection"
    );

    channel.cleanup().await;
}

/// `DELETE /v1/notes/connections/{id}/blobs/{token}` with `claims` of `owner`.
async fn close(
    channel: &Channel,
    owner: Uuid,
    connection: Uuid,
    token: &str,
    claims: &[(&str, String)],
) -> Answer {
    send_with(
        &channel.router,
        Method::DELETE,
        &format!("/v1/notes/connections/{connection}/blobs/{token}"),
        owner,
        claims,
        None,
    )
    .await
}

/// The claims of the plugin of `owner`.
fn plugin(owner: Uuid) -> [(&'static str, String); 2] {
    [
        (DEVICE_ID_HEADER, device_of(owner).to_string()),
        (DEVICE_KIND_HEADER, "obsidian_plugin".to_owned()),
    ]
}

/// The number of staged copies of the blob store of `channel`.
fn staged(channel: &Channel) -> usize {
    std::fs::read_dir(channel.blobs.root().join("staging")).map_or(0, Iterator::count)
}

/// The resumption token of an answer that opened a session.
fn token(opened: &Answer) -> String {
    opened.body["resumption_token"]
        .as_str()
        .unwrap_or_default()
        .to_owned()
}

#[tokio::test]
async fn closed_session_deletes_its_chunks_and_frees_its_slot() {
    let channel = Channel::new().await;
    let connection = channel.plugin_connection(OWNER, "vault-key-close").await;
    let bytes = b"never finalized\n";
    let mut tokens = Vec::new();
    for _ in 0..SESSIONS_PER_CONNECTION {
        let opened = channel
            .device_call_as(OWNER, connection, "blobs", declaration(bytes))
            .await;
        tokens.push(token(&opened));
    }
    let chunk = channel
        .put_chunk(OWNER, connection, &tokens[0], 0, bytes)
        .await;
    let before = staged(&channel);

    let closed = close(&channel, OWNER, connection, &tokens[0], &plugin(OWNER)).await;
    let after = staged(&channel);
    let again = channel
        .device_call_as(OWNER, connection, "blobs", declaration(bytes))
        .await;
    let replay = channel
        .put_chunk(OWNER, connection, &tokens[0], 0, bytes)
        .await;

    assert_eq!(chunk.status, StatusCode::OK, "{:?}", chunk.body);
    assert_eq!(closed.status, StatusCode::NO_CONTENT, "{:?}", closed.body);
    assert_eq!(
        after,
        before - 1,
        "the staged copy of the closed session is deleted"
    );
    assert_eq!(again.status, StatusCode::CREATED, "{:?}", again.body);
    assert_eq!(replay.status, StatusCode::NOT_FOUND, "{:?}", replay.body);

    channel.cleanup().await;
}

#[tokio::test]
async fn closing_unknown_or_foreign_token_is_not_found() {
    let channel = Channel::new().await;
    let connection = channel.plugin_connection(OWNER, "vault-key-mine").await;
    let other = channel.plugin_connection(OWNER, "vault-key-theirs").await;
    let opened = channel
        .device_call_as(OWNER, other, "blobs", declaration(b"other\n"))
        .await;
    let unknown = "rst_00000000000000000000000000";

    let answers = [
        close(&channel, OWNER, connection, unknown, &plugin(OWNER)).await,
        close(&channel, OWNER, connection, &token(&opened), &plugin(OWNER)).await,
    ];
    let status = send_with(
        &channel.router,
        Method::GET,
        &format!(
            "/v1/notes/connections/{other}/blobs/{}/status",
            token(&opened)
        ),
        OWNER,
        &plugin(OWNER),
        None,
    )
    .await;

    for answer in &answers {
        assert_eq!(answer.status, StatusCode::NOT_FOUND, "{:?}", answer.body);
        assert_eq!(answer.body["code"], "blob_transfer.session_unknown");
    }
    assert_eq!(
        status.status,
        StatusCode::OK,
        "the other session stays open"
    );

    channel.cleanup().await;
}

#[tokio::test]
async fn site_owner_closes_its_upload_session() {
    let writer = Writer::new().await;
    let connection = writer.vault(&[("Home.md", b"home\n")]).await;
    let site = [
        (SESSION_KIND_HEADER, "site_owner".to_owned()),
        (ORIGIN_HEADER, "site:site-a".to_owned()),
    ];
    let opened = writer
        .claims_call(OWNER, connection, "blobs", &site, declaration(b"edit\n"))
        .await;

    let nameless = close(
        &writer.channel,
        OWNER,
        connection,
        &token(&opened),
        &site[..1],
    )
    .await;
    let closed = close(&writer.channel, OWNER, connection, &token(&opened), &site).await;

    assert_eq!(opened.status, StatusCode::CREATED, "{:?}", opened.body);
    assert_eq!(
        nameless.status,
        StatusCode::UNAUTHORIZED,
        "{:?}",
        nameless.body
    );
    assert_eq!(closed.status, StatusCode::NO_CONTENT, "{:?}", closed.body);

    writer.cleanup().await;
}
