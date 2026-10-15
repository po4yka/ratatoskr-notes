//! The content of a write is uploaded first, by the owner's session, and checked when the write
//! is taken (workspace change `add-notes-write-back`, specs `notes-write-command` and
//! `notes-device-channels`, task 4.9).

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions read the answers they wait for"
)]

mod support;

use axum::http::StatusCode;
use ratatoskr_notes::writes::Handled;
use serde_json::Value;

use crate::support::channel::{OTHER_OWNER, OWNER, change, sha256};
use crate::support::writes::{Writer, blob_ref, command, update};

#[tokio::test]
async fn missing_blob_fails_the_write() {
    let writer = Writer::new().await;
    let base = b"# Plan\n\nIn the vault.\n";
    let uploaded = b"# Plan\n\nUploaded by the browser.\n";
    let never = b"# Plan\n\nNever uploaded.\n";
    let connection = writer.vault(&[("Plan.md", base)]).await;

    let [opened, _, finalized] = writer.session_upload(OWNER, connection, uploaded).await;
    let write = command(
        connection,
        &update("Plan.md", &sha256(base), &blob_ref(never, "text/markdown")),
    );
    let handled = writer.send(&write).await;

    assert_eq!(opened.status, StatusCode::CREATED, "{:?}", opened.body);
    assert_eq!(finalized.body["outcome"], "stored", "{:?}", finalized.body);
    assert_eq!(
        handled,
        Handled::Refused {
            code: "notes.write.content_missing"
        }
    );
    assert!(writer.deliveries(connection).await.is_empty());
    assert_eq!(
        writer.current(connection, "Plan.md").await,
        Some(sha256(base))
    );

    writer.cleanup().await;
}

#[tokio::test]
async fn uploaded_blob_is_not_a_revision() {
    let writer = Writer::new().await;
    let uploaded = b"# Draft\n\nUploaded, and no write names it.\n";
    let connection = writer.vault(&[("Home.md", b"home\n")]).await;

    let [opened, chunk, finalized] = writer.session_upload(OWNER, connection, uploaded).await;
    let revisions: i64 =
        sqlx::query_scalar("select count(*) from notes.revisions where revision = $1")
            .bind(sha256(uploaded))
            .fetch_one(writer.channel.fixture.pool())
            .await
            .expect("the revisions can be counted");

    assert_eq!(opened.status, StatusCode::CREATED, "{:?}", opened.body);
    assert_eq!(chunk.status, StatusCode::OK, "{:?}", chunk.body);
    assert_eq!(finalized.body["outcome"], "stored", "{:?}", finalized.body);
    assert!(
        writer.registered(connection, &sha256(uploaded)).await,
        "the blob is registered for the connection"
    );
    assert_eq!(revisions, 0, "an upload is not a revision");
    assert!(!writer.changed(connection).await.contains(&sha256(uploaded)));

    writer.cleanup().await;
}

#[tokio::test]
async fn session_cannot_send_change_batches() {
    let writer = Writer::new().await;
    let bytes = b"# Intruder\n";
    let connection = writer.vault(&[("Home.md", b"home\n")]).await;
    writer.channel.hold(OWNER, connection, bytes).await;
    let events_before = writer.changed(connection).await;

    let answer = writer
        .session_call(
            OWNER,
            connection,
            "changes",
            serde_json::json!({ "changes": [change(1, "created", "Intruder.md", bytes)] }),
        )
        .await;
    let manifest = writer
        .session_call(
            OWNER,
            connection,
            "manifest",
            serde_json::json!({ "entries": [] }),
        )
        .await;

    assert_eq!(answer.status, StatusCode::CONFLICT, "{:?}", answer.body);
    assert_eq!(answer.body["code"], "notes.channel_mismatch");
    assert_eq!(manifest.status, StatusCode::CONFLICT, "{:?}", manifest.body);
    assert_eq!(manifest.body["code"], "notes.channel_mismatch");
    assert_eq!(writer.current(connection, "Intruder.md").await, None);
    assert_eq!(writer.changed(connection).await, events_before);

    writer.cleanup().await;
}

#[tokio::test]
async fn foreign_session_upload_is_not_found() {
    let writer = Writer::new().await;
    let bytes = b"# Foreign\n\nFrom another owner.\n";
    let connection = writer.vault(&[("Home.md", b"home\n")]).await;

    let [opened, _, finalized] = writer.session_upload(OTHER_OWNER, connection, bytes).await;

    assert_eq!(opened.status, StatusCode::NOT_FOUND, "{:?}", opened.body);
    assert_eq!(opened.body["code"], "notes.connection.not_found");
    assert_ne!(finalized.body["outcome"], Value::from("stored"));
    assert!(
        !writer.registered(connection, &sha256(bytes)).await,
        "no blob is registered"
    );

    writer.cleanup().await;
}
