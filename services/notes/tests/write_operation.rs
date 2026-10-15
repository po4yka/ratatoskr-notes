//! Every write is a Platform operation that notes reports (workspace change
//! `add-notes-write-back`, spec `notes-write-command`, task 4.15).

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions read the JSON reports they wait for"
)]

mod support;

use axum::http::StatusCode;
use serde_json::{Value, json};

use crate::support::channel::{OTHER_OWNER, OWNER, sha256};
use crate::support::writes::{Writer, blob_ref, command, command_with, update};

/// The status and the stage of each report.
fn steps(reports: &[Value]) -> Vec<(String, Option<String>)> {
    reports
        .iter()
        .map(|report| {
            (
                report["status"].as_str().unwrap_or_default().to_owned(),
                report["stage"].as_str().map(str::to_owned),
            )
        })
        .collect()
}

#[tokio::test]
async fn write_reports_to_success() {
    let writer = Writer::new().await;
    let base = b"# Plan\n\nIn the vault.\n";
    let mine = b"# Plan\n\nFollowed to the end.\n";
    let connection = writer.vault(&[("Plan.md", base)]).await;
    let blob = writer.stage(connection, mine).await;

    let write = command(connection, &update("Plan.md", &sha256(base), &blob));
    writer.send(&write).await;
    for delivery in writer.deliveries(connection).await {
        writer.acknowledge(connection, &delivery).await;
    }
    let reports = writer.reports(write.operation_id).await;
    let file_id: String = sqlx::query_scalar(
        "select file_id::text from notes.files where connection_id = $1 and path = 'Plan.md'",
    )
    .bind(connection)
    .fetch_one(writer.channel.fixture.pool())
    .await
    .expect("the file exists");

    assert_eq!(
        steps(&reports),
        vec![
            ("running".to_owned(), Some("queued".to_owned())),
            ("running".to_owned(), Some("awaiting_device".to_owned())),
            ("succeeded".to_owned(), None),
        ]
    );
    let result = &reports[2]["results"][0];
    assert_eq!(result["result_kind"], "notes.file");
    assert_eq!(result["target"], json!(format!("notes_file:{file_id}")));
    assert_eq!(result["blob"]["digest"]["hex"], json!(sha256(mine)));
    assert_eq!(result["blob"]["owner_service"], "ratatoskr-notes");

    writer.cleanup().await;
}

#[tokio::test]
async fn conflict_fails_with_its_id() {
    let writer = Writer::new().await;
    let base = b"# Plan\n\nThe draft that the writer read.\n";
    let phone = b"# Plan\n\nThe draft that the phone saved later.\n";
    let mine = b"# Plan\n\nThe draft that the writer saved.\n";
    let connection = writer.vault(&[("Plan.md", base)]).await;
    writer.vault_change(connection, "Plan.md", phone).await;
    let blob = writer.stage(connection, mine).await;

    let write = command_with(
        OWNER,
        connection,
        "site:docs",
        "conflict-key",
        &update("Plan.md", &sha256(base), &blob),
    );
    writer.send(&write).await;
    let reports = writer.reports(write.operation_id).await;
    let last = reports.last().cloned().unwrap_or_default();
    let conflict_id = last["error"]["conflict_id"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    let read = writer.conflict(OWNER, &conflict_id).await;
    let foreign = writer.conflict(OTHER_OWNER, &conflict_id).await;

    assert_eq!(
        steps(&reports),
        vec![
            ("running".to_owned(), Some("queued".to_owned())),
            ("failed".to_owned(), None),
        ]
    );
    assert_eq!(last["error"]["code"], "notes.write.conflict");
    assert_eq!(last["error"]["retryable"], false);
    assert_eq!(read.status, StatusCode::OK, "{:?}", read.body);
    assert_eq!(read.body["conflict_id"], json!(conflict_id));
    assert_eq!(read.body["operation_id"], json!(write.operation_id));
    assert_eq!(read.body["connection_id"], json!(connection));
    assert_eq!(read.body["path"], "Plan.md");
    assert_eq!(read.body["origin"], "site:docs");
    assert_eq!(read.body["state"], "open");
    assert_eq!(read.body["base_hash"], json!(sha256(base)));
    assert_eq!(read.body["current_hash"], json!(sha256(phone)));
    assert_eq!(read.body["writer_hash"], json!(sha256(mine)));
    assert_eq!(foreign.status, StatusCode::NOT_FOUND, "{:?}", foreign.body);
    assert_eq!(foreign.body["code"], "notes.conflict.not_found");

    writer.cleanup().await;
}

#[tokio::test]
async fn refused_write_fails_its_operation() {
    let writer = Writer::new().await;
    let base = b"# Plan\n\nIn the vault.\n";
    let connection = writer.vault(&[("Plan.md", base)]).await;

    let missing = command(
        connection,
        &update(
            "Plan.md",
            &sha256(base),
            &blob_ref(b"never uploaded\n", "text/markdown"),
        ),
    );
    writer.send(&missing).await;
    let reports = writer.reports(missing.operation_id).await;

    assert_eq!(steps(&reports), vec![("failed".to_owned(), None)]);
    assert_eq!(reports[0]["error"]["code"], "notes.write.content_missing");

    writer.cleanup().await;
}
