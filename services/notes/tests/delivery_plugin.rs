//! The ordered queue of a device connection (workspace change `add-notes-write-back`, spec
//! `notes-write-delivery`, requirements "A write is delivered only through the connection's
//! channel" and "Device channels read an ordered queue through Edge", task 7.3).

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions read the answers they wait for"
)]

mod support;

use axum::http::StatusCode;
use ratatoskr_notes::writes::{Handled, Outcome};
use serde_json::{Value, json};

use crate::support::channel::{OTHER_OWNER, OWNER, SECOND_DEVICE, sha256};
use crate::support::device_of;
use crate::support::writes::{Writer, command, update};

/// The operations of a queue answer.
fn operations(answer: &Value) -> Vec<Value> {
    answer["operations"].as_array().cloned().unwrap_or_default()
}

#[tokio::test]
async fn offline_device_delivers_on_reconnect() {
    let writer = Writer::new().await;
    let base = b"# Plan\n\nIn the vault.\n";
    let mine = b"# Plan\n\nWritten while the phone was away.\n";
    let connection = writer.vault(&[("Plan.md", base)]).await;
    let blob = writer.stage(connection, mine).await;
    let write = command(connection, &update("Plan.md", &sha256(base), &blob));
    assert!(matches!(writer.send(&write).await, Handled::Queued { .. }));

    // The device has not connected: the write waits for it, and its stage names the wait.
    assert_eq!(writer.outcome(&write).await, Some(Outcome::Pending));
    let reports = writer.reports(write.operation_id).await;
    let last = reports.last().expect("a report");
    assert_eq!(
        (&last["status"], &last["stage"]),
        (&json!("running"), &json!("awaiting_device")),
        "{reports:?}"
    );

    // The device connects and reads its queue.
    let device = device_of(OWNER);
    let read = writer
        .device_operations(OWNER, device, connection, None)
        .await;
    assert_eq!(read.status, StatusCode::OK, "{:?}", read.body);
    let queued = operations(&read.body);
    assert_eq!(queued.len(), 1, "{:?}", read.body);
    let operation = &queued[0];
    assert_eq!(operation["operation_id"], write.operation_id.to_string());
    assert_eq!(operation["kind"], "update");
    assert_eq!(operation["path"], "Plan.md");
    assert_eq!(operation["base_hash"], sha256(base));
    assert_eq!(operation["content"]["digest"]["hex"], sha256(mine));
    let operation_id = operation["operation_id"].as_str().expect("an id");
    let (status, bytes) = writer
        .device_content(OWNER, device, connection, operation_id)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(bytes, mine, "the device reads the bytes of the write");

    let answer = writer
        .device_result(
            OWNER,
            device,
            connection,
            operation_id,
            json!({ "outcome": "applied", "sha256": sha256(mine) }),
        )
        .await;
    assert_eq!(answer.status, StatusCode::OK, "{:?}", answer.body);

    assert_eq!(
        writer.outcome(&write).await,
        Some(Outcome::Succeeded {
            revision: Some(sha256(mine)),
        })
    );
    assert_eq!(
        writer.current(connection, "Plan.md").await,
        Some(sha256(mine))
    );
    let again = writer
        .device_operations(OWNER, device, connection, read.body["next_cursor"].as_str())
        .await;
    assert_eq!(
        operations(&again.body),
        Vec::<Value>::new(),
        "the queue is empty"
    );

    writer.cleanup().await;
}

#[tokio::test]
async fn queue_keeps_order() {
    let writer = Writer::new().await;
    let base = b"# Alpha\n\nThe first plan.\n";
    let edited = b"# Alpha\n\nThe plan after the rename.\n";
    let connection = writer.vault(&[("A.md", base)]).await;
    let rename = command(
        connection,
        &json!({ "rename": { "path": "A.md", "target_path": "B.md", "base_hash": sha256(base) } }),
    );
    assert!(matches!(writer.send(&rename).await, Handled::Queued { .. }));
    let blob = writer.stage(connection, edited).await;
    let edit = command(connection, &update("B.md", &sha256(base), &blob));
    assert!(matches!(writer.send(&edit).await, Handled::Queued { .. }));

    let device = device_of(OWNER);
    let mut received = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..2 {
        let read = writer
            .device_operations(OWNER, device, connection, cursor.as_deref())
            .await;
        assert_eq!(read.status, StatusCode::OK, "{:?}", read.body);
        for operation in operations(&read.body) {
            let (hash, kind) = match operation["kind"].as_str() {
                Some("rename") => (sha256(base), "rename"),
                _ => (sha256(edited), "update"),
            };
            received.push(kind.to_owned());
            let answer = writer
                .device_result(
                    OWNER,
                    device,
                    connection,
                    operation["operation_id"].as_str().expect("an id"),
                    json!({ "outcome": "applied", "sha256": hash }),
                )
                .await;
            assert_eq!(answer.status, StatusCode::OK, "{:?}", answer.body);
        }
        cursor = read.body["next_cursor"].as_str().map(str::to_owned);
    }

    assert_eq!(
        received,
        ["rename", "update"],
        "the device receives the rename before the update of the new path"
    );
    assert_eq!(writer.current(connection, "A.md").await, None);
    assert_eq!(
        writer.current(connection, "B.md").await,
        Some(sha256(edited))
    );

    writer.cleanup().await;
}

#[tokio::test]
async fn foreign_device_reads_nothing() {
    let writer = Writer::new().await;
    let base = b"# Plan\n\nIn the vault.\n";
    let mine = b"# Plan\n\nA private edit.\n";
    let connection = writer.vault(&[("Plan.md", base)]).await;
    let blob = writer.stage(connection, mine).await;
    let write = command(connection, &update("Plan.md", &sha256(base), &blob));
    assert!(matches!(writer.send(&write).await, Handled::Queued { .. }));
    let own = writer
        .device_operations(OWNER, device_of(OWNER), connection, None)
        .await;
    assert_eq!(own.status, StatusCode::OK, "{:?}", own.body);
    assert_eq!(operations(&own.body).len(), 1, "{:?}", own.body);
    let operation_id = write.operation_id.to_string();

    for (owner, device) in [
        (OWNER, SECOND_DEVICE),
        (OTHER_OWNER, device_of(OTHER_OWNER)),
    ] {
        let read = writer
            .device_operations(owner, device, connection, None)
            .await;
        assert_eq!(read.status, StatusCode::NOT_FOUND, "{:?}", read.body);
        assert_eq!(read.body["code"], "notes.connection.not_found");
        assert!(read.body.get("operations").is_none(), "{:?}", read.body);
        let (status, bytes) = writer
            .device_content(owner, device, connection, &operation_id)
            .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(!String::from_utf8_lossy(&bytes).contains("private edit"));
        let answer = writer
            .device_result(
                owner,
                device,
                connection,
                &operation_id,
                json!({ "outcome": "applied", "sha256": sha256(mine) }),
            )
            .await;
        assert_eq!(answer.status, StatusCode::NOT_FOUND, "{:?}", answer.body);
    }
    assert_eq!(
        writer.outcome(&write).await,
        Some(Outcome::Pending),
        "no other device answers the write"
    );

    writer.cleanup().await;
}

#[tokio::test]
async fn cursor_never_skips_a_later_delivered_write() {
    let writer = Writer::new().await;
    let x0: &[u8] = b"# X\n\nIn the vault.\n";
    let x1: &[u8] = b"# X\n\nThe first edit.\n";
    let x2: &[u8] = b"# X\n\nThe second edit.\n";
    let y0: &[u8] = b"# Y\n\nIn the vault.\n";
    let y1: &[u8] = b"# Y\n\nAn edit.\n";
    let connection = writer.vault(&[("X.md", x0), ("Y.md", y0)]).await;
    // W1 and W2 edit X.md in turn; W3 edits Y.md. W2 is accepted before W3 and waits behind W1.
    let mut sent = Vec::new();
    for (path, base, bytes) in [("X.md", x0, x1), ("X.md", x1, x2), ("Y.md", y0, y1)] {
        let blob = writer.stage(connection, bytes).await;
        let write = command(connection, &update(path, &sha256(base), &blob));
        assert!(matches!(writer.send(&write).await, Handled::Queued { .. }));
        sent.push(write);
    }
    let device = device_of(OWNER);

    let first = writer
        .device_operations(OWNER, device, connection, None)
        .await;
    let ids: Vec<Value> = operations(&first.body)
        .iter()
        .map(|operation| operation["operation_id"].clone())
        .collect();
    assert_eq!(
        ids,
        [
            json!(sent[0].operation_id.to_string()),
            json!(sent[2].operation_id.to_string())
        ],
        "the device reads W1 and W3"
    );
    let answer = writer
        .device_result(
            OWNER,
            device,
            connection,
            &sent[0].operation_id.to_string(),
            json!({ "outcome": "applied", "sha256": sha256(x1) }),
        )
        .await;
    assert_eq!(answer.status, StatusCode::OK, "{:?}", answer.body);

    // W1 is acknowledged, so W2 is delivered now; the cursor of the first read must not hide it.
    let next = writer
        .device_operations(
            OWNER,
            device,
            connection,
            first.body["next_cursor"].as_str(),
        )
        .await;
    let ids: Vec<Value> = operations(&next.body)
        .iter()
        .map(|operation| operation["operation_id"].clone())
        .collect();
    assert_eq!(
        ids,
        [json!(sent[1].operation_id.to_string())],
        "the device reads W2 after the cursor of its first read"
    );

    writer.cleanup().await;
}
