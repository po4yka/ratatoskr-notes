//! The owner turns the write mode of a connection on or off (workspace change
//! `add-notes-write-back`, spec `notes-write-command`, requirement "The owner turns write mode on
//! or off", task 6.1).

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions read the answers they wait for"
)]

mod support;

use axum::http::StatusCode;
use ratatoskr_notes::writes::{Handled, Outcome};

use crate::support::channel::{OTHER_OWNER, OWNER, sha256};
use crate::support::writes::{Writer, command, update};

#[tokio::test]
async fn turning_on_enables_writes() {
    let writer = Writer::new().await;
    let base = b"# Plan\n\nIn the vault.\n";
    let mine = b"# Plan\n\nWritten after write mode is on.\n";
    let connection = writer.vault(&[("Plan.md", base)]).await;
    writer.set_write_mode(connection, "off").await;
    let blob = writer.stage(connection, mine).await;

    let (operation_id, answer) = writer.put_write_mode(OWNER, connection, "on").await;

    assert_eq!(answer.status, StatusCode::ACCEPTED, "{:?}", answer.body);
    assert_eq!(answer.body["write_mode"], "on", "{:?}", answer.body);
    assert_eq!(
        answer.body["operation"]["kind"], "notes.connection.write_mode",
        "{:?}",
        answer.body
    );
    let ended = writer.terminal(operation_id).await;
    assert_eq!(ended["status"], "succeeded", "{ended:?}");
    assert_eq!(writer.write_mode(connection).await, "on");

    let write = command(connection, &update("Plan.md", &sha256(base), &blob));
    assert!(matches!(writer.send(&write).await, Handled::Queued { .. }));
    let deliveries = writer.deliveries(connection).await;
    assert_eq!(deliveries.len(), 1, "a following write is delivered");
    assert_eq!(deliveries[0].operation_id, write.operation_id);

    writer.cleanup().await;
}

#[tokio::test]
async fn turning_off_fails_waiting_writes() {
    let writer = Writer::new().await;
    let base: &[u8] = b"# Plan\n\nIn the vault.\n";
    let first: &[u8] = b"# Plan\n\nThe first edit.\n";
    let second: &[u8] = b"# Plan\n\nThe second edit.\n";
    let third: &[u8] = b"# Plan\n\nThe third edit.\n";
    let connection = writer.vault(&[("Plan.md", base)]).await;
    let mut sent = Vec::new();
    for (on, bytes) in [(base, first), (first, second), (second, third)] {
        let blob = writer.stage(connection, bytes).await;
        let write = command(connection, &update("Plan.md", &sha256(on), &blob));
        assert!(matches!(writer.send(&write).await, Handled::Queued { .. }));
        sent.push(write);
    }
    let [delivered, waiting @ ..] = sent.as_slice() else {
        unreachable!("three writes");
    };
    assert_eq!(writer.deliveries(connection).await.len(), 1);

    let (operation_id, answer) = writer.put_write_mode(OWNER, connection, "off").await;

    assert_eq!(answer.status, StatusCode::ACCEPTED, "{:?}", answer.body);
    let ended = writer.terminal(operation_id).await;
    assert_eq!(ended["status"], "succeeded", "{ended:?}");
    assert_eq!(writer.write_mode(connection).await, "off");
    for write in waiting {
        assert_eq!(
            writer.outcome(write).await,
            Some(Outcome::Failed {
                code: "notes.write.disabled".to_owned(),
                conflict_id: None,
            }),
            "a write that waits in the queue fails"
        );
    }
    assert_eq!(
        writer.outcome(delivered).await,
        Some(Outcome::Pending),
        "a delivered write finishes"
    );
    assert_eq!(
        writer.current(connection, "Plan.md").await,
        Some(sha256(base)),
        "the current revision does not change"
    );

    writer.cleanup().await;
}

#[tokio::test]
async fn foreign_toggle_is_not_found() {
    let writer = Writer::new().await;
    let connection = writer.vault(&[("Plan.md", b"# Plan\n")]).await;
    writer.set_write_mode(connection, "off").await;

    let (operation_id, answer) = writer.put_write_mode(OTHER_OWNER, connection, "on").await;

    assert_eq!(answer.status, StatusCode::NOT_FOUND, "{:?}", answer.body);
    assert_eq!(
        answer.body["code"], "notes.connection.not_found",
        "{:?}",
        answer.body
    );
    assert_eq!(
        writer.write_mode(connection).await,
        "off",
        "the mode does not change"
    );
    assert!(
        writer.reports(operation_id).await.is_empty(),
        "notes reports nothing: Edge fails the operation with the refusal"
    );

    writer.cleanup().await;
}

#[tokio::test]
async fn connection_list_shows_the_write_mode() {
    let writer = Writer::new().await;
    let connection = writer.vault(&[("Plan.md", b"# Plan\n")]).await;
    writer.set_write_mode(connection, "off").await;

    let mode_listed = || async {
        let listed = crate::support::send(
            &writer.channel.router,
            axum::http::Method::GET,
            "/v1/notes/connections",
            OWNER,
            None,
        )
        .await;
        assert_eq!(listed.status, StatusCode::OK, "{:?}", listed.body);
        listed.body["connections"]
            .as_array()
            .and_then(|connections| {
                connections
                    .iter()
                    .find(|listed| listed["connection_id"] == connection.to_string())
            })
            .map(|listed| listed["write_mode"].clone())
            .expect("the connection is listed")
    };
    assert_eq!(mode_listed().await, "off", "every connection starts off");

    let (operation_id, answer) = writer.put_write_mode(OWNER, connection, "on").await;
    assert_eq!(answer.status, StatusCode::ACCEPTED, "{:?}", answer.body);
    assert_eq!(writer.terminal(operation_id).await["status"], "succeeded");
    assert_eq!(mode_listed().await, "on", "the owner sees the mode it set");

    writer.cleanup().await;
}
