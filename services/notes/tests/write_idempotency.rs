//! The same idempotency key gives the same result (workspace change `add-notes-write-back`, spec
//! `notes-write-command`, task 4.11).

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions read the deliveries they wait for"
)]

mod support;

use ratatoskr_notes::writes::{Handled, Outcome};
use uuid::Uuid;

use crate::support::channel::{OWNER, sha256};
use crate::support::writes::{Writer, command_with, update};

/// The number of writes that notes recorded for `connection_id`.
async fn writes(writer: &Writer, connection_id: Uuid) -> i64 {
    sqlx::query_scalar("select count(*) from notes.writes where connection_id = $1")
        .bind(connection_id)
        .fetch_one(writer.channel.fixture.pool())
        .await
        .expect("the writes can be counted")
}

#[tokio::test]
async fn retry_returns_same_revision() {
    let writer = Writer::new().await;
    let base = b"# Plan\n\nIn the vault.\n";
    let mine = b"# Plan\n\nSent twice after a timeout.\n";
    let connection = writer.vault(&[("Plan.md", base)]).await;
    let blob = writer.stage(connection, mine).await;
    let operation = update("Plan.md", &sha256(base), &blob);

    let first = command_with(OWNER, connection, "platform", "retry-key", &operation);
    let retry = command_with(OWNER, connection, "platform", "retry-key", &operation);
    let first_handled = writer.send(&first).await;
    let retry_handled = writer.send(&retry).await;
    let delivered = writer.deliveries(connection).await;
    for delivery in &delivered {
        writer.acknowledge(connection, delivery).await;
    }
    let late = command_with(OWNER, connection, "platform", "retry-key", &operation);
    let late_handled = writer.send(&late).await;

    let write_id = match first_handled {
        Handled::Queued { write_id } => Some(write_id),
        _ => None,
    }
    .expect("the first command is queued");
    assert_eq!(retry_handled, Handled::Repeated { write_id });
    assert_eq!(late_handled, Handled::Repeated { write_id });
    assert_eq!(delivered.len(), 1, "one write is delivered");
    assert_eq!(writes(&writer, connection).await, 1, "one write is queued");
    let succeeded = Some(Outcome::Succeeded {
        revision: Some(sha256(mine)),
    });
    assert_eq!(writer.outcome(&first).await, succeeded);
    assert_eq!(writer.outcome(&retry).await, succeeded);
    assert_eq!(writer.outcome(&late).await, succeeded);
    assert_eq!(
        writer
            .changed(connection)
            .await
            .iter()
            .filter(|revision| **revision == sha256(mine))
            .count(),
        1
    );

    writer.cleanup().await;
}

#[tokio::test]
async fn reused_key_with_other_content_is_invalid() {
    let writer = Writer::new().await;
    let base = b"# Plan\n\nIn the vault.\n";
    let mine = b"# Plan\n\nThe first content of the key.\n";
    let other = b"# Plan\n\nOther content under the same key.\n";
    let connection = writer.vault(&[("Plan.md", base)]).await;
    let blob = writer.stage(connection, mine).await;
    let other_blob = writer.stage(connection, other).await;

    let first = command_with(
        OWNER,
        connection,
        "platform",
        "reused-key",
        &update("Plan.md", &sha256(base), &blob),
    );
    writer.send(&first).await;
    for delivery in writer.deliveries(connection).await {
        writer.acknowledge(connection, &delivery).await;
    }
    let reused = command_with(
        OWNER,
        connection,
        "platform",
        "reused-key",
        &update("Plan.md", &sha256(mine), &other_blob),
    );
    let handled = writer.send(&reused).await;

    assert_eq!(
        handled,
        Handled::Refused {
            code: "notes.request.invalid"
        }
    );
    assert!(writer.deliveries(connection).await.is_empty());
    assert_eq!(writes(&writer, connection).await, 1);
    assert_eq!(
        writer.current(connection, "Plan.md").await,
        Some(sha256(mine))
    );
    assert_eq!(writer.outcome(&reused).await, None);

    writer.cleanup().await;
}
