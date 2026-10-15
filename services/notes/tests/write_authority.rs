//! Notes checks the owner and the write mode of a connection when it takes a write (workspace
//! change `add-notes-write-back`, spec `notes-write-command`, task 4.13).

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions read the answers they wait for"
)]

mod support;

use ratatoskr_notes::writes::Handled;

use crate::support::channel::{OTHER_OWNER, sha256};
use crate::support::writes::{Writer, command, command_with, update};

#[tokio::test]
async fn foreign_connection_is_not_found() {
    let writer = Writer::new().await;
    let base = b"# Plan\n\nThe note of owner A.\n";
    let mine = b"# Plan\n\nWritten by owner B.\n";
    let connection = writer.vault(&[("Plan.md", base)]).await;
    let blob = writer.stage(connection, mine).await;

    let foreign = command_with(
        OTHER_OWNER,
        connection,
        "platform",
        "foreign-key",
        &update("Plan.md", &sha256(base), &blob),
    );
    let handled = writer.send(&foreign).await;

    assert_eq!(
        handled,
        Handled::Refused {
            code: "notes.connection.not_found"
        }
    );
    assert!(writer.deliveries(connection).await.is_empty());
    assert_eq!(
        writer.current(connection, "Plan.md").await,
        Some(sha256(base))
    );
    assert_eq!(writer.outcome(&foreign).await, None);

    writer.cleanup().await;
}

#[tokio::test]
async fn write_mode_off_is_refused() {
    let writer = Writer::new().await;
    let base = b"# Plan\n\nIn the vault.\n";
    let mine = b"# Plan\n\nWritten while write mode is off.\n";
    let connection = writer.vault(&[("Plan.md", base)]).await;
    let blob = writer.stage(connection, mine).await;
    writer.set_write_mode(connection, "off").await;

    let write = command(connection, &update("Plan.md", &sha256(base), &blob));
    let handled = writer.send(&write).await;

    assert_eq!(
        handled,
        Handled::Refused {
            code: "notes.write.disabled"
        }
    );
    assert!(writer.deliveries(connection).await.is_empty());
    assert_eq!(
        writer.current(connection, "Plan.md").await,
        Some(sha256(base))
    );
    assert!(!writer.changed(connection).await.contains(&sha256(mine)));

    writer.cleanup().await;
}
