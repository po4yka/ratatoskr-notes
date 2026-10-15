//! The echo of Ratatoskr's own write is its acknowledgement, and an acknowledgement with another
//! hash makes the vault's bytes current (workspace change `add-notes-write-back`, spec
//! `notes-write-delivery`, task 7.1). The tests use a fake `sync` channel: the test takes each
//! delivery and reports what the Headless client leaves in the working tree, as the watcher does.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions read the answers they wait for"
)]

mod support;

use ratatoskr_notes::writes::{Handled, Outcome};

use crate::support::channel::sha256;
use crate::support::writes::{Writer, blob_ref, command, update};

#[tokio::test]
async fn own_write_is_acknowledged_once() {
    let writer = Writer::new().await;
    let base = b"# Plan\n\nIn the vault.\n";
    let mine = b"# Plan\n\nWritten through Ratatoskr.\n";
    let connection = writer.sync_vault(&[("Plan.md", base)]).await;
    writer.keep_for_sync(connection, mine).await;
    let write = command(
        connection,
        &update("Plan.md", &sha256(base), &blob_ref(mine, "text/markdown")),
    );
    assert!(matches!(writer.send(&write).await, Handled::Queued { .. }));
    assert_eq!(writer.deliveries(connection).await.len(), 1);

    // The channel writes the file, and the Headless client reports the same path with the same
    // hash.
    writer.tree_change(connection, "Plan.md", mine).await;

    assert_eq!(
        writer.outcome(&write).await,
        Some(Outcome::Succeeded {
            revision: Some(sha256(mine)),
        }),
        "the echo acknowledges the write"
    );
    assert!(writer.deliveries(connection).await.is_empty());
    assert_eq!(
        writer.changed(connection).await,
        [sha256(base), sha256(mine)],
        "exactly one notes.file.changed.v1 names the write"
    );
    assert_eq!(
        writer.channel.revisions(connection).await,
        2,
        "exactly one revision is added for the write"
    );
    assert_eq!(
        writer.current(connection, "Plan.md").await,
        Some(sha256(mine))
    );

    writer.cleanup().await;
}

#[tokio::test]
async fn different_ack_hash_becomes_current() {
    let writer = Writer::new().await;
    let base = b"# Plan\n\nIn the vault.\n";
    let mine = b"# Plan\n\nWritten through Ratatoskr.\n";
    // A plugin in the vault formats the file when it is saved.
    let formatted = b"# Plan\n\nWritten through Ratatoskr.\n\n";
    let connection = writer.sync_vault(&[("Plan.md", base)]).await;
    writer.keep_for_sync(connection, mine).await;
    let write = command(
        connection,
        &update("Plan.md", &sha256(base), &blob_ref(mine, "text/markdown")),
    );
    assert!(matches!(writer.send(&write).await, Handled::Queued { .. }));
    let delivery = writer.deliveries(connection).await.remove(0);

    writer.store(formatted);
    let acknowledged = writer
        .writes
        .acknowledge(connection, delivery.write_id, Some(&sha256(formatted)))
        .await
        .expect("the acknowledgement is recorded");
    assert!(acknowledged);

    assert_eq!(
        writer.current(connection, "Plan.md").await,
        Some(sha256(formatted)),
        "the bytes that the vault holds are current"
    );
    assert_eq!(
        writer.changed(connection).await,
        [sha256(base), sha256(formatted)],
        "exactly one event names the formatted bytes, and none the expected ones"
    );
    assert_eq!(
        writer.outcome(&write).await,
        Some(Outcome::Succeeded {
            revision: Some(sha256(formatted)),
        })
    );
    let reports = writer.reports(write.operation_id).await;
    let ended = reports.last().expect("a report");
    assert_eq!(ended["status"], "succeeded", "{ended:?}");
    assert_eq!(
        ended["warnings"][0]["code"], "notes.write.transformed",
        "{ended:?}"
    );

    writer.cleanup().await;
}
