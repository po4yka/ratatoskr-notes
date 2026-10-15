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

use ratatoskr_notes::sync_write::SyncWriter;
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
    let stages: Vec<(String, String)> = writer
        .reports(write.operation_id)
        .await
        .iter()
        .map(|report| {
            (
                report["status"].as_str().unwrap_or_default().to_owned(),
                report["stage"].as_str().unwrap_or_default().to_owned(),
            )
        })
        .collect();
    assert_eq!(
        stages,
        [
            ("running".to_owned(), "queued".to_owned()),
            ("running".to_owned(), "awaiting_obsidian".to_owned()),
            ("succeeded".to_owned(), String::new()),
        ],
        "a sync write waits for Obsidian Sync"
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

#[tokio::test]
async fn formatted_echo_after_the_write_landed_acknowledges_it() {
    let writer = Writer::new().await;
    let base: &[u8] = b"# Plan\n\nIn the vault.\n";
    let mine: &[u8] = b"# Plan\n\nWritten through Ratatoskr.\n";
    // Obsidian formats the file after the write landed in the working tree.
    let formatted: &[u8] = b"# Plan\n\nWritten through Ratatoskr.\n\n";
    let connection = writer.sync_vault(&[("Plan.md", base)]).await;
    let trees = tempfile::tempdir().expect("a trees root");
    let tree = trees.path().join(connection.to_string());
    std::fs::create_dir_all(&tree).expect("the working tree");
    std::fs::write(tree.join("Plan.md"), base).expect("the file is in the tree");
    writer.keep_for_sync(connection, mine).await;
    let write = command(
        connection,
        &update("Plan.md", &sha256(base), &blob_ref(mine, "text/markdown")),
    );
    assert!(matches!(writer.send(&write).await, Handled::Queued { .. }));

    SyncWriter::new(writer.recorder(), trees.path())
        .deliver(connection)
        .await
        .expect("the delivery runs");
    assert_eq!(
        std::fs::read(tree.join("Plan.md")).expect("the file"),
        mine,
        "the write landed"
    );
    // The first change of the path that the watcher reports has the formatted bytes.
    std::fs::write(tree.join("Plan.md"), formatted).expect("Obsidian formats the file");
    writer.tree_change(connection, "Plan.md", formatted).await;

    assert_eq!(
        writer.outcome(&write).await,
        Some(Outcome::Succeeded {
            revision: Some(sha256(formatted)),
        }),
        "the change after the write landed acknowledges it"
    );
    assert_eq!(
        writer.changed(connection).await,
        [sha256(base), sha256(formatted)],
        "one event names the formatted bytes, and none the bytes of the write"
    );
    let reports = writer.reports(write.operation_id).await;
    let ended = reports.last().expect("a report");
    assert_eq!(
        ended["warnings"][0]["code"], "notes.write.transformed",
        "{ended:?}"
    );

    writer.cleanup().await;
}
