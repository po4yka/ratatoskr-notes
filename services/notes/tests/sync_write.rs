//! The `sync` channel writes the working tree atomically (workspace change `add-notes-write-back`,
//! spec `notes-write-delivery`, requirement "The sync channel writes the working tree atomically",
//! task 7.5).

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
async fn tree_change_before_write_conflicts() {
    let writer = Writer::new().await;
    let base = b"# Plan\n\nIn the vault.\n";
    let mine = b"# Plan\n\nWritten through Ratatoskr.\n";
    let theirs = b"# Plan\n\nWritten on the phone a moment ago.\n";
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
    assert_eq!(
        writer.deliveries(connection).await.len(),
        1,
        "the write passed the queue check"
    );

    // The Headless client changes the file after the queue check and before notes writes it.
    std::fs::write(tree.join("Plan.md"), theirs).expect("the client writes the file");
    SyncWriter::new(writer.recorder(), trees.path())
        .deliver(connection)
        .await
        .expect("the delivery runs");

    let outcome = writer.outcome(&write).await;
    assert!(
        matches!(
            &outcome,
            Some(Outcome::Failed { code, conflict_id: Some(_) }) if code == "notes.write.conflict"
        ),
        "the write fails with a conflict: {outcome:?}"
    );
    assert_eq!(
        std::fs::read(tree.join("Plan.md")).expect("the file"),
        theirs,
        "notes does not write the file"
    );
    let conflicts: Vec<Option<String>> =
        sqlx::query_scalar("select current_hash from notes.conflicts where connection_id = $1")
            .bind(connection)
            .fetch_all(writer.channel.fixture.pool())
            .await
            .expect("the conflicts can be read");
    assert_eq!(
        conflicts,
        [Some(sha256(theirs))],
        "one conflict names the bytes of the tree"
    );

    writer.cleanup().await;
}
