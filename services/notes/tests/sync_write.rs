//! The `sync` channel writes the working tree atomically (workspace change `add-notes-write-back`,
//! spec `notes-write-delivery`, requirement "The sync channel writes the working tree atomically",
//! task 7.5).

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions read the answers they wait for"
)]

mod support;

use ratatoskr_notes::sync_write::{SyncWriter, write_atomically};
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

/// How many times the writer replaces the file while the reader reads it.
const WRITES: usize = 1_000;

/// The hashes that a reader saw while `write` replaced the file `WRITES` times with old and new
/// bytes in turn, other than the hashes of the old and the new bytes.
fn torn_reads(write: fn(&std::path::Path, &[u8]) -> std::io::Result<()>) -> Vec<String> {
    let folder = tempfile::tempdir().expect("a working tree");
    let path = folder.path().join("Plan.md");
    let old = vec![b'o'; 256 * 1024];
    let new = vec![b'n'; 256 * 1024];
    std::fs::write(&path, &old).expect("the old bytes are in the tree");
    let whole = [sha256(&old), sha256(&new)];
    let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let reader = {
        let (path, done) = (path.clone(), std::sync::Arc::clone(&done));
        std::thread::spawn(move || {
            let mut seen = Vec::new();
            while !done.load(std::sync::atomic::Ordering::Acquire) {
                if let Ok(bytes) = std::fs::read(&path) {
                    seen.push(sha256(&bytes));
                }
            }
            seen
        })
    };
    for round in 0..WRITES {
        let bytes = if round % 2 == 0 { &new } else { &old };
        write(&path, bytes).expect("the file is written");
    }
    done.store(true, std::sync::atomic::Ordering::Release);
    let seen = reader.join().expect("the reader ends");
    assert!(!seen.is_empty(), "the reader read the file");
    seen.into_iter()
        .filter(|hash| !whole.contains(hash))
        .collect()
}

#[test]
fn reader_never_sees_partial_file() {
    let torn = torn_reads(write_atomically);
    assert!(
        torn.is_empty(),
        "every hash that the reader saw is the old or the new one; {} were not",
        torn.len()
    );
}
