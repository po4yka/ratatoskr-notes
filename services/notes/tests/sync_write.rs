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

/// The fault mode of this test: a direct write, which truncates the file and then fills it.
fn direct_write(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    std::fs::write(path, bytes)
}

#[test]
fn reader_never_sees_partial_file() {
    // The reader can see a torn file: against the fault mode it sees one.
    assert!(
        !torn_reads(direct_write).is_empty(),
        "the reader sees a torn file of a direct write"
    );
    let torn = torn_reads(write_atomically);
    assert!(
        torn.is_empty(),
        "every hash that the reader saw is the old or the new one; {} were not",
        torn.len()
    );
}

#[tokio::test]
async fn symlinks_never_lead_out_of_the_tree() {
    let writer = Writer::new().await;
    let secret: &[u8] = b"# Plan\n\nA file outside the tree.\n";
    let note: &[u8] = b"# Note\n\nAnother file outside the tree.\n";
    let private: &[u8] = b"# Private\n\nBytes that notes never held.\n";
    let decoy: &[u8] = b"# Lure\n\nWhat the database holds for the link.\n";
    let mine: &[u8] = b"# Plan\n\nWritten through Ratatoskr.\n";
    let outside = tempfile::tempdir().expect("a folder outside the tree");
    std::fs::write(outside.path().join("Plan.md"), secret).expect("an outside file");
    std::fs::write(outside.path().join("Note.md"), note).expect("an outside file");
    std::fs::write(outside.path().join("Private.md"), private).expect("an outside file");
    let connection = writer
        .sync_vault(&[
            ("Linked/Plan.md", secret),
            ("Link.md", note),
            ("Lure.md", decoy),
        ])
        .await;
    let trees = tempfile::tempdir().expect("a trees root");
    let tree = trees.path().join(connection.to_string());
    std::fs::create_dir_all(&tree).expect("the working tree");
    // A link to a folder outside the tree, and two links to files outside it.
    std::os::unix::fs::symlink(outside.path(), tree.join("Linked")).expect("a folder link");
    std::os::unix::fs::symlink(outside.path().join("Note.md"), tree.join("Link.md"))
        .expect("a file link");
    std::os::unix::fs::symlink(outside.path().join("Private.md"), tree.join("Lure.md"))
        .expect("a file link");
    writer.keep_for_sync(connection, mine).await;
    let mine_ref = blob_ref(mine, "text/markdown");
    let through_links = [
        command(
            connection,
            &update("Linked/Plan.md", &sha256(secret), &mine_ref),
        ),
        command(
            connection,
            &serde_json::json!({ "delete": { "path": "Link.md", "base_hash": sha256(note) } }),
        ),
        command(connection, &update("Lure.md", &sha256(decoy), &mine_ref)),
    ];
    for write in &through_links {
        assert!(matches!(writer.send(write).await, Handled::Queued { .. }));
    }

    SyncWriter::new(writer.recorder(), trees.path())
        .deliver(connection)
        .await
        .expect("the delivery runs");

    for (name, bytes) in [
        ("Plan.md", secret),
        ("Note.md", note),
        ("Private.md", private),
    ] {
        assert_eq!(
            std::fs::read(outside.path().join(name)).expect("the outside file is there"),
            bytes,
            "{name} outside the tree does not change"
        );
    }
    for write in &through_links {
        assert_eq!(
            writer.outcome(write).await,
            Some(Outcome::Failed {
                code: "notes.write.unsafe_path".to_owned(),
                conflict_id: None,
            }),
            "a write through a link fails with its code"
        );
    }
    let pulled = writer
        .channel
        .blobs
        .path_of(&sha256(private))
        .expect("a blob path");
    assert!(
        !pulled.exists(),
        "bytes outside the tree never reach the blob store"
    );

    writer.cleanup().await;
}

#[tokio::test]
async fn set_property_changes_only_its_key() {
    let writer = Writer::new().await;
    let plan: &[u8] =
        b"---\ntitle: Plan\nstatus: draft\ntags:\n  - a\n  - b\n---\n# Plan\n\nThe body.\n";
    let note: &[u8] = b"---\ntitle: Note\n---\nBody: not a key.\n";
    let bare: &[u8] = b"# Bare\n\nNo frontmatter.\n";
    let connection = writer
        .sync_vault(&[("Plan.md", plan), ("Note.md", note), ("Bare.md", bare)])
        .await;
    let trees = tempfile::tempdir().expect("a trees root");
    let tree = trees.path().join(connection.to_string());
    std::fs::create_dir_all(&tree).expect("the working tree");
    for (name, bytes) in [("Plan.md", plan), ("Note.md", note), ("Bare.md", bare)] {
        std::fs::write(tree.join(name), bytes).expect("the file is in the tree");
    }
    let done: &[u8] = br#""done""#;
    let owner: &[u8] = br#"["Ann","Bo"]"#;
    writer.keep_for_sync(connection, done).await;
    writer.keep_for_sync(connection, owner).await;
    let set = |path: &str, base: &[u8], key: &str, value: &[u8]| {
        command(
            connection,
            &serde_json::json!({ "set_property": {
                "path": path,
                "base_hash": sha256(base),
                "property_key": key,
                "property_change": { "set": blob_ref(value, "application/json") },
            } }),
        )
    };
    let settings = [
        // Replace the value of a key, and keep every other line byte for byte.
        set("Plan.md", plan, "status", done),
        // Add a key that the frontmatter does not have.
        set("Note.md", note, "owner", owner),
        // Give a file without frontmatter one.
        set("Bare.md", bare, "status", done),
    ];
    let removal = command(
        connection,
        &serde_json::json!({ "set_property": {
            "path": "Plan.md",
            "base_hash": sha256(
                b"---\ntitle: Plan\nstatus: \"done\"\ntags:\n  - a\n  - b\n---\n# Plan\n\nThe body.\n"
            ),
            "property_key": "tags",
            "property_change": "remove",
        } }),
    );
    for write in settings.iter().chain([&removal]) {
        assert!(matches!(writer.send(write).await, Handled::Queued { .. }));
    }

    let sync = SyncWriter::new(writer.recorder(), trees.path());
    sync.deliver(connection).await.expect("the delivery runs");
    sync.deliver(connection).await.expect("the delivery runs");

    let expected: [(&str, &[u8]); 3] = [
        (
            "Plan.md",
            b"---\ntitle: Plan\nstatus: \"done\"\n---\n# Plan\n\nThe body.\n",
        ),
        (
            "Note.md",
            b"---\ntitle: Note\nowner: [\"Ann\",\"Bo\"]\n---\nBody: not a key.\n",
        ),
        (
            "Bare.md",
            b"---\nstatus: \"done\"\n---\n# Bare\n\nNo frontmatter.\n",
        ),
    ];
    for (name, bytes) in expected {
        assert_eq!(
            String::from_utf8_lossy(&std::fs::read(tree.join(name)).expect("the file")),
            String::from_utf8_lossy(bytes),
            "{name} changes only its property"
        );
        assert_eq!(
            writer.current(connection, name).await,
            Some(sha256(bytes)),
            "the new bytes of {name} are current"
        );
    }
    for write in settings.iter().chain([&removal]) {
        assert!(
            matches!(writer.outcome(write).await, Some(Outcome::Succeeded { .. })),
            "{:?}",
            writer.outcome(write).await
        );
    }

    writer.cleanup().await;
}

#[tokio::test]
async fn tree_writes_are_group_writable() {
    use std::os::unix::fs::PermissionsExt as _;

    let writer = Writer::new().await;
    let mine: &[u8] = b"# Plan\n\nA new note in a new folder.\n";
    let connection = writer.sync_vault(&[]).await;
    let trees = tempfile::tempdir().expect("a trees root");
    let tree = trees.path().join(connection.to_string());
    std::fs::create_dir_all(&tree).expect("the working tree");
    writer.keep_for_sync(connection, mine).await;
    let write = command(
        connection,
        &serde_json::json!({ "create": {
            "path": "Projects/Plan.md",
            "blob": blob_ref(mine, "text/markdown"),
        } }),
    );
    assert!(matches!(writer.send(&write).await, Handled::Queued { .. }));

    SyncWriter::new(writer.recorder(), trees.path())
        .deliver(connection)
        .await
        .expect("the delivery runs");

    let mode = |path: &std::path::Path| {
        std::fs::metadata(path)
            .expect("the path exists")
            .permissions()
            .mode()
            & 0o7777
    };
    // ob runs as another member of the group of the trees: it must replace what notes writes.
    assert_eq!(
        mode(&tree.join("Projects")),
        0o2770,
        "a folder that notes creates"
    );
    assert_eq!(
        mode(&tree.join("Projects/Plan.md")),
        0o660,
        "a file that notes writes"
    );

    writer.cleanup().await;
}

#[tokio::test]
async fn write_mode_off_fails_a_sync_write_that_did_not_land() {
    let writer = Writer::new().await;
    let plan: &[u8] = b"# Plan\n\nIn the vault.\n";
    let note: &[u8] = b"# Note\n\nIn the vault.\n";
    let mine: &[u8] = b"# Edit\n\nWritten through Ratatoskr.\n";
    let connection = writer
        .sync_vault(&[("Plan.md", plan), ("Note.md", note)])
        .await;
    let trees = tempfile::tempdir().expect("a trees root");
    let tree = trees.path().join(connection.to_string());
    std::fs::create_dir_all(&tree).expect("the working tree");
    std::fs::write(tree.join("Plan.md"), plan).expect("the file is in the tree");
    std::fs::write(tree.join("Note.md"), note).expect("the file is in the tree");
    writer.keep_for_sync(connection, mine).await;
    let mine_ref = blob_ref(mine, "text/markdown");
    let sync = SyncWriter::new(writer.recorder(), trees.path());
    // The write of Note.md lands in the tree; the write of Plan.md is delivered after that pass.
    let landed = command(connection, &update("Note.md", &sha256(note), &mine_ref));
    assert!(matches!(writer.send(&landed).await, Handled::Queued { .. }));
    sync.deliver(connection).await.expect("the delivery runs");
    let waiting = command(connection, &update("Plan.md", &sha256(plan), &mine_ref));
    assert!(matches!(
        writer.send(&waiting).await,
        Handled::Queued { .. }
    ));

    let (operation_id, answer) = writer
        .put_write_mode(crate::support::channel::OWNER, connection, "off")
        .await;
    assert_eq!(
        answer.status,
        axum::http::StatusCode::ACCEPTED,
        "{:?}",
        answer.body
    );
    assert_eq!(writer.terminal(operation_id).await["status"], "succeeded");
    sync.deliver(connection).await.expect("the delivery runs");

    assert_eq!(
        writer.outcome(&waiting).await,
        Some(Outcome::Failed {
            code: "notes.write.disabled".to_owned(),
            conflict_id: None,
        }),
        "a sync write that did not land is not delivered yet, so off fails it"
    );
    assert_eq!(
        std::fs::read(tree.join("Plan.md")).expect("the file"),
        plan,
        "the tree does not get the failed write"
    );
    assert_eq!(
        writer.outcome(&landed).await,
        Some(Outcome::Succeeded {
            revision: Some(sha256(mine)),
        }),
        "a write that landed finishes: the next pass finds it in the tree"
    );

    writer.cleanup().await;
}
