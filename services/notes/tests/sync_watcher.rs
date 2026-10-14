//! The watcher of the working tree (workspace design D3): a file becomes a revision when its size
//! and modification time have not changed for 2 seconds, and a full rescan every 15 minutes finds
//! what an event did not report.
//!
//! A file is read only when its path stays inside the tree: a symbolic link in the tree never makes
//! a revision of a file outside it (review item M3 of group 6).
//!
//! The tests run under paused tokio time, so the settle window and the rescan take no real time.
//! The watcher reads inotify, so the tests run on Linux only. On macOS they are ignored.

#![allow(
    clippy::expect_used,
    reason = "test assertions read the changes they receive"
)]

use std::io::Write as _;
use std::path::PathBuf;
use std::time::Duration;

use ratatoskr_notes::blobs::BlobStore;
use ratatoskr_notes::watcher::{self, TreeChange};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// The three parts of the note, written within one second.
const PARTS: [&str; 3] = ["# Plan\n", "- one\n", "- two\n"];

/// The SHA-256 of the three parts together, from `shasum -a 256`.
const DIGEST: &str = "cc6bfbd39a13ab5a4a831b1f232ebc9adeb66111d3876272729cb3254265c9cc";

#[tokio::test(start_paused = true)]
#[cfg_attr(
    not(target_os = "linux"),
    ignore = "the watcher reads inotify, which only Linux has"
)]
async fn settled_change_becomes_one_revision() {
    let tree = tempfile::tempdir().expect("a working tree");
    std::fs::create_dir(tree.path().join("Daily")).expect("a folder of the vault");
    let blob_root = tempfile::tempdir().expect("a blob root");
    let (sender, mut changes) = mpsc::channel(16);
    let stop = CancellationToken::new();
    let watching = watcher::watch(
        tree.path().to_owned(),
        BlobStore::new(blob_root.path()),
        sender,
        stop.clone(),
    );

    // ob writes the file in three parts within one second.
    let note = tree.path().join("Daily/Plan.md");
    for (index, part) in PARTS.iter().enumerate() {
        if index > 0 {
            tokio::time::advance(Duration::from_millis(400)).await;
        }
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&note)
            .and_then(|mut file| file.write_all(part.as_bytes()))
            .expect("a part of the note is written");
    }

    // The settle window has not ended 1.5 seconds after the last part.
    let early = tokio::time::timeout(Duration::from_millis(1500), changes.recv()).await;
    assert!(
        early.is_err(),
        "no revision inside the settle window: {early:?}"
    );

    // The window ends 2 seconds after the last part. The inotify event of the last part starts it:
    // a watcher that saw only the first part would wait 2 seconds more.
    let settled = tokio::time::timeout(Duration::from_secs(1), changes.recv()).await;
    assert_eq!(
        settled,
        Ok(Some(TreeChange::Revision {
            path: PathBuf::from("Daily/Plan.md"),
            digest: DIGEST.to_owned(),
            length: 19,
        })),
        "the settled note becomes one revision of all three parts"
    );

    // The rescan at 15 minutes finds the same state and makes no second revision.
    let later = tokio::time::timeout(Duration::from_mins(20), changes.recv()).await;
    assert!(later.is_err(), "exactly one revision: {later:?}");

    stop.cancel();
    watching.await.expect("the watcher stops");
}

/// The bytes of a file outside the working tree.
const OUTSIDE: &str = "outside the tree: not a note of this vault\n";

/// Whether a file under `root` holds `bytes`.
fn holds(root: &std::path::Path, bytes: &str) -> bool {
    let mut open = vec![root.to_owned()];
    while let Some(directory) = open.pop() {
        for entry in std::fs::read_dir(&directory).expect("a directory of the blob root") {
            let path = entry.expect("a directory entry").path();
            if path.is_dir() {
                open.push(path);
            } else if std::fs::read_to_string(&path).is_ok_and(|text| text == bytes) {
                return true;
            }
        }
    }
    false
}

#[tokio::test(start_paused = true)]
#[cfg_attr(
    not(target_os = "linux"),
    ignore = "the watcher reads inotify, which only Linux has"
)]
async fn linked_folder_does_not_leak_outside_content() {
    let host = tempfile::tempdir().expect("a host directory");
    let tree = host.path().join("tree");
    let outside = host.path().join("outside");
    std::fs::create_dir_all(tree.join("Daily")).expect("a folder of the vault");
    std::fs::create_dir(&outside).expect("a directory outside the tree");
    let blob_root = tempfile::tempdir().expect("a blob root");
    let (sender, mut changes) = mpsc::channel(16);
    let stop = CancellationToken::new();
    let watching = watcher::watch(
        tree.clone(),
        BlobStore::new(blob_root.path()),
        sender,
        stop.clone(),
    );

    // A first note: when it is a revision, the watcher watches the folder.
    std::fs::write(tree.join("Daily/first.md"), "# First\n").expect("a note is written");
    let first = tokio::time::timeout(Duration::from_secs(5), changes.recv()).await;
    assert!(
        matches!(&first, Ok(Some(TreeChange::Revision { path, .. })) if path == &PathBuf::from("Daily/first.md")),
        "the first note becomes a revision: {first:?}"
    );

    // The folder moves out of the tree, and a symbolic link to it takes its place. The watch of
    // the folder stays on the moved folder. Then a file is written there.
    std::fs::rename(tree.join("Daily"), outside.join("Daily")).expect("the folder moves out");
    std::os::unix::fs::symlink(outside.join("Daily"), tree.join("Daily"))
        .expect("a link takes the place of the folder");
    std::fs::write(outside.join("Daily/secret.md"), OUTSIDE).expect("a file outside is written");

    // Every change for the settle window and the next rescan.
    let mut seen = Vec::new();
    while let Ok(Some(change)) = tokio::time::timeout(Duration::from_mins(20), changes.recv()).await
    {
        seen.push(change);
    }
    assert!(
        !seen
            .iter()
            .any(|change| matches!(change, TreeChange::Revision { .. })),
        "no revision comes from a file outside the tree: {seen:?}"
    );
    assert!(
        !holds(blob_root.path(), OUTSIDE),
        "no blob holds the content outside the tree"
    );
    assert_eq!(
        seen,
        [TreeChange::Removed {
            path: PathBuf::from("Daily/first.md")
        }],
        "the first note is no longer in the tree"
    );

    stop.cancel();
    watching.await.expect("the watcher stops");
}
