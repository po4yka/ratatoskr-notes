//! The watcher of the working tree (workspace design D3): a file becomes a revision when its size
//! and modification time have not changed for 2 seconds, and a full rescan every 15 minutes finds
//! what an event did not report.
//!
//! The test runs under paused tokio time, so the settle window and the rescan take no real time.
//! The watcher reads inotify, so the test runs on Linux only. On macOS it is ignored.

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
