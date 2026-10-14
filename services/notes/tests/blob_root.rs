//! The read group of the notes blob root (workspace spec `notes-file-changes`, requirement "The
//! notes blob root is readable only by its read group", workspace design D12).
//!
//! The test prepares a blob root as the deploy role does: the group of the root is the read group,
//! and its mode is 2750. As root the read group is gid 4242, which no file of the test has. As
//! another user it is a supplementary group of that user that is not its effective group, or the
//! effective group when the user has no other one, because a user can give a file only to one of
//! its own groups.

#![allow(
    clippy::expect_used,
    reason = "test assertions read the files they create"
)]

use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};

use ratatoskr_notes::blobs::BlobStore;

/// The read group of the test blob root.
fn read_group() -> u32 {
    if rustix::process::geteuid().is_root() {
        return 4242;
    }
    let effective = rustix::process::getegid();
    rustix::process::getgroups()
        .unwrap_or_default()
        .into_iter()
        .find(|group| *group != effective)
        .unwrap_or(effective)
        .as_raw()
}

/// Every directory and every file under `root`, without `root`.
fn entries_under(root: &Path) -> (Vec<PathBuf>, Vec<PathBuf>) {
    let (mut directories, mut files) = (Vec::new(), Vec::new());
    let mut open = vec![root.to_owned()];
    while let Some(directory) = open.pop() {
        for entry in std::fs::read_dir(&directory).expect("a directory of the blob root") {
            let path = entry.expect("a directory entry").path();
            if path.is_dir() {
                directories.push(path.clone());
                open.push(path);
            } else {
                files.push(path);
            }
        }
    }
    (directories, files)
}

/// The permission bits with setuid, setgid and sticky, in octal, and the group.
fn mode_and_group(path: &Path) -> (String, u32) {
    let metadata = std::fs::symlink_metadata(path).expect("metadata");
    (
        format!("{:04o}", metadata.permissions().mode() & 0o7777),
        metadata.gid(),
    )
}

#[test]
fn new_blob_is_group_readable_and_not_world_readable() {
    let host = tempfile::tempdir().expect("a directory for the blob root");
    let root = host.path().join("ratatoskr-notes");
    std::fs::create_dir(&root).expect("the blob root is created");
    let group = read_group();
    std::os::unix::fs::chown(&root, None, Some(group)).expect("the root gets the read group");
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o2750))
        .expect("the root is mode 2750");
    assert_eq!(
        mode_and_group(&root),
        ("2750".to_owned(), group),
        "the prepared root"
    );

    let tree = tempfile::tempdir().expect("a working tree");
    let note = tree.path().join("Plan.md");
    std::fs::write(&note, "# Plan\n- one\n- two\n").expect("a note is written");

    let store = BlobStore::new(&root);
    let blob = store.store_file(&note).expect("the note is stored");

    let path = store.path_of(&blob.digest).expect("a content path");
    assert_eq!(
        mode_and_group(&path),
        ("0640".to_owned(), group),
        "a new revision is readable by the read group and by no other user"
    );
    let (directories, files) = entries_under(&root);
    assert_eq!(files, [path], "the staged copy is gone");
    assert!(!directories.is_empty(), "the store made its directories");
    for directory in directories {
        assert_eq!(
            mode_and_group(&directory),
            ("2750".to_owned(), group),
            "{} keeps the setgid bit and the read group",
            directory.display()
        );
    }
}
