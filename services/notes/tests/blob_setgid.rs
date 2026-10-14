//! The blob store keeps the setgid bit of its directories (workspace design D12, review item M4
//! of group 6).
//!
//! On the host the service user owns the blob root and is not a member of its group,
//! `ratatoskr-notes-blobs`. On Linux, a `chmod` by such an owner removes the setgid bit, and the
//! store runs `chmod` on a new directory whose mode the umask changed. So a strict umask could make
//! a directory that the read group can no longer keep. The test prepares that case: a root of a
//! group that the store user is not a member of, and the umask 0077. As root it prepares the root
//! itself and runs the store on a thread whose credentials are those of `nobody`; as another user
//! it prepares the root with `sudo -n`, which the GitHub runner allows.
//!
//! The umask is the umask of the process, so this file holds one test only. Linux only: the
//! credentials of one thread can change only there.

#![cfg(target_os = "linux")]
#![allow(
    clippy::expect_used,
    reason = "test assertions read the files they create"
)]

use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};

use ratatoskr_notes::blobs::BlobStore;
use rustix::fs::Mode;
use rustix::process::{Gid, Uid};

/// The uid and gid of `nobody` on Debian and Ubuntu.
const NOBODY: u32 = 65_534;

/// A group that no user of the test is a member of.
const FOREIGN_GROUP: u32 = 4242;

/// Gives `root` the owner `owner`, the group [`FOREIGN_GROUP`] and the mode 2750.
fn prepare_root(root: &Path, owner: u32, as_root: bool) {
    if as_root {
        std::os::unix::fs::chown(root, Some(owner), Some(FOREIGN_GROUP))
            .expect("root gives the blob root away");
        std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o2750))
            .expect("the root is mode 2750");
        return;
    }
    for command in [
        vec!["chown".to_owned(), format!("{owner}:{FOREIGN_GROUP}")],
        vec!["chmod".to_owned(), "2750".to_owned()],
    ] {
        let status = std::process::Command::new("sudo")
            .arg("-n")
            .args(command)
            .arg(root)
            .status()
            .expect("sudo can be started");
        assert!(status.success(), "sudo -n prepares the blob root");
    }
}

/// The credentials of the calling thread become those of `nobody`, without any capability.
fn become_nobody() {
    rustix::thread::set_thread_groups(&[]).expect("no supplementary group");
    let group = Gid::from_raw(NOBODY);
    rustix::thread::set_thread_res_gid(group, group, group).expect("the group of nobody");
    let user = Uid::from_raw(NOBODY);
    rustix::thread::set_thread_res_uid(user, user, user).expect("the user nobody");
}

/// Every directory under `root`, without `root`.
fn directories_under(root: &Path) -> Vec<PathBuf> {
    let mut directories = Vec::new();
    let mut open = vec![root.to_owned()];
    while let Some(directory) = open.pop() {
        for entry in std::fs::read_dir(&directory).expect("a directory of the blob root") {
            let path = entry.expect("a directory entry").path();
            if path.is_dir() {
                directories.push(path.clone());
                open.push(path);
            }
        }
    }
    directories
}

#[test]
fn strict_umask_cannot_drop_setgid() {
    let host = tempfile::tempdir().expect("a host directory");
    std::fs::set_permissions(host.path(), std::fs::Permissions::from_mode(0o755))
        .expect("every user can reach the host directory");
    let root = host.path().join("ratatoskr-notes");
    std::fs::create_dir(&root).expect("the blob root is created");
    let note = host.path().join("Plan.md");
    std::fs::write(&note, "# Plan\n- one\n").expect("a note is written");

    let as_root = rustix::process::geteuid().is_root();
    let owner = if as_root {
        NOBODY
    } else {
        rustix::process::geteuid().as_raw()
    };
    prepare_root(&root, owner, as_root);

    let previous = rustix::process::umask(Mode::from_raw_mode(0o077));
    let refused = std::thread::scope(|scope| {
        scope
            .spawn(|| {
                if as_root {
                    become_nobody();
                }
                match BlobStore::open(&root) {
                    Err(error) => Some(error.to_string()),
                    Ok(store) => {
                        store.store_file(&note).expect("the note is stored");
                        None
                    }
                }
            })
            .join()
            .expect("the store thread ends")
    });
    rustix::process::umask(previous);

    if let Some(reason) = refused {
        assert!(
            reason.contains("umask"),
            "the store refuses the root because of the umask: {reason}"
        );
        return;
    }
    let directories = directories_under(&root);
    assert!(!directories.is_empty(), "the store made its directories");
    for directory in directories {
        let metadata = std::fs::metadata(&directory).expect("metadata");
        assert_eq!(
            (
                format!("{:04o}", metadata.permissions().mode() & 0o7777),
                metadata.gid()
            ),
            ("2750".to_owned(), FOREIGN_GROUP),
            "{} keeps the setgid bit and the group of the root, or the store refuses the umask",
            directory.display()
        );
    }
}
