//! The shipped template units follow the owner decisions of 2026-10-14.
//!
//! Decision A: the sync unit logs to the journal namespace that the service reads, and to no other
//! journal. Decision B: the working tree of a connection is `/mnt/nvme/ratatoskr/notes/trees/<id>`,
//! which the service reads through the group `ratatoskr-notes-trees`, and the client state is
//! `/mnt/nvme/ratatoskr/notes/clients/<id>`, which only the user of `ob` can enter. The sync and
//! login units create files with `UMask=0027`, so the setgid bit of the trees root gives each file
//! of a tree that group and mode 0640 or 0750.
//!
//! The tests read the unit files in `deploy/systemd/` with `%i` replaced, and compare them with
//! the paths that the service builds.

#![allow(
    clippy::expect_used,
    reason = "test assertions read the shipped unit files"
)]

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use ratatoskr_notes::systemd::{SYNC_LOG_NAMESPACE, client_dir, tree_dir};
use uuid::Uuid;

const ID: Uuid = Uuid::from_u128(0x0192_0000_0000_7000_8000_0000_0000_0617);

/// The settings of a shipped template unit for [`ID`], in order, without comments.
fn settings(template: &str) -> Vec<(String, String)> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../deploy/systemd")
        .join(template);
    std::fs::read_to_string(&path)
        .expect("the unit file")
        .lines()
        .map(str::trim)
        .filter(|line| !line.starts_with('#'))
        .filter_map(|line| line.split_once('='))
        .map(|(key, value)| (key.to_owned(), value.replace("%i", &ID.to_string())))
        .collect()
}

/// The values of one setting.
fn values(settings: &[(String, String)], key: &str) -> Vec<String> {
    settings
        .iter()
        .filter(|(name, _)| name == key)
        .map(|(_, value)| value.clone())
        .collect()
}

/// The words of every `ReadWritePaths=` line, each without its optional `-`.
fn read_write_paths(settings: &[(String, String)]) -> BTreeSet<String> {
    values(settings, "ReadWritePaths")
        .iter()
        .flat_map(|value| value.split_whitespace())
        .map(|word| word.trim_start_matches('-').to_owned())
        .collect()
}

fn text(path: &Path) -> String {
    path.display().to_string()
}

/// `HOME` and `XDG_CONFIG_HOME` are the client state of the connection. Other variables, such as
/// the mode of the sync unit, are not read here.
fn assert_client_environment(unit: &str, settings: &[(String, String)]) {
    let client = text(&client_dir(ID));
    let homes: Vec<String> = values(settings, "Environment")
        .into_iter()
        .filter(|value| value.starts_with("HOME=") || value.starts_with("XDG_CONFIG_HOME="))
        .collect();
    assert_eq!(
        homes,
        [
            format!("HOME={client}"),
            format!("XDG_CONFIG_HOME={client}")
        ],
        "{unit}: ob keeps its state in the client state of the connection"
    );
}

#[test]
fn path_builder_puts_tree_and_client_state_in_split_roots() {
    assert_eq!(
        tree_dir(ID),
        PathBuf::from(format!("/mnt/nvme/ratatoskr/notes/trees/{ID}")),
        "the working tree"
    );
    assert_eq!(
        client_dir(ID),
        PathBuf::from(format!("/mnt/nvme/ratatoskr/notes/clients/{ID}")),
        "the client state"
    );
}

#[test]
fn sync_unit_logs_to_its_namespace_and_reads_the_split_roots() {
    let unit = settings("ratatoskr-notes-sync@.service");
    let (tree, client) = (text(&tree_dir(ID)), text(&client_dir(ID)));
    assert_eq!(
        values(&unit, "LogNamespace"),
        [SYNC_LOG_NAMESPACE],
        "decision A: the namespace that the service reads"
    );
    assert_eq!(values(&unit, "UMask"), ["0027"], "decision B: the umask");
    assert_client_environment("sync", &unit);
    for key in ["ExecStartPre", "ExecStart"] {
        for command in values(&unit, key) {
            assert!(
                command.contains(&format!(" --path {tree} "))
                    || command.ends_with(&format!(" --path {tree}")),
                "{key}= runs ob in the working tree: {command}"
            );
        }
    }
    assert_eq!(
        read_write_paths(&unit),
        BTreeSet::from([tree, client]),
        "the sync unit writes the tree and the client state of its connection only"
    );
}

#[test]
fn login_and_cleanup_units_use_the_split_roots() {
    let (tree, client) = (tree_dir(ID), client_dir(ID));
    let roots = BTreeSet::from([
        text(tree.parent().expect("the trees root")),
        text(client.parent().expect("the clients root")),
    ]);

    let login = settings("ratatoskr-notes-login@.service");
    assert_eq!(values(&login, "UMask"), ["0027"], "decision B: the umask");
    assert_client_environment("login", &login);
    let steps = values(&login, "ExecStart");
    assert!(
        steps
            .first()
            .is_some_and(|step| step.contains("umask 0077;")),
        "ob login creates the client state with mode 0700: {steps:?}"
    );
    assert!(
        steps
            .last()
            .is_some_and(|step| step.contains(&format!(" --path {} ", text(&tree)))),
        "ob sync-setup sets up the working tree: {steps:?}"
    );
    assert_eq!(
        read_write_paths(&login),
        roots,
        "the login unit creates the tree and the client state in the two roots"
    );

    let cleanup = settings("ratatoskr-notes-cleanup@.service");
    assert_client_environment("cleanup", &cleanup);
    assert_eq!(
        values(&cleanup, "ExecStart"),
        [
            format!("-/usr/bin/ob sync-unlink --path {}", text(&tree)),
            format!("/bin/rm -rf -- {} {}", text(&tree), text(&client)),
        ],
        "the cleanup unit removes the tree and the client state"
    );
    assert_eq!(
        read_write_paths(&cleanup),
        roots,
        "the cleanup unit removes an entry of each root"
    );
}

#[test]
fn no_unit_uses_the_old_connections_root() {
    for template in [
        "ratatoskr-notes-sync@.service",
        "ratatoskr-notes-login@.service",
        "ratatoskr-notes-cleanup@.service",
    ] {
        let old: Vec<(String, String)> = settings(template)
            .into_iter()
            .filter(|(_, value)| value.contains("/mnt/nvme/ratatoskr/notes/connections"))
            .collect();
        assert!(old.is_empty(), "{template}: {old:?}");
    }
}
