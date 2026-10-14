//! Paths, path keys, collisions and exclusions (workspace spec `notes-file-changes`, task 7.3).

#![allow(
    clippy::panic,
    reason = "a test fails with the observation it did not expect"
)]

use ratatoskr_notes_index::paths::{
    Admission, Exclusion, Observation, PATH_COLLISION, PathRules, PathTable,
};

/// The SHA-256 of a revision, made up for the test.
fn revision(digit: char) -> String {
    std::iter::repeat_n(digit, 64).collect()
}

#[test]
fn case_fold_collision_publishes_one() {
    let mut table = PathTable::new();
    let rules = PathRules::default();

    let first = table.observe("Note.md", &revision('a'), 10, &rules);
    let second = table.observe("note.md", &revision('b'), 10, &rules);

    let published = [&first, &second]
        .into_iter()
        .filter(|observation| matches!(observation, Observation::Published { .. }))
        .count();
    assert_eq!(
        published, 1,
        "exactly one of the two is published: {first:?}, {second:?}"
    );
    assert!(
        matches!(&first, Observation::Published { path, admission: Admission::Created, .. } if path == "Note.md"),
        "the file observed first is published: {first:?}"
    );
    assert!(
        matches!(&second, Observation::Collision(_)),
        "the second is a collision: {second:?}"
    );
    assert_eq!(
        table.conditions(),
        [(
            PATH_COLLISION,
            vec!["Note.md".to_owned(), "note.md".to_owned()]
        )],
        "the connection reports notes.file.path_collision with both paths"
    );
}

#[test]
fn obsidian_config_is_never_published() {
    let mut table = PathTable::new();
    let rules = PathRules {
        excluded_folders: vec!["Private".to_owned()],
        ..PathRules::default()
    };

    for (path, length, why) in [
        (
            ".obsidian/plugins/x/data.json",
            10,
            Exclusion::ObsidianConfig,
        ),
        (".trash/old.md", 10, Exclusion::Trash),
        ("Private/plan.md", 10, Exclusion::ExcludedFolder),
        ("big.bin", 64 * 1024 * 1024 + 1, Exclusion::TooLarge),
    ] {
        assert_eq!(
            table.observe(path, &revision('c'), length, &rules),
            Observation::Excluded(why),
            "{path} is not published"
        );
    }
    assert!(
        matches!(
            table.observe("Privateer.md", &revision('d'), 10, &rules),
            Observation::Published { .. }
        ),
        "a name that only starts with an excluded folder is published"
    );
}

#[test]
fn nfd_path_is_published_as_nfc() {
    let mut table = PathTable::new();
    let rules = PathRules::default();

    let nfd = table.observe("Cafe\u{301}.md", &revision('e'), 10, &rules);
    let nfc = table.observe("Caf\u{e9}.md", &revision('f'), 10, &rules);

    let Observation::Published {
        file_id: first_id,
        path: first_path,
        admission: Admission::Created,
    } = nfd
    else {
        panic!("the NFD path is published: {nfd:?}");
    };
    assert_eq!(first_path, "Caf\u{e9}.md", "the event carries the NFC form");
    let Observation::Published {
        file_id: second_id,
        admission: Admission::Updated,
        ..
    } = nfc
    else {
        panic!("a later change with the NFC form updates the file: {nfc:?}");
    };
    assert_eq!(
        first_id, second_id,
        "one file_id for the NFD and the NFC path"
    );
}

#[test]
fn obsidian_and_trash_match_by_path_key() {
    let rules = PathRules {
        excluded_folders: vec!["Private".to_owned()],
        ..PathRules::default()
    };

    let config = rules.exclusion(".Obsidian/plugins/x/data.json", 10);
    let trash = rules.exclusion(".TRASH/old.md", 10);
    let owner_folder = rules.exclusion("PRIVATE/a.md", 10);

    assert_eq!(
        config,
        Some(Exclusion::ObsidianConfig),
        "the vault configuration matches by its path key"
    );
    assert_eq!(
        trash,
        Some(Exclusion::Trash),
        "the trash matches by its path key"
    );
    assert_eq!(
        owner_folder,
        Some(Exclusion::ExcludedFolder),
        "as an excluded folder of the owner does"
    );
}

#[test]
fn case_only_rename_publishes_the_new_path() {
    let mut table = PathTable::new();
    let rules = PathRules::default();

    // As the service sees a rename in Finder: the new path while the old one is present, then the
    // removal of the old path.
    table.observe("Note.md", &revision('a'), 10, &rules);
    let waiting = table.observe("note.md", &revision('a'), 10, &rules);
    let removed = table.remove("Note.md");
    let again = table.observe("note.md", &revision('a'), 10, &rules);

    assert!(matches!(waiting, Observation::Collision(_)), "{waiting:?}");
    assert!(removed.is_some(), "the old path is deleted");
    assert!(
        table.conditions().is_empty(),
        "no collision is left: {:?}",
        table.conditions()
    );
    assert_eq!(
        again,
        Observation::Unchanged,
        "the new path already holds its revision"
    );
}
