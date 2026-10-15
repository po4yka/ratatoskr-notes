//! The link rewrite against the golden fixtures of the real Obsidian app (workspace change
//! `add-notes-write-back`, spec `notes-link-rewrite`, task 8.2).
//!
//! Each folder of `tests/fixtures/link-rewrite/` holds a vault before a rename (`before/`), the
//! rename and the setting `newLinkFormat` (`case.json`), the vault that Obsidian left after it
//! (`after/`), the Obsidian version (`recording.json`) and the SHA-256 of each of those files
//! (`SHA256SUMS`). `tools/record-link-fixtures.sh` records them; nobody edits them by hand.
//!
//! A replay renames the file in the `before/` vault and applies `rewrite_links`, and the result
//! must be the `after/` vault byte for byte: the same paths, and the same bytes in each file.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "the helpers read fixture files that the test owns"
)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use ratatoskr_notes_index::{NewLinkFormat, rewrite_links};
use serde_json::Value;
use sha2::{Digest as _, Sha256};

/// The hard cases that the corpus must cover (spec `notes-link-rewrite`, requirement "The fixture
/// corpus covers the known hard cases"), as `case.json` names them in `covers`.
const REQUIRED_CASES: [&str; 12] = [
    "shortest-path",
    "shortest-path-collision",
    "relative-path",
    "absolute-path",
    "alias",
    "heading",
    "block-reference",
    "embed-note",
    "embed-attachment",
    "markdown-encoded-space",
    "letter-case",
    "nfd-nfc",
];

fn corpus() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/link-rewrite")
}

/// The name of each fixture, sorted.
fn fixtures() -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(corpus())
        .expect("the corpus is readable")
        .map(|entry| entry.expect("a corpus entry").path())
        .filter(|path| path.join("case.json").is_file())
        .map(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .expect("a fixture name is UTF-8")
                .to_owned()
        })
        .collect();
    names.sort();
    names
}

/// Every file under `root`, by its path relative to it with `/`.
fn files(root: &Path) -> BTreeMap<String, Vec<u8>> {
    let mut found = BTreeMap::new();
    let mut folders = vec![root.to_path_buf()];
    while let Some(folder) = folders.pop() {
        for entry in fs::read_dir(&folder).expect("a fixture folder is readable") {
            let path = entry.expect("a fixture entry").path();
            if path.is_dir() {
                folders.push(path);
                continue;
            }
            let relative = path
                .strip_prefix(root)
                .expect("the entry is under the root")
                .components()
                .map(|part| part.as_os_str().to_str().expect("a fixture path is UTF-8"))
                .collect::<Vec<_>>()
                .join("/");
            found.insert(
                relative,
                fs::read(&path).expect("a fixture file is readable"),
            );
        }
    }
    found
}

fn case(name: &str) -> Value {
    let text = fs::read_to_string(corpus().join(name).join("case.json")).expect("case.json");
    serde_json::from_str(&text).expect("case.json is JSON")
}

/// The vault that the rename of the fixture `name` and its rewrite give.
fn replayed(name: &str) -> BTreeMap<String, Vec<u8>> {
    let spec = case(name);
    let from = spec["from"].as_str().expect("from");
    let to = spec["to"].as_str().expect("to");
    let format = match spec["settings"]["newLinkFormat"].as_str() {
        Some("relative") => NewLinkFormat::Relative,
        Some("absolute") => NewLinkFormat::Absolute,
        _ => NewLinkFormat::Shortest,
    };
    let before = files(&corpus().join(name).join("before"));
    let paths: Vec<String> = before.keys().cloned().collect();
    let notes: BTreeMap<String, String> = before
        .iter()
        .filter(|(path, _)| {
            Path::new(path)
                .extension()
                .is_some_and(|extension| extension == "md")
        })
        .map(|(path, bytes)| {
            let text = String::from_utf8(bytes.clone()).expect("a note is UTF-8");
            (path.clone(), text)
        })
        .collect();
    let rewritten = rewrite_links(&paths, &notes, from, to, format);
    let mut after: BTreeMap<String, Vec<u8>> = before
        .into_iter()
        .map(|(path, bytes)| (if path == from { to.to_owned() } else { path }, bytes))
        .collect();
    for (path, text) in rewritten {
        after.insert(path, text.into_bytes());
    }
    after
}

/// `Err` names the first file whose path or bytes differ from the recorded `after/` vault.
fn replay(name: &str) -> Result<(), String> {
    let recorded = files(&corpus().join(name).join("after"));
    let replayed = replayed(name);
    let paths: BTreeSet<&String> = recorded.keys().chain(replayed.keys()).collect();
    for path in paths {
        match (recorded.get(path), replayed.get(path)) {
            (Some(expected), Some(actual)) if expected == actual => {}
            (Some(expected), Some(actual)) => {
                return Err(format!(
                    "{name}: {path} differs\n  recorded: {:?}\n  replayed: {:?}",
                    String::from_utf8_lossy(expected),
                    String::from_utf8_lossy(actual)
                ));
            }
            (Some(_), None) => return Err(format!("{name}: {path} is missing from the replay")),
            (None, _) => return Err(format!("{name}: {path} is not in the recorded vault")),
        }
    }
    Ok(())
}

/// The recorded text of `path` in the `after/` vault of the fixture `name`.
fn recorded(name: &str, path: &str) -> String {
    fs::read_to_string(corpus().join(name).join("after").join(path)).expect("a recorded file")
}

#[test]
fn every_fixture_matches() {
    let failures: Vec<String> = fixtures()
        .iter()
        .filter_map(|name| replay(name).err())
        .collect();
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn shortest_path_collision() {
    assert!(
        recorded("shortest-path-collision", "Index.md").contains("- [[Archive/Plan]]\n"),
        "Obsidian writes the full path when the name of the moved file is no longer unique"
    );
    assert_eq!(replay("shortest-path-collision"), Ok(()));
}

#[test]
fn code_block_link() {
    let note = recorded("code-block-link", "Note.md");
    assert!(
        note.contains("```\n[[A]] inside a fence\n```") && note.contains("Inline `[[A]]` code."),
        "a link in code does not change: {note:?}"
    );
    assert_eq!(replay("code-block-link"), Ok(()));
}

#[test]
fn alias_and_heading() {
    let before = fs::read_to_string(corpus().join("alias-and-heading/before/Notes.md"))
        .expect("the note before");
    assert_eq!(
        recorded("alias-and-heading", "Notes.md"),
        before
            .replace("[[A#Goals|our goals]]", "[[B#Goals|our goals]]")
            .replace("[[A]]", "[[B]]"),
        "the alias and the heading survive, and no other byte changes"
    );
    assert_eq!(replay("alias-and-heading"), Ok(()));
}

#[test]
fn corpus_covers_required_cases() {
    let covered: BTreeSet<String> = fixtures()
        .iter()
        .flat_map(|name| {
            case(name)["covers"]
                .as_array()
                .expect("covers is a list")
                .iter()
                .map(|tag| tag.as_str().expect("a tag").to_owned())
                .collect::<Vec<_>>()
        })
        .collect();
    let missing: Vec<&str> = REQUIRED_CASES
        .into_iter()
        .filter(|tag| !covered.contains(*tag))
        .collect();
    assert!(missing.is_empty(), "no fixture covers {missing:?}");
}

#[test]
fn fixtures_match_their_recording() {
    let mut failures = Vec::new();
    for name in fixtures() {
        let folder = corpus().join(&name);
        let sums = fs::read_to_string(folder.join("SHA256SUMS")).unwrap_or_default();
        let listed: BTreeMap<&str, &str> = sums
            .lines()
            .filter_map(|line| line.split_once("  "))
            .map(|(hash, path)| (path, hash))
            .collect();
        let mut present = files(&folder);
        present.remove("SHA256SUMS");
        for (path, bytes) in &present {
            let hash = hex(&Sha256::digest(bytes));
            if listed.get(path.as_str()) != Some(&hash.as_str()) {
                failures.push(format!("{name}: {path} does not match SHA256SUMS"));
            }
        }
        for path in listed.keys() {
            if !present.contains_key(*path) {
                failures.push(format!("{name}: {path} is listed and missing"));
            }
        }
        if !present.keys().any(|path| path.starts_with("after/")) {
            failures.push(format!("{name}: no recorded after/ vault"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut text, byte| {
        let _ = write!(text, "{byte:02x}");
        text
    })
}
