//! The index against the golden output of the real Obsidian engine (workspace design D8, task 7.1).
//!
//! Each directory `tests/fixtures/obsidian-<version>/` holds a vault (`vault/`) and the metadata
//! cache that Obsidian of that version made from it (`metadata.json`, spike 3.6). For each file of
//! the vault the test compares the links, the embeds, the frontmatter links, the resolved and the
//! unresolved links, the backlinks, the tags, the properties, the headings and the block ids.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "the helpers read fixture files and the JSON that the test owns"
)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use ratatoskr_notes_index::{LinkGraph, NoteIndex, Vault, parse};
use serde_json::Value;
use unicode_normalization::UnicodeNormalization as _;

/// Every file under `root`, relative to it, in NFC, without the `.obsidian` folder.
fn vault_files(root: &Path) -> BTreeMap<String, PathBuf> {
    let mut found = BTreeMap::new();
    let mut folders = vec![root.to_path_buf()];
    while let Some(folder) = folders.pop() {
        for entry in fs::read_dir(&folder).expect("the fixture folder is readable") {
            let path = entry.expect("a fixture entry is readable").path();
            let relative = path
                .strip_prefix(root)
                .expect("the entry is under the vault")
                .to_str()
                .expect("a fixture path is UTF-8")
                .nfc()
                .collect::<String>();
            if relative == ".obsidian" {
                continue;
            }
            if path.is_dir() {
                folders.push(path);
            } else {
                found.insert(relative, path);
            }
        }
    }
    found
}

/// `[[link, displayText], ...]` of a list of the metadata cache.
fn pairs(list: Option<&Value>) -> Vec<(String, String)> {
    list.and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .map(|item| (text(&item["link"]), text(&item["displayText"])))
                .collect()
        })
        .unwrap_or_default()
}

fn text(value: &Value) -> String {
    value.as_str().unwrap_or_default().to_owned()
}

/// A map of the metadata cache from a path to a count.
fn counts(value: &Value) -> BTreeMap<String, u32> {
    value
        .as_object()
        .map(|map| {
            map.iter()
                .map(|(key, count)| {
                    let count = u32::try_from(count.as_u64().unwrap_or_default()).unwrap_or(0);
                    (key.clone(), count)
                })
                .collect()
        })
        .unwrap_or_default()
}

/// What the index of one Markdown file gives, in the shape of the metadata cache.
#[derive(Debug, PartialEq, Eq)]
struct Observed {
    links: Vec<(String, String)>,
    embeds: Vec<(String, String)>,
    frontmatter_links: Vec<(String, String, String)>,
    tags: Vec<String>,
    frontmatter: Option<Value>,
    headings: Vec<(u64, String)>,
    blocks: BTreeSet<String>,
    resolved: BTreeMap<String, u32>,
    unresolved: BTreeMap<String, u32>,
}

fn observed(index: &NoteIndex, path: &str, graph: &LinkGraph) -> Observed {
    Observed {
        links: index
            .links
            .iter()
            .map(|link| (link.link.clone(), link.display_text.clone()))
            .collect(),
        embeds: index
            .embeds
            .iter()
            .map(|link| (link.link.clone(), link.display_text.clone()))
            .collect(),
        frontmatter_links: index
            .frontmatter_links
            .iter()
            .map(|link| {
                (
                    link.key.clone(),
                    link.link.clone(),
                    link.display_text.clone(),
                )
            })
            .collect(),
        tags: index.tags.clone(),
        frontmatter: index.frontmatter.clone().map(Value::Object),
        headings: index
            .headings
            .iter()
            .map(|heading| (u64::from(heading.level), heading.heading.clone()))
            .collect(),
        blocks: index.blocks.iter().cloned().collect(),
        resolved: graph.resolved.get(path).cloned().unwrap_or_default(),
        unresolved: graph.unresolved.get(path).cloned().unwrap_or_default(),
    }
}

fn expected(file: &Value) -> Observed {
    let cache = &file["cache"];
    Observed {
        links: pairs(cache.get("links")),
        embeds: pairs(cache.get("embeds")),
        frontmatter_links: cache
            .get("frontmatterLinks")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .map(|item| {
                        (
                            text(&item["key"]),
                            text(&item["link"]),
                            text(&item["displayText"]),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default(),
        tags: file["getAllTags"]
            .as_array()
            .map(|tags| tags.iter().map(text).collect())
            .unwrap_or_default(),
        frontmatter: cache.get("frontmatter").cloned(),
        headings: cache
            .get("headings")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .map(|item| (item["level"].as_u64().unwrap_or(0), text(&item["heading"])))
                    .collect()
            })
            .unwrap_or_default(),
        blocks: cache
            .get("blocks")
            .and_then(Value::as_object)
            .map(|blocks| blocks.keys().cloned().collect())
            .unwrap_or_default(),
        resolved: counts(&file["resolvedLinks"]),
        unresolved: counts(&file["unresolvedLinks"]),
    }
}

/// Every difference between the index of one fixture directory and its engine output.
fn differences(fixture: &Path) -> Vec<String> {
    let metadata: Value = serde_json::from_str(
        &fs::read_to_string(fixture.join("metadata.json")).expect("metadata.json is readable"),
    )
    .expect("metadata.json is JSON");
    let files = vault_files(&fixture.join("vault"));
    let vault = Vault::new(files.keys().cloned());
    let notes: BTreeMap<&str, NoteIndex> = files
        .iter()
        .filter(|(path, _)| {
            path.rsplit_once('.')
                .is_some_and(|(_, extension)| extension == "md")
        })
        .map(|(path, on_disk)| {
            let text = fs::read_to_string(on_disk).expect("a note is UTF-8");
            (path.as_str(), parse(&text))
        })
        .collect();
    let graph = LinkGraph::build(&vault, notes.iter().map(|(path, index)| (*path, index)));

    let expected_files = metadata["files"].as_object().expect("files is an object");
    let mut problems = Vec::new();
    let listed: BTreeSet<&String> = expected_files.keys().collect();
    let present: BTreeSet<&String> = files.keys().collect();
    if listed != present {
        problems.push(format!(
            "the vault holds {present:?}, the engine listed {listed:?}"
        ));
    }
    for (path, file) in expected_files {
        let backlinks = counts(&file["backlinks"]);
        if graph.backlinks(path) != backlinks {
            problems.push(format!(
                "{path}: backlinks {:?}, the engine has {backlinks:?}",
                graph.backlinks(path)
            ));
        }
        let Some(index) = notes.get(path.as_str()) else {
            continue;
        };
        let ours = observed(index, path, &graph);
        let theirs = expected(file);
        if ours != theirs {
            problems.push(format!("{path}:\n  index  {ours:?}\n  engine {theirs:?}"));
        }
    }
    problems
}

#[test]
fn index_matches_obsidian_engine_output() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let mut fixtures: Vec<PathBuf> = fs::read_dir(&root)
        .expect("the fixtures folder exists")
        .map(|entry| entry.expect("a fixture entry is readable").path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("obsidian-"))
        })
        .collect();
    fixtures.sort();
    assert!(
        !fixtures.is_empty(),
        "no fixture directory obsidian-<version>"
    );

    let problems: Vec<String> = fixtures
        .iter()
        .flat_map(|fixture| {
            differences(fixture)
                .into_iter()
                .map(move |problem| format!("{}: {problem}", fixture.display()))
        })
        .collect();
    assert!(
        problems.is_empty(),
        "{} differences from the engine output:\n{}",
        problems.len(),
        problems.join("\n")
    );
}
