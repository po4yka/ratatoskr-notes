//! The read routes of the files and their index (workspace spec `notes-file-index`, requirement
//! "Clients read files and the index only through Edge", tasks 7.7 and 7.13).

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions read the answers they build"
)]

mod support;

use std::collections::BTreeMap;
use std::path::PathBuf;

use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use http_body_util::BodyExt as _;
use ratatoskr_notes::api::{ApiState, domain_router};
use ratatoskr_notes::blobs::BlobStore;
use ratatoskr_notes::files::FileRecorder;
use ratatoskr_notes::watcher::TreeChange;
use ratatoskr_notes_persistence::test_support::TestDatabase;
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use tower::ServiceExt as _;
use uuid::Uuid;

use crate::support::systemd::FakeSystemd;
use crate::support::{USER_HEADER, connect, connection_id};

const OWNER_A: Uuid = Uuid::from_u128(0x0192_0000_0000_7000_8000_0000_0000_00a7);
const OWNER_B: Uuid = Uuid::from_u128(0x0192_0000_0000_7000_8000_0000_0000_00b7);
const NOTE: &[u8] = b"# Note\n\nSee [[Other]].\n";

/// A router with a blob root, one plugin connection of `owner` and the note `Note.md` in it.
struct Reader {
    fixture: TestDatabase,
    _systemd: FakeSystemd,
    router: Router,
    connection_id: Uuid,
    blobs: BlobStore,
    scratch: tempfile::TempDir,
}

impl Reader {
    async fn new(owner: Uuid) -> Self {
        let fixture = TestDatabase::create().await.expect("a test database");
        let systemd = FakeSystemd::new();
        let root = systemd.blob_root();
        std::fs::create_dir_all(&root).expect("the blob root");
        let blobs = BlobStore::new(root);
        let router = domain_router(
            ApiState::new(fixture.database.clone(), systemd.clone())
                .with_key_file(systemd.key_file())
                .with_credentials_dir(systemd.credentials_root())
                .with_blobs(blobs.clone()),
        );
        let connection_id =
            connection_id(&connect(&router, owner, "plugin", "vault-key-read").await);
        let reader = Self {
            fixture,
            _systemd: systemd,
            router,
            connection_id,
            blobs,
            scratch: tempfile::tempdir().expect("a scratch directory"),
        };
        reader.save("Note.md", NOTE).await;
        reader
    }

    /// Store `bytes` and record them as a revision of `path`, as the watcher does.
    async fn save(&self, path: &str, bytes: &[u8]) {
        let source = self.scratch.path().join(Uuid::now_v7().to_string());
        std::fs::write(&source, bytes).expect("the source file");
        let stored = self.blobs.store_file(&source).expect("the blob is stored");
        self.record(TreeChange::Revision {
            path: PathBuf::from(path),
            digest: stored.digest,
            length: stored.length,
        })
        .await;
    }

    /// Record that `path` left the vault.
    async fn remove(&self, path: &str) {
        self.record(TreeChange::Removed {
            path: PathBuf::from(path),
        })
        .await;
    }

    async fn record(&self, change: TreeChange) {
        FileRecorder::new(self.fixture.database.clone(), self.blobs.clone())
            .record(self.connection_id, &change)
            .await
            .expect("the change is recorded");
    }

    /// One request with the owner claim; the status and the raw body.
    async fn send(&self, method: Method, path: &str, owner: Uuid) -> (StatusCode, Vec<u8>) {
        self.send_body(method, path, owner, Body::empty()).await
    }

    /// One request with the owner claim and a body; the status and the raw body.
    async fn send_body(
        &self,
        method: Method,
        path: &str,
        owner: Uuid,
        body: Body,
    ) -> (StatusCode, Vec<u8>) {
        let request = Request::builder()
            .method(method)
            .uri(path)
            .header(USER_HEADER, owner.to_string())
            .header("content-type", "application/json")
            .body(body)
            .expect("a request");
        let response = self
            .router
            .clone()
            .oneshot(request)
            .await
            .expect("the router answers");
        let status = response.status();
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("a body")
            .to_bytes();
        (status, bytes.to_vec())
    }

    async fn revisions(&self) -> i64 {
        sqlx::query_scalar("select count(*) from notes.revisions")
            .fetch_one(self.fixture.pool())
            .await
            .expect("the revisions can be counted")
    }

    async fn cleanup(self) {
        self.fixture
            .cleanup()
            .await
            .expect("the test database is dropped");
    }
}

#[tokio::test]
async fn foreign_connection_is_not_found() {
    let reader = Reader::new(OWNER_B).await;
    let file = format!(
        "/v1/notes/connections/{}/files/Note.md",
        reader.connection_id
    );

    let (own_status, own_bytes) = reader.send(Method::GET, &file, OWNER_B).await;
    let (foreign_status, foreign_bytes) = reader.send(Method::GET, &file, OWNER_A).await;

    assert_eq!(
        own_status,
        StatusCode::OK,
        "owner B reads a file of their own connection"
    );
    assert_eq!(own_bytes, NOTE, "the answer is the bytes of the file");
    assert_eq!(
        foreign_status,
        StatusCode::NOT_FOUND,
        "owner A gets 404 for the same path"
    );
    assert!(
        !String::from_utf8_lossy(&foreign_bytes).contains("Other"),
        "the 404 holds nothing of the file"
    );

    reader.cleanup().await;
}

#[tokio::test]
async fn file_routes_refuse_writes() {
    let reader = Reader::new(OWNER_B).await;
    let file = format!(
        "/v1/notes/connections/{}/files/Note.md",
        reader.connection_id
    );
    let before = reader.revisions().await;

    let (put, _) = reader.send(Method::PUT, &file, OWNER_B).await;
    let (delete, _) = reader.send(Method::DELETE, &file, OWNER_B).await;

    assert_eq!(put, StatusCode::METHOD_NOT_ALLOWED, "PUT on a file route");
    assert_eq!(
        delete,
        StatusCode::METHOD_NOT_ALLOWED,
        "DELETE on a file route"
    );
    assert_eq!(reader.revisions().await, before, "no revision is created");

    reader.cleanup().await;
}

/// The JSON of an answer.
fn json(bytes: &[u8]) -> Value {
    serde_json::from_slice(bytes).unwrap_or(Value::Null)
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// `path` with each byte outside the unreserved set and `/` percent-encoded, as a client sends it.
fn encoded(path: &str) -> String {
    path.bytes()
        .map(|byte| {
            if byte.is_ascii_alphanumeric() || b"-._~/".contains(&byte) {
                char::from(byte).to_string()
            } else {
                format!("%{byte:02X}")
            }
        })
        .collect()
}

#[tokio::test]
async fn list_shows_only_the_owners_connections() {
    let reader = Reader::new(OWNER_A).await;
    let second = connection_id(&connect(&reader.router, OWNER_A, "cli", "vault-key-cli").await);
    let foreign =
        connection_id(&connect(&reader.router, OWNER_B, "plugin", "vault-key-foreign").await);

    let (status_a, list_a) = reader
        .send(Method::GET, "/v1/notes/connections", OWNER_A)
        .await;
    let (status_b, list_b) = reader
        .send(Method::GET, "/v1/notes/connections", OWNER_B)
        .await;

    let ids = |list: &[u8]| -> Vec<String> {
        let mut ids: Vec<String> = json(list)["connections"]
            .as_array()
            .map(|connections| {
                connections
                    .iter()
                    .map(|connection| {
                        connection["connection_id"]
                            .as_str()
                            .unwrap_or_default()
                            .to_owned()
                    })
                    .collect()
            })
            .unwrap_or_default();
        ids.sort();
        ids
    };
    let mut own = vec![reader.connection_id.to_string(), second.to_string()];
    own.sort();
    assert_eq!(status_a, StatusCode::OK);
    assert_eq!(
        ids(&list_a),
        own,
        "owner A sees its two connections and no other"
    );
    assert_eq!(status_b, StatusCode::OK);
    assert_eq!(
        ids(&list_b),
        [foreign.to_string()],
        "owner B sees only its own"
    );

    reader.cleanup().await;
}

#[tokio::test]
async fn files_list_the_current_revisions() {
    let reader = Reader::new(OWNER_A).await;
    reader.save("Plan.md", b"plan v1\n").await;
    reader.save("Plan.md", b"plan v2\n").await;
    reader.save("Gone.md", b"gone\n").await;
    reader.remove("Gone.md").await;
    reader.save("Private/Secret.md", b"secret\n").await;
    reader.save(".obsidian/app.json", b"{}").await;
    let excluded = format!(
        "/v1/notes/connections/{}/excluded-folders",
        reader.connection_id
    );
    let (set, _) = reader
        .send_body(
            Method::PUT,
            &excluded,
            OWNER_A,
            Body::from(r#"{"excluded_folders":["Private"]}"#),
        )
        .await;

    let (status, list) = reader
        .send(
            Method::GET,
            &format!("/v1/notes/connections/{}/files", reader.connection_id),
            OWNER_A,
        )
        .await;

    assert_eq!(set, StatusCode::OK);
    assert_eq!(status, StatusCode::OK);
    let files: Vec<(String, String)> = json(&list)["files"]
        .as_array()
        .map(|files| {
            files
                .iter()
                .map(|file| {
                    (
                        file["path"].as_str().unwrap_or_default().to_owned(),
                        file["revision"].as_str().unwrap_or_default().to_owned(),
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    assert_eq!(
        files,
        [
            ("Note.md".to_owned(), sha256(NOTE)),
            ("Plan.md".to_owned(), sha256(b"plan v2\n")),
        ],
        "each live file once with its current revision; no deleted or excluded file"
    );

    reader.cleanup().await;
}

#[tokio::test]
async fn file_bytes_and_a_past_revision_are_served() {
    let reader = Reader::new(OWNER_A).await;
    reader.save("Plan.md", b"plan v1\n").await;
    reader.save("Plan.md", b"plan v2\n").await;
    let file = |path: &str| {
        format!(
            "/v1/notes/connections/{}/files/{path}",
            reader.connection_id
        )
    };

    let current = reader.send(Method::GET, &file("Plan.md"), OWNER_A).await;
    let past = reader
        .send(
            Method::GET,
            &format!("{}?revision={}", file("Plan.md"), sha256(b"plan v1\n")),
            OWNER_A,
        )
        .await;
    let unknown_path = reader.send(Method::GET, &file("Missing.md"), OWNER_A).await;
    let unknown_revision = reader
        .send(
            Method::GET,
            &format!("{}?revision={}", file("Plan.md"), sha256(b"never saved\n")),
            OWNER_A,
        )
        .await;

    assert_eq!(
        current,
        (StatusCode::OK, b"plan v2\n".to_vec()),
        "the current bytes"
    );
    assert_eq!(
        past,
        (StatusCode::OK, b"plan v1\n".to_vec()),
        "the bytes of the named revision"
    );
    for (case, (status, body)) in [("path", unknown_path), ("revision", unknown_revision)] {
        assert_eq!(status, StatusCode::NOT_FOUND, "an unknown {case}");
        assert_eq!(
            json(&body)["code"],
            "notes.file.not_found",
            "an unknown {case}"
        );
    }

    reader.cleanup().await;
}

/// The fixture vault of task 7.1 and the metadata cache of the Obsidian engine.
fn golden_fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../crates/notes-index/tests/fixtures/obsidian-1.14.4")
}

/// Every file of the fixture vault, relative to it, without `.obsidian`.
fn fixture_files(root: &std::path::Path) -> Vec<(String, PathBuf)> {
    let mut found = Vec::new();
    let mut folders = vec![root.to_path_buf()];
    while let Some(folder) = folders.pop() {
        for entry in std::fs::read_dir(&folder).expect("the fixture folder is readable") {
            let path = entry.expect("a fixture entry").path();
            let relative = path
                .strip_prefix(root)
                .expect("under the vault")
                .to_str()
                .expect("UTF-8")
                .to_owned();
            if relative == ".obsidian" {
                continue;
            }
            if path.is_dir() {
                folders.push(path);
            } else {
                found.push((relative, path));
            }
        }
    }
    found
}

/// A map of the metadata cache from a path to a count.
fn counts(value: &Value) -> BTreeMap<String, u64> {
    value
        .as_object()
        .map(|map| {
            map.iter()
                .map(|(key, count)| (key.clone(), count.as_u64().unwrap_or_default()))
                .collect()
        })
        .unwrap_or_default()
}

/// The link texts of a list of the metadata cache.
fn link_texts(list: Option<&Value>) -> Vec<String> {
    list.and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .map(|item| item["link"].as_str().unwrap_or_default().to_owned())
                .collect()
        })
        .unwrap_or_default()
}

/// Every difference between the index route of one file and the engine output for it.
fn index_differences(path: &str, answer: &Value, engine: &Value) -> Vec<String> {
    let cache = &engine["cache"];
    let mut problems = Vec::new();
    let links = answer["links"].as_array().cloned().unwrap_or_default();
    let ours = |embed: bool| -> Vec<String> {
        links
            .iter()
            .filter(|link| link["embed"] == embed)
            .map(|link| link["link"].as_str().unwrap_or_default().to_owned())
            .collect()
    };
    let mut expected_links = link_texts(cache.get("links"));
    expected_links.extend(link_texts(cache.get("frontmatterLinks")));
    if ours(false) != expected_links {
        problems.push(format!(
            "{path}: links {:?}, engine {expected_links:?}",
            ours(false)
        ));
    }
    let expected_embeds = link_texts(cache.get("embeds"));
    if ours(true) != expected_embeds {
        problems.push(format!(
            "{path}: embeds {:?}, engine {expected_embeds:?}",
            ours(true)
        ));
    }
    let mut resolved = BTreeMap::new();
    for target in links.iter().filter_map(|link| link["target"].as_str()) {
        *resolved.entry(target.to_owned()).or_insert(0_u64) += 1;
    }
    if resolved != counts(&engine["resolvedLinks"]) {
        problems.push(format!(
            "{path}: resolved {resolved:?}, engine {:?}",
            counts(&engine["resolvedLinks"])
        ));
    }
    let backlinks: BTreeMap<String, u64> = answer["backlinks"]
        .as_array()
        .map(|items| {
            items
                .iter()
                .map(|item| {
                    (
                        item["path"].as_str().unwrap_or_default().to_owned(),
                        item["count"].as_u64().unwrap_or_default(),
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    if backlinks != counts(&engine["backlinks"]) {
        problems.push(format!(
            "{path}: backlinks {backlinks:?}, engine {:?}",
            counts(&engine["backlinks"])
        ));
    }
    let tags = answer["index"]["tags"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let engine_tags = engine["getAllTags"].as_array().cloned().unwrap_or_default();
    if tags != engine_tags {
        problems.push(format!("{path}: tags {tags:?}, engine {engine_tags:?}"));
    }
    let properties = answer["index"]["frontmatter"].clone();
    let engine_properties = cache.get("frontmatter").cloned().unwrap_or(Value::Null);
    if properties != engine_properties {
        problems.push(format!(
            "{path}: properties {properties}, engine {engine_properties}"
        ));
    }
    problems
}

#[tokio::test]
async fn index_answers_the_links_of_a_file() {
    let reader = Reader::new(OWNER_A).await;
    reader.remove("Note.md").await;
    let fixture = golden_fixture();
    for (path, on_disk) in fixture_files(&fixture.join("vault")) {
        reader
            .save(&path, &std::fs::read(on_disk).expect("a fixture file"))
            .await;
    }
    let metadata: Value = serde_json::from_slice(
        &std::fs::read(fixture.join("metadata.json")).expect("metadata.json"),
    )
    .expect("metadata.json is JSON");

    let mut problems = Vec::new();
    for (path, engine) in metadata["files"].as_object().expect("files") {
        let route = format!(
            "/v1/notes/connections/{}/index/{}",
            reader.connection_id,
            encoded(path)
        );
        let (status, body) = reader.send(Method::GET, &route, OWNER_A).await;
        if status != StatusCode::OK {
            problems.push(format!("{path}: {status}"));
            continue;
        }
        problems.extend(index_differences(path, &json(&body), engine));
    }

    assert!(
        problems.is_empty(),
        "{} differences from the engine output:\n{}",
        problems.len(),
        problems.join("\n")
    );

    reader.cleanup().await;
}

#[tokio::test]
async fn excluded_folders_of_a_foreign_connection_are_not_found() {
    let reader = Reader::new(OWNER_A).await;
    let route = format!(
        "/v1/notes/connections/{}/excluded-folders",
        reader.connection_id
    );

    let (status, body) = reader
        .send_body(
            Method::PUT,
            &route,
            OWNER_B,
            Body::from(r#"{"excluded_folders":["Notes"]}"#),
        )
        .await;
    let folders: Vec<String> = sqlx::query_scalar(
        "select unnest(excluded_folders) from notes.connections where connection_id = $1",
    )
    .bind(reader.connection_id)
    .fetch_all(reader.fixture.pool())
    .await
    .expect("the connection can be read");

    assert_eq!(status, StatusCode::NOT_FOUND, "{:?}", json(&body));
    assert_eq!(json(&body)["code"], "notes.connection.not_found");
    assert!(
        folders.is_empty(),
        "the folders of owner A are unchanged: {folders:?}"
    );

    reader.cleanup().await;
}
