//! The read routes of the files and their index (workspace spec `notes-file-index`, requirement
//! "Clients read files and the index only through Edge", task 7.7).

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions read the answers they build"
)]

mod support;

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
        let scratch = tempfile::tempdir().expect("a scratch directory");
        let source = scratch.path().join("note");
        std::fs::write(&source, NOTE).expect("the source file");
        let stored = blobs.store_file(&source).expect("the blob is stored");
        FileRecorder::new(fixture.database.clone(), blobs)
            .record(
                connection_id,
                &TreeChange::Revision {
                    path: PathBuf::from("Note.md"),
                    digest: stored.digest,
                    length: stored.length,
                },
            )
            .await
            .expect("the note is recorded");
        Self {
            fixture,
            _systemd: systemd,
            router,
            connection_id,
        }
    }

    /// One request with the owner claim; the status and the raw body.
    async fn send(&self, method: Method, path: &str, owner: Uuid) -> (StatusCode, Vec<u8>) {
        let request = Request::builder()
            .method(method)
            .uri(path)
            .header(USER_HEADER, owner.to_string())
            .body(Body::empty())
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
