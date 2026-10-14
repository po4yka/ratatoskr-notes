//! The device channel helpers of `device_channel.rs`: a router with its blob store, and the
//! requests of the plugin of an owner.

use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use http_body_util::BodyExt as _;
use ratatoskr_notes::api::{ApiState, domain_router};
use ratatoskr_notes::blobs::BlobStore;
use ratatoskr_notes_persistence::test_support::TestDatabase;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use tower::ServiceExt as _;
use uuid::Uuid;

use crate::support::systemd::FakeSystemd;
use crate::support::{
    Answer, DEVICE_ID_HEADER, DEVICE_KIND_HEADER, USER_HEADER, connect, connection_id, device_of,
    send_with,
};

pub(crate) const OWNER: Uuid = Uuid::from_u128(0x0192_0000_0000_7000_8000_0000_0000_0dc1);

/// The domain router of one test database, with its blob store.
pub(crate) struct Channel {
    pub(crate) fixture: TestDatabase,
    pub(crate) systemd: FakeSystemd,
    pub(crate) router: Router,
    pub(crate) blobs: BlobStore,
}

impl Channel {
    pub(crate) async fn new() -> Self {
        Self::with(|state| state).await
    }

    /// A router whose state `settings` adjusts.
    pub(crate) async fn with(
        settings: impl FnOnce(ApiState<FakeSystemd>) -> ApiState<FakeSystemd>,
    ) -> Self {
        let fixture = TestDatabase::create().await.expect("a test database");
        let systemd = FakeSystemd::new();
        let root = systemd.blob_root();
        std::fs::create_dir_all(&root).expect("the blob root");
        let blobs = BlobStore::new(root);
        let mut channel = Self {
            fixture,
            systemd,
            router: Router::new(),
            blobs,
        };
        channel.router = domain_router(settings(channel.state()));
        channel
    }

    /// The state of the domain routes of this test database, as the service builds it at start.
    pub(crate) fn state(&self) -> ApiState<FakeSystemd> {
        ApiState::new(self.fixture.database.clone(), self.systemd.clone())
            .with_key_file(self.systemd.key_file())
            .with_credentials_dir(self.systemd.credentials_root())
            .with_blobs(self.blobs.clone())
    }

    /// A `plugin` connection of `owner`, from the device of `owner`.
    pub(crate) async fn plugin_connection(&self, owner: Uuid, vault_key: &str) -> Uuid {
        connection_id(&connect(&self.router, owner, "plugin", vault_key).await)
    }

    /// Upload `bytes` through an upload session of `connection_id` from the plugin of `owner`, so
    /// that a batch or a manifest of that connection may name them.
    pub(crate) async fn hold(&self, owner: Uuid, connection_id: Uuid, bytes: &[u8]) {
        let [_, _, finalized] = self
            .upload_as(owner, connection_id, &sha256(bytes), bytes)
            .await;
        assert_eq!(finalized.body["outcome"], "stored", "{:?}", finalized.body);
    }

    /// `POST /v1/notes/connections/{id}/changes` from `device` of the kind `kind`.
    pub(crate) async fn batch_from(
        &self,
        owner: Uuid,
        device: Uuid,
        kind: &str,
        connection_id: Uuid,
        changes: Vec<Value>,
    ) -> Answer {
        let claims = [
            (DEVICE_ID_HEADER, device.to_string()),
            (DEVICE_KIND_HEADER, kind.to_owned()),
        ];
        send_with(
            &self.router,
            Method::POST,
            &format!("/v1/notes/connections/{connection_id}/changes"),
            owner,
            &claims,
            Some(json!({ "changes": changes })),
        )
        .await
    }

    /// A batch from the plugin of `owner`.
    pub(crate) async fn batch(
        &self,
        owner: Uuid,
        connection_id: Uuid,
        changes: Vec<Value>,
    ) -> Answer {
        self.batch_from(
            owner,
            device_of(owner),
            "obsidian_plugin",
            connection_id,
            changes,
        )
        .await
    }

    /// The number of `notes.file.changed.v1` events of a connection.
    pub(crate) async fn events(&self, connection_id: Uuid) -> i64 {
        sqlx::query_scalar(
            "select count(*) from notes.outbox
             where event_type = 'notes.file.changed.v1'
               and payload -> 'payload' ->> 'connection_id' = $1",
        )
        .bind(connection_id.to_string())
        .fetch_one(self.fixture.pool())
        .await
        .expect("the outbox can be read")
    }

    /// The number of revisions of the files of a connection.
    pub(crate) async fn revisions(&self, connection_id: Uuid) -> i64 {
        sqlx::query_scalar(
            "select count(*) from notes.revisions r
             join notes.files f on f.file_id = r.file_id
             where f.connection_id = $1",
        )
        .bind(connection_id)
        .fetch_one(self.fixture.pool())
        .await
        .expect("the revisions can be read")
    }

    pub(crate) async fn cleanup(self) {
        self.fixture
            .cleanup()
            .await
            .expect("the test database is dropped");
    }
}

pub(crate) fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// One `created` or `updated` change with the facts of `bytes`.
pub(crate) fn change(sequence: u64, kind: &str, path: &str, bytes: &[u8]) -> Value {
    json!({
        "sequence": sequence,
        "kind": kind,
        "path": path,
        "sha256": sha256(bytes),
        "length_bytes": bytes.len(),
    })
}
pub(crate) const OTHER_OWNER: Uuid = Uuid::from_u128(0x0192_0000_0000_7000_8000_0000_0000_0dc2);

/// A second device of [`OWNER`], for example a phone that holds a copy of the vault folder.
pub(crate) const SECOND_DEVICE: Uuid = Uuid::from_u128(0x0192_0000_0000_7000_8000_0000_0000_0dc3);

/// The words of the hint that points the owner to Obsidian Sync.
pub(crate) const SYNC_HINT: &str = "Obsidian Sync";

impl Channel {
    /// `POST /v1/notes/connections` with `channel = plugin` from `device`.
    pub(crate) async fn connect_from(&self, owner: Uuid, device: Uuid, vault_key: &str) -> Answer {
        let claims = [
            (DEVICE_ID_HEADER, device.to_string()),
            (DEVICE_KIND_HEADER, "obsidian_plugin".to_owned()),
        ];
        let body = json!({
            "channel": "plugin",
            "vault_key": vault_key,
            "vault_display_name": "Personal",
        });
        send_with(
            &self.router,
            Method::POST,
            "/v1/notes/connections",
            owner,
            &claims,
            Some(body),
        )
        .await
    }

    /// The device that a connection records.
    pub(crate) async fn device(&self, connection_id: Uuid) -> Option<Uuid> {
        sqlx::query_scalar("select device_id from notes.connections where connection_id = $1")
            .bind(connection_id)
            .fetch_one(self.fixture.pool())
            .await
            .expect("the connection exists")
    }
}

impl Channel {
    /// A JSON request from the plugin of [`OWNER`] to a route of `connection_id`.
    pub(crate) async fn device_call(
        &self,
        connection_id: Uuid,
        route: &str,
        body: Value,
    ) -> Answer {
        self.device_call_as(OWNER, connection_id, route, body).await
    }

    /// A JSON request from the plugin of `owner` to a route of `connection_id`.
    pub(crate) async fn device_call_as(
        &self,
        owner: Uuid,
        connection_id: Uuid,
        route: &str,
        body: Value,
    ) -> Answer {
        let claims = [
            (DEVICE_ID_HEADER, device_of(owner).to_string()),
            (DEVICE_KIND_HEADER, "obsidian_plugin".to_owned()),
        ];
        send_with(
            &self.router,
            Method::POST,
            &format!("/v1/notes/connections/{connection_id}/{route}"),
            owner,
            &claims,
            Some(body),
        )
        .await
    }

    /// `PUT .../blobs/{token}/chunks/{index}` with the raw bytes of one chunk.
    pub(crate) async fn put_chunk(
        &self,
        owner: Uuid,
        connection_id: Uuid,
        token: &str,
        index: u32,
        bytes: &[u8],
    ) -> Answer {
        let request = Request::builder()
            .method(Method::PUT)
            .uri(format!(
                "/v1/notes/connections/{connection_id}/blobs/{token}/chunks/{index}"
            ))
            .header(USER_HEADER, owner.to_string())
            .header(DEVICE_ID_HEADER, device_of(owner).to_string())
            .header(DEVICE_KIND_HEADER, "obsidian_plugin")
            .header("content-type", "application/octet-stream")
            .body(Body::from(bytes.to_vec()))
            .expect("a request");
        let response = self
            .router
            .clone()
            .oneshot(request)
            .await
            .expect("the router answers");
        let status = response.status();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("a body")
            .to_bytes();
        Answer {
            status,
            body: serde_json::from_slice(&body).unwrap_or(Value::Null),
        }
    }

    /// Upload `bytes` under the declared SHA-256 `declared` through the transfer routes, in one
    /// chunk. Returns the answers of the open, the chunk and the finalize.
    pub(crate) async fn upload(
        &self,
        connection_id: Uuid,
        declared: &str,
        bytes: &[u8],
    ) -> [Answer; 3] {
        self.upload_as(OWNER, connection_id, declared, bytes).await
    }

    /// [`Self::upload`] from the plugin of `owner`.
    pub(crate) async fn upload_as(
        &self,
        owner: Uuid,
        connection_id: Uuid,
        declared: &str,
        bytes: &[u8],
    ) -> [Answer; 3] {
        let opened = self
            .device_call_as(
                owner,
                connection_id,
                "blobs",
                json!({
                    "declared_size_bytes": bytes.len(),
                    "media_type": "text/markdown",
                    "digest": { "algorithm": "sha256", "hex": declared },
                    "chunk_size_bytes": 65_536,
                }),
            )
            .await;
        let token = opened.body["resumption_token"]
            .as_str()
            .unwrap_or("rst_00000000000000000000000000")
            .to_owned();
        let chunk = self.put_chunk(owner, connection_id, &token, 0, bytes).await;
        let finalized = self
            .device_call_as(
                owner,
                connection_id,
                &format!("blobs/{token}/finalize"),
                json!({ "resumption_token": token }),
            )
            .await;
        [opened, chunk, finalized]
    }
}

impl Channel {
    /// The payloads of the `notes.file.changed.v1` events of a connection, in order.
    pub(crate) async fn changes(&self, connection_id: Uuid) -> Vec<Value> {
        sqlx::query_scalar(
            "select payload -> 'payload' from notes.outbox
             where event_type = 'notes.file.changed.v1'
               and payload -> 'payload' ->> 'connection_id' = $1
             order by position",
        )
        .bind(connection_id.to_string())
        .fetch_all(self.fixture.pool())
        .await
        .expect("the outbox can be read")
    }
}

/// One manifest entry with the facts of `bytes`.
pub(crate) fn entry(path: &str, bytes: &[u8]) -> Value {
    json!({ "path": path, "sha256": sha256(bytes), "length_bytes": bytes.len() })
}

impl Channel {
    /// The state of a connection.
    pub(crate) async fn connection_state(&self, connection_id: Uuid) -> String {
        sqlx::query_scalar("select state from notes.connections where connection_id = $1")
            .bind(connection_id)
            .fetch_one(self.fixture.pool())
            .await
            .expect("the connection exists")
    }

    /// The number of `notes.vault.connected.v1` events of a connection.
    pub(crate) async fn connected_events(&self, connection_id: Uuid) -> i64 {
        sqlx::query_scalar(
            "select count(*) from notes.outbox
             where event_type = 'notes.vault.connected.v1'
               and payload -> 'payload' ->> 'connection_id' = $1",
        )
        .bind(connection_id.to_string())
        .fetch_one(self.fixture.pool())
        .await
        .expect("the outbox can be read")
    }
}

/// A short bound for the first manifest, so that a test sees its end.
pub(crate) const SHORT_DEADLINE: Duration = Duration::from_millis(200);

impl Channel {
    /// `GET /v1/notes/connections/{id}/files/{path}` for `owner`: the status and the bytes.
    pub(crate) async fn read_file(
        &self,
        owner: Uuid,
        connection_id: Uuid,
        path: &str,
    ) -> (StatusCode, Vec<u8>) {
        let request = Request::builder()
            .method(Method::GET)
            .uri(format!(
                "/v1/notes/connections/{connection_id}/files/{path}"
            ))
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
        let body = response
            .into_body()
            .collect()
            .await
            .expect("a body")
            .to_bytes();
        (status, body.to_vec())
    }
}
