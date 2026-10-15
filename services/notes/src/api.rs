//! The domain API on the loopback listener that Edge forwards `/v1/notes` to.
//!
//! Every route reads the owner from the claim that Edge mints, `x-ratatoskr-user-id`. Edge strips
//! any client-supplied copy, and this listener is reachable only on loopback.

use std::fmt;
use std::future::Future;
use std::path::{Path as FsPath, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::rejection::{JsonRejection, PathRejection};
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use ratatoskr_notes_contracts::VaultChannel;
use ratatoskr_notes_persistence::{Database, OutboxEvent};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use uuid::Uuid;

use crate::blobs::BlobStore;
use crate::channel::device_kind_of;
use crate::events;
use crate::files::{self, ExcludedFoldersSet, FileRecorder};
use crate::operation::{self, ReportRow};
use crate::supervisor::Supervisor;
use crate::tree_sync::Pipeline;
use crate::uploads::Uploads;

pub use connect::OPERATION_HEADER;

mod blobs;
mod channel;
mod connect;
mod device_connect;
mod fault;
mod read;
mod removals;

use crate::systemd::{JobResult, SystemdBus, UnitName};
use fault::ApiFault;
use removals::Removals;

/// The owner claim that Edge mints.
const USER_HEADER: &str = "x-ratatoskr-user-id";

/// A connect body holds a few short strings; anything larger is not a connect request.
const MAX_REQUEST_BYTES: usize = 16 * 1024;

/// The longest vault key, in characters.
const MAX_VAULT_KEY_CHARS: usize = 255;

/// Where the host keeps the key that encrypts the Obsidian secrets (workspace design D4).
pub const DEFAULT_KEY_FILE: &str = "/etc/ratatoskr/notes-secrets.key";

/// Where the service writes the credential files of a login, on tmpfs (workspace design D4). The
/// login unit names the same path in its `LoadCredential=` lines.
pub const DEFAULT_CREDENTIALS_DIR: &str = "/run/ratatoskr-notes/credentials";

/// How long a `plugin` or `cli` connect waits for the first manifest of its device.
pub const DEFAULT_MANIFEST_DEADLINE: Duration = Duration::from_mins(30);

/// How many first logins run at the same time when the configuration does not say.
pub const DEFAULT_MAX_LOGINS: usize = 2;

/// The future of one [`TestHook`] pause.
type Pause = Pin<Box<dyn Future<Output = ()> + Send>>;

/// A pause at one step of a connection, for example between the insert of a connection and the
/// start of its login. For tests.
#[derive(Clone)]
pub(crate) struct TestHook(pub(crate) Arc<dyn Fn(Uuid) -> Pause + Send + Sync>);

impl fmt::Debug for TestHook {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("TestHook")
    }
}

impl TestHook {
    fn of<F, Fut>(hook: F) -> Self
    where
        F: Fn(Uuid) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        Self(Arc::new(move |connection_id| Box::pin(hook(connection_id))))
    }
}

/// What the domain handlers share.
#[derive(Debug)]
pub struct ApiState<B> {
    database: Database,
    systemd: Arc<B>,
    supervisor: Supervisor<B>,
    key_file: PathBuf,
    credentials_dir: PathBuf,
    logins: Arc<Semaphore>,
    connects: TaskTracker,
    stopping: CancellationToken,
    removals: Removals,
    before_login: Option<TestHook>,
    after_manifest: Option<TestHook>,
    after_terminal_state: Option<TestHook>,
    after_sync_start: Option<TestHook>,
    blobs: BlobStore,
    trees_root: PathBuf,
    max_file_bytes: u64,
    uploads: Uploads,
    manifest_deadline: Duration,
}

impl<B> Clone for ApiState<B> {
    fn clone(&self) -> Self {
        Self {
            database: self.database.clone(),
            systemd: Arc::clone(&self.systemd),
            supervisor: self.supervisor.clone(),
            key_file: self.key_file.clone(),
            credentials_dir: self.credentials_dir.clone(),
            logins: Arc::clone(&self.logins),
            connects: self.connects.clone(),
            stopping: self.stopping.clone(),
            removals: self.removals.clone(),
            before_login: self.before_login.clone(),
            after_manifest: self.after_manifest.clone(),
            after_terminal_state: self.after_terminal_state.clone(),
            after_sync_start: self.after_sync_start.clone(),
            blobs: self.blobs.clone(),
            trees_root: self.trees_root.clone(),
            max_file_bytes: self.max_file_bytes,
            uploads: self.uploads.clone(),
            manifest_deadline: self.manifest_deadline,
        }
    }
}

impl<B: SystemdBus> ApiState<B> {
    /// The state of the domain routes.
    #[must_use]
    pub fn new(database: Database, systemd: B) -> Self {
        let systemd = Arc::new(systemd);
        let blobs = BlobStore::new(crate::blobs::DEFAULT_ROOT);
        let trees_root = PathBuf::from(crate::systemd::TREES_ROOT);
        let max_file_bytes = ratatoskr_notes_index::paths::DEFAULT_MAX_FILE_BYTES;
        let pipeline = pipeline_of(&database, &blobs, &trees_root, max_file_bytes);
        Self {
            supervisor: Supervisor::new(database.clone(), Arc::clone(&systemd), pipeline),
            database,
            systemd,
            key_file: PathBuf::from(DEFAULT_KEY_FILE),
            credentials_dir: PathBuf::from(DEFAULT_CREDENTIALS_DIR),
            logins: Arc::new(Semaphore::new(DEFAULT_MAX_LOGINS)),
            connects: TaskTracker::new(),
            stopping: CancellationToken::new(),
            removals: Removals::default(),
            before_login: None,
            after_manifest: None,
            after_terminal_state: None,
            after_sync_start: None,
            blobs,
            trees_root,
            max_file_bytes,
            uploads: Uploads::default(),
            manifest_deadline: DEFAULT_MANIFEST_DEADLINE,
        }
    }

    /// Wait at most `deadline` for the first manifest of a `plugin` or `cli` connection instead of
    /// [`DEFAULT_MANIFEST_DEADLINE`].
    #[must_use]
    pub const fn with_manifest_deadline(mut self, deadline: Duration) -> Self {
        self.manifest_deadline = deadline;
        self
    }

    /// Watch the working trees under `root` instead of [`crate::systemd::TREES_ROOT`]. For tests:
    /// the sync units on the host write under [`crate::systemd::TREES_ROOT`].
    #[must_use]
    pub fn with_trees_root(mut self, root: impl Into<PathBuf>) -> Self {
        self.trees_root = root.into();
        self.refresh_pipeline();
        self
    }

    /// Read the bytes of the files from `blobs` instead of the store at
    /// [`crate::blobs::DEFAULT_ROOT`].
    #[must_use]
    pub fn with_blobs(mut self, blobs: BlobStore) -> Self {
        self.blobs = blobs;
        self.refresh_pipeline();
        self
    }

    /// Index no file larger than `max_file_bytes` instead of
    /// [`ratatoskr_notes_index::paths::DEFAULT_MAX_FILE_BYTES`].
    #[must_use]
    pub fn with_max_file_bytes(mut self, max_file_bytes: u64) -> Self {
        self.max_file_bytes = max_file_bytes;
        self.refresh_pipeline();
        self
    }

    /// The answer to a device request for a connection that takes no changes: `410`
    /// `notes.connection.removed` when its owner removed it and its own device asks, so that the
    /// device stops; `404` `notes.connection.not_found` otherwise, for a connection that failed,
    /// of another owner or of another device, so that nothing leaks.
    async fn missing_connection(
        &self,
        owner_id: Uuid,
        device_id: Uuid,
        connection_id: Uuid,
    ) -> ApiFault {
        match self
            .database
            .removed_for_device(owner_id, connection_id, device_id)
            .await
        {
            Ok(true) => ApiFault::ConnectionRemoved,
            Ok(false) => ApiFault::NotFound,
            Err(error) => {
                tracing::error!(%error, %connection_id, "a connection could not be read");
                ApiFault::Unavailable
            }
        }
    }

    /// The recorder of the file changes, with the blob store and the size limit of the settings.
    fn recorder(&self) -> FileRecorder {
        FileRecorder::new(self.database.clone(), self.blobs.clone())
            .with_max_file_bytes(self.max_file_bytes)
    }

    /// Give the supervisor the pipeline of the working trees that the settings name now.
    fn refresh_pipeline(&mut self) {
        let pipeline = pipeline_of(
            &self.database,
            &self.blobs,
            &self.trees_root,
            self.max_file_bytes,
        );
        self.supervisor.set_pipeline(pipeline);
    }

    /// Let at most `max_logins` first logins run at the same time instead of
    /// [`DEFAULT_MAX_LOGINS`].
    #[must_use]
    pub fn with_max_logins(mut self, max_logins: usize) -> Self {
        self.logins = Arc::new(Semaphore::new(max_logins));
        self
    }

    /// Wait for `hook` after the insert of a `sync` connection and before its login starts. For
    /// tests: it holds a connect in that step, so that a test can act there.
    #[must_use]
    pub fn with_before_login<F, Fut>(mut self, hook: F) -> Self
    where
        F: Fn(Uuid) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.before_login = Some(TestHook::of(hook));
        self
    }

    /// Wait for `hook` after a connection was failed, removed or its login expired, in the step
    /// that ends its connect operation. For tests: a hook that never ends stands for a stop of the
    /// process at that step.
    #[must_use]
    pub fn with_after_terminal_state<F, Fut>(mut self, hook: F) -> Self
    where
        F: Fn(Uuid) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let hook = TestHook::of(hook);
        self.supervisor.set_after_terminal_state(hook.clone());
        self.after_terminal_state = Some(hook);
        self
    }

    /// Wait for `hook` after the first login of a `sync` connection started its sync unit and
    /// before the login is recorded. For tests: the sync unit can report an expired login there.
    #[must_use]
    pub fn with_after_sync_start<F, Fut>(mut self, hook: F) -> Self
    where
        F: Fn(Uuid) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.after_sync_start = Some(TestHook::of(hook));
        self
    }

    /// Run the hook of [`Self::with_after_terminal_state`] for `connection_id`, when there is one.
    pub(crate) async fn after_terminal_state(&self, connection_id: Uuid) {
        if let Some(hook) = &self.after_terminal_state {
            (hook.0)(connection_id).await;
        }
    }

    /// Wait for `hook` after a manifest of a device connection is applied and before the answer.
    /// For tests: a test can run the end of the manifest bound there.
    #[must_use]
    pub fn with_after_manifest<F, Fut>(mut self, hook: F) -> Self
    where
        F: Fn(Uuid) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.after_manifest = Some(TestHook::of(hook));
        self
    }

    /// Fail the connects that still run, stop reading the journals of the sync units, and wait
    /// until each one ended. The service calls it when the stop signal arrives, and gives it the
    /// grace window. The sync units continue to run.
    ///
    /// A connect whose login runs stops the login unit, runs the cleanup unit, and marks its
    /// connection `failed` without its ciphertext, as after a failed login. Its operation fails
    /// with `notes.service.unavailable`. A connect that starts after this call answers `503` and
    /// creates nothing.
    pub async fn shut_down(&self) {
        self.stopping.cancel();
        self.connects.close();
        tokio::join!(self.connects.wait(), self.supervisor.shut_down());
    }

    /// Fail each `sync` connection whose first login a stop of the service interrupted: stop the
    /// sync unit and the login unit, run the cleanup unit, mark the connection `failed` without
    /// its ciphertext, and fail its connect operation with `notes.service.unavailable`. A
    /// connection whose login finished is left alone.
    ///
    /// The service calls it at start, after it binds the listeners and before they accept a
    /// request, so no connect of this process runs yet. A failure is logged and the start
    /// continues; the connection then stays `connecting` until the next start or a disconnect.
    pub async fn reconcile_interrupted_logins(&self) {
        let interrupted = match self.database.interrupted_logins().await {
            Ok(interrupted) => interrupted,
            Err(error) => {
                tracing::error!(%error, "the interrupted logins could not be read");
                return;
            }
        };
        for interrupted in interrupted {
            let connection_id = interrupted.connection_id;
            if clean_up_sync_client(self.systemd.as_ref(), connection_id)
                .await
                .is_err()
            {
                continue;
            }
            // The failure and the end of its operation are one transaction.
            let report: Vec<ReportRow> = ReportRow::failure(
                interrupted.owner_id,
                interrupted.operation_id,
                ApiFault::Unavailable.fields(),
            )
            .into_iter()
            .collect();
            match self
                .database
                .fail_connection_reporting(connection_id, &operation::events_of(&report))
                .await
            {
                Ok(_) => {
                    tracing::info!(%connection_id, "a login that a stop interrupted is failed");
                    self.after_terminal_state(connection_id).await;
                }
                Err(error) => {
                    tracing::error!(%error, %connection_id, "an interrupted login could not be failed");
                }
            }
        }
    }

    /// Start the sync unit of each `sync` connection whose first login finished. The service calls
    /// it at start, after [`Self::reconcile_interrupted_logins`] and before the listeners accept a
    /// request, so that a disconnect cannot run while a unit starts.
    pub async fn resume_sync_connections(&self) {
        self.supervisor.resume().await;
    }

    /// Read the secrets key from `path` instead of [`DEFAULT_KEY_FILE`].
    #[must_use]
    pub fn with_key_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.key_file = path.into();
        self
    }

    /// Write the credential files of a login under `path` instead of [`DEFAULT_CREDENTIALS_DIR`].
    /// For tests: the login unit reads [`DEFAULT_CREDENTIALS_DIR`] on the host.
    #[must_use]
    pub fn with_credentials_dir(mut self, path: impl Into<PathBuf>) -> Self {
        self.credentials_dir = path.into();
        self
    }
}

/// The watcher and the recorder of the working trees.
fn pipeline_of(
    database: &Database,
    blobs: &BlobStore,
    trees_root: &FsPath,
    max_file_bytes: u64,
) -> Pipeline {
    Pipeline {
        recorder: FileRecorder::new(database.clone(), blobs.clone())
            .with_max_file_bytes(max_file_bytes),
        blobs: blobs.clone(),
        trees_root: trees_root.to_path_buf(),
    }
}

/// The domain router. It is served only on the domain listener, never on the operator listener.
///
/// The service builds it once, at start, before any login can run. It first removes the
/// credentials root: a directory under it is what a crash during a login left behind.
pub fn domain_router<B: SystemdBus>(state: ApiState<B>) -> Router {
    remove_stale_credentials(&state.credentials_dir);
    Router::new()
        .route(
            "/v1/notes/connections",
            post(connect::create_connection::<B>).get(read::list_connections::<B>),
        )
        .route(
            "/v1/notes/connections/{connection_id}/files",
            get(read::list_files::<B>),
        )
        .route(
            "/v1/notes/connections/{connection_id}/files/{*path}",
            get(read::read_file::<B>),
        )
        .route(
            "/v1/notes/connections/{connection_id}/index/{*path}",
            get(read::read_index::<B>),
        )
        .route(
            "/v1/notes/connections/{connection_id}",
            delete(remove_connection::<B>),
        )
        .route(
            "/v1/notes/connections/{connection_id}/excluded-folders",
            put(set_excluded_folders::<B>),
        )
        .route(
            "/v1/notes/connections/{connection_id}/blobs",
            post(blobs::open_upload::<B>),
        )
        .route(
            "/v1/notes/connections/{connection_id}/blobs/{token}/chunks/{index}",
            put(blobs::put_chunk::<B>).layer(DefaultBodyLimit::max(blobs::MAX_CHUNK_BYTES)),
        )
        .route(
            "/v1/notes/connections/{connection_id}/blobs/{token}/status",
            get(blobs::upload_status::<B>),
        )
        .route(
            "/v1/notes/connections/{connection_id}/blobs/{token}/finalize",
            post(blobs::finalize_upload::<B>),
        )
        .route(
            "/v1/notes/connections/{connection_id}/manifest",
            post(channel::post_manifest::<B>)
                .layer(DefaultBodyLimit::max(channel::MAX_MANIFEST_BYTES)),
        )
        .route(
            "/v1/notes/connections/{connection_id}/changes",
            post(channel::post_changes::<B>).layer(DefaultBodyLimit::max(channel::MAX_BATCH_BYTES)),
        )
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES))
        .with_state(state)
}

/// Remove the credential files that earlier logins left behind. A failure is logged, and the
/// start continues: the files of a new login are in a new directory.
fn remove_stale_credentials(root: &FsPath) {
    match std::fs::remove_dir_all(root) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            tracing::error!(%error, root = %root.display(), "the stale credential files could not be removed");
        }
    }
}

/// `DELETE /v1/notes/connections/{id}`: remove a connection of this owner.
///
/// A `sync` connection first stops its sync unit and a login unit that still runs, and runs its
/// cleanup unit, which deletes the
/// working tree and the client state. Only then does one transaction mark the connection removed,
/// delete the stored ciphertext, and write `notes.vault.disconnected.v1` to the outbox. When the
/// cleanup fails nothing is committed, and the owner can repeat the request.
async fn remove_connection<B: SystemdBus>(
    State(state): State<ApiState<B>>,
    headers: HeaderMap,
    connection_id: Result<Path<Uuid>, PathRejection>,
) -> Result<StatusCode, ApiFault> {
    let owner_id = owner(&headers)?;
    // An id that is not a UUID cannot name a connection.
    let Path(connection_id) = connection_id.map_err(|_| ApiFault::NotFound)?;
    let connection = state
        .database
        .connection_of_owner(owner_id, connection_id)
        .await
        .map_err(|error| {
            tracing::error!(%error, "a connection could not be read");
            ApiFault::Unavailable
        })?
        .ok_or(ApiFault::NotFound)?;

    // A connect whose login this disconnect stops waits for the end of the disconnect.
    let _removal = state.removals.begin(connection_id);
    if connection.channel == "sync" {
        state.supervisor.forget(connection_id);
        clean_up_sync_client(state.systemd.as_ref(), connection_id).await?;
    }

    let event_id = Uuid::now_v7();
    let envelope = events::vault_disconnected(
        event_id,
        owner_id,
        connection_id,
        &connection.channel,
        &connection.vault_display_name,
    )
    .and_then(|envelope| {
        serde_json::to_value(&envelope).map_err(|_| events::EnvelopeBuildError::SERIALIZE)
    })
    .map_err(|error| {
        tracing::error!(%error, %connection_id, "the disconnect event could not be built");
        ApiFault::Unavailable
    })?;
    let aggregate_id = events::connection_aggregate(connection_id);
    // A connect that waits for its first scan or manifest ends with the removal, in its
    // transaction. A connect whose login runs reports its own end.
    let report: Vec<ReportRow> = ReportRow::failure(
        owner_id,
        connection.operation_id,
        ApiFault::NotFound.fields(),
    )
    .into_iter()
    .collect();
    let removed = state
        .database
        .remove_connection(
            owner_id,
            connection_id,
            &OutboxEvent {
                event_id,
                event_type: "notes.vault.disconnected.v1",
                aggregate_type: events::CONNECTION_AGGREGATE,
                aggregate_id: &aggregate_id,
                envelope: &envelope,
            },
            &operation::events_of(&report),
        )
        .await
        .map_err(|error| {
            tracing::error!(%error, %connection_id, "a connection could not be removed");
            ApiFault::Unavailable
        })?;
    if removed {
        tracing::info!(%connection_id, channel = %connection.channel, "a connection was removed");
        state.after_terminal_state(connection_id).await;
    }
    Ok(StatusCode::NO_CONTENT)
}

/// The body of `PUT /v1/notes/connections/{id}/excluded-folders`.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct ExcludedFolders {
    excluded_folders: Vec<String>,
}

/// `PUT /v1/notes/connections/{id}/excluded-folders`: set the folders of a connection that are
/// not indexed (workspace design D7). Each present file under them is published `deleted` once,
/// and a later change under them publishes nothing. The answer names the folders as stored.
async fn set_excluded_folders<B: SystemdBus>(
    State(state): State<ApiState<B>>,
    headers: HeaderMap,
    connection_id: Result<Path<Uuid>, PathRejection>,
    body: Result<Json<ExcludedFolders>, JsonRejection>,
) -> Result<Json<ExcludedFolders>, ApiFault> {
    let owner_id = owner(&headers)?;
    let Path(connection_id) = connection_id.map_err(|_| ApiFault::NotFound)?;
    let Json(request) = body.map_err(|_| ApiFault::InvalidRequest)?;
    let outcome = files::set_excluded_folders(
        &state.database,
        owner_id,
        connection_id,
        &request.excluded_folders,
    )
    .await
    .map_err(|error| {
        tracing::error!(%error, %connection_id, "the excluded folders could not be set");
        ApiFault::Unavailable
    })?;
    match outcome {
        ExcludedFoldersSet::Set { folders, deleted } => {
            tracing::info!(%connection_id, deleted, "the excluded folders of a connection were set");
            Ok(Json(ExcludedFolders {
                excluded_folders: folders,
            }))
        }
        ExcludedFoldersSet::NotFound => Err(ApiFault::NotFound),
        ExcludedFoldersSet::Invalid => Err(ApiFault::InvalidRequest),
    }
}

/// Stop the sync unit and the login unit, then run the cleanup unit and wait for it (workspace
/// design D3). The stop of a unit that does not run does nothing. The login unit is stopped so
/// that it cannot write to the client state after the cleanup unit removed it.
async fn clean_up_sync_client<B: SystemdBus>(
    systemd: &B,
    connection_id: Uuid,
) -> Result<(), ApiFault> {
    for (unit, stop) in [
        (UnitName::sync(connection_id), true),
        (UnitName::login(connection_id), true),
        (UnitName::cleanup(connection_id), false),
    ] {
        let job = if stop {
            systemd.stop_unit(&unit).await
        } else {
            systemd.start_unit(&unit).await
        };
        match job {
            Ok(JobResult::Done) => {}
            Ok(JobResult::NotDone(result)) => {
                tracing::error!(%unit, %result, "a systemd job did not finish");
                return Err(ApiFault::CleanupFailed);
            }
            Err(error) => {
                tracing::error!(%unit, %error, "a systemd call failed");
                return Err(ApiFault::CleanupFailed);
            }
        }
    }
    Ok(())
}

/// The claim that names the paired device of a request (Platform ADR-0016).
const DEVICE_ID_HEADER: &str = "x-ratatoskr-device-id";

/// The claim that names the kind of that device, for example `obsidian_plugin`.
const DEVICE_KIND_HEADER: &str = "x-ratatoskr-device-kind";

/// The device of a request on a device route, from the claims that Edge mints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DeviceClaim<'a> {
    device_id: Uuid,
    kind: &'a str,
}

/// The device claims of a request on a device route. A missing or malformed claim answers `401`:
/// Edge sends both for every request of a paired device.
fn device_claim(headers: &HeaderMap) -> Result<DeviceClaim<'_>, ApiFault> {
    let device_id = headers
        .get(DEVICE_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| Uuid::parse_str(value).ok())
        .ok_or(ApiFault::Unauthenticated)?;
    let kind = headers
        .get(DEVICE_KIND_HEADER)
        .and_then(|value| value.to_str().ok())
        .filter(|kind| !kind.is_empty())
        .ok_or(ApiFault::Unauthenticated)?;
    Ok(DeviceClaim { device_id, kind })
}

/// The owner from the minted claim.
fn owner(headers: &HeaderMap) -> Result<Uuid, ApiFault> {
    headers
        .get(USER_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| Uuid::parse_str(value).ok())
        .ok_or(ApiFault::Unauthenticated)
}

/// The wire word that the database stores for a channel. `None` for a channel this build does
/// not know, which the request then refuses.
const fn channel_word(channel: VaultChannel) -> Option<&'static str> {
    match channel {
        VaultChannel::Sync => Some("sync"),
        VaultChannel::Plugin => Some("plugin"),
        VaultChannel::Cli => Some("cli"),
        _ => None,
    }
}

/// 1 to 255 characters with no control character, the rule `schema.sql` also states.
fn valid_vault_key(key: &str) -> bool {
    let length = key.chars().count();
    (1..=MAX_VAULT_KEY_CHARS).contains(&length) && !key.chars().any(char::is_control)
}
