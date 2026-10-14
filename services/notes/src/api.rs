//! The domain API on the loopback listener that Edge forwards `/v1/notes` to.
//!
//! Every route reads the owner from the claim that Edge mints, `x-ratatoskr-user-id`. Edge strips
//! any client-supplied copy, and this listener is reachable only on loopback.

use std::fmt;
use std::future::Future;
use std::path::{Path as FsPath, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use axum::extract::rejection::{JsonRejection, PathRejection};
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{delete, post, put};
use axum::{Json, Router};
use ratatoskr_notes_contracts::{VaultChannel, VaultDisplayName};
use ratatoskr_notes_persistence::{CreateOutcome, Database, NewConnection, NewSecret, OutboxEvent};
use secrecy::ExposeSecret as _;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use uuid::Uuid;

use crate::blobs::BlobStore;
use crate::events;
use crate::files::{self, ExcludedFoldersSet};
use crate::login::{self, CredentialFiles, SyncLogin};
use crate::secrets::{Sealed, SecretKind, SecretsKey};
use crate::supervisor::Supervisor;

mod fault;
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

/// How many first logins run at the same time when the configuration does not say.
pub const DEFAULT_MAX_LOGINS: usize = 2;

/// The future of one [`BeforeLogin`] pause.
type Pause = Pin<Box<dyn Future<Output = ()> + Send>>;

/// A pause between the insert of a connection and the start of its login. For tests.
#[derive(Clone)]
struct BeforeLogin(Arc<dyn Fn(Uuid) -> Pause + Send + Sync>);

impl fmt::Debug for BeforeLogin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("BeforeLogin")
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
    before_login: Option<BeforeLogin>,
    blobs: BlobStore,
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
            blobs: self.blobs.clone(),
        }
    }
}

impl<B: SystemdBus> ApiState<B> {
    /// The state of the domain routes.
    #[must_use]
    pub fn new(database: Database, systemd: B) -> Self {
        let systemd = Arc::new(systemd);
        Self {
            supervisor: Supervisor::new(database.clone(), Arc::clone(&systemd)),
            database,
            systemd,
            key_file: PathBuf::from(DEFAULT_KEY_FILE),
            credentials_dir: PathBuf::from(DEFAULT_CREDENTIALS_DIR),
            logins: Arc::new(Semaphore::new(DEFAULT_MAX_LOGINS)),
            connects: TaskTracker::new(),
            stopping: CancellationToken::new(),
            removals: Removals::default(),
            before_login: None,
            blobs: BlobStore::new(crate::blobs::DEFAULT_ROOT),
        }
    }

    /// Read the bytes of the files from `blobs` instead of the store at
    /// [`crate::blobs::DEFAULT_ROOT`].
    #[must_use]
    pub fn with_blobs(mut self, blobs: BlobStore) -> Self {
        self.blobs = blobs;
        self
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
        self.before_login = Some(BeforeLogin(Arc::new(move |connection_id| {
            Box::pin(hook(connection_id))
        })));
        self
    }

    /// Fail the connects that still run, stop reading the journals of the sync units, and wait
    /// until each one ended. The service calls it when the stop signal arrives, and gives it the
    /// grace window. The sync units continue to run.
    ///
    /// A connect whose login runs stops the login unit, runs the cleanup unit, and marks its
    /// connection `failed` without its ciphertext, as after a failed login. Its request answers
    /// `503`. A connect that starts after this call answers `503` and creates nothing.
    pub async fn shut_down(&self) {
        self.stopping.cancel();
        self.connects.close();
        tokio::join!(self.connects.wait(), self.supervisor.shut_down());
    }

    /// Fail each `sync` connection whose first login a stop of the service interrupted: stop the
    /// sync unit and the login unit, run the cleanup unit, and mark the connection `failed`
    /// without its ciphertext. A connection whose login finished is left alone.
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
        for connection_id in interrupted {
            if clean_up_sync_client(self.systemd.as_ref(), connection_id)
                .await
                .is_err()
            {
                continue;
            }
            match self.database.fail_connection(connection_id).await {
                Ok(_) => {
                    tracing::info!(%connection_id, "a login that a stop interrupted is failed");
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

/// The domain router. It is served only on the domain listener, never on the operator listener.
///
/// The service builds it once, at start, before any login can run. It first removes the
/// credentials root: a directory under it is what a crash during a login left behind.
pub fn domain_router<B: SystemdBus>(state: ApiState<B>) -> Router {
    remove_stale_credentials(&state.credentials_dir);
    Router::new()
        .route("/v1/notes/connections", post(create_connection::<B>))
        .route(
            "/v1/notes/connections/{connection_id}",
            delete(remove_connection::<B>),
        )
        .route(
            "/v1/notes/connections/{connection_id}/excluded-folders",
            put(set_excluded_folders::<B>),
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

/// The body of `POST /v1/notes/connections`.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ConnectRequest {
    channel: VaultChannel,
    vault_key: String,
    vault_display_name: VaultDisplayName,
    /// The Obsidian login. Required for `sync`, refused for the other channels.
    #[serde(default)]
    sync_login: Option<SyncLogin>,
}

/// The `202` answer.
#[derive(Debug, serde::Serialize)]
struct ConnectAccepted {
    connection_id: Uuid,
    channel: &'static str,
    state: &'static str,
}

/// `POST /v1/notes/connections`: create a connection of this owner.
///
/// A `sync` connection stores its two passwords as ciphertext with the new row, in one
/// transaction, and then runs its login unit and waits for it. When the login fails, the cleanup
/// unit removes what the login left on the host, the connection becomes `failed` without its
/// ciphertext, and the answer is `422` with one of the four stable login codes.
///
/// After the checks, the connect runs to its end in a task that the request does not own. When the
/// client goes away, the request future is dropped, but the task continues: it stops the login,
/// runs the cleanup unit and marks the connection `failed`, so that the connection does not stay
/// `connecting` and the vault key is free again. A stop of the service does the same within the
/// grace window (see [`ApiState::shut_down`]).
///
/// At most the configured number of `sync` logins run at the same time. A `sync` connect over the
/// limit answers `503` at once and creates nothing.
async fn create_connection<B: SystemdBus>(
    State(state): State<ApiState<B>>,
    headers: HeaderMap,
    body: Result<Json<ConnectRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<ConnectAccepted>), ApiFault> {
    let owner_id = owner(&headers)?;
    // The rejection is not logged: its text can quote a value of the body.
    let Json(request) = body.map_err(|_| ApiFault::InvalidRequest)?;
    let channel = channel_word(request.channel).ok_or(ApiFault::InvalidRequest)?;
    if !valid_vault_key(&request.vault_key) {
        return Err(ApiFault::InvalidRequest);
    }
    match (channel, &request.sync_login) {
        ("sync", Some(sync_login))
            if sync_login.is_valid() && login::valid_remote_vault_id(&request.vault_key) => {}
        ("plugin" | "cli", None) => {}
        _ => return Err(ApiFault::InvalidRequest),
    }

    let login_permit = match &request.sync_login {
        Some(_) => Some(Arc::clone(&state.logins).try_acquire_owned().map_err(|_| {
            tracing::warn!("a connect is refused: the limit of concurrent logins is reached");
            ApiFault::Unavailable
        })?),
        None => None,
    };

    let connection_id = Uuid::now_v7();
    let sealed = match &request.sync_login {
        Some(sync_login) => seal_secrets(&state.key_file, connection_id, sync_login)?,
        None => Vec::new(),
    };

    // The sender lives as long as the request. The task sees its drop as the end of the request.
    let (request_alive, request_gone) = oneshot::channel::<()>();
    let connects = state.connects.clone();
    let connect = connects.spawn(async move {
        // The place of this login is free when the connect ends, after its cleanup.
        let _login_permit: Option<OwnedSemaphorePermit> = login_permit;
        finish_connect(
            state,
            connection_id,
            owner_id,
            channel,
            request,
            sealed,
            request_gone,
        )
        .await
    });
    let finished = connect.await;
    drop(request_alive);
    finished.map_err(|error| {
        tracing::error!(%error, %connection_id, "the connect task stopped before its end");
        ApiFault::Unavailable
    })??;
    Ok((
        StatusCode::ACCEPTED,
        Json(ConnectAccepted {
            connection_id,
            channel,
            state: "connecting",
        }),
    ))
}

/// Insert the connection with its ciphertext and, for `sync`, run the first login. It runs in its
/// own task, so a dropped request does not stop it halfway. After the stop signal it creates
/// nothing.
async fn finish_connect<B: SystemdBus>(
    state: ApiState<B>,
    connection_id: Uuid,
    owner_id: Uuid,
    channel: &'static str,
    request: ConnectRequest,
    sealed: Vec<Sealed>,
    request_gone: oneshot::Receiver<()>,
) -> Result<(), ApiFault> {
    if state.stopping.is_cancelled() {
        return Err(ApiFault::Unavailable);
    }
    let secrets: Vec<NewSecret<'_>> = sealed
        .iter()
        .map(|sealed| NewSecret {
            kind: sealed.kind.as_str(),
            nonce: &sealed.nonce,
            ciphertext: &sealed.ciphertext,
        })
        .collect();
    let outcome = state
        .database
        .create_connection(&NewConnection {
            connection_id,
            owner_id,
            channel,
            vault_key: &request.vault_key,
            vault_display_name: request.vault_display_name.as_str(),
            secrets: &secrets,
        })
        .await
        .map_err(|error| {
            tracing::error!(%error, "a connection could not be created");
            ApiFault::Unavailable
        })?;
    if let CreateOutcome::VaultAlreadyConnected {
        connection_id,
        channel,
    } = outcome
    {
        return Err(ApiFault::VaultAlreadyConnected {
            connection_id,
            channel,
        });
    }
    tracing::info!(%connection_id, channel, "a connection was created");

    if let Some(sync_login) = &request.sync_login {
        if let Some(before_login) = &state.before_login {
            (before_login.0)(connection_id).await;
        }
        log_in(
            &state,
            connection_id,
            &request.vault_key,
            sync_login,
            request_gone,
        )
        .await?;
    }
    Ok(())
}

/// Seal the account password and, when the owner gave one, the end-to-end password. The MFA
/// code is not sealed: it is never stored.
fn seal_secrets(
    key_file: &FsPath,
    connection_id: Uuid,
    sync_login: &SyncLogin,
) -> Result<Vec<Sealed>, ApiFault> {
    let key = SecretsKey::load(key_file).map_err(|error| {
        tracing::error!(%error, key_file = %key_file.display(), "the secrets key cannot be used");
        ApiFault::Unavailable
    })?;
    let mut plaintexts = vec![(SecretKind::AccountPassword, &sync_login.password)];
    if let Some(e2e_password) = &sync_login.e2e_password {
        plaintexts.push((SecretKind::E2ePassword, e2e_password));
    }
    plaintexts
        .into_iter()
        .map(|(kind, secret)| {
            key.seal(connection_id, kind, secret.expose_secret().as_bytes())
                .map_err(|error| {
                    tracing::error!(%error, %connection_id, kind = kind.as_str(), "a secret could not be sealed");
                    ApiFault::Unavailable
                })
        })
        .collect()
}

/// Run the first login of a new `sync` connection, and record it when it succeeded.
///
/// The owner can remove the connection between its insert and this step. The login then does not
/// start, and the answer is `404`. When the owner removes it while the login runs, the disconnect
/// stops the login unit; the connect waits for the end of the disconnect, and the answer is `404`
/// when the connection is removed. When the login finished before the disconnect stopped it, the
/// login can write to the client state after the cleanup of the disconnect, so the cleanup runs again.
///
/// After the login, the sync unit of the connection starts before the login is recorded, so the
/// cleanup that runs again after a disconnect also stops the sync unit. A sync unit that does not
/// start fails the connect as a failed login does, with `503`.
///
/// When the login fails, or the request ends or the service stops while the login runs, the
/// cleanup step stops the login unit, the cleanup unit removes what the login left in the client state
/// (a token, when only the setup step failed), and the connection becomes `failed`. When the
/// cleanup fails, the connection stays `connecting` with its ciphertext, and the owner removes it
/// with a disconnect, or the next start of the service fails it.
async fn log_in<B: SystemdBus>(
    state: &ApiState<B>,
    connection_id: Uuid,
    vault_id: &str,
    sync_login: &SyncLogin,
    request_gone: oneshot::Receiver<()>,
) -> Result<(), ApiFault> {
    let fault = match state.database.is_connecting(connection_id).await {
        Ok(true) => match run_login(state, connection_id, vault_id, sync_login, request_gone).await
        {
            Ok(()) => match state.database.record_login(connection_id).await {
                Ok(true) => return Ok(()),
                Ok(false) => {
                    tracing::info!(%connection_id, "the connection was removed while its login ran; the cleanup runs again");
                    ApiFault::NotFound
                }
                Err(error) => {
                    tracing::error!(%error, %connection_id, "a finished login could not be recorded");
                    ApiFault::Unavailable
                }
            },
            Err(fault) => {
                if removed_meanwhile(state, connection_id).await {
                    tracing::info!(%connection_id, "the connection was removed while its login ran");
                    return Err(ApiFault::NotFound);
                }
                fault
            }
        },
        Ok(false) => {
            tracing::info!(%connection_id, "the connection was removed before its login started; no login runs");
            return Err(ApiFault::NotFound);
        }
        Err(error) => {
            tracing::error!(%error, %connection_id, "the state of a connection could not be read");
            ApiFault::Unavailable
        }
    };
    state.supervisor.forget(connection_id);
    clean_up_sync_client(state.systemd.as_ref(), connection_id).await?;
    let failed = state
        .database
        .fail_connection(connection_id)
        .await
        .map_err(|error| {
            tracing::error!(%error, %connection_id, "a failed connection could not be marked");
            ApiFault::Unavailable
        })?;
    if failed {
        tracing::info!(%connection_id, "a connection failed at its first login");
    }
    Err(fault)
}

/// Whether a disconnect of the connection ran while its login ran, and removed it. A disconnect
/// stops the login unit before it runs the cleanup unit, so the login ends before the disconnect
/// does: the wait for the end of the disconnect comes first.
async fn removed_meanwhile<B: SystemdBus>(state: &ApiState<B>, connection_id: Uuid) -> bool {
    state.removals.wait(connection_id).await
        && matches!(state.database.is_connecting(connection_id).await, Ok(false))
}

/// Write the credential files of the login, run the login unit, wait for it, and delete the files.
/// When the login succeeded, start the sync unit of the connection.
///
/// The wait ends early when the request ends or the service stops. The wait for the unit is then
/// dropped, and the caller stops the unit. When both happened, the end of the request counts.
async fn run_login<B: SystemdBus>(
    state: &ApiState<B>,
    connection_id: Uuid,
    vault_id: &str,
    sync_login: &SyncLogin,
    request_gone: oneshot::Receiver<()>,
) -> Result<(), ApiFault> {
    let credentials =
        CredentialFiles::write(&state.credentials_dir, connection_id, vault_id, sync_login)
            .map_err(|error| {
                tracing::error!(%error, %connection_id, "the credential files of a login could not be written");
                ApiFault::Unavailable
            })?;
    let outcome = tokio::select! {
        biased;
        _ = request_gone => {
            tracing::warn!(%connection_id, "the connect request ended while the login ran; the login is stopped");
            None
        }
        () = state.stopping.cancelled() => {
            tracing::warn!(%connection_id, "the service stops while a login runs; the login is stopped");
            None
        }
        outcome = login::run(state.systemd.as_ref(), connection_id) => Some(outcome),
    };
    // The files are deleted whatever the result of the unit.
    drop(credentials);
    match outcome {
        Some(Ok(None)) if state.supervisor.start(connection_id).await => Ok(()),
        Some(Ok(Some(failure))) => Err(ApiFault::Login(failure)),
        Some(Err(error)) => {
            tracing::error!(%error, %connection_id, "the login unit could not run");
            Err(ApiFault::Unavailable)
        }
        // The sync unit did not start, or the wait for the login ended early.
        Some(Ok(None)) | None => Err(ApiFault::Unavailable),
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
        )
        .await
        .map_err(|error| {
            tracing::error!(%error, %connection_id, "a connection could not be removed");
            ApiFault::Unavailable
        })?;
    if removed {
        tracing::info!(%connection_id, channel = %connection.channel, "a connection was removed");
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
