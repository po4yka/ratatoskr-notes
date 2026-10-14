//! `POST /v1/notes/connections`: the connect operation (workspace design D4 and D10).
//!
//! The route checks the request, stores the connection with the ciphertext of its passwords, and
//! answers `202` at once with the operation that Edge minted. The login, the setup and the start
//! of the sync unit of a `sync` connection run in a task that the service tracks, and each phase
//! and the end are reports of the operation. A refusal that the route can decide at once (an
//! invalid request, a second channel for the vault, the limit of logins, a stop of the service)
//! is the answer itself, and Edge fails the operation with its code.

use std::path::Path as FsPath;
use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use axum::extract::rejection::JsonRejection;
use axum::http::{HeaderMap, StatusCode};
use ratatoskr_notes_contracts::{VaultChannel, VaultDisplayName};
use ratatoskr_notes_persistence::{CreateOutcome, NewConnection, NewSecret};
use secrecy::ExposeSecret as _;
use tokio::sync::OwnedSemaphorePermit;
use uuid::Uuid;

use super::{
    ApiFault, ApiState, channel_word, clean_up_sync_client, device_claim, device_kind_of, owner,
    valid_vault_key,
};
use crate::login::{self, CredentialFiles, SyncLogin};
use crate::operation::{self, Completion, OPERATION_KIND, OPERATION_LANE, Phase, Report};
use crate::secrets::{Sealed, SecretKind, SecretsKey};
use crate::systemd::SystemdBus;

/// The claim that carries the id of the operation that Edge mints for a connect.
pub const OPERATION_HEADER: &str = "x-ratatoskr-operation-id";

/// The body of `POST /v1/notes/connections`.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ConnectRequest {
    channel: VaultChannel,
    vault_key: String,
    vault_display_name: VaultDisplayName,
    /// The Obsidian login. Required for `sync`, refused for the other channels.
    #[serde(default)]
    sync_login: Option<SyncLogin>,
}

/// The `202` answer.
#[derive(Debug, serde::Serialize)]
pub(super) struct ConnectAccepted {
    connection_id: Uuid,
    channel: &'static str,
    state: &'static str,
    operation: OperationView,
}

/// The operation of a connect, as the `202` answer names it.
#[derive(Debug, serde::Serialize)]
struct OperationView {
    operation_id: Uuid,
    kind: &'static str,
    lane: &'static str,
}

/// The operation id that Edge minted.
fn operation_id(headers: &HeaderMap) -> Result<Uuid, ApiFault> {
    headers
        .get(OPERATION_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| Uuid::parse_str(value).ok())
        .ok_or(ApiFault::InvalidRequest)
}

/// `POST /v1/notes/connections`: create a connection of this owner, and answer `202` with its
/// operation at once.
///
/// A `sync` connection stores its two passwords as ciphertext with the new row, in one
/// transaction. Its login then runs in a task that the service tracks (see
/// [`finish_sync_connect`]). The first report is `login` for `sync`, and `first_sync` for the
/// device channels, whose first changes complete the operation (group 8 of the workspace change).
///
/// At most the configured number of `sync` logins run at the same time. A `sync` connect over the
/// limit answers `503` at once and creates nothing.
pub(super) async fn create_connection<B: SystemdBus>(
    State(state): State<ApiState<B>>,
    headers: HeaderMap,
    body: Result<Json<ConnectRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<ConnectAccepted>), ApiFault> {
    let owner_id = owner(&headers)?;
    let operation_id = operation_id(&headers)?;
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
    // A device connects a `plugin` or `cli` vault: the connection takes changes from that device
    // only, and the kind of the device must be the kind of the channel.
    let device_id = match device_kind_of(channel) {
        Some(wanted) => {
            let device = device_claim(&headers)?;
            if device.kind != wanted {
                return Err(ApiFault::ChannelMismatch);
            }
            Some(device.device_id)
        }
        None => None,
    };

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
    if state.stopping.is_cancelled() {
        return Err(ApiFault::Unavailable);
    }
    let new = Insert {
        connection_id,
        owner_id,
        operation_id,
        device_id,
        channel,
    };
    insert(&state, &new, &request, &sealed).await?;
    tracing::info!(%connection_id, channel, "a connection was created");

    let first = if request.sync_login.is_some() {
        Phase::Login
    } else {
        Phase::FirstSync
    };
    operation::report(
        &state.database,
        owner_id,
        operation_id,
        Report::Running(first),
    )
    .await;
    if let Some(sync_login) = request.sync_login {
        let connect = SyncConnect {
            connection_id,
            owner_id,
            operation_id,
            vault_id: request.vault_key,
            vault_display_name: request.vault_display_name.as_str().to_owned(),
        };
        let task_state = state.clone();
        state.connects.spawn(async move {
            // The place of this login is free when the connect ends, after its cleanup.
            let _login_permit: Option<OwnedSemaphorePermit> = login_permit;
            finish_sync_connect(task_state, connect, sync_login).await;
        });
    }
    Ok((
        StatusCode::ACCEPTED,
        Json(ConnectAccepted {
            connection_id,
            channel,
            state: "connecting",
            operation: OperationView {
                operation_id,
                kind: OPERATION_KIND,
                lane: OPERATION_LANE,
            },
        }),
    ))
}

/// The ids of a connection to insert.
#[derive(Debug, Clone, Copy)]
struct Insert {
    connection_id: Uuid,
    owner_id: Uuid,
    operation_id: Uuid,
    device_id: Option<Uuid>,
    channel: &'static str,
}

/// Insert the connection with its ciphertext, or refuse a second connection of the vault.
async fn insert<B: SystemdBus>(
    state: &ApiState<B>,
    new: &Insert,
    request: &ConnectRequest,
    sealed: &[Sealed],
) -> Result<(), ApiFault> {
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
            connection_id: new.connection_id,
            owner_id: new.owner_id,
            channel: new.channel,
            vault_key: &request.vault_key,
            vault_display_name: request.vault_display_name.as_str(),
            secrets: &secrets,
            operation_id: new.operation_id,
            device_id: new.device_id,
        })
        .await
        .map_err(|error| {
            tracing::error!(%error, "a connection could not be created");
            ApiFault::Unavailable
        })?;
    match outcome {
        CreateOutcome::Created => Ok(()),
        // A second device of the same channel presents the vault id of a connection that has its
        // device: the vault folder was copied or synced by another tool (workspace design D5).
        CreateOutcome::VaultAlreadyConnected {
            channel, device_id, ..
        } if channel == new.channel
            && device_id.is_some()
            && new.device_id.is_some()
            && device_id != new.device_id =>
        {
            Err(ApiFault::ChannelMismatch)
        }
        CreateOutcome::VaultAlreadyConnected {
            connection_id,
            channel,
            ..
        } => Err(ApiFault::VaultAlreadyConnected {
            connection_id,
            channel,
        }),
    }
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

/// A `sync` connect whose login runs in its own task.
#[derive(Debug)]
struct SyncConnect {
    connection_id: Uuid,
    owner_id: Uuid,
    operation_id: Uuid,
    vault_id: String,
    vault_display_name: String,
}

/// Run the first login of a new `sync` connection, and report its failure. A success continues in
/// the pipeline of the working tree, which completes the operation after the first full scan.
async fn finish_sync_connect<B: SystemdBus>(
    state: ApiState<B>,
    connect: SyncConnect,
    sync_login: SyncLogin,
) {
    if let Some(before_login) = &state.before_login {
        (before_login.0)(connect.connection_id).await;
    }
    if let Err(fault) = log_in(&state, &connect, &sync_login).await {
        let (code, message, retryable) = fault.fields();
        tracing::info!(connection_id = %connect.connection_id, code, "a connect failed");
        operation::report(
            &state.database,
            connect.owner_id,
            connect.operation_id,
            Report::Failed(code, message, retryable),
        )
        .await;
    }
}

/// Run the first login of a new `sync` connection, and record it when it succeeded.
///
/// The owner can remove the connection between its insert and this step. The login then does not
/// start, and the connect fails with `notes.connection.not_found`. When the owner removes it while
/// the login runs, the disconnect stops the login unit; the connect waits for the end of the
/// disconnect, and fails with `notes.connection.not_found` when the connection is removed. When
/// the login finished before the disconnect stopped it, the login can write to the client state
/// after the cleanup of the disconnect, so the cleanup runs again.
///
/// After the login, the sync unit of the connection starts before the login is recorded, so the
/// cleanup that runs again after a disconnect also stops the sync unit. A sync unit that does not
/// start fails the connect as a failed login does, with `notes.service.unavailable`.
///
/// When the login fails, or the service stops while the login runs, the cleanup step stops the
/// login unit, the cleanup unit removes what the login left in the client state (a token, when
/// only the setup step failed), and the connection becomes `failed`. When the cleanup fails, the
/// connection stays `connecting` with its ciphertext, and the owner removes it with a disconnect,
/// or the next start of the service fails it.
async fn log_in<B: SystemdBus>(
    state: &ApiState<B>,
    connect: &SyncConnect,
    sync_login: &SyncLogin,
) -> Result<(), ApiFault> {
    let connection_id = connect.connection_id;
    let fault = match state.database.is_connecting(connection_id).await {
        Ok(true) => match run_login(state, connect, sync_login).await {
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
/// When the login succeeded, report `setup` and start the sync unit of the connection with the
/// completion of its operation.
///
/// The wait ends early when the service stops. The wait for the unit is then dropped, and the
/// caller stops the unit.
async fn run_login<B: SystemdBus>(
    state: &ApiState<B>,
    connect: &SyncConnect,
    sync_login: &SyncLogin,
) -> Result<(), ApiFault> {
    let connection_id = connect.connection_id;
    let credentials = CredentialFiles::write(
        &state.credentials_dir,
        connection_id,
        &connect.vault_id,
        sync_login,
    )
    .map_err(|error| {
        tracing::error!(%error, %connection_id, "the credential files of a login could not be written");
        ApiFault::Unavailable
    })?;
    let outcome = tokio::select! {
        biased;
        () = state.stopping.cancelled() => {
            tracing::warn!(%connection_id, "the service stops while a login runs; the login is stopped");
            None
        }
        outcome = login::run(state.systemd.as_ref(), connection_id) => Some(outcome),
    };
    // The files are deleted whatever the result of the unit.
    drop(credentials);
    match outcome {
        Some(Ok(None)) => {
            operation::report(
                &state.database,
                connect.owner_id,
                connect.operation_id,
                Report::Running(Phase::Setup),
            )
            .await;
            let completion = Completion {
                connection_id,
                owner_id: connect.owner_id,
                operation_id: connect.operation_id,
                channel: "sync".to_owned(),
                vault_display_name: connect.vault_display_name.clone(),
                journal_readable: true,
            };
            if state
                .supervisor
                .start(connection_id, Some(completion))
                .await
            {
                Ok(())
            } else {
                Err(ApiFault::Unavailable)
            }
        }
        Some(Ok(Some(failure))) => Err(ApiFault::Login(failure)),
        Some(Err(error)) => {
            tracing::error!(%error, %connection_id, "the login unit could not run");
            Err(ApiFault::Unavailable)
        }
        // The wait for the login ended early.
        None => Err(ApiFault::Unavailable),
    }
}
