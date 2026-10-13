//! The domain API on the loopback listener that Edge forwards `/v1/notes` to.
//!
//! Every route reads the owner from the claim that Edge mints, `x-ratatoskr-user-id`. Edge strips
//! any client-supplied copy, and this listener is reachable only on loopback.

use std::sync::Arc;

use axum::extract::rejection::{JsonRejection, PathRejection};
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, post};
use axum::{Json, Router};
use ratatoskr_error_contracts::{ErrorCode, ErrorEnvelope};
use ratatoskr_identifiers::SafeMessage;
use ratatoskr_notes_contracts::{VaultChannel, VaultDisplayName};
use ratatoskr_notes_persistence::{CreateOutcome, Database, NewConnection, OutboxEvent};
use uuid::Uuid;

use crate::events;
use crate::systemd::{JobResult, SystemdBus, UnitName};

/// The owner claim that Edge mints.
const USER_HEADER: &str = "x-ratatoskr-user-id";

/// A connect body holds three short strings; anything larger is not a connect request.
const MAX_REQUEST_BYTES: usize = 16 * 1024;

/// The longest vault key, in characters.
const MAX_VAULT_KEY_CHARS: usize = 255;

/// What the domain handlers share.
#[derive(Debug)]
pub struct ApiState<B> {
    database: Database,
    systemd: Arc<B>,
}

impl<B> Clone for ApiState<B> {
    fn clone(&self) -> Self {
        Self {
            database: self.database.clone(),
            systemd: Arc::clone(&self.systemd),
        }
    }
}

impl<B: SystemdBus> ApiState<B> {
    /// The state of the domain routes.
    #[must_use]
    pub fn new(database: Database, systemd: B) -> Self {
        Self {
            database,
            systemd: Arc::new(systemd),
        }
    }
}

/// The domain router. It is served only on the domain listener, never on the operator listener.
pub fn domain_router<B: SystemdBus>(state: ApiState<B>) -> Router {
    Router::new()
        .route("/v1/notes/connections", post(create_connection::<B>))
        .route(
            "/v1/notes/connections/{connection_id}",
            delete(remove_connection::<B>),
        )
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES))
        .with_state(state)
}

/// The body of `POST /v1/notes/connections`.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ConnectRequest {
    channel: VaultChannel,
    vault_key: String,
    vault_display_name: VaultDisplayName,
}

/// The `202` answer.
#[derive(Debug, serde::Serialize)]
struct ConnectAccepted {
    connection_id: Uuid,
    channel: &'static str,
    state: &'static str,
}

async fn create_connection<B: SystemdBus>(
    State(state): State<ApiState<B>>,
    headers: HeaderMap,
    body: Result<Json<ConnectRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<ConnectAccepted>), ApiFault> {
    let owner_id = owner(&headers)?;
    let Json(request) = body.map_err(|_| ApiFault::InvalidRequest)?;
    let channel = channel_word(request.channel).ok_or(ApiFault::InvalidRequest)?;
    if !valid_vault_key(&request.vault_key) {
        return Err(ApiFault::InvalidRequest);
    }

    let connection_id = Uuid::now_v7();
    let outcome = state
        .database
        .create_connection(&NewConnection {
            connection_id,
            owner_id,
            channel,
            vault_key: &request.vault_key,
            vault_display_name: request.vault_display_name.as_str(),
        })
        .await
        .map_err(|error| {
            tracing::error!(%error, "a connection could not be created");
            ApiFault::Unavailable
        })?;

    match outcome {
        CreateOutcome::Created => {
            tracing::info!(%connection_id, channel, "a connection was created");
            Ok((
                StatusCode::ACCEPTED,
                Json(ConnectAccepted {
                    connection_id,
                    channel,
                    state: "connecting",
                }),
            ))
        }
        CreateOutcome::VaultAlreadyConnected {
            connection_id,
            channel,
        } => Err(ApiFault::VaultAlreadyConnected {
            connection_id,
            channel,
        }),
    }
}

/// `DELETE /v1/notes/connections/{id}`: remove a connection of this owner.
///
/// A `sync` connection first stops its sync unit and runs its cleanup unit, which deletes the
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

    if connection.channel == "sync" {
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

/// Stop the sync unit, then run the cleanup unit and wait for it (workspace design D3).
async fn clean_up_sync_client<B: SystemdBus>(
    systemd: &B,
    connection_id: Uuid,
) -> Result<(), ApiFault> {
    for (unit, stop) in [
        (UnitName::sync(connection_id), true),
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

/// Every failure a domain route answers with. Each one renders as the contract error envelope.
#[derive(Debug)]
enum ApiFault {
    Unauthenticated,
    InvalidRequest,
    VaultAlreadyConnected {
        connection_id: Uuid,
        channel: String,
    },
    NotFound,
    CleanupFailed,
    Unavailable,
}

impl ApiFault {
    const fn status(&self) -> StatusCode {
        match self {
            Self::Unauthenticated => StatusCode::UNAUTHORIZED,
            Self::InvalidRequest => StatusCode::BAD_REQUEST,
            Self::VaultAlreadyConnected { .. } => StatusCode::CONFLICT,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::CleanupFailed | Self::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    const fn fields(&self) -> (&'static str, &'static str, bool) {
        match self {
            Self::Unauthenticated => (
                "notes.request.unauthenticated",
                "Authentication is required.",
                false,
            ),
            Self::InvalidRequest => ("notes.request.invalid", "The request is invalid.", false),
            Self::VaultAlreadyConnected { .. } => (
                "notes.connection.vault_already_connected",
                "This vault already has a connection. Remove it before you connect the vault \
                 through another channel.",
                false,
            ),
            Self::NotFound => (
                "notes.connection.not_found",
                "The connection does not exist.",
                false,
            ),
            Self::CleanupFailed => (
                "notes.connection.cleanup_failed",
                "The connection could not be removed now. Repeat the request later.",
                true,
            ),
            Self::Unavailable => (
                "notes.service.unavailable",
                "The notes service is temporarily unavailable.",
                true,
            ),
        }
    }
}

impl IntoResponse for ApiFault {
    fn into_response(self) -> Response {
        let (code, message, retryable) = self.fields();
        let (Ok(code), Ok(message)) = (ErrorCode::parse(code), SafeMessage::parse(message)) else {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        };
        let mut envelope = ErrorEnvelope::new(code, message, retryable);
        if let Self::VaultAlreadyConnected {
            connection_id,
            channel,
        } = &self
        {
            envelope.extensions.insert(
                "connection_id",
                serde_json::Value::String(connection_id.to_string()),
            );
            envelope
                .extensions
                .insert("channel", serde_json::Value::String(channel.clone()));
        }
        (self.status(), Json(envelope)).into_response()
    }
}
