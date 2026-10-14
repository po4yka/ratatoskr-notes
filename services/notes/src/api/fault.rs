//! The failures of the domain routes and their error envelopes.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use ratatoskr_error_contracts::{ErrorCode, ErrorEnvelope};
use ratatoskr_identifiers::SafeMessage;
use uuid::Uuid;

use crate::login::LoginFailure;

/// Every failure a domain route answers with. Each one renders as the contract error envelope.
#[derive(Debug)]
pub(super) enum ApiFault {
    Unauthenticated,
    InvalidRequest,
    VaultAlreadyConnected {
        connection_id: Uuid,
        channel: String,
    },
    NotFound,
    Login(LoginFailure),
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
            Self::Login(_) => StatusCode::UNPROCESSABLE_ENTITY,
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
            Self::Login(failure) => {
                let (code, message) = failure.fields();
                (code, message, false)
            }
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
