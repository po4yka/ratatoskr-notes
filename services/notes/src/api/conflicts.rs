//! `GET /v1/notes/conflicts/{conflict_id}`: one conflict of the owner (workspace design D2).
//!
//! The answer names the three sides by hash, as `notes.conflict.raised.v1` does, and carries the
//! text of each side of a Markdown or plain-text file, so that a client can show them. The bus
//! never carries this text; only this route of the owner does.

use axum::Json;
use axum::extract::rejection::PathRejection;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use serde_json::{Map, Value, json};
use uuid::Uuid;

use super::{ApiFault, ApiState, owner};
use crate::systemd::SystemdBus;

/// The media types whose sides the answer carries as text.
const TEXT_MEDIA_TYPES: [&str; 2] = ["text/markdown", "text/plain"];

/// `GET /v1/notes/conflicts/{conflict_id}`: the conflict, when it belongs to a connection of the
/// owner. Any other id answers `404` `notes.conflict.not_found`.
pub(super) async fn read_conflict<B: SystemdBus>(
    State(state): State<ApiState<B>>,
    headers: HeaderMap,
    conflict_id: Result<Path<Uuid>, PathRejection>,
) -> Result<Json<Value>, ApiFault> {
    let owner_id = owner(&headers)?;
    let Path(conflict_id) = conflict_id.map_err(|_| ApiFault::ConflictNotFound)?;
    let conflict = state
        .database
        .conflict_of_owner(owner_id, conflict_id)
        .await
        .map_err(|error| {
            tracing::error!(%error, %conflict_id, "a conflict could not be read");
            ApiFault::Unavailable
        })?
        .ok_or(ApiFault::ConflictNotFound)?;
    let mut texts = Map::new();
    if TEXT_MEDIA_TYPES.contains(&conflict.media_type.as_str()) {
        for (side, hash) in [
            ("base", &conflict.base_hash),
            ("current", &conflict.current_hash),
            ("writer", &conflict.writer_hash),
        ] {
            if let Some(text) = text_of(&state, hash.as_deref()).await {
                texts.insert(side.to_owned(), Value::String(text));
            }
        }
    }
    let mut answer = json!({
        "conflict_id": conflict.conflict_id,
        "operation_id": conflict.operation_id,
        "connection_id": conflict.connection_id,
        "path": conflict.path,
        "origin": conflict.origin,
        "media_type": conflict.media_type,
        "state": conflict.state,
        "texts": texts,
    });
    if let Value::Object(members) = &mut answer {
        for (name, hash) in [
            ("base_hash", conflict.base_hash),
            ("current_hash", conflict.current_hash),
            ("writer_hash", conflict.writer_hash),
        ] {
            if let Some(hash) = hash {
                members.insert(name.to_owned(), Value::String(hash));
            }
        }
    }
    Ok(Json(answer))
}

/// The UTF-8 text of the blob `hash` when the blob store holds it and it is not larger than the
/// size limit of the service.
async fn text_of<B: SystemdBus>(state: &ApiState<B>, hash: Option<&str>) -> Option<String> {
    let blob = state.blobs.path_of(hash?)?;
    let metadata = tokio::fs::metadata(&blob).await.ok()?;
    if metadata.len() > state.max_file_bytes {
        return None;
    }
    String::from_utf8(tokio::fs::read(&blob).await.ok()?).ok()
}
