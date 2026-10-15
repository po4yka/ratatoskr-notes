//! `GET /v1/notes/conflicts/{conflict_id}`: one conflict of the owner (workspace design D2).
//!
//! The answer names the three sides and the merge proposal by hash, as
//! `notes.conflict.raised.v1` does, and carries the text of each side and of the proposal of a
//! Markdown or plain-text file, so that a client can show them. A text larger than the cap of a
//! merge (1 MiB) is not read; `texts_over_limit` names it. The bus never carries this text; only
//! this route of the owner does.

use axum::Json;
use axum::extract::rejection::PathRejection;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use serde_json::{Map, Value, json};
use uuid::Uuid;

use super::{ApiFault, ApiState, owner};
use crate::systemd::SystemdBus;
use crate::writes::MAX_SIDE_BYTES;

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
    let mut over_limit = Vec::new();
    if TEXT_MEDIA_TYPES.contains(&conflict.media_type.as_str()) {
        for (side, hash) in [
            ("base", &conflict.base_hash),
            ("current", &conflict.current_hash),
            ("writer", &conflict.writer_hash),
            ("merged", &conflict.merged_hash),
        ] {
            match text_of(&state, hash.as_deref()).await {
                Text::Read(text) => {
                    texts.insert(side.to_owned(), Value::String(text));
                }
                Text::OverLimit => over_limit.push(Value::from(side)),
                Text::Absent => {}
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
        "texts_over_limit": over_limit,
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
        // A conflict of a rename and its link rewrites names every stale or unapplied file.
        if let Some(paths) = conflict.stale_paths {
            members.insert("stale_paths".to_owned(), json!(paths));
        }
        if let (Some(merged_hash), Some(overlapping)) =
            (conflict.merged_hash, conflict.has_overlapping_hunks)
        {
            members.insert(
                "merge_proposal".to_owned(),
                json!({ "merged_hash": merged_hash, "has_overlapping_hunks": overlapping }),
            );
        }
    }
    Ok(Json(answer))
}

/// The text of one side of a conflict.
#[derive(Debug)]
enum Text {
    /// The UTF-8 text of the side.
    Read(String),
    /// The side is larger than [`MAX_SIDE_BYTES`]; it is not read.
    OverLimit,
    /// The side does not exist, notes does not hold it, or it is not UTF-8.
    Absent,
}

/// The text of the blob `hash`. The length comes from the file system first, so a blob larger
/// than [`MAX_SIDE_BYTES`], the cap of a merge, is never read into memory.
async fn text_of<B: SystemdBus>(state: &ApiState<B>, hash: Option<&str>) -> Text {
    let Some(blob) = hash.and_then(|hash| state.blobs.path_of(hash)) else {
        return Text::Absent;
    };
    let Ok(metadata) = tokio::fs::metadata(&blob).await else {
        return Text::Absent;
    };
    if metadata.len() > MAX_SIDE_BYTES {
        return Text::OverLimit;
    }
    match tokio::fs::read(&blob).await.map(String::from_utf8) {
        Ok(Ok(text)) => Text::Read(text),
        _ => Text::Absent,
    }
}
