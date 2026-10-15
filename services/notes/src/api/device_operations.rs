//! The queue of the writes of a `plugin` or `cli` connection, which its device reads (workspace
//! design D5 and D6, spec `notes-write-delivery`, requirement "Device channels read an ordered
//! queue through Edge"):
//!
//! - `GET /v1/notes/connections/{id}/device-operations?after=<cursor>` lists the delivered writes
//!   after the cursor, in the order of acceptance;
//! - `GET .../device-operations/{operation_id}/content` streams the bytes that the operation puts
//!   in the vault (the file, or the JSON value of a `set_property`);
//! - `POST .../device-operations/{operation_id}/result` reports what the device did.
//!
//! A rename whose links notes rewrites is one operation of the queue with the list `rewrites`: the
//! path, the base hash and the expected hash of each linking note (spec `notes-link-rewrite`). The
//! device renames through Obsidian, which rewrites the links on the device, and reports the hash of
//! each linking note in the list `rewrites` of its result: `applied` with `sha256`, or
//! `base_mismatch` with `local_sha256`. A note that the result does not name counts as not applied.
//!
//! Only the device that the connection registered, of the kind of its channel, reads or answers the
//! queue. Any other device, also one of the owner, gets `404` `notes.connection.not_found`, so that
//! no operation is disclosed. Paths are not logged: a file name is the owner's content.

use axum::Json;
use axum::body::Body;
use axum::extract::rejection::{JsonRejection, PathRejection, QueryRejection};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, header};
use axum::response::{IntoResponse as _, Response};
use ratatoskr_notes_persistence::WriteRow;
use serde_json::{Value, json};
use tokio_util::io::ReaderStream;
use uuid::Uuid;

use super::{ApiFault, ApiState, device_claim, owner};
use crate::channel::device_kind_of;
use crate::files::media_type;
use crate::systemd::SystemdBus;
use crate::writes::{Outcome, Refusal, Writes};

/// The most operations of one answer of the queue.
const PAGE: i64 = 256;

/// The query of the queue.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct QueueQuery {
    /// The cursor of the last operation that the device read; the start of the queue when absent.
    after: Option<String>,
}

/// The result that the device reports.
#[derive(Debug, serde::Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum DeviceResult {
    /// The device applied the operation; the file at the path (the target of a `rename` or a
    /// `move`) has the SHA-256 `sha256` now. Absent after a `delete`. For a rename with link
    /// rewrites, what the device did with each linking note.
    Applied {
        #[serde(default)]
        sha256: Option<String>,
        #[serde(default)]
        rewrites: Vec<RewriteResult>,
    },
    /// The local file did not have the base hash; nothing was applied.
    BaseMismatch {
        #[serde(default)]
        local_sha256: Option<String>,
    },
    /// The device could not apply the operation, with a typed error code.
    Failed { code: String },
}

/// What the device did with one linking note of a rename.
#[derive(Debug, serde::Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum RewriteResult {
    /// The note at `path` has the SHA-256 `sha256` now.
    Applied { path: String, sha256: String },
    /// The note at `path` did not have the base hash.
    BaseMismatch {
        path: String,
        #[serde(default)]
        local_sha256: Option<String>,
    },
}

/// `GET /v1/notes/connections/{id}/device-operations?after=<cursor>`: at most 256 delivered writes
/// after the cursor, in the order of delivery, and the cursor to read on. The cursor is the delivery
/// sequence of the connection, so a write that is delivered later always has a later cursor.
pub(super) async fn list_operations<B: SystemdBus>(
    State(state): State<ApiState<B>>,
    headers: HeaderMap,
    connection_id: Result<Path<Uuid>, PathRejection>,
    query: Result<Query<QueueQuery>, QueryRejection>,
) -> Result<Json<Value>, ApiFault> {
    let connection_id = registered(&state, &headers, connection_id).await?;
    let Query(query) = query.map_err(|_| ApiFault::InvalidRequest)?;
    let after = match query.after.as_deref() {
        Some(cursor) => cursor
            .parse::<i64>()
            .ok()
            .filter(|cursor| *cursor >= 0)
            .ok_or(ApiFault::InvalidRequest)?,
        None => 0,
    };
    let writes = state
        .database
        .device_queue(connection_id, after, PAGE)
        .await
        .map_err(|error| {
            tracing::error!(%error, %connection_id, "the queue of a device could not be read");
            ApiFault::Unavailable
        })?;
    let next = writes
        .last()
        .and_then(|write| write.delivery_sequence)
        .unwrap_or(after);
    let mut operations = Vec::with_capacity(writes.len());
    for write in &writes {
        let rewrites = rewrites_of(&state, write).await?;
        operations.push(operation_view(write, &rewrites));
    }
    Ok(Json(json!({
        "operations": operations,
        "next_cursor": next.to_string(),
    })))
}

/// `GET .../device-operations/{operation_id}/content`: the bytes of a delivered operation.
pub(super) async fn read_content<B: SystemdBus>(
    State(state): State<ApiState<B>>,
    headers: HeaderMap,
    ids: Result<Path<(Uuid, Uuid)>, PathRejection>,
) -> Result<Response, ApiFault> {
    let (connection_id, operation_id) = ids_of(ids)?;
    let connection_id = registered(&state, &headers, Ok(Path(connection_id))).await?;
    let write = queued_write(&state, connection_id, operation_id)
        .await?
        .filter(|write| write.state == "delivering")
        .ok_or(ApiFault::DeviceOperationNotFound)?;
    let blob = write
        .content_hash
        .as_deref()
        .and_then(|digest| state.blobs.path_of(digest))
        .ok_or(ApiFault::DeviceOperationNotFound)?;
    let opened = tokio::fs::File::open(&blob).await.map_err(|error| {
        tracing::error!(%error, "the bytes of a delivered write cannot be opened");
        ApiFault::Unavailable
    })?;
    let mut response = Body::from_stream(ReaderStream::new(opened)).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    Ok(response)
}

/// `POST .../device-operations/{operation_id}/result`: end a delivered operation with what the
/// device did. An `applied` result names bytes that the device uploaded to this connection first;
/// otherwise the answer is `409` `notes.channel.bytes_missing` with the hash, and the device sends
/// the result again after the upload. A result for an operation that already ended answers its end
/// again.
pub(super) async fn post_result<B: SystemdBus>(
    State(state): State<ApiState<B>>,
    headers: HeaderMap,
    ids: Result<Path<(Uuid, Uuid)>, PathRejection>,
    body: Result<Json<DeviceResult>, JsonRejection>,
) -> Result<Json<Value>, ApiFault> {
    let (connection_id, operation_id) = ids_of(ids)?;
    let connection_id = registered(&state, &headers, Ok(Path(connection_id))).await?;
    // The rejection is not logged: its text can quote a value of the body.
    let Json(result) = body.map_err(|_| ApiFault::InvalidRequest)?;
    let write = queued_write(&state, connection_id, operation_id)
        .await?
        .ok_or(ApiFault::DeviceOperationNotFound)?;
    let writes = Writes::new(state.recorder());
    let unavailable = |error: crate::writes::WriteError| {
        tracing::error!(%error, %connection_id, "the result of a device could not be recorded");
        ApiFault::Unavailable
    };
    let rewrites = rewrites_of(&state, &write).await?;
    if write.state == "delivering" {
        match result {
            DeviceResult::Applied {
                sha256,
                rewrites: results,
            } => {
                let ends = rewrite_ends(&rewrites, results);
                for digest in ends.iter().filter_map(|(_, end)| match end {
                    RewriteEnd::Applied(digest) => Some(digest),
                    RewriteEnd::Refused(_) => None,
                }) {
                    if !writes
                        .may_name(connection_id, digest)
                        .await
                        .map_err(unavailable)?
                    {
                        return Err(ApiFault::BytesMissing {
                            missing: vec![digest.clone()],
                        });
                    }
                }
                let revision = if write.kind == "delete" {
                    None
                } else {
                    let digest = sha256.filter(|digest| is_sha256(digest));
                    let digest = digest.ok_or(ApiFault::InvalidRequest)?;
                    if !writes
                        .may_name(connection_id, &digest)
                        .await
                        .map_err(unavailable)?
                    {
                        return Err(ApiFault::BytesMissing {
                            missing: vec![digest],
                        });
                    }
                    Some(digest)
                };
                writes
                    .acknowledge(connection_id, write.write_id, revision.as_deref())
                    .await
                    .map_err(unavailable)?;
                for (member, end) in ends {
                    match end {
                        RewriteEnd::Applied(digest) => {
                            writes
                                .acknowledge(connection_id, member, Some(&digest))
                                .await
                        }
                        RewriteEnd::Refused(refusal) => {
                            writes.refuse(connection_id, member, refusal).await
                        }
                    }
                    .map_err(unavailable)?;
                }
            }
            DeviceResult::BaseMismatch { local_sha256 } => {
                let local_hash = local_sha256.filter(|digest| is_sha256(digest));
                let refusal = Refusal::BaseMismatch { local_hash };
                refuse_all(&writes, connection_id, &write, &rewrites, &refusal)
                    .await
                    .map_err(unavailable)?;
            }
            DeviceResult::Failed { code } => {
                if ratatoskr_error_contracts::ErrorCode::parse(&code).is_err() {
                    return Err(ApiFault::InvalidRequest);
                }
                let refusal = Refusal::Failed { code };
                refuse_all(&writes, connection_id, &write, &rewrites, &refusal)
                    .await
                    .map_err(unavailable)?;
            }
        }
    }
    let state_word = match writes.outcome(operation_id).await.map_err(unavailable)? {
        Some(Outcome::Succeeded { .. }) => "succeeded",
        Some(Outcome::Failed { .. }) => "failed",
        _ => "delivering",
    };
    Ok(Json(
        json!({ "operation_id": operation_id, "state": state_word }),
    ))
}

/// The connection of the path when the device of the request is the one that the connection
/// registered, of the kind of its channel; `404` otherwise.
async fn registered<B: SystemdBus>(
    state: &ApiState<B>,
    headers: &HeaderMap,
    connection_id: Result<Path<Uuid>, PathRejection>,
) -> Result<Uuid, ApiFault> {
    let owner_id = owner(headers)?;
    let device = device_claim(headers)?;
    let Path(connection_id) = connection_id.map_err(|_| ApiFault::NotFound)?;
    let connection = state
        .database
        .connection_of_owner(owner_id, connection_id)
        .await
        .map_err(|error| {
            tracing::error!(%error, %connection_id, "a connection could not be read");
            ApiFault::Unavailable
        })?
        .ok_or(ApiFault::NotFound)?;
    let registered = connection.device_id == Some(device.device_id)
        && device_kind_of(&connection.channel) == Some(device.kind);
    if !registered {
        return Err(ApiFault::NotFound);
    }
    Ok(connection_id)
}

/// The write of the operation `operation_id` when it belongs to `connection_id`.
async fn queued_write<B: SystemdBus>(
    state: &ApiState<B>,
    connection_id: Uuid,
    operation_id: Uuid,
) -> Result<Option<WriteRow>, ApiFault> {
    let write = state
        .database
        .write_of_operation(operation_id)
        .await
        .map_err(|error| {
            tracing::error!(%error, %connection_id, "a write could not be read");
            ApiFault::Unavailable
        })?;
    // The operation id that the queue names is the first operation of its write.
    Ok(write
        .filter(|write| write.operation_id == operation_id && write.connection_id == connection_id))
}

/// The two ids of an operation route.
fn ids_of(ids: Result<Path<(Uuid, Uuid)>, PathRejection>) -> Result<(Uuid, Uuid), ApiFault> {
    ids.map(|Path(ids)| ids)
        .map_err(|_| ApiFault::DeviceOperationNotFound)
}

/// Whether `digest` is a lowercase hex SHA-256.
fn is_sha256(digest: &str) -> bool {
    digest.len() == 64
        && digest
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

/// How the device ended one linking note of a rename.
#[derive(Debug)]
enum RewriteEnd {
    /// The note has these bytes now.
    Applied(String),
    /// The device did not apply the rewrite.
    Refused(Refusal),
}

/// The link rewrites of the rename `write`, which the device applies with it: the other writes of
/// its group. Empty for any other write.
async fn rewrites_of<B: SystemdBus>(
    state: &ApiState<B>,
    write: &WriteRow,
) -> Result<Vec<WriteRow>, ApiFault> {
    let Some(group_id) = write
        .group_id
        .filter(|group_id| *group_id == write.write_id)
    else {
        return Ok(Vec::new());
    };
    let group = state
        .database
        .group_writes(write.connection_id, group_id)
        .await
        .map_err(|error| {
            tracing::error!(%error, "the rewrites of a rename could not be read");
            ApiFault::Unavailable
        })?;
    Ok(group
        .into_iter()
        .filter(|member| member.write_id != group_id)
        .collect())
}

/// How the device ended each rewrite of `rewrites` that is still delivering, by its write: from
/// `results`, and as a base mismatch when the result does not name its note.
fn rewrite_ends(rewrites: &[WriteRow], results: Vec<RewriteResult>) -> Vec<(Uuid, RewriteEnd)> {
    let mut by_path: std::collections::HashMap<String, RewriteEnd> = results
        .into_iter()
        .map(|result| match result {
            RewriteResult::Applied { path, sha256 } => (path, RewriteEnd::Applied(sha256)),
            RewriteResult::BaseMismatch { path, local_sha256 } => {
                let local_hash = local_sha256.filter(|digest| is_sha256(digest));
                (
                    path,
                    RewriteEnd::Refused(Refusal::BaseMismatch { local_hash }),
                )
            }
        })
        .collect();
    rewrites
        .iter()
        .filter(|member| member.state == "delivering")
        .map(|member| {
            let end = by_path
                .remove(&member.path)
                .filter(|end| !matches!(end, RewriteEnd::Applied(digest) if !is_sha256(digest)))
                .unwrap_or(RewriteEnd::Refused(Refusal::BaseMismatch {
                    local_hash: None,
                }));
            (member.write_id, end)
        })
        .collect()
}

/// Refuse `write` and each of its `rewrites` that is still delivering with `refusal`: the device
/// applied nothing of the group.
async fn refuse_all(
    writes: &Writes,
    connection_id: Uuid,
    write: &WriteRow,
    rewrites: &[WriteRow],
    refusal: &Refusal,
) -> Result<(), crate::writes::WriteError> {
    writes
        .refuse(connection_id, write.write_id, refusal.clone())
        .await?;
    for member in rewrites
        .iter()
        .filter(|member| member.state == "delivering")
    {
        writes
            .refuse(connection_id, member.write_id, refusal.clone())
            .await?;
    }
    Ok(())
}

/// One operation of the queue, as the device reads it. A resolution is an `update` of the file of
/// its conflict, or a `create` when that file was deleted: the device does not need to know the
/// conflict. A rename lists the link rewrites of its group.
fn operation_view(write: &WriteRow, rewrites: &[WriteRow]) -> Value {
    let kind = match (write.kind.as_str(), &write.base_hash) {
        ("resolve_conflict", Some(_)) => "update",
        ("resolve_conflict", None) => "create",
        (kind, _) => kind,
    };
    let content_type = if write.kind == "set_property" {
        "application/json"
    } else {
        media_type(&write.path)
    };
    let content = match (&write.content_hash, write.content_length) {
        (Some(digest), Some(length)) => json!({
            "owner_service": "ratatoskr-notes",
            "digest": { "algorithm": "sha256", "hex": digest },
            "media_type": content_type,
            "length_bytes": length,
        }),
        _ => Value::Null,
    };
    json!({
        "operation_id": write.operation_id,
        "cursor": write.delivery_sequence.unwrap_or_default().to_string(),
        "kind": kind,
        "path": write.path,
        "target_path": write.target_path,
        "base_hash": write.base_hash,
        "content": content,
        "property_key": write.property_key,
        "property_change": write.property_change,
        "expected_sha256": crate::writes::expected_hash(write),
        "rewrites": rewrites
            .iter()
            .map(|member| json!({
                "path": member.path,
                "base_hash": member.base_hash,
                "expected_sha256": crate::writes::expected_hash(member),
            }))
            .collect::<Vec<_>>(),
    })
}
