//! The blob receipt of the device channels on the `transfer` route class (spec
//! `notes-device-channels`): the canonical HTTP binding of the shared transfer contract under
//! `/v1/notes/connections/{id}/blobs`.
//!
//! - `POST .../blobs` opens a session, `201` with `UploadSessionOpened`;
//! - `PUT .../blobs/{token}/chunks/{index}` takes the raw bytes of one chunk;
//! - `GET .../blobs/{token}/status` answers the resume view;
//! - `POST .../blobs/{token}/finalize` answers `stored` or `digest_mismatch` at `200`.
//!
//! The device of the connection uploads the bytes of its changes. The owner's session uploads the
//! content of a write (OBS-024): an upload registers the bytes for the connection, and only a
//! write that the channel acknowledges makes them a revision. A site owner session uploads the
//! content of an edit from a site (SITE-027): its claim names the site in `x-ratatoskr-origin`, and
//! the upload only registers the bytes, as for the owner's session.

use axum::Json;
use axum::body::Bytes;
use axum::extract::rejection::{JsonRejection, PathRejection};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use ratatoskr_blob_transfer_contracts::{
    UploadChunkReceipt, UploadCompletionOutcome, UploadFinalizeRequest, UploadSessionOpened,
    UploadSessionRequest, UploadStatusResponse,
};
use ratatoskr_notes_contracts::WriteOrigin;
use uuid::Uuid;

use super::{ApiFault, ApiState, SESSION_KIND_HEADER, device_claim, is_session, owner};
use crate::channel::device_kind_of;
use crate::systemd::SystemdBus;
use crate::uploads::UploadRefusal;

/// The largest chunk body: the largest chunk size of the contract.
pub(super) const MAX_CHUNK_BYTES: usize =
    ratatoskr_blob_transfer_contracts::CHUNK_SIZE_MAX_BYTES as usize;

/// The session kind of a site owner session (SITE-027).
const SITE_OWNER_KIND: &str = "site_owner";

/// The claim that names the site of a site owner session, `site:<site id>`.
const ORIGIN_HEADER: &str = "x-ratatoskr-origin";

/// A site owner session names its site: Edge mints the origin `site:<site id>` for each of its
/// requests, as `WriteOrigin` spells a site. A site owner claim without it is malformed and
/// answers `401`. Another session kind needs no origin.
fn check_site_origin(headers: &HeaderMap) -> Result<(), ApiFault> {
    let claim = |name: &str| headers.get(name).and_then(|value| value.to_str().ok());
    if claim(SESSION_KIND_HEADER) != Some(SITE_OWNER_KIND) {
        return Ok(());
    }
    claim(ORIGIN_HEADER)
        .and_then(|origin| WriteOrigin::parse(origin).ok())
        .filter(|origin| origin.as_str().starts_with("site:"))
        .map(|_| ())
        .ok_or(ApiFault::Unauthenticated)
}

/// The connection exists for this owner, and the request comes from a session of the owner or from
/// the device of the connection, of the kind of its channel. A site owner session must name its
/// site. A connection of another owner answers `404`; another device or kind answers
/// `notes.channel_mismatch`.
async fn authorize<B: SystemdBus>(
    state: &ApiState<B>,
    headers: &HeaderMap,
    connection_id: Uuid,
) -> Result<(), ApiFault> {
    let owner_id = owner(headers)?;
    if is_session(headers) {
        check_site_origin(headers)?;
        let connection = state
            .database
            .connection_of_owner(owner_id, connection_id)
            .await
            .map_err(|error| {
                tracing::error!(%error, "a connection could not be read");
                ApiFault::Unavailable
            })?;
        return connection.map(|_| ()).ok_or(ApiFault::NotFound);
    }
    let device = device_claim(headers)?;
    let connection = state
        .database
        .connection_of_owner(owner_id, connection_id)
        .await
        .map_err(|error| {
            tracing::error!(%error, "a connection could not be read");
            ApiFault::Unavailable
        })?;
    let Some(connection) = connection else {
        return Err(state
            .missing_connection(owner_id, device.device_id, connection_id)
            .await);
    };
    if device_kind_of(&connection.channel) != Some(device.kind)
        || connection.device_id != Some(device.device_id)
    {
        return Err(ApiFault::ChannelMismatch);
    }
    Ok(())
}

/// The fault of a refused step of an upload.
fn fault(refusal: UploadRefusal) -> ApiFault {
    match refusal {
        UploadRefusal::Transfer(code) => ApiFault::Transfer(code),
        UploadRefusal::Invalid => ApiFault::InvalidRequest,
        UploadRefusal::Busy => {
            tracing::warn!("an upload is refused: the limit of open upload sessions is reached");
            ApiFault::Unavailable
        }
        UploadRefusal::Store(error) => {
            tracing::error!(%error, "the blob store failed during an upload");
            ApiFault::Unavailable
        }
    }
}

/// `POST /v1/notes/connections/{id}/blobs`: open an upload session.
pub(super) async fn open_upload<B: SystemdBus>(
    State(state): State<ApiState<B>>,
    headers: HeaderMap,
    connection_id: Result<Path<Uuid>, PathRejection>,
    body: Result<Json<UploadSessionRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<UploadSessionOpened>), ApiFault> {
    let Path(connection_id) = connection_id.map_err(|_| ApiFault::NotFound)?;
    authorize(&state, &headers, connection_id).await?;
    let Json(request) = body.map_err(|_| ApiFault::InvalidRequest)?;
    let opened = state
        .uploads
        .open(&state.blobs, connection_id, &request, state.max_file_bytes)
        .await
        .map_err(fault)?;
    Ok((StatusCode::CREATED, Json(opened)))
}

/// `PUT /v1/notes/connections/{id}/blobs/{token}/chunks/{index}`: record one chunk.
pub(super) async fn put_chunk<B: SystemdBus>(
    State(state): State<ApiState<B>>,
    headers: HeaderMap,
    path: Result<Path<(Uuid, String, u32)>, PathRejection>,
    bytes: Bytes,
) -> Result<Json<UploadChunkReceipt>, ApiFault> {
    let Path((connection_id, token, index)) = path.map_err(|_| ApiFault::NotFound)?;
    authorize(&state, &headers, connection_id).await?;
    let receipt = state
        .uploads
        .chunk(connection_id, &token, index, bytes.to_vec())
        .await
        .map_err(fault)?;
    Ok(Json(receipt))
}

/// `GET /v1/notes/connections/{id}/blobs/{token}/status`: the resume view.
pub(super) async fn upload_status<B: SystemdBus>(
    State(state): State<ApiState<B>>,
    headers: HeaderMap,
    path: Result<Path<(Uuid, String)>, PathRejection>,
) -> Result<Json<UploadStatusResponse>, ApiFault> {
    let Path((connection_id, token)) = path.map_err(|_| ApiFault::NotFound)?;
    authorize(&state, &headers, connection_id).await?;
    let status = state
        .uploads
        .status(connection_id, &token)
        .await
        .map_err(fault)?;
    Ok(Json(status))
}

/// `POST /v1/notes/connections/{id}/blobs/{token}/finalize`: verify the SHA-256 of the bytes and
/// store them, or answer `digest_mismatch` and store nothing.
pub(super) async fn finalize_upload<B: SystemdBus>(
    State(state): State<ApiState<B>>,
    headers: HeaderMap,
    path: Result<Path<(Uuid, String)>, PathRejection>,
    body: Result<Json<UploadFinalizeRequest>, JsonRejection>,
) -> Result<Json<UploadCompletionOutcome>, ApiFault> {
    let Path((connection_id, token)) = path.map_err(|_| ApiFault::NotFound)?;
    authorize(&state, &headers, connection_id).await?;
    let Json(request) = body.map_err(|_| ApiFault::InvalidRequest)?;
    if request.resumption_token.as_str() != token {
        return Err(ApiFault::InvalidRequest);
    }
    let outcome = state
        .uploads
        .finalize(&state.blobs, connection_id, &token)
        .await
        .map_err(fault)?;
    match &outcome {
        UploadCompletionOutcome::Stored { blob_ref, .. } => {
            // From now on a batch or a manifest of this connection may name these bytes.
            state
                .database
                .record_connection_blob(
                    connection_id,
                    blob_ref.digest.hex.as_str(),
                    blob_ref.length_bytes,
                )
                .await
                .map_err(|error| {
                    tracing::error!(%error, %connection_id, "a finished upload could not be recorded");
                    ApiFault::Unavailable
                })?;
        }
        UploadCompletionOutcome::DigestMismatch { .. } => {
            tracing::info!(%connection_id, "an upload ended with bytes that do not match their declared SHA-256");
        }
        _ => {}
    }
    Ok(Json(outcome))
}
