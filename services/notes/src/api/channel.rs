//! The routes of the device channels (workspace design D5 and D6, spec `notes-device-channels`):
//! `POST /v1/notes/connections/{id}/changes` takes a change batch of a device, and
//! `POST /v1/notes/connections/{id}/manifest` reconciles its vault after a gap.

use axum::Json;
use axum::extract::rejection::{JsonRejection, PathRejection};
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use uuid::Uuid;

use super::{ApiFault, ApiState, DeviceClaim, device_claim, owner};
use crate::channel::{self, BatchOutcome, DeviceChange, Sender};
use crate::manifest::{self, ManifestEntry, ManifestOutcome};
use crate::systemd::SystemdBus;

/// The largest body of a change batch. 256 changes with two paths of 1024 bytes each fit, and so
/// does a batch over the limit, which then gets its own code.
pub(super) const MAX_BATCH_BYTES: usize = 2 * 1024 * 1024;

/// The largest body of a manifest: 100 000 entries of an average path fit.
pub(super) const MAX_MANIFEST_BYTES: usize = 32 * 1024 * 1024;

/// The body of `POST /v1/notes/connections/{id}/changes`.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ChangeBatch {
    changes: Vec<DeviceChange>,
}

/// The answer to a batch: the highest device sequence that is committed.
#[derive(Debug, serde::Serialize)]
pub(super) struct BatchCommitted {
    committed_sequence: u64,
}

/// `POST /v1/notes/connections/{id}/changes`: commit the changes of a batch after the committed
/// device sequence, in one transaction. Only the device that created the connection, of the kind
/// of its channel, sends them; a connection of another owner answers `404`.
pub(super) async fn post_changes<B: SystemdBus>(
    State(state): State<ApiState<B>>,
    headers: HeaderMap,
    connection_id: Result<Path<Uuid>, PathRejection>,
    body: Result<Json<ChangeBatch>, JsonRejection>,
) -> Result<Json<BatchCommitted>, ApiFault> {
    let owner_id = owner(&headers)?;
    let device = device_claim(&headers)?;
    let sender = sender(owner_id, device);
    let Path(connection_id) = connection_id.map_err(|_| ApiFault::NotFound)?;
    // The rejection is not logged: its text can quote a path of the body.
    let Json(batch) = body.map_err(|_| ApiFault::InvalidRequest)?;
    let outcome = channel::apply_batch(&state.recorder(), &sender, connection_id, &batch.changes)
        .await
        .map_err(|error| {
            tracing::error!(%error, %connection_id, "a change batch could not be committed");
            ApiFault::Unavailable
        })?;
    match outcome {
        BatchOutcome::Committed(committed_sequence) => {
            Ok(Json(BatchCommitted { committed_sequence }))
        }
        refused => Err(fault(refused)),
    }
}

/// The sender of a request on a device route.
const fn sender(owner_id: Uuid, device: DeviceClaim<'_>) -> Sender<'_> {
    Sender {
        owner_id,
        device_id: device.device_id,
        kind: device.kind,
    }
}

/// The fault of a refused batch or manifest.
fn fault(refused: BatchOutcome) -> ApiFault {
    match refused {
        BatchOutcome::SequenceGap(committed) => ApiFault::SequenceGap { committed },
        BatchOutcome::BytesMissing(missing) => ApiFault::BytesMissing { missing },
        BatchOutcome::TooLarge => ApiFault::BatchTooLarge,
        BatchOutcome::NotFound => ApiFault::NotFound,
        BatchOutcome::ChannelMismatch => ApiFault::ChannelMismatch,
        BatchOutcome::Invalid | BatchOutcome::Committed(_) => ApiFault::InvalidRequest,
    }
}

/// The body of `POST /v1/notes/connections/{id}/manifest`.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Manifest {
    entries: Vec<ManifestEntry>,
}

/// The answer to a manifest: the hashes whose bytes the service needs first, empty when the
/// manifest is applied, and the committed device sequence.
#[derive(Debug, serde::Serialize)]
pub(super) struct ManifestAnswer {
    needed_sha256: Vec<String>,
    committed_sequence: u64,
}

/// `POST /v1/notes/connections/{id}/manifest`: name the bytes that the service needs, or make the
/// files of the connection equal to the manifest in one transaction.
pub(super) async fn post_manifest<B: SystemdBus>(
    State(state): State<ApiState<B>>,
    headers: HeaderMap,
    connection_id: Result<Path<Uuid>, PathRejection>,
    body: Result<Json<Manifest>, JsonRejection>,
) -> Result<Json<ManifestAnswer>, ApiFault> {
    let owner_id = owner(&headers)?;
    let device = device_claim(&headers)?;
    let sender = sender(owner_id, device);
    let Path(connection_id) = connection_id.map_err(|_| ApiFault::NotFound)?;
    // The rejection is not logged: its text can quote a path of the body.
    let Json(manifest) = body.map_err(|_| ApiFault::InvalidRequest)?;
    let outcome =
        manifest::apply_manifest(&state.recorder(), &sender, connection_id, &manifest.entries)
            .await
            .map_err(|error| {
                tracing::error!(%error, %connection_id, "a manifest could not be applied");
                ApiFault::Unavailable
            })?;
    match outcome {
        ManifestOutcome::Needed { needed, committed } => Ok(Json(ManifestAnswer {
            needed_sha256: needed,
            committed_sequence: committed,
        })),
        ManifestOutcome::Applied { committed } => {
            tracing::info!(%connection_id, "a manifest was applied");
            state.complete_device_connect(connection_id).await;
            Ok(Json(ManifestAnswer {
                needed_sha256: Vec::new(),
                committed_sequence: committed,
            }))
        }
        ManifestOutcome::Refused(refused) => Err(fault(refused)),
    }
}
