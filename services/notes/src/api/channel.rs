//! The routes of the device channels (workspace design D5 and D6, spec `notes-device-channels`):
//! `POST /v1/notes/connections/{id}/changes` takes a change batch of a device.

use axum::Json;
use axum::extract::rejection::{JsonRejection, PathRejection};
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use uuid::Uuid;

use super::{ApiFault, ApiState, owner};
use crate::channel::{self, BatchOutcome, DeviceChange};
use crate::systemd::SystemdBus;

/// The largest body of a change batch. 256 changes with two paths of 1024 bytes each fit, and so
/// does a batch over the limit, which then gets its own code.
pub(super) const MAX_BATCH_BYTES: usize = 2 * 1024 * 1024;

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
/// device sequence, in one transaction.
pub(super) async fn post_changes<B: SystemdBus>(
    State(state): State<ApiState<B>>,
    headers: HeaderMap,
    connection_id: Result<Path<Uuid>, PathRejection>,
    body: Result<Json<ChangeBatch>, JsonRejection>,
) -> Result<Json<BatchCommitted>, ApiFault> {
    owner(&headers)?;
    let Path(connection_id) = connection_id.map_err(|_| ApiFault::NotFound)?;
    // The rejection is not logged: its text can quote a path of the body.
    let Json(batch) = body.map_err(|_| ApiFault::InvalidRequest)?;
    let outcome = channel::apply_batch(&state.recorder(), connection_id, &batch.changes)
        .await
        .map_err(|error| {
            tracing::error!(%error, %connection_id, "a change batch could not be committed");
            ApiFault::Unavailable
        })?;
    match outcome {
        BatchOutcome::Committed(committed_sequence) => {
            Ok(Json(BatchCommitted { committed_sequence }))
        }
        BatchOutcome::SequenceGap(committed) => Err(ApiFault::SequenceGap { committed }),
        BatchOutcome::BytesMissing(missing) => Err(ApiFault::BytesMissing { missing }),
        BatchOutcome::TooLarge => Err(ApiFault::BatchTooLarge),
        BatchOutcome::Invalid => Err(ApiFault::InvalidRequest),
        BatchOutcome::NotFound => Err(ApiFault::NotFound),
    }
}
