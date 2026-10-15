//! `PUT /v1/notes/connections/{id}/write-mode`: the owner turns the write mode of a connection on
//! or off (workspace design D8, spec `notes-write-command`).
//!
//! Edge mints the operation `notes.connection.write_mode`, sends its id in
//! `x-ratatoskr-operation-id`, and answers the client with `202` on any success status of this
//! route. Notes reports the operation. A refusal is an error envelope, which Edge relays and with
//! which it fails the operation, so notes writes no report for it: a connection of another owner,
//! and one that is not `connecting` or `active`, answer `404` `notes.connection.not_found`.

use axum::Json;
use axum::extract::rejection::{JsonRejection, PathRejection};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use uuid::Uuid;

use super::connect::operation_id;
use super::{ApiFault, ApiState, owner};
use crate::operation::OPERATION_LANE;
use crate::systemd::SystemdBus;
use crate::writes::{ModeChange, WRITE_MODE_OPERATION_KIND, Writes};

/// The body of the route.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WriteModeRequest {
    write_mode: Mode,
}

/// The wire word of a write mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum Mode {
    On,
    Off,
}

/// The `202` answer.
#[derive(Debug, serde::Serialize)]
pub(super) struct WriteModeAccepted {
    connection_id: Uuid,
    write_mode: Mode,
    operation: OperationView,
}

/// The operation of the change.
#[derive(Debug, serde::Serialize)]
struct OperationView {
    operation_id: Uuid,
    kind: &'static str,
    lane: &'static str,
}

/// `PUT /v1/notes/connections/{id}/write-mode`: set the write mode of a connection of the owner,
/// report the operation `succeeded`, and answer `202`.
pub(super) async fn put_write_mode<B: SystemdBus>(
    State(state): State<ApiState<B>>,
    headers: HeaderMap,
    connection_id: Result<Path<Uuid>, PathRejection>,
    body: Result<Json<WriteModeRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<WriteModeAccepted>), ApiFault> {
    let owner_id = owner(&headers)?;
    let operation_id = operation_id(&headers)?;
    let Path(connection_id) = connection_id.map_err(|_| ApiFault::NotFound)?;
    let Json(request) = body.map_err(|_| ApiFault::InvalidRequest)?;
    let on = request.write_mode == Mode::On;
    let changed = Writes::new(state.recorder())
        .set_write_mode(owner_id, connection_id, operation_id, on, true)
        .await
        .map_err(|error| {
            tracing::error!(%error, %connection_id, "the write mode could not be set");
            ApiFault::Unavailable
        })?;
    match changed {
        ModeChange::NotFound => Err(ApiFault::NotFound),
        ModeChange::Changed { .. } => {
            tracing::info!(%connection_id, on, "the write mode of a connection was set");
            Ok((
                StatusCode::ACCEPTED,
                Json(WriteModeAccepted {
                    connection_id,
                    write_mode: request.write_mode,
                    operation: OperationView {
                        operation_id,
                        kind: WRITE_MODE_OPERATION_KIND,
                        lane: OPERATION_LANE,
                    },
                }),
            ))
        }
    }
}
