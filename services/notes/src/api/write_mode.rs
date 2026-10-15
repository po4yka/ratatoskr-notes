//! `PUT /v1/notes/connections/{id}/write-mode`: the owner turns the write mode of a connection on
//! or off (workspace design D8, spec `notes-write-command`).
//!
//! Edge mints the operation `notes.connection.write_mode`, sends its id in
//! `x-ratatoskr-operation-id`, and answers the client with `202` on any success status of this
//! route. Notes reports the operation. A refusal is an error envelope, which Edge relays and with
//! which it fails the operation, so notes writes no report for it: a connection of another owner,
//! and one that is not `connecting` or `active`, answer `404` `notes.connection.not_found`.
//!
//! A `plugin` or `cli` connection changes in the request. A `sync` connection must configure its
//! Headless client before the mode is true (design D8), and starting its sync unit again can take
//! longer than the budget of Edge, so the change runs in a task of the service after the `202`, one
//! at a time: on configures `bidirectional` first and then sets the mode; off sets the mode first,
//! so that no write is delivered any more, and then configures `pull-only`.

use axum::Json;
use axum::extract::rejection::{JsonRejection, PathRejection};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use uuid::Uuid;

use super::connect::operation_id;
use super::{ApiFault, ApiState, owner};
use crate::operation::OPERATION_LANE;
use crate::systemd::SystemdBus;
use crate::writes::{
    CONNECTION_NOT_FOUND, ModeChange, RECONFIGURE_FAILED, WRITE_MODE_OPERATION_KIND, Writes,
};

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
    let connection = state
        .database
        .connection_of_owner(owner_id, connection_id)
        .await
        .map_err(|error| {
            tracing::error!(%error, %connection_id, "a connection could not be read");
            ApiFault::Unavailable
        })?
        .filter(|connection| matches!(connection.state.as_str(), "connecting" | "active"))
        .ok_or(ApiFault::NotFound)?;
    let changed = if connection.channel == "sync" {
        if state.stopping.is_cancelled() {
            return Err(ApiFault::Unavailable);
        }
        let task_state = state.clone();
        let change = Change {
            owner_id,
            connection_id,
            operation_id,
            on,
        };
        state
            .connects
            .spawn(async move { change_sync(task_state, change).await });
        true
    } else {
        let changed = Writes::new(state.recorder())
            .set_write_mode(owner_id, connection_id, operation_id, on, true)
            .await
            .map_err(|error| {
                tracing::error!(%error, %connection_id, "the write mode could not be set");
                ApiFault::Unavailable
            })?;
        matches!(changed, ModeChange::Changed { .. })
    };
    if !changed {
        return Err(ApiFault::NotFound);
    }
    {
        {
            tracing::info!(%connection_id, on, "the write mode of a connection is set");
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

/// One write-mode change of a `sync` connection.
#[derive(Debug, Clone, Copy)]
struct Change {
    owner_id: Uuid,
    connection_id: Uuid,
    operation_id: Uuid,
    on: bool,
}

/// Change the write mode of a `sync` connection and its Headless client, and report the end of the
/// operation. One change runs at a time. When the client cannot be configured, the operation fails
/// with the retryable `notes.sync.reconfigure_failed`; a failed "on" leaves the mode off and the
/// client `pull-only`.
async fn change_sync<B: SystemdBus>(state: ApiState<B>, change: Change) {
    let _one_at_a_time = state.write_modes.lock().await;
    let Change {
        owner_id,
        connection_id,
        operation_id,
        on,
    } = change;
    let writes = Writes::new(state.recorder());
    let supervisor = &state.supervisor;
    let failed = if on {
        if supervisor.reconfigure(connection_id, true).await {
            match writes
                .set_write_mode(owner_id, connection_id, operation_id, true, true)
                .await
            {
                Ok(ModeChange::Changed { .. }) => return,
                Ok(ModeChange::NotFound) => CONNECTION_NOT_FOUND,
                Err(error) => {
                    tracing::error!(%error, %connection_id, "the write mode could not be set");
                    RECONFIGURE_FAILED
                }
            }
        } else {
            RECONFIGURE_FAILED
        }
    } else {
        match writes
            .set_write_mode(owner_id, connection_id, operation_id, false, false)
            .await
        {
            Ok(ModeChange::Changed { .. }) => {
                if supervisor.reconfigure(connection_id, false).await {
                    report(&writes, owner_id, operation_id, None).await;
                    return;
                }
                RECONFIGURE_FAILED
            }
            Ok(ModeChange::NotFound) => CONNECTION_NOT_FOUND,
            Err(error) => {
                tracing::error!(%error, %connection_id, "the write mode could not be set");
                RECONFIGURE_FAILED
            }
        }
    };
    if on {
        // The mode stays off, so the client goes back to pull-only.
        supervisor.reconfigure(connection_id, false).await;
    }
    tracing::warn!(%connection_id, code = failed, "a write-mode change of a sync connection failed");
    report(&writes, owner_id, operation_id, Some(failed)).await;
}

/// Write the end of the operation; a failure is logged, and Platform's reaper ends the operation.
async fn report(writes: &Writes, owner_id: Uuid, operation_id: Uuid, failed: Option<&str>) {
    if let Err(error) = writes.report_mode(owner_id, operation_id, failed).await {
        tracing::error!(%error, %operation_id, "the end of a write-mode change could not be reported");
    }
}
