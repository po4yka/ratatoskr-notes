//! The connect operation (workspace design D10): Edge mints the operation of kind
//! `notes.connection.connect` and sends its id in `x-ratatoskr-operation-id`; notes is its only
//! reporter (lane `ratatoskr-notes`). Notes writes each report to its outbox as
//! `platform.operation.reported.v1`, owned by `user:<owner>` and correlated to
//! `operation:<operation id>`, and the relay publishes it on `evt.platform.operation.reported.v1`.
//!
//! The phases of a `sync` connect are `login`, `setup`, `first_sync` and `indexing`; a `plugin` or
//! `cli` connect starts at `first_sync`. The operation succeeds in the transaction that makes the
//! connection `active` and writes `notes.vault.connected.v1`, after the first full scan of the
//! vault is indexed. A failure ends it as `failed` with the stable code of the failure. A report
//! carries no secret and no path: the codes and messages are fixed texts.

use ratatoskr_error_contracts::{ErrorCode, ErrorEnvelope, WarningEnvelope};
use ratatoskr_event_envelope::{EventEnvelope, EventPayload as _};
use ratatoskr_identifiers::{Extensions, OperationId, SafeMessage};
use ratatoskr_notes_persistence::{Database, OutboxEvent};
use ratatoskr_operation_contracts::{OperationReported, OperationStage, OperationStatus};
use uuid::Uuid;

use crate::events::{self, EnvelopeBuildError};

/// The kind of the connect operation.
pub const OPERATION_KIND: &str = "notes.connection.connect";

/// The one reporter lane of the connect operation.
pub const OPERATION_LANE: &str = "ratatoskr-notes";

/// The event type of a report.
const REPORTED: &str = "platform.operation.reported.v1";

/// The aggregate kind of a report.
const OPERATION_AGGREGATE: &str = "operation";

/// The warning of a connect that completed while the service cannot read the journal of the sync
/// units: an expired Sync login of this connection is not found then.
pub const JOURNAL_UNREADABLE_WARNING: (&str, &str) = (
    "notes.sync.journal_unreadable",
    "The vault is connected, but the service cannot see when the Obsidian login expires.",
);

/// A phase of the connect operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// The Obsidian login runs.
    Login,
    /// The service configures and starts the sync client.
    Setup,
    /// The first full scan of the vault runs.
    FirstSync,
    /// The files of the first full scan are indexed.
    Indexing,
}

impl Phase {
    const fn stage(self) -> &'static str {
        match self {
            Self::Login => "login",
            Self::Setup => "setup",
            Self::FirstSync => "first_sync",
            Self::Indexing => "indexing",
        }
    }
}

/// One report of the connect operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Report {
    /// The operation runs in a phase.
    Running(Phase),
    /// The connection is active.
    Succeeded,
    /// The connection is active, with a warning `(code, message)`.
    SucceededWithWarning((&'static str, &'static str)),
    /// The operation ended with the failure `(code, message, retryable)`.
    Failed(&'static str, &'static str, bool),
}

/// The envelope of `report` for the operation `operation_id` of `owner_id`.
///
/// # Errors
///
/// [`EnvelopeBuildError`] when a value does not satisfy the contract.
pub fn report_envelope(
    event_id: Uuid,
    owner_id: Uuid,
    operation_id: Uuid,
    report: Report,
) -> Result<EventEnvelope, EnvelopeBuildError> {
    let mut payload = OperationReported {
        operation_id: OperationId(operation_id),
        status: OperationStatus::Running,
        stage: None,
        progress_percent: None,
        results: Vec::new(),
        error: None,
        warnings: Vec::new(),
        extensions: Extensions::new(),
    };
    match report {
        Report::Running(phase) => {
            payload.stage =
                Some(OperationStage::parse(phase.stage()).map_err(|_| EnvelopeBuildError::STAGE)?);
        }
        Report::Succeeded => payload.status = OperationStatus::Succeeded,
        Report::SucceededWithWarning((code, message)) => {
            payload.status = OperationStatus::PartiallySucceeded;
            payload.warnings.push(WarningEnvelope {
                code: ErrorCode::parse(code).map_err(|_| EnvelopeBuildError::STAGE)?,
                message: SafeMessage::parse(message).map_err(|_| EnvelopeBuildError::STAGE)?,
                field_path: None,
                extensions: Extensions::new(),
            });
        }
        Report::Failed(code, message, retryable) => {
            payload.status = OperationStatus::Failed;
            payload.error = Some(ErrorEnvelope::new(
                ErrorCode::parse(code).map_err(|_| EnvelopeBuildError::STAGE)?,
                SafeMessage::parse(message).map_err(|_| EnvelopeBuildError::STAGE)?,
                retryable,
            ));
        }
    }
    payload.validate().map_err(|_| EnvelopeBuildError::STAGE)?;
    // A report correlates to its operation, as the other reporters of the fleet do.
    let operation = operation_aggregate(operation_id);
    events::correlated_envelope(
        event_id,
        OperationReported::event_type(),
        &operation,
        &operation,
        owner_id,
        &payload,
    )
}

/// Write `report` of the operation `operation_id` of `owner_id` to the outbox. A failure is logged:
/// Platform's stale-operation reaper ends an operation whose report is lost.
pub async fn report(database: &Database, owner_id: Uuid, operation_id: Uuid, report: Report) {
    let event_id = Uuid::now_v7();
    let aggregate_id = operation_aggregate(operation_id);
    let written = match outbox_value(report_envelope(event_id, owner_id, operation_id, report)) {
        Ok(envelope) => database
            .insert_event(&report_event(event_id, &aggregate_id, &envelope))
            .await
            .map_err(|error| error.to_string()),
        Err(error) => Err(error.to_string()),
    };
    if let Err(error) = written {
        tracing::error!(%error, %operation_id, "a report of the connect operation could not be written");
    }
}

/// The envelope as the JSON that the outbox stores.
pub(crate) fn outbox_value(
    envelope: Result<EventEnvelope, EnvelopeBuildError>,
) -> Result<serde_json::Value, EnvelopeBuildError> {
    serde_json::to_value(envelope?).map_err(|_| EnvelopeBuildError::SERIALIZE)
}

/// The aggregate reference of an operation, `operation:<uuid>`.
pub(crate) fn operation_aggregate(operation_id: Uuid) -> String {
    format!("{OPERATION_AGGREGATE}:{operation_id}")
}

/// The outbox row of a report.
pub(crate) fn report_event<'a>(
    event_id: Uuid,
    aggregate_id: &'a str,
    envelope: &'a serde_json::Value,
) -> OutboxEvent<'a> {
    OutboxEvent {
        event_id,
        event_type: REPORTED,
        aggregate_type: OPERATION_AGGREGATE,
        aggregate_id,
        envelope,
    }
}

/// What completes the connect operation of one connection after its first full scan.
#[derive(Debug, Clone)]
pub struct Completion {
    /// The connection.
    pub connection_id: Uuid,
    /// Its owner.
    pub owner_id: Uuid,
    /// Its connect operation.
    pub operation_id: Uuid,
    /// The wire word of its channel.
    pub channel: String,
    /// The display name of its vault.
    pub vault_display_name: String,
    /// Whether the service can read the journal of the sync units.
    pub journal_readable: bool,
}

impl Completion {
    /// Report the phase `indexing`.
    pub async fn indexing(&self, database: &Database) {
        report(
            database,
            self.owner_id,
            self.operation_id,
            Report::Running(Phase::Indexing),
        )
        .await;
    }

    /// Make the connection `active`, and write `notes.vault.connected.v1` and the last report, in
    /// one transaction. A connection that is no longer `connecting`, for example because the owner
    /// removed it, changes nothing.
    pub async fn complete(&self, database: &Database) {
        let connected_id = Uuid::now_v7();
        let report_id = Uuid::now_v7();
        let last = if self.journal_readable {
            Report::Succeeded
        } else {
            Report::SucceededWithWarning(JOURNAL_UNREADABLE_WARNING)
        };
        let connected = outbox_value(events::vault_connected(
            connected_id,
            self.owner_id,
            self.connection_id,
            &self.channel,
            &self.vault_display_name,
        ));
        let reported = outbox_value(report_envelope(
            report_id,
            self.owner_id,
            self.operation_id,
            last,
        ));
        let (Ok(connected), Ok(reported)) = (connected, reported) else {
            tracing::error!(connection_id = %self.connection_id, "the events of an active connection could not be built");
            return;
        };
        let aggregate_id = events::connection_aggregate(self.connection_id);
        let operation = operation_aggregate(self.operation_id);
        let activated = database
            .activate_connection(
                self.connection_id,
                &[
                    OutboxEvent {
                        event_id: connected_id,
                        event_type: "notes.vault.connected.v1",
                        aggregate_type: events::CONNECTION_AGGREGATE,
                        aggregate_id: &aggregate_id,
                        envelope: &connected,
                    },
                    report_event(report_id, &operation, &reported),
                ],
            )
            .await;
        match activated {
            Ok(true) => {
                tracing::info!(connection_id = %self.connection_id, "the first scan is indexed; the connection is active");
            }
            Ok(false) => {}
            Err(error) => {
                tracing::error!(%error, connection_id = %self.connection_id, "a connection could not be made active");
            }
        }
    }
}
