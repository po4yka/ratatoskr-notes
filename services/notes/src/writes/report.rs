//! The reports of the operation `notes.file.write` (workspace spec `notes-write-command`).
//!
//! Edge mints the operation; notes is its only reporter (lane `ratatoskr-notes`). Each report goes
//! to the outbox as `platform.operation.reported.v1`, owned by `user:<owner>` and correlated to
//! `operation:<operation id>`:
//!
//! - `running` with the stage `queued` when the write waits in the queue of its file;
//! - `running` with the stage `awaiting_device` (`plugin`, `cli`) or `awaiting_obsidian` (`sync`)
//!   when the channel of the connection delivers it: the stage names what the write waits for;
//! - `succeeded` after the acknowledgement, with one result `notes.file` that names the file and
//!   the `BlobRef` of its new revision (no `BlobRef` after a delete), and the warning
//!   `notes.write.transformed` when the vault acknowledged other bytes than the write named;
//! - `failed` with a typed error otherwise. A conflict is the non-retryable
//!   `notes.write.conflict` with the extension `conflict_id`.
//!
//! A report carries no path and no content: the codes and the messages are fixed texts.

use ratatoskr_error_contracts::{ErrorCode, ErrorEnvelope, WarningEnvelope};
use ratatoskr_event_envelope::EventPayload as _;
use ratatoskr_identifiers::{
    BlobOwner, BlobRef, ContentDigest, DigestAlgorithm, DigestHex, EntityRef, Extensions,
    MediaType, OperationId, SafeMessage,
};
use ratatoskr_notes_persistence::OutboxEvent;
use ratatoskr_operation_contracts::{
    OperationReported, OperationResultKind, OperationResultRef, OperationStage, OperationStatus,
};
use uuid::Uuid;

use crate::events::{self, EnvelopeBuildError};
use crate::operation;

/// The kind of the result of a write that succeeded.
const RESULT_KIND: &str = "notes.file";

/// The warning of a write whose acknowledged bytes differ from the bytes it named.
const TRANSFORMED: (&str, &str) = (
    "notes.write.transformed",
    "The vault changed the bytes of the write when it applied it. Its bytes are current.",
);

/// The file that a write that succeeded leaves.
#[derive(Debug, Clone, Copy)]
pub(super) struct FileResult<'a> {
    /// The file.
    pub(super) file_id: Uuid,
    /// The SHA-256 of its bytes now, its media type and their length; `None` after a delete.
    pub(super) revision: Option<(&'a str, &'a str, u64)>,
    /// The vault holds other bytes than the write named, for example because a plugin formatted
    /// the file on save: the report carries the warning `notes.write.transformed`.
    pub(super) transformed: bool,
}

/// One report of a write.
#[derive(Debug, Clone, Copy)]
pub(super) enum Step<'a> {
    /// The write waits in the queue of its file.
    Queued,
    /// The channel delivers the write: the stage names what it waits for, `awaiting_device` on the
    /// `plugin` and `cli` channels and `awaiting_obsidian` on `sync`.
    Awaiting(&'a str),
    /// The channel acknowledged the write.
    Succeeded(Option<FileResult<'a>>),
    /// The write failed with the code, and with the conflict that it raised.
    Failed(&'a str, Option<Uuid>),
}

/// The fixed message and the retry flag of a failure code. A code that a channel reports and that
/// notes does not name gets a general message.
pub(super) fn failure(code: &str) -> (&'static str, bool) {
    match code {
        "notes.write.conflict" => (
            "The file changed after the writer read it. Resolve the conflict.",
            false,
        ),
        "notes.write.disabled" => ("Write mode is off for this connection.", false),
        "notes.write.content_missing" => (
            "The content of the write was not uploaded to this connection.",
            false,
        ),
        "notes.connection.not_found" => ("The connection does not exist.", false),
        "notes.conflict.not_found" => ("The conflict does not exist or is resolved.", false),
        "notes.request.invalid" => ("The request is invalid.", false),
        "notes.sync.reconfigure_failed" => (
            "The sync client could not be configured for the write mode. Try again.",
            true,
        ),
        _ => ("The vault did not apply the write.", false),
    }
}

/// The outbox row of `step` for the operation `operation_id` of `owner_id`.
pub(super) struct ReportRow {
    event_id: Uuid,
    aggregate_id: String,
    envelope: serde_json::Value,
}

impl ReportRow {
    /// The report `step` of the operation `operation_id` of `owner_id`.
    pub(super) fn new(
        owner_id: Uuid,
        operation_id: Uuid,
        step: Step<'_>,
    ) -> Result<Self, EnvelopeBuildError> {
        let event_id = Uuid::now_v7();
        let payload = payload(operation_id, step)?;
        let aggregate_id = operation::operation_aggregate(operation_id);
        let envelope = events::correlated_envelope(
            event_id,
            OperationReported::event_type(),
            &aggregate_id,
            &aggregate_id,
            owner_id,
            &payload,
        )?;
        Ok(Self {
            event_id,
            aggregate_id,
            envelope: serde_json::to_value(envelope).map_err(|_| EnvelopeBuildError::SERIALIZE)?,
        })
    }

    /// The outbox event of the row.
    pub(super) fn event(&self) -> OutboxEvent<'_> {
        operation::report_event(self.event_id, &self.aggregate_id, &self.envelope)
    }
}

fn payload(operation_id: Uuid, step: Step<'_>) -> Result<OperationReported, EnvelopeBuildError> {
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
    match step {
        Step::Queued => payload.stage = Some(stage("queued")?),
        Step::Awaiting(wait) => payload.stage = Some(stage(wait)?),
        Step::Succeeded(file) => {
            payload.status = OperationStatus::Succeeded;
            if let Some(file) = file {
                payload.results.push(result(file)?);
                if file.transformed {
                    let (code, message) = TRANSFORMED;
                    payload.warnings.push(WarningEnvelope {
                        code: ErrorCode::parse(code).map_err(|_| EnvelopeBuildError::STAGE)?,
                        message: SafeMessage::parse(message)
                            .map_err(|_| EnvelopeBuildError::STAGE)?,
                        field_path: None,
                        extensions: Extensions::new(),
                    });
                }
            }
        }
        Step::Failed(code, conflict_id) => {
            let (message, retryable) = failure(code);
            payload.status = OperationStatus::Failed;
            let mut error = ErrorEnvelope::new(
                ErrorCode::parse(code).map_err(|_| EnvelopeBuildError::STAGE)?,
                SafeMessage::parse(message).map_err(|_| EnvelopeBuildError::STAGE)?,
                retryable,
            );
            if let Some(conflict_id) = conflict_id {
                error.extensions.insert(
                    "conflict_id",
                    serde_json::Value::String(conflict_id.to_string()),
                );
            }
            payload.error = Some(error);
        }
    }
    payload.validate().map_err(|_| EnvelopeBuildError::STAGE)?;
    Ok(payload)
}

fn stage(stage: &str) -> Result<OperationStage, EnvelopeBuildError> {
    OperationStage::parse(stage).map_err(|_| EnvelopeBuildError::STAGE)
}

fn result(file: FileResult<'_>) -> Result<OperationResultRef, EnvelopeBuildError> {
    let blob = match file.revision {
        Some((revision, media_type, length_bytes)) => Some(BlobRef {
            owner_service: BlobOwner::parse(events::PRODUCER)
                .map_err(|_| EnvelopeBuildError::STAGE)?,
            digest: ContentDigest {
                algorithm: DigestAlgorithm::Sha256,
                hex: DigestHex::parse(revision).map_err(|_| EnvelopeBuildError::STAGE)?,
            },
            media_type: MediaType::parse(media_type).map_err(|_| EnvelopeBuildError::STAGE)?,
            length_bytes,
        }),
        None => None,
    };
    Ok(OperationResultRef {
        result_kind: OperationResultKind::parse(RESULT_KIND)
            .map_err(|_| EnvelopeBuildError::STAGE)?,
        target: EntityRef::parse(&events::file_aggregate(file.file_id))
            .map_err(|_| EnvelopeBuildError::STAGE)?,
        blob,
        ai_archive_import_summary: None,
        extensions: Extensions::new(),
    })
}
