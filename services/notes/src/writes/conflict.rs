//! The events of a conflict and its merge proposal (workspace design D2, contract
//! `notes/conflict.v1`).
//!
//! `notes.conflict.raised.v1` names the three sides by hash and never carries file content. Its
//! aggregate is `notes_conflict:<conflict id>`, and it correlates to the operation of the write.
//! For `text/markdown` and `text/plain`, when notes holds the bytes of all three sides as UTF-8
//! text within the caps of a merge, the conflict carries a merge proposal: the merged bytes go to
//! the blob root, and the event names their hash and whether the merge has overlapping hunks. The
//! merge runs after the commit that records the conflict, outside the lock of the connection row;
//! a second short transaction stores the proposal and writes the event.
//!
//! `notes.conflict.resolved.v1` follows the acknowledgement of the `resolve_conflict` write, with
//! the operation and the origin of that write and the revision after it.

use ratatoskr_event_envelope::{EventEnvelope, EventPayload as _};
use ratatoskr_identifiers::{
    DigestHex, MediaType, NotesConflictId, NotesConnectionId, OperationId,
};
use ratatoskr_notes_contracts::{
    ConflictRaised, ConflictResolved, MergeProposal, VaultPath, WriteOrigin,
};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use super::merge;
use crate::events::{self, EnvelopeBuildError};
use crate::files::FileRecorder;
use crate::operation;

/// The aggregate kind of a conflict.
pub(super) const CONFLICT_AGGREGATE: &str = "notes_conflict";

/// The media types that get a merge proposal.
const MERGEABLE: [&str; 2] = ["text/markdown", "text/plain"];

/// The largest side of a merge, in bytes. A larger side gets no proposal and is not read.
///
/// Measured on the merge of three sides whose lines are two thirds equal (macOS, release build):
/// 1 MiB and 20 000 lines per side peak at about 11 MiB above the process baseline and take about
/// 8 ms; 4 MiB and 100 000 lines at about 50 MiB; 16 MiB and 600 000 lines at about 300 MiB, which a
/// service of the 512 MiB class cannot afford. A Markdown note is rarely larger than 1 MiB.
pub(crate) const MAX_SIDE_BYTES: u64 = 1024 * 1024;

/// The most lines of a side of a merge. The cost of the line diff grows with the lines more than
/// with the bytes, so a side of short lines also needs a bound: 20 000 lines fit the measurement
/// of [`MAX_SIDE_BYTES`].
pub(super) const MAX_SIDE_LINES: usize = 20_000;

/// The hash of the merged bytes and whether the merge has overlapping hunks.
pub(super) type Proposal = (String, bool);

/// The three sides of a conflict.
#[derive(Debug, Clone, Copy)]
pub(super) struct Sides<'a> {
    pub(super) base: Option<&'a str>,
    pub(super) current: Option<&'a str>,
    pub(super) writer: Option<&'a str>,
}

/// The merge proposal of a conflict on a file of `media_type`, with its bytes stored in the blob
/// root, or `None`: another media type, a missing side, bytes that notes does not hold or that are
/// not UTF-8, or a side over [`MAX_SIDE_BYTES`] or [`MAX_SIDE_LINES`]. It runs outside the lock of
/// the connection row.
pub(super) async fn propose(
    recorder: &FileRecorder,
    media_type: &str,
    sides: Sides<'_>,
) -> Option<Proposal> {
    if !MERGEABLE.contains(&media_type) {
        return None;
    }
    let base = text_of(recorder, sides.base?).await?;
    let current = text_of(recorder, sides.current?).await?;
    let writer = text_of(recorder, sides.writer?).await?;
    let merged = tokio::task::spawn_blocking(move || merge::merge(&base, &current, &writer))
        .await
        .ok()?;
    let digest = format!("{:x}", Sha256::digest(merged.text.as_bytes()));
    let store = recorder.blobs().clone();
    let sealed = digest.clone();
    let stored = tokio::task::spawn_blocking(move || {
        let part = store.create_part()?;
        crate::blobs::BlobStore::write_part(&part, 0, merged.text.as_bytes())?;
        store.seal_part(&part, &sealed)
    })
    .await
    .ok()?;
    match stored {
        Ok(_) => Some((digest, merged.overlapping)),
        Err(error) => {
            tracing::error!(%error, "the bytes of a merge proposal could not be stored");
            None
        }
    }
}

/// The UTF-8 text of the blob `digest`, when the store holds it within [`MAX_SIDE_BYTES`] and it
/// has at most [`MAX_SIDE_LINES`] lines. The length is read from the file system first, so a
/// larger blob is never read.
async fn text_of(recorder: &FileRecorder, digest: &str) -> Option<String> {
    let length = recorder.blob_length(digest).await?;
    if length > MAX_SIDE_BYTES {
        return None;
    }
    let bytes = tokio::fs::read(recorder.blobs().path_of(digest)?)
        .await
        .ok()?;
    let text = String::from_utf8(bytes).ok()?;
    (text.lines().count() <= MAX_SIDE_LINES).then_some(text)
}

/// What a raised conflict names.
#[derive(Debug, Clone, Copy)]
pub(super) struct Raised<'a> {
    pub(super) conflict_id: Uuid,
    pub(super) owner_id: Uuid,
    pub(super) operation_id: Uuid,
    pub(super) connection_id: Uuid,
    pub(super) path: &'a str,
    pub(super) stale_paths: Option<&'a [String]>,
    pub(super) origin: &'a str,
    pub(super) media_type: &'a str,
    pub(super) sides: Sides<'a>,
    pub(super) proposal: Option<&'a Proposal>,
}

/// The envelope of `notes.conflict.raised.v1` for `raised`.
///
/// # Errors
///
/// [`EnvelopeBuildError`] when a value does not satisfy the contract.
pub(super) fn raised_envelope(
    event_id: Uuid,
    raised: &Raised<'_>,
) -> Result<EventEnvelope, EnvelopeBuildError> {
    let hash = |side: Option<&str>| {
        side.map(DigestHex::parse)
            .transpose()
            .map_err(|_| EnvelopeBuildError::STAGE)
    };
    let merge_proposal = match raised.proposal {
        Some((merged_hash, overlapping)) => Some(MergeProposal {
            merged_hash: DigestHex::parse(merged_hash).map_err(|_| EnvelopeBuildError::STAGE)?,
            has_overlapping_hunks: *overlapping,
        }),
        None => None,
    };
    let stale_paths = raised
        .stale_paths
        .map(|paths| {
            paths
                .iter()
                .map(|path| VaultPath::parse(path))
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()
        .map_err(|_| EnvelopeBuildError::STAGE)?;
    let payload = ConflictRaised {
        conflict_id: NotesConflictId(raised.conflict_id),
        operation_id: OperationId(raised.operation_id),
        connection_id: NotesConnectionId(raised.connection_id),
        path: VaultPath::parse(raised.path).map_err(|_| EnvelopeBuildError::STAGE)?,
        stale_paths,
        origin: WriteOrigin::parse(raised.origin).map_err(|_| EnvelopeBuildError::STAGE)?,
        media_type: MediaType::parse(raised.media_type).map_err(|_| EnvelopeBuildError::STAGE)?,
        base_hash: hash(raised.sides.base)?,
        current_hash: hash(raised.sides.current)?,
        writer_hash: hash(raised.sides.writer)?,
        merge_proposal,
    };
    payload.validate().map_err(|_| EnvelopeBuildError::STAGE)?;
    events::correlated_envelope(
        event_id,
        ConflictRaised::event_type(),
        &conflict_aggregate(raised.conflict_id),
        &operation::operation_aggregate(raised.operation_id),
        raised.owner_id,
        &payload,
    )
}

/// What a resolved conflict names: the resolution write and the revision after it.
#[derive(Debug, Clone, Copy)]
pub(super) struct Resolved<'a> {
    pub(super) conflict_id: Uuid,
    pub(super) owner_id: Uuid,
    pub(super) operation_id: Uuid,
    pub(super) connection_id: Uuid,
    pub(super) path: &'a str,
    pub(super) origin: &'a str,
    pub(super) revision: &'a str,
}

/// The envelope of `notes.conflict.resolved.v1` for `resolved`.
///
/// # Errors
///
/// [`EnvelopeBuildError`] when a value does not satisfy the contract.
pub(super) fn resolved_envelope(
    event_id: Uuid,
    resolved: &Resolved<'_>,
) -> Result<EventEnvelope, EnvelopeBuildError> {
    let payload = ConflictResolved {
        conflict_id: NotesConflictId(resolved.conflict_id),
        operation_id: OperationId(resolved.operation_id),
        connection_id: NotesConnectionId(resolved.connection_id),
        path: VaultPath::parse(resolved.path).map_err(|_| EnvelopeBuildError::STAGE)?,
        origin: WriteOrigin::parse(resolved.origin).map_err(|_| EnvelopeBuildError::STAGE)?,
        revision: DigestHex::parse(resolved.revision).map_err(|_| EnvelopeBuildError::STAGE)?,
    };
    events::correlated_envelope(
        event_id,
        ConflictResolved::event_type(),
        &conflict_aggregate(resolved.conflict_id),
        &operation::operation_aggregate(resolved.operation_id),
        resolved.owner_id,
        &payload,
    )
}

/// The aggregate reference of a conflict, `notes_conflict:<uuid>`.
pub(super) fn conflict_aggregate(conflict_id: Uuid) -> String {
    format!("{CONFLICT_AGGREGATE}:{conflict_id}")
}
