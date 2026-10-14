//! The device channels `plugin` and `cli` (workspace design D5 and D6, spec
//! `notes-device-channels`): the change batches of a device, ordered by its change sequence.
//!
//! A batch holds 1 to [`MAX_BATCH_CHANGES`] changes whose sequences increase by one. The
//! connection keeps the highest device sequence that is committed. A batch that starts after the
//! next number is a gap and commits nothing; a change at or below the committed sequence is a
//! repeat and changes nothing; the other changes are committed in one transaction with the new
//! committed sequence. A change that names bytes the blob store does not hold commits nothing, and
//! the answer lists the missing hashes.
//!
//! Paths are not logged: a file name is the owner's content.

use std::collections::BTreeSet;

use ratatoskr_identifiers::DigestHex;
use ratatoskr_notes_contracts::{FileChangeKind, VaultPath};
use ratatoskr_notes_index::paths::nfc_path;
use ratatoskr_notes_persistence::{FileTransaction, LockedConnection};
use uuid::Uuid;

use crate::files::{FileRecorder, RecordError, Revision};

/// The most changes of one batch.
pub const MAX_BATCH_CHANGES: usize = 256;

/// The device kind that a device channel takes (workspace design D5 and D6).
#[must_use]
pub const fn device_kind_of(channel: &str) -> Option<&'static str> {
    match channel.as_bytes() {
        b"plugin" => Some("obsidian_plugin"),
        b"cli" => Some("export_agent"),
        _ => None,
    }
}

/// The owner and the device of a request, from the claims that Edge mints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sender<'a> {
    /// The owner.
    pub owner_id: Uuid,
    /// The paired device.
    pub device_id: Uuid,
    /// The kind of the device, for example `obsidian_plugin`.
    pub kind: &'a str,
}

/// Whether `sender` may send the changes of `connection`. A connection of another owner is
/// [`BatchOutcome::NotFound`], as one that does not exist. A device of another kind than the
/// channel, or another device than the one that created the connection, is
/// [`BatchOutcome::ChannelMismatch`].
#[must_use]
pub fn admits(sender: &Sender<'_>, connection: &LockedConnection) -> Option<BatchOutcome> {
    if connection.owner_id != sender.owner_id {
        return Some(BatchOutcome::NotFound);
    }
    if device_kind_of(&connection.channel) != Some(sender.kind)
        || connection.device_id != Some(sender.device_id)
    {
        return Some(BatchOutcome::ChannelMismatch);
    }
    None
}

/// One change of a batch, as a device sends it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceChange {
    /// The device change sequence, which increases by one per change.
    pub sequence: u64,
    /// What happened to the file.
    pub kind: FileChangeKind,
    /// The path after the change, relative to the vault root.
    pub path: String,
    /// The path before a rename. Present exactly when the kind is `renamed`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_path: Option<String>,
    /// The lowercase hex SHA-256 of the new bytes. Absent exactly when the kind is `deleted`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    /// The number of new bytes. Absent exactly when the kind is `deleted`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub length_bytes: Option<u64>,
}

/// What [`apply_batch`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchOutcome {
    /// The changes up to this device sequence are committed.
    Committed(u64),
    /// The batch starts after the next sequence; nothing is committed. Holds the committed one.
    SequenceGap(u64),
    /// The blob store does not hold these hashes; nothing is committed.
    BytesMissing(Vec<String>),
    /// The batch holds more than [`MAX_BATCH_CHANGES`] changes; nothing is committed.
    TooLarge,
    /// A change breaks a rule of the batch format; nothing is committed.
    Invalid,
    /// No connection of the owner takes changes under this id.
    NotFound,
    /// The device is of another kind than the channel, or is not the device of the connection.
    ChannelMismatch,
}

/// A change after its checks: an NFC path that the contract accepts, and the facts of the bytes
/// for every kind except `deleted`.
#[derive(Debug)]
struct Checked {
    sequence: u64,
    path: String,
    previous_path: Option<String>,
    facts: Option<(String, u64)>,
}

/// Apply the batch `changes` of a device to the connection `connection_id` in one transaction.
///
/// # Errors
///
/// [`RecordError`] when the database fails or an event cannot be built.
pub async fn apply_batch(
    recorder: &FileRecorder,
    sender: &Sender<'_>,
    connection_id: Uuid,
    changes: &[DeviceChange],
) -> Result<BatchOutcome, RecordError> {
    if changes.len() > MAX_BATCH_CHANGES {
        return Ok(BatchOutcome::TooLarge);
    }
    let Some(checked) = check(changes) else {
        return Ok(BatchOutcome::Invalid);
    };
    // The index is read from the blob store before the lock of the connection is taken.
    let mut indexes = Vec::with_capacity(checked.len());
    for change in &checked {
        indexes.push(match &change.facts {
            Some((digest, length)) => recorder.note_index(&change.path, digest, *length).await,
            None => None,
        });
    }
    let Some((mut transaction, connection)) =
        recorder.database().begin_file_change(connection_id).await?
    else {
        return Ok(BatchOutcome::NotFound);
    };
    if let Some(refused) = admits(sender, &connection) {
        return Ok(refused);
    }
    let committed = connection.device_sequence;
    let first = checked.first().map_or(0, |change| change.sequence);
    if first > committed.saturating_add(1) {
        return Ok(BatchOutcome::SequenceGap(committed));
    }
    let pending: Vec<(&Checked, Option<&serde_json::Value>)> = checked
        .iter()
        .zip(indexes.iter().map(Option::as_ref))
        .filter(|(change, _)| change.sequence > committed)
        .collect();
    let Some(last) = pending.last().map(|(change, _)| change.sequence) else {
        return Ok(BatchOutcome::Committed(committed));
    };
    let rules = recorder.rules(&connection);
    let mut missing = BTreeSet::new();
    for (change, _) in &pending {
        let Some((digest, length)) = &change.facts else {
            continue;
        };
        if rules.exclusion(&change.path, *length).is_some() {
            continue;
        }
        match recorder.usable_length(&mut transaction, digest).await? {
            None => {
                missing.insert(digest.clone());
            }
            Some(stored) if stored != *length => return Ok(BatchOutcome::Invalid),
            Some(_) => {}
        }
    }
    if !missing.is_empty() {
        return Ok(BatchOutcome::BytesMissing(missing.into_iter().collect()));
    }
    for (change, note_index) in pending {
        apply(
            recorder,
            &mut transaction,
            &connection,
            connection_id,
            change,
            note_index,
        )
        .await?;
    }
    transaction.set_device_sequence(last).await?;
    transaction.commit().await?;
    Ok(BatchOutcome::Committed(last))
}

/// Apply one change that passed its checks.
async fn apply(
    recorder: &FileRecorder,
    transaction: &mut FileTransaction,
    connection: &LockedConnection,
    connection_id: Uuid,
    change: &Checked,
    note_index: Option<&serde_json::Value>,
) -> Result<(), RecordError> {
    let Some((digest, length)) = &change.facts else {
        recorder
            .apply_removal(transaction, connection, connection_id, &change.path)
            .await?;
        return Ok(());
    };
    let revision = Revision {
        path: &change.path,
        digest,
        length: *length,
        note_index,
    };
    match &change.previous_path {
        Some(from) => {
            recorder
                .apply_rename(transaction, connection, connection_id, from, &revision)
                .await?;
        }
        None => {
            recorder
                .apply_revision(transaction, connection, connection_id, &revision)
                .await?;
        }
    }
    Ok(())
}

/// The changes after their checks, or `None` when one breaks a rule: the batch is empty, a
/// sequence is 0 or does not follow the one before it, a path is not a vault path, a rename names
/// no previous path or another kind names one, or the facts of the bytes are missing, malformed,
/// or present on a `deleted` change.
fn check(changes: &[DeviceChange]) -> Option<Vec<Checked>> {
    let mut checked: Vec<Checked> = Vec::with_capacity(changes.len());
    for change in changes {
        let expected = match checked.last() {
            Some(previous) => previous.sequence.checked_add(1)?,
            None => change.sequence.max(1),
        };
        if change.sequence != expected {
            return None;
        }
        let previous_path = match (change.kind, &change.previous_path) {
            (FileChangeKind::Renamed, Some(path)) => Some(vault_path(path)?),
            (FileChangeKind::Created | FileChangeKind::Updated | FileChangeKind::Deleted, None) => {
                None
            }
            _ => return None,
        };
        let facts = match (change.kind, &change.sha256, change.length_bytes) {
            (FileChangeKind::Deleted, None, None) => None,
            (FileChangeKind::Deleted, _, _) | (_, None, _) | (_, _, None) => return None,
            (_, Some(digest), Some(length)) => {
                DigestHex::parse(digest).ok()?;
                Some((digest.clone(), length))
            }
        };
        checked.push(Checked {
            sequence: change.sequence,
            path: vault_path(&change.path)?,
            previous_path,
            facts,
        });
    }
    (!checked.is_empty()).then_some(checked)
}

/// `path` in NFC when the contract accepts it as a vault path.
pub(crate) fn vault_path(path: &str) -> Option<String> {
    let path = nfc_path(path);
    VaultPath::parse(&path).ok()?;
    Some(path)
}
