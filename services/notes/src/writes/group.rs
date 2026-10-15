//! The write group of a rename or a move and the link rewrites it needs (workspace spec
//! `notes-link-rewrite`, design D7).
//!
//! When notes accepts a `rename` or a `move`, it computes with [`rewrite_links`] the notes whose
//! links change, from their current revisions, and stores the new bytes of each. Each linking note
//! gets one more write, an `update` on its current revision, in the group of the rename; the
//! renamed note itself, when its own links change, gets its new bytes on the rename. Only notes
//! that link to the old or the new name are read: the links of any other note resolve as before.
//!
//! The group waits until each of its writes is the head of every queue it is in. Then each base
//! is checked: when every base is current, the whole group is delivered; otherwise nothing is,
//! and one conflict names every stale file. The writes of a delivered group end one by one as the
//! channel acknowledges or refuses them, and each acknowledged revision becomes current at once.
//! The operation of the rename ends with the last of them: `succeeded` with one result per file,
//! or, when the channel applied only a part, `failed` with `notes.write.partially_applied` and one
//! conflict that names every file that was not applied.
//!
//! Notes does not read the vault setting `newLinkFormat` (the `.obsidian` folder is not indexed),
//! so it writes links as Obsidian's default, `shortest`, does.

use std::collections::BTreeMap;

use ratatoskr_notes_index::paths::path_key;
use ratatoskr_notes_index::{NewLinkFormat, NoteIndex, rewrite_links};
use ratatoskr_notes_persistence::{FileTransaction, NewConflict, NewWrite, WriteRow};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use super::queue::Locked;
use super::report::{FileResult, Step};
use super::request::Request;
use super::{CONFLICT, PARTIALLY_APPLIED};
use crate::files::{FileRecorder, RecordError, media_type};

/// One link rewrite of a group: an `update` of a linking note.
#[derive(Debug, Clone)]
pub(super) struct Member {
    path: String,
    base_hash: String,
    content_hash: String,
    content_length: u64,
}

/// The rewrites that a rename needs.
#[derive(Debug, Clone, Default)]
pub(super) struct Plan {
    /// The linking notes whose links change.
    pub(super) members: Vec<Member>,
    /// The new bytes of the renamed note, when its own links change: their SHA-256 and length.
    pub(super) own: Option<(String, u64)>,
}

/// The rewrites of the rename `request`, from the current revisions of its connection, with the
/// new bytes of each stored in the blob store and registered for the connection.
pub(super) async fn plan(
    recorder: &FileRecorder,
    transaction: &mut FileTransaction,
    request: &Request,
) -> Result<Plan, RecordError> {
    let (Some(to), Some(base)) = (request.target_path.as_deref(), request.base_hash.as_deref())
    else {
        return Ok(Plan::default());
    };
    let from = request.path.as_str();
    let names = [name_key(from), name_key(to)];
    let paths: Vec<String> = transaction
        .present_files()
        .await?
        .into_iter()
        .map(|file| file.path)
        .collect();
    let mut notes = BTreeMap::new();
    let mut bases = BTreeMap::new();
    for row in transaction.note_rows().await? {
        let renamed = row.path == from;
        if !renamed && !links_a_name(row.note_index, &names) {
            continue;
        }
        // The renamed note is read at the revision that the rename names, which can be the result
        // of a write before it in its queue.
        let digest = if renamed { base } else { row.revision.as_str() };
        let Some(text) = text_of(recorder, digest).await else {
            continue;
        };
        bases.insert(row.path.clone(), row.revision.clone());
        notes.insert(row.path, text);
    }
    let mut plan = Plan::default();
    let rewritten = rewrite_links(&paths, &notes, from, to, NewLinkFormat::Shortest);
    for (path, text) in rewritten {
        let (content_hash, content_length) = store(recorder, transaction, text).await?;
        if path == to {
            plan.own = Some((content_hash, content_length));
        } else if let Some(base_hash) = bases.remove(&path) {
            plan.members.push(Member {
                path,
                base_hash,
                content_hash,
                content_length,
            });
        }
    }
    Ok(plan)
}

/// Record the rewrites of `plan` as `update` writes in the group `group_id` of `leader`.
pub(super) async fn insert_members(
    transaction: &mut FileTransaction,
    leader: &NewWrite<'_>,
    plan: &Plan,
) -> Result<(), RecordError> {
    for member in &plan.members {
        // One key of a connection names one write: the key of each rewrite comes from the key of
        // the rename and the path.
        let key = format!(
            "{:x}",
            Sha256::digest(format!("{}\n{}", leader.idempotency_key, member.path))
        );
        let key_path = path_key(&member.path);
        transaction
            .insert_write(&NewWrite {
                write_id: Uuid::now_v7(),
                kind: "update",
                idempotency_key: &key,
                path: &member.path,
                path_key: &key_path,
                target_path: None,
                target_path_key: None,
                base_hash: Some(&member.base_hash),
                content_hash: Some(&member.content_hash),
                content_length: Some(member.content_length),
                resolves: None,
                property: None,
                ..*leader
            })
            .await?;
    }
    Ok(())
}

/// The code of the failure of the group `group_id`: the code of its rename when the rename failed,
/// else the code of its first failed write.
pub(super) fn failure_code(group: &[WriteRow], group_id: Uuid) -> Option<&str> {
    group
        .iter()
        .find(|write| write.write_id == group_id && write.state == "failed")
        .or_else(|| group.iter().find(|write| write.state == "failed"))
        .and_then(|write| write.error_code.as_deref())
}

/// The file name of `path` in lower case: the key of Obsidian's lookup by name.
fn name_key(path: &str) -> String {
    path.rsplit_once('/')
        .map_or(path, |(_, name)| name)
        .to_lowercase()
}

/// Whether a link or an embed of the note index `index` names one of `names` (a file name in lower
/// case), with or without `.md`. A note whose index cannot be read counts as linking.
fn links_a_name(index: Option<serde_json::Value>, names: &[String]) -> bool {
    let Some(index) = index.and_then(|value| serde_json::from_value::<NoteIndex>(value).ok())
    else {
        return true;
    };
    index.links.iter().chain(&index.embeds).any(|link| {
        let name = name_key(link.linkpath());
        names
            .iter()
            .any(|wanted| *wanted == name || *wanted == format!("{name}.md"))
    })
}

/// The UTF-8 text of the blob `digest`, or `None` when the store does not hold it or it is not
/// UTF-8.
async fn text_of(recorder: &FileRecorder, digest: &str) -> Option<String> {
    let path = recorder.blobs().path_of(digest)?;
    let bytes = tokio::fs::read(path).await.ok()?;
    String::from_utf8(bytes).ok()
}

/// Store `text` in the blob store and register it for the connection of `transaction`, so that
/// the channel may name it. Returns its SHA-256 and its length.
async fn store(
    recorder: &FileRecorder,
    transaction: &mut FileTransaction,
    text: String,
) -> Result<(String, u64), RecordError> {
    let bytes = text.into_bytes();
    let digest = format!("{:x}", Sha256::digest(&bytes));
    let length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    let blobs = recorder.blobs().clone();
    let declared = digest.clone();
    let stored = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        let part = blobs.create_part()?;
        std::fs::write(&part, &bytes)?;
        blobs.seal_part(&part, &declared).map(|_| ())
    })
    .await;
    if !matches!(stored, Ok(Ok(()))) {
        return Err(RecordError::REWRITE);
    }
    transaction.record_blob(&digest, length).await?;
    Ok((digest, length))
}

impl Locked<'_> {
    /// Deliver the group of `head`, the head of a queue, when each of its writes is the head of
    /// every queue it is in and every base is current. A stale base fails the whole group with one
    /// conflict. Returns the path keys of the queues of the group when it ended or was delivered,
    /// and `None` while it waits.
    pub(super) async fn advance_group(
        &mut self,
        head: &WriteRow,
    ) -> Result<Option<Vec<String>>, RecordError> {
        let Some(group_id) = head.group_id else {
            return Ok(None);
        };
        let group = self.transaction.group_writes(group_id).await?;
        let keys: Vec<String> = group
            .iter()
            .flat_map(|write| super::queue::keys_of(write).into_iter().map(str::to_owned))
            .collect();
        let mut stale = Vec::new();
        for write in &group {
            if write.state != "queued" || !self.heads_every_queue(write).await? {
                return Ok(None);
            }
            if let super::queue::Check::Conflict(_) | super::queue::Check::Fail(_) =
                self.check_base(write).await?
            {
                stale.push(write.write_id);
            }
        }
        if stale.is_empty() {
            for write in &group {
                self.transaction.deliver_write(write.write_id).await?;
            }
            let wait = self.wait();
            self.report(group_id, Step::Awaiting(wait)).await?;
            return Ok(Some(keys));
        }
        for write in &group {
            self.transaction
                .fail_write(write.write_id, CONFLICT)
                .await?;
        }
        self.end_group(group_id, &stale, false).await?;
        Ok(Some(keys))
    }

    /// Fail with `code` each rewrite of the group `group_id` that is delivering and did not land:
    /// when the rename itself is refused, no link may name a path whose file did not move.
    pub(super) async fn fail_unlanded_rewrites(
        &mut self,
        group_id: Uuid,
        code: &str,
    ) -> Result<(), RecordError> {
        for member in self.transaction.group_writes(group_id).await? {
            if member.state == "delivering" && !member.landed {
                self.transaction.fail_write(member.write_id, code).await?;
            }
        }
        Ok(())
    }

    /// After a write of the group `group_id` ended, end the operation of the rename when no write
    /// of the group is open any more: `succeeded` when every write succeeded; with one conflict when
    /// a part was applied (`notes.write.partially_applied`) or a file was stale
    /// (`notes.write.conflict`); otherwise, when the channel failed the group and applied nothing,
    /// with the code of that failure and no conflict.
    pub(super) async fn settle_group(&mut self, group_id: Uuid) -> Result<(), RecordError> {
        let group = self.transaction.group_writes(group_id).await?;
        if group
            .iter()
            .any(|write| matches!(write.state.as_str(), "queued" | "delivering"))
        {
            return Ok(());
        }
        let failed: Vec<Uuid> = group
            .iter()
            .filter(|write| write.state == "failed")
            .map(|write| write.write_id)
            .collect();
        if failed.is_empty() {
            return self.group_succeeded(group_id, &group).await;
        }
        let applied = group.iter().any(|write| write.state == "succeeded");
        let stale = group
            .iter()
            .any(|write| write.state == "failed" && write.error_code.as_deref() == Some(CONFLICT));
        if !applied && !stale {
            let code = failure_code(&group, group_id).unwrap_or(CONFLICT);
            return self.report(group_id, Step::Failed(code, None)).await;
        }
        self.end_group(group_id, &failed, applied).await
    }

    /// Raise one conflict for the `stale` writes of the group `group_id`, all of which failed, name
    /// it on every failed write of the group, and fail the operation of the rename: with
    /// `notes.write.partially_applied` when the channel `applied` a part of the group, else with
    /// `notes.write.conflict`.
    async fn end_group(
        &mut self,
        group_id: Uuid,
        stale: &[Uuid],
        applied: bool,
    ) -> Result<(), RecordError> {
        let group = self.transaction.group_writes(group_id).await?;
        let mut stale: Vec<&WriteRow> = group
            .iter()
            .filter(|write| stale.contains(&write.write_id))
            .collect();
        stale.sort_by(|a, b| a.path.cmp(&b.path));
        let Some(first) = stale.first() else {
            return Ok(());
        };
        // The hash that the channel reported for the file, or else its current revision.
        let current = match &first.local_hash {
            Some(local) => Some(local.clone()),
            None => self
                .transaction
                .file_by_key(&first.path_key)
                .await?
                .filter(|file| file.present)
                .map(|file| file.revision),
        };
        let conflict_id = Uuid::now_v7();
        self.transaction
            .raise_conflict(&NewConflict {
                conflict_id,
                write: first,
                media_type: media_type(&first.path),
                base_hash: first.base_hash.as_deref(),
                current_hash: current.as_deref(),
                writer_hash: first.content_hash.as_deref(),
            })
            .await?;
        let paths: Vec<String> = stale.iter().map(|write| write.path.clone()).collect();
        self.transaction
            .set_stale_paths(conflict_id, &paths)
            .await?;
        for write in group.iter().filter(|write| write.state == "failed") {
            self.transaction
                .join_conflict(write.write_id, conflict_id)
                .await?;
        }
        self.raised.push(conflict_id);
        let code = if applied { PARTIALLY_APPLIED } else { CONFLICT };
        self.report(group_id, Step::Failed(code, Some(conflict_id)))
            .await
    }

    /// Report the operation of the rename `succeeded`, with one result for each file of `group`.
    async fn group_succeeded(
        &mut self,
        group_id: Uuid,
        group: &[WriteRow],
    ) -> Result<(), RecordError> {
        let mut files = Vec::new();
        for write in group {
            let path = write.target_path.as_deref().unwrap_or(&write.path);
            let Some(file) = self.transaction.file_by_key(&path_key(path)).await? else {
                continue;
            };
            let Some(revision) = write.revision.clone() else {
                continue;
            };
            let length = self.recorder.blob_length(&revision).await.unwrap_or(0);
            let transformed = super::expected_hash(write) != Some(revision.as_str());
            files.push((
                file.file_id,
                revision,
                media_type(path),
                length,
                transformed,
            ));
        }
        let results: Vec<FileResult<'_>> = files
            .iter()
            .map(
                |(file_id, revision, media, length, transformed)| FileResult {
                    file_id: *file_id,
                    revision: Some((revision.as_str(), *media, *length)),
                    transformed: *transformed,
                },
            )
            .collect();
        self.report(group_id, Step::SucceededGroup(&results)).await
    }
}
