//! `GET /internal/connections/{connection_id}/files`: the index of every present file of a
//! connection, for `ratatoskr-sites`, its only caller (workspace change `add-site-publications`,
//! design D3).
//!
//! The route is under `/internal/`, as Knowledge serves `/internal/search`. It is on the domain
//! listener, which binds loopback, and it is not reached through Edge: Edge proxies only
//! `/v1/notes`. It reads no claim of Edge, because no user calls it.
//!
//! The answer is `{"files": [...]}`, in the order of the paths. Each file has `path`, `revision`
//! (the notes-owned `BlobRef` of its current revision), `frontmatter` (an object, or `null`),
//! `tags`, `headings` (`level`, `heading`), `blocks`, `links` and `embeds`. Each link and embed has
//! `link`, `display_text`, `form` (`wiki` or `markdown`) and `target`: the vault path that the notes
//! resolver finds for it, or `null` when it is unresolved. The resolution is the one of the read
//! routes (workspace change `add-notes-vault-connections`).

use axum::Json;
use axum::extract::rejection::PathRejection;
use axum::extract::{Path, State};
use ratatoskr_notes_index::{Link, NoteIndex, Vault};
use ratatoskr_notes_persistence::IndexedRow;
use serde_json::{Value, json};
use uuid::Uuid;

use super::{ApiFault, ApiState};
use crate::systemd::SystemdBus;

/// `GET /internal/connections/{connection_id}/files`. A connection that does not exist, or that
/// failed or was removed, answers `404` `notes.connection.not_found`.
pub(super) async fn list_indexed_files<B: SystemdBus>(
    State(state): State<ApiState<B>>,
    connection_id: Result<Path<Uuid>, PathRejection>,
) -> Result<Json<Value>, ApiFault> {
    let Path(connection_id) = connection_id.map_err(|_| ApiFault::NotFound)?;
    let rows = state
        .database
        .indexed_files(connection_id)
        .await
        .map_err(|error| {
            tracing::error!(%error, %connection_id, "the index of a connection could not be read");
            ApiFault::Unavailable
        })?
        .ok_or(ApiFault::NotFound)?;
    let vault = Vault::new(rows.iter().map(|row| row.path.clone()));
    let files: Vec<Value> = rows.iter().map(|row| file_view(&vault, row)).collect();
    Ok(Json(json!({ "files": files })))
}

/// One file of the answer.
fn file_view(vault: &Vault, row: &IndexedRow) -> Value {
    let index: NoteIndex = row
        .note_index
        .clone()
        .and_then(|value| serde_json::from_value(value).ok())
        .unwrap_or_default();
    let links = |links: &[Link]| -> Vec<Value> {
        links
            .iter()
            .map(|link| {
                json!({
                    "link": link.link,
                    "display_text": link.display_text,
                    "form": link.form,
                    "target": vault.resolve(&row.path, link.linkpath(), link.form),
                })
            })
            .collect()
    };
    json!({
        "path": row.path,
        "revision": {
            "owner_service": "ratatoskr-notes",
            "digest": { "algorithm": "sha256", "hex": row.revision },
            "media_type": row.media_type,
            "length_bytes": row.length_bytes,
        },
        "frontmatter": index.frontmatter,
        "tags": index.tags,
        "headings": index.headings,
        "blocks": index.blocks,
        "links": links(&index.links),
        "embeds": links(&index.embeds),
    })
}
