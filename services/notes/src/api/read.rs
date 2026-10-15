//! The read routes (workspace spec `notes-file-index`, requirement "Clients read files and the
//! index only through Edge"): the connections of the owner with their status, the files of a
//! connection, the bytes of a file at its current or a named revision, and the index of a file
//! with its links and backlinks. Every route reads only data of the owner in the claim, and a
//! connection of another owner answers `404` as one that does not exist. The routes are read-only:
//! another method on a file route answers `405`.

use axum::Json;
use axum::body::Body;
use axum::extract::rejection::PathRejection;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, header};
use axum::response::{IntoResponse as _, Response};
use ratatoskr_notes_index::paths::{nfc_path, path_key};
use ratatoskr_notes_index::{LinkGraph, NoteIndex, Vault};
use ratatoskr_notes_persistence::{ConnectionStatus, FileSummary};
use tokio_util::io::ReaderStream;
use uuid::Uuid;

use super::{ApiFault, ApiState, owner};
use crate::systemd::SystemdBus;

/// The query of a file route.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RevisionQuery {
    /// A named revision, the lowercase hex SHA-256 of its bytes. The current one when absent.
    revision: Option<String>,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct Connections {
    connections: Vec<ConnectionView>,
}

#[derive(Debug, serde::Serialize)]
struct ConnectionView {
    connection_id: Uuid,
    channel: String,
    vault_display_name: String,
    state: String,
    excluded_folders: Vec<String>,
    /// `on` or `off`.
    write_mode: String,
    conditions: Vec<Condition>,
}

#[derive(Debug, serde::Serialize)]
struct Condition {
    code: &'static str,
    paths: Vec<String>,
}

impl From<ConnectionStatus> for ConnectionView {
    fn from(status: ConnectionStatus) -> Self {
        Self {
            connection_id: status.connection_id,
            channel: status.channel,
            vault_display_name: status.vault_display_name,
            state: status.state,
            excluded_folders: status.excluded_folders,
            write_mode: status.write_mode,
            conditions: status
                .collisions
                .into_iter()
                .map(|(path, existing)| Condition {
                    code: ratatoskr_notes_index::paths::PATH_COLLISION,
                    paths: vec![existing, path],
                })
                .collect(),
        }
    }
}

#[derive(Debug, serde::Serialize)]
pub(super) struct Files {
    files: Vec<FileView>,
}

#[derive(Debug, serde::Serialize)]
struct FileView {
    file_id: Uuid,
    path: String,
    revision: String,
    media_type: String,
    length_bytes: i64,
}

impl From<FileSummary> for FileView {
    fn from(file: FileSummary) -> Self {
        Self {
            file_id: file.file_id,
            path: file.path,
            revision: file.revision,
            media_type: file.media_type,
            length_bytes: file.length_bytes,
        }
    }
}

#[derive(Debug, serde::Serialize)]
pub(super) struct IndexView {
    path: String,
    revision: String,
    /// The index of a Markdown note; `null` for another file.
    index: Option<NoteIndex>,
    /// Each link, embed and frontmatter link, with the file it resolves to.
    links: Vec<LinkView>,
    /// The notes that link to this file, with the number of links of each.
    backlinks: Vec<Backlink>,
}

#[derive(Debug, serde::Serialize)]
struct LinkView {
    link: String,
    embed: bool,
    target: Option<String>,
}

#[derive(Debug, serde::Serialize)]
struct Backlink {
    path: String,
    count: u32,
}

fn database_fault(error: &ratatoskr_notes_persistence::PersistenceError) -> ApiFault {
    tracing::error!(%error, "a read of the notes database failed");
    ApiFault::Unavailable
}

/// `GET /v1/notes/connections`: the connections of the owner with their status.
pub(super) async fn list_connections<B: SystemdBus>(
    State(state): State<ApiState<B>>,
    headers: HeaderMap,
) -> Result<Json<Connections>, ApiFault> {
    let owner_id = owner(&headers)?;
    let connections = state
        .database
        .connections_of(owner_id)
        .await
        .map_err(|error| database_fault(&error))?;
    Ok(Json(Connections {
        connections: connections.into_iter().map(ConnectionView::from).collect(),
    }))
}

/// The connection of the path if the owner reads it, or `404`.
async fn readable<B: SystemdBus>(
    state: &ApiState<B>,
    headers: &HeaderMap,
    connection_id: Uuid,
) -> Result<Uuid, ApiFault> {
    let owner_id = owner(headers)?;
    if state
        .database
        .owner_reads(owner_id, connection_id)
        .await
        .map_err(|error| database_fault(&error))?
    {
        Ok(connection_id)
    } else {
        Err(ApiFault::NotFound)
    }
}

/// `GET /v1/notes/connections/{id}/files`: the present files with their paths and revisions.
pub(super) async fn list_files<B: SystemdBus>(
    State(state): State<ApiState<B>>,
    headers: HeaderMap,
    connection_id: Result<Path<Uuid>, PathRejection>,
) -> Result<Json<Files>, ApiFault> {
    let Path(connection_id) = connection_id.map_err(|_| ApiFault::NotFound)?;
    let connection_id = readable(&state, &headers, connection_id).await?;
    let files = state
        .database
        .present_files_of(connection_id)
        .await
        .map_err(|error| database_fault(&error))?;
    Ok(Json(Files {
        files: files.into_iter().map(FileView::from).collect(),
    }))
}

/// `GET /v1/notes/connections/{id}/files/{path}`: the bytes of the file at its current revision,
/// or at `?revision=<sha256>`. The path is matched in NFC and without case.
pub(super) async fn read_file<B: SystemdBus>(
    State(state): State<ApiState<B>>,
    headers: HeaderMap,
    target: Result<Path<(Uuid, String)>, PathRejection>,
    query: Result<Query<RevisionQuery>, axum::extract::rejection::QueryRejection>,
) -> Result<Response, ApiFault> {
    let Path((connection_id, path)) = target.map_err(|_| ApiFault::NotFound)?;
    let Query(query) = query.map_err(|_| ApiFault::InvalidRequest)?;
    let connection_id = readable(&state, &headers, connection_id).await?;
    let file = state
        .database
        .revision_of(
            connection_id,
            &path_key(&nfc_path(&path)),
            query.revision.as_deref(),
        )
        .await
        .map_err(|error| database_fault(&error))?
        .ok_or(ApiFault::FileNotFound)?;
    let blob = state
        .blobs
        .path_of(&file.revision)
        .ok_or(ApiFault::FileNotFound)?;
    let opened = tokio::fs::File::open(&blob).await.map_err(|error| {
        tracing::error!(%error, "the blob of a recorded revision cannot be opened");
        ApiFault::Unavailable
    })?;
    let mut response = Body::from_stream(ReaderStream::new(opened)).into_response();
    let headers = response.headers_mut();
    if let Ok(media_type) = HeaderValue::from_str(&file.media_type) {
        headers.insert(header::CONTENT_TYPE, media_type);
    }
    if let Ok(etag) = HeaderValue::from_str(&format!("\"{}\"", file.revision)) {
        headers.insert(header::ETAG, etag);
    }
    Ok(response)
}

/// `GET /v1/notes/connections/{id}/index/{path}`: the index of the current revision of a file,
/// each of its links with the file it resolves to, and its backlinks.
pub(super) async fn read_index<B: SystemdBus>(
    State(state): State<ApiState<B>>,
    headers: HeaderMap,
    target: Result<Path<(Uuid, String)>, PathRejection>,
) -> Result<Json<IndexView>, ApiFault> {
    let Path((connection_id, path)) = target.map_err(|_| ApiFault::NotFound)?;
    let connection_id = readable(&state, &headers, connection_id).await?;
    let file = state
        .database
        .revision_of(connection_id, &path_key(&nfc_path(&path)), None)
        .await
        .map_err(|error| database_fault(&error))?
        .ok_or(ApiFault::FileNotFound)?;
    let indexes = state
        .database
        .current_indexes(connection_id)
        .await
        .map_err(|error| database_fault(&error))?;
    let notes: Vec<(String, NoteIndex)> = indexes
        .iter()
        .filter_map(|(path, index)| {
            let index = serde_json::from_value(index.clone()?).ok()?;
            Some((path.clone(), index))
        })
        .collect();
    let vault = Vault::new(indexes.into_iter().map(|(path, _)| path));
    let graph = LinkGraph::build(
        &vault,
        notes.iter().map(|(path, index)| (path.as_str(), index)),
    );
    let index = notes
        .iter()
        .find(|(path, _)| *path == file.path)
        .map(|(_, index)| index.clone());
    let links = index
        .as_ref()
        .map(|index| links_of(&vault, &file.path, index))
        .unwrap_or_default();
    let backlinks = graph
        .backlinks(&file.path)
        .into_iter()
        .map(|(path, count)| Backlink { path, count })
        .collect();
    Ok(Json(IndexView {
        path: file.path,
        revision: file.revision,
        index,
        links,
        backlinks,
    }))
}

/// Each link, embed and frontmatter link of `index`, with the file it resolves to.
fn links_of(vault: &Vault, source: &str, index: &NoteIndex) -> Vec<LinkView> {
    let body = index
        .links
        .iter()
        .map(|link| (link, false))
        .chain(index.embeds.iter().map(|link| (link, true)))
        .map(|(link, embed)| LinkView {
            link: link.link.clone(),
            embed,
            target: vault
                .resolve(source, link.linkpath(), link.form)
                .map(str::to_owned),
        });
    let frontmatter = index.frontmatter_links.iter().map(|link| LinkView {
        link: link.link.clone(),
        embed: false,
        target: vault
            .resolve(
                source,
                link.link.split('#').next().unwrap_or_default(),
                ratatoskr_notes_index::LinkForm::Wiki,
            )
            .map(str::to_owned),
    });
    body.chain(frontmatter).collect()
}
