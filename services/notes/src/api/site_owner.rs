//! The routes that a site owner session reaches (SITE-027). Edge sends a claim of the session kind
//! `site_owner` only to the blob routes and the conflict read, and notes does not rely on that
//! alone: a guard on the domain router serves the claim on the routes of [`SITE_OWNER_ROUTES`],
//! answers `notes.channel_mismatch` on [`DEVICE_CHANNEL_ROUTES`], and answers `401` on every
//! other route.

use axum::extract::{MatchedPath, Request};
use axum::http::Method;
use axum::middleware::Next;
use axum::response::{IntoResponse as _, Response};

use super::{ApiFault, SESSION_KIND_HEADER};

/// The session kind of a site owner session.
pub(super) const SITE_OWNER_KIND: &str = "site_owner";

/// The route that opens an upload session.
pub(super) const BLOBS: &str = "/v1/notes/connections/{connection_id}/blobs";

/// The route that closes an upload session.
pub(super) const BLOB: &str = "/v1/notes/connections/{connection_id}/blobs/{token}";

/// The route that takes one chunk of an upload.
pub(super) const BLOB_CHUNK: &str =
    "/v1/notes/connections/{connection_id}/blobs/{token}/chunks/{index}";

/// The route that finalizes an upload.
pub(super) const BLOB_FINALIZE: &str =
    "/v1/notes/connections/{connection_id}/blobs/{token}/finalize";

/// The conflict read.
pub(super) const CONFLICT: &str = "/v1/notes/conflicts/{conflict_id}";

/// The change batches of a device channel.
pub(super) const CHANGES: &str = "/v1/notes/connections/{connection_id}/changes";

/// The manifest of a device channel.
pub(super) const MANIFEST: &str = "/v1/notes/connections/{connection_id}/manifest";

/// The routes that serve a site owner claim: the upload of the content of an edit, and the read of
/// a conflict that an edit raised.
const SITE_OWNER_ROUTES: [(Method, &str); 5] = [
    (Method::POST, BLOBS),
    (Method::PUT, BLOB_CHUNK),
    (Method::POST, BLOB_FINALIZE),
    (Method::DELETE, BLOB),
    (Method::GET, CONFLICT),
];

/// The device channel routes. They refuse every session with `notes.channel_mismatch`.
const DEVICE_CHANNEL_ROUTES: [(Method, &str); 2] =
    [(Method::POST, CHANGES), (Method::POST, MANIFEST)];

/// Serve a request with a site owner claim only on [`SITE_OWNER_ROUTES`]. Every other request
/// passes unchanged.
pub(super) async fn guard(request: Request, next: Next) -> Response {
    let site_owner = request
        .headers()
        .get(SESSION_KIND_HEADER)
        .and_then(|value| value.to_str().ok())
        == Some(SITE_OWNER_KIND);
    if !site_owner {
        return next.run(request).await;
    }
    // Decided before the request moves on: a borrow of the request must not live across an await.
    let (allowed, device_channel) = {
        let route = request
            .extensions()
            .get::<MatchedPath>()
            .map(MatchedPath::as_str);
        let listed = |routes: &[(Method, &str)]| {
            routes
                .iter()
                .any(|(method, path)| method == request.method() && route == Some(*path))
        };
        (listed(&SITE_OWNER_ROUTES), listed(&DEVICE_CHANNEL_ROUTES))
    };
    if allowed {
        next.run(request).await
    } else if device_channel {
        ApiFault::ChannelMismatch.into_response()
    } else {
        ApiFault::Unauthenticated.into_response()
    }
}
