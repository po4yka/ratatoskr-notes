## Context

The workspace design `add-site-owner-editing-and-live` D6 decides the cross-repository shape. This
file records only the decisions that are local to this repository.

## Decisions

### 1. A site owner claim must name its site

Before this change, the blob routes took every session kind except `device` as a session of the
owner. A `site_owner` claim therefore already registered a blob, and the change batch and manifest
routes already refused it with `notes.channel_mismatch`. What was missing is the origin: Edge
mints `x-ratatoskr-origin: site:<site id>` for every request of a `site_owner` session, so a
`site_owner` claim without it is a malformed claim. The blob routes answer it `401` with
`notes.request.unauthenticated`, as they answer a malformed device claim.

The origin is valid when the contract type `WriteOrigin` parses it and it starts with `site:`.
`platform` is a valid `WriteOrigin`, but it does not name a site, so it is refused.

### 2. Only the blob routes read the origin

The change batch and manifest routes refuse every session, so they do not read the origin. The
conflict read `GET /v1/notes/conflicts/{id}` returns a conflict of the owner to any session; Edge
checks that the file is in the scope of the site.

### 3. One list names the routes of a site owner claim

Notes does not rely only on the route table of Edge. A guard on the domain router reads the session
kind and the matched route. One list in `services/notes/src/api/site_owner.rs` names the routes that
serve a `site_owner` claim, and the router uses the same path constants. On the change and manifest
routes the guard answers `notes.channel_mismatch`, the code that these routes give every session,
as the workspace spec `notes-device-channels` requires. Every other route answers `401`
`notes.request.unauthenticated`. Notes has no code for a wrong session kind other than these two.
The upload status route is not on the list: Edge does not call it for a site edit.
