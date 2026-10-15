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
checks that the file is in the scope of the site. The other owner routes are not on the route
table of a `site_owner` session at Edge, and this change does not add a second check to them.

## Risks / Trade-offs

- A `site_owner` claim that reaches another owner route of this listener is served as a session of
  the owner. Edge's route rule is the only guard. The listener is reachable only on loopback, and
  Edge strips every client copy of the claims.
