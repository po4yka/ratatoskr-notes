## Why

The workspace change `add-site-owner-editing-and-live` (SITE-027) lets the owner edit a published
note from a site. Edge streams the content of the edit to the transfer route of this repository
with a minted claim of the session kind `site_owner` and the origin `x-ratatoskr-origin:
site:<site id>`. The cross-repository rule is the workspace spec `notes-device-channels`,
requirement "A site owner session may register write content and nothing else". This change
implements group 4 of the workspace task list, and does not restate that rule.

## What Changes

- The blob routes `/v1/notes/connections/{id}/blobs` accept a `site_owner` claim only when its
  origin claim is `site:<site id>`, as the contract type `WriteOrigin` spells a site. A `site_owner`
  claim without that origin answers `401` with `notes.request.unauthenticated`.
- A `site_owner` upload registers the blob for the connection and does nothing else: no revision
  and no `notes.file.changed.v1` event.
- The change batch and manifest routes refuse a `site_owner` claim with `notes.channel_mismatch`.
- Add `DELETE /v1/notes/connections/{id}/blobs/{token}`, which closes an open upload session: it
  deletes the received chunks, frees the slot of the session, and answers `204`. Edge calls it
  after a failed chunk or finalize of a site edit.

## Capabilities

### New Capabilities

- `site-owner-uploads`: what a request with a `site_owner` claim can do on the transfer, change
  and manifest routes of a connection, and the close of an open upload session.

### Modified Capabilities

None.

## Impact

`services/notes/src/api/blobs.rs`, `services/notes/src/uploads.rs`, `services/notes/src/api.rs`,
the README, and the tests in `services/notes/tests/device_channel.rs` and `device_uploads.rs`. No
schema change. The write command with `origin: "site:<site id>"` and the conflict read
`GET /v1/notes/conflicts/{id}` need no change: OBS-024 already accepts the site origin, and the
conflict read reads only the owner claim.
