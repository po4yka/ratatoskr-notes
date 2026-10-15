## ADDED Requirements

### Requirement: A site owner claim registers write content and nothing else

The blob routes under `/v1/notes/connections/{id}/blobs` SHALL accept a request whose minted claim
names the connection's owner, the session kind `site_owner` and the origin `site:<site id>`, and
SHALL only register the blob for the connection: no revision and no `notes.file.changed.v1` event
follow from it. The change batch and manifest routes SHALL refuse such a claim with `409` and
`notes.channel_mismatch`, as the workspace spec `notes-device-channels` requires.

#### Scenario: A site owner uploads edit content

- **WHEN** an upload with the session kind `site_owner`, the origin `site:site-a` and one chunk of a declared 16 MiB chunk size completes for the owner's connection
- **THEN** the blob is registered for the connection, and the number of revisions and of `notes.file.changed.v1` events of the connection does not change, verified by `services/notes/tests/device_channel.rs::site_owner_upload_registers_blob_only`

#### Scenario: A site owner sends a change batch or a manifest

- **WHEN** a request with the session kind `site_owner` and the origin `site:site-a` sends a change batch or a manifest for the owner's connection
- **THEN** both answers are `409` with `notes.channel_mismatch`, and no file and no event is recorded, verified by `services/notes/tests/device_channel.rs::site_owner_cannot_send_change_batches`

### Requirement: A site owner claim names its site

A request on the blob routes with the session kind `site_owner` SHALL carry the origin claim
`x-ratatoskr-origin` with a value that the contract type `WriteOrigin` accepts and that starts with
`site:`. Without such an origin, the request SHALL be refused with `401` and
`notes.request.unauthenticated`, and no blob is registered.

#### Scenario: A site owner claim without a site origin

- **WHEN** an upload with the session kind `site_owner` has no origin claim, or the origin `platform`, `site:` or `site:Site_A`
- **THEN** the open answers `401` with `notes.request.unauthenticated`, and no blob is registered, verified by `services/notes/tests/device_channel.rs::site_owner_without_site_origin_is_refused`

### Requirement: An open upload session can be closed

`DELETE /v1/notes/connections/{id}/blobs/{token}` SHALL accept the claims of the other blob routes,
the `site_owner` claim with its site origin included. For an open session of that connection it
SHALL delete the received chunks, end the session, and answer `204`. A closed session SHALL NOT
count toward the limit of open sessions of its connection. An unknown token, or a token of another
connection, SHALL answer `404` with `blob_transfer.session_unknown` and SHALL leave every session
as it is.

#### Scenario: A device closes one of four open sessions

- **WHEN** a connection has four open sessions, one with a received chunk, and the device closes that session
- **THEN** the answer is `204`, the staged copy of the session is deleted, a new session opens with `201`, and a chunk for the closed token answers `404`, verified by `services/notes/tests/device_uploads.rs::closed_session_deletes_its_chunks_and_frees_its_slot`

#### Scenario: An unknown or foreign token is closed

- **WHEN** the device closes an unknown token, or a token of another connection
- **THEN** both answers are `404` with `blob_transfer.session_unknown`, and the session of the other connection stays open, verified by `services/notes/tests/device_uploads.rs::closing_unknown_or_foreign_token_is_not_found`

#### Scenario: A site owner closes its upload session

- **WHEN** a `site_owner` claim closes its open session, first without its origin and then with the origin `site:site-a`
- **THEN** the first answer is `401` and the second is `204`, verified by `services/notes/tests/device_uploads.rs::site_owner_closes_its_upload_session`
