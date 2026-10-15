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

### Requirement: A site owner claim reaches only the upload and conflict routes

The domain router SHALL serve a request with the session kind `site_owner` only on the blob routes
`POST /v1/notes/connections/{id}/blobs`, `PUT .../blobs/{token}/chunks/{index}`,
`POST .../blobs/{token}/finalize` and `DELETE .../blobs/{token}`, and on
`GET /v1/notes/conflicts/{id}`. The change batch and manifest routes SHALL refuse it with `409` and
`notes.channel_mismatch`. Every other route SHALL refuse it with `401` and
`notes.request.unauthenticated`, and SHALL change nothing.

#### Scenario: A site owner claim walks the domain router

- **WHEN** a request with the session kind `site_owner` and the origin `site:site-a` is sent to each entry of the domain router
- **THEN** the upload and conflict routes are served, the change and manifest routes answer `409` `notes.channel_mismatch`, every other route answers `401` `notes.request.unauthenticated`, and the connection of the owner is not removed, verified by `services/notes/tests/site_owner_routes.rs::site_owner_reaches_only_uploads_and_conflicts`

### Requirement: An unknown session kind is unauthenticated

The domain router SHALL serve a request whose session kind claim is present only when the kind is
one that Platform mints: `browser`, `device`, `telegram_mini_app`, `service`, `api_token` or
`site_owner`. Any other kind, a different spelling of a known kind included, SHALL be refused on
every route with `401` and `notes.request.unauthenticated`.

#### Scenario: A request names an unknown session kind

- **WHEN** a request with the session kind `Site_Owner`, `robot` or `anonymous` reaches the upload open, the file list, the conflict read or the write mode
- **THEN** each answer is `401` with `notes.request.unauthenticated`, and the kinds `browser`, `api_token`, `telegram_mini_app` and `service` still read the file list, verified by `services/notes/tests/site_owner_routes.rs::unknown_session_kind_is_unauthenticated`
