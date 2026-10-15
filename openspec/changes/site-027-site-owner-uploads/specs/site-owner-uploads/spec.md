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
