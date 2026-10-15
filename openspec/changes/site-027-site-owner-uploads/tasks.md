## 1. Site uploads (workspace tasks 4.1, 4.2)

Workspace task 4.1 expects its first test to fail because the blob route refuses a claim that is
not a primary session or a paired device. That is not the state of this repository: the blob routes
took every session kind except `device` as a session of the owner, and the change and manifest
routes refused every session. The two tests of 4.1 pass when they first run. The behaviour that is
missing is the origin claim of a `site_owner` session, and the third test of 1.1 holds it.

- [x] 1.1 RED: in `services/notes/tests/device_channel.rs`, add `site_owner_upload_registers_blob_only` (an upload with the session kind `site_owner`, the origin `site:site-a` and a declared chunk size of 16 MiB is registered, and no revision and no `notes.file.changed.v1` event follow), `site_owner_cannot_send_change_batches` (a change batch and a manifest with that claim answer `409` `notes.channel_mismatch`) and `site_owner_without_site_origin_is_refused` (no origin, `platform`, `site:` and `site:Site_A` answer `401` `notes.request.unauthenticated`, and no blob is registered). Confirm that the first two pass at once, and that the third fails on its status assertion (`201`, expected `401`), because the blob routes do not read the origin claim.
- [x] 1.2 GREEN: on the blob routes, refuse a `site_owner` claim whose origin is not a `WriteOrigin` that starts with `site:`. Verify 1.1, and verify that the other tests of `device_channel.rs` and `write_content.rs` pass unchanged.

## 2. Close an open upload session (request of the SITE-027 coordinator)

Edge closes the upload session of a site edit after a failed chunk or finalize, so that the session
does not hold one of the four slots of its connection for an hour.

- [x] 2.1 RED: in `services/notes/tests/device_uploads.rs`, add `closed_session_deletes_its_chunks_and_frees_its_slot` (with four open sessions of one connection, `DELETE .../blobs/{token}` answers `204`, the staged copy of the session is deleted, a fifth open answers `201`, and a chunk for the closed token answers `404`), `closing_unknown_or_foreign_token_is_not_found` (an unknown token and a token of another connection answer `404` `blob_transfer.session_unknown`, and the other session stays open) and `site_owner_closes_its_upload_session` (a `site_owner` claim without its origin answers `401`, and with the origin `site:site-a` answers `204`). Confirm that the three fail by assertion, because the route does not exist and the router answers `404` with no body.
- [ ] 2.2 GREEN: add `DELETE /v1/notes/connections/{id}/blobs/{token}` with the checks of the other blob routes, and `Uploads::close`, which ends the session, deletes its staged copy and frees its slot. Verify 2.1 and the other tests of `device_uploads.rs`, and document the route in the README.
