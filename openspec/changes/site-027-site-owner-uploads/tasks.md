## 1. Site uploads (workspace tasks 4.1, 4.2)

Workspace task 4.1 expects its first test to fail because the blob route refuses a claim that is
not a primary session or a paired device. That is not the state of this repository: the blob routes
took every session kind except `device` as a session of the owner, and the change and manifest
routes refused every session. The two tests of 4.1 pass when they first run. The behaviour that is
missing is the origin claim of a `site_owner` session, and the third test of 1.1 holds it.
So the guard that tasks 4.1 and 4.2 added is the origin claim: the first two tests of 1.1 passed
at once, and only `site_owner_without_site_origin_is_refused` failed first.

- [x] 1.1 RED: in `services/notes/tests/device_channel.rs`, add `site_owner_upload_registers_blob_only` (an upload with the session kind `site_owner`, the origin `site:site-a` and a declared chunk size of 16 MiB is registered, and no revision and no `notes.file.changed.v1` event follow), `site_owner_cannot_send_change_batches` (a change batch and a manifest with that claim answer `409` `notes.channel_mismatch`) and `site_owner_without_site_origin_is_refused` (no origin, `platform`, `site:` and `site:Site_A` answer `401` `notes.request.unauthenticated`, and no blob is registered). Confirm that the first two pass at once, and that the third fails on its status assertion (`201`, expected `401`), because the blob routes do not read the origin claim.
- [x] 1.2 GREEN: on the blob routes, refuse a `site_owner` claim whose origin is not a `WriteOrigin` that starts with `site:`. Verify 1.1, and verify that the other tests of `device_channel.rs` and `write_content.rs` pass unchanged.

## 2. Close an open upload session (request of the SITE-027 coordinator)

Edge closes the upload session of a site edit after a failed chunk or finalize, so that the session
does not hold one of the four slots of its connection for an hour.

- [x] 2.1 RED: in `services/notes/tests/device_uploads.rs`, add `closed_session_deletes_its_chunks_and_frees_its_slot` (with four open sessions of one connection, `DELETE .../blobs/{token}` answers `204`, the staged copy of the session is deleted, a fifth open answers `201`, and a chunk for the closed token answers `404`), `closing_unknown_or_foreign_token_is_not_found` (an unknown token and a token of another connection answer `404` `blob_transfer.session_unknown`, and the other session stays open) and `site_owner_closes_its_upload_session` (a `site_owner` claim without its origin answers `401`, and with the origin `site:site-a` answers `204`). Confirm that the three fail by assertion, because the route does not exist and the router answers `404` with no body.
- [x] 2.2 GREEN: add `DELETE /v1/notes/connections/{id}/blobs/{token}` with the checks of the other blob routes, and `Uploads::close`, which ends the session, deletes its staged copy and frees its slot. Verify 2.1 and the other tests of `device_uploads.rs`, and document the route in the README.

## 3. A site owner claim reaches only its routes (request of the SITE-027 coordinator)

Notes does not rely only on the route table of Edge. One list in `services/notes/src/api/site_owner.rs`
names the routes that a `site_owner` claim may reach: the blob routes `POST .../blobs`,
`PUT .../chunks/{index}`, `POST .../finalize` and `DELETE .../blobs/{token}`, and
`GET /v1/notes/conflicts/{id}`. Notes has no route that resolves a conflict: a resolution is a write
command.

- [x] 3.1 RED: add `services/notes/tests/site_owner_routes.rs::site_owner_reaches_only_uploads_and_conflicts`, one table row for each entry of the domain router, sent with the claims `site_owner` and `site:site-a`. The listed routes must not answer `401` `notes.request.unauthenticated`, the change and manifest routes answer `409` `notes.channel_mismatch`, and every other route answers `401` `notes.request.unauthenticated`; after the walk the owner still has one connection and it is not removed. Confirm that it fails on its assertion that no row is wrong, because nine routes serve the claim: the upload status, the connection list, the file list, a file read, an index read, the excluded folders (`200`), the write mode (`202`), the internal file list, and the removal of the connection (`204`).
- [x] 3.2 GREEN: add a guard to the domain router that reads the session kind and the matched route. It passes a `site_owner` claim on a listed route, answers `notes.channel_mismatch` on the change and manifest routes, and answers `401` `notes.request.unauthenticated` on every other route. Verify 3.1, and verify that the other tests pass unchanged.

## 4. Review follow-ups (review of SITE-027 in notes, items L1 to L4)

- [x] 4.1 Refactor (L1): move every domain route into `routes()` in `services/notes/src/api.rs`, and apply the guard of the session claims to its result, so that each route is guarded by construction. No test: no behaviour changes; `site_owner_routes.rs` passes unchanged.
- [x] 4.2 RED (L2): add `services/notes/tests/site_owner_routes.rs::unknown_session_kind_is_unauthenticated`. The session kinds `Site_Owner`, `robot` and `anonymous` must get `401` `notes.request.unauthenticated` on the upload open, the file list, the conflict read and the write mode, and the kinds `browser`, `api_token`, `telegram_mini_app` and `service` must still read the file list. Confirm that it fails on its assertion that no row is wrong, because an unknown kind is served as a session of the owner (`201`, `200`, `404` `notes.conflict.not_found`, `202`).
- [x] 4.3 GREEN (L2): make `is_session` an allow-list of the session kinds that Platform mints, and let the guard of the session claims answer `401` `notes.request.unauthenticated` to a kind that is not on it. Verify 4.2 and the other tests unchanged.
- [x] 4.4 Test (L3): add the unit test `services/notes/src/uploads.rs::tests::chunk_that_waited_for_a_closed_session_finds_no_session`. With the lock of a session held, a close and then a chunk wait for it; when the lock is free, the close ends the session and the chunk answers `blob_transfer.session_unknown`. No failing test first: the behaviour exists since task 2.2. Instead, confirm that the test fails when the check of the `closed` flag is removed (the chunk then answers a store error, because its staged copy is deleted).
- [x] 4.5 Test (L4): in `site_owner_routes.rs`, a `Served` row also requires a non-empty code of the handler that is not `notes.channel_mismatch`, so that an empty `404` of the router does not count as served. The upload open row names an unknown connection, so its handler answers `notes.connection.not_found`; `device_channel.rs` covers a site owner upload that is stored. No failing test first: no behaviour changes. Instead, confirm that the test fails when a `Served` row names a path that the router does not have.
