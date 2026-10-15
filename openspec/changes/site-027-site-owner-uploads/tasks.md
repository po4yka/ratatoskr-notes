## 1. Site uploads (workspace tasks 4.1, 4.2)

Workspace task 4.1 expects its first test to fail because the blob route refuses a claim that is
not a primary session or a paired device. That is not the state of this repository: the blob routes
took every session kind except `device` as a session of the owner, and the change and manifest
routes refused every session. The two tests of 4.1 pass when they first run. The behaviour that is
missing is the origin claim of a `site_owner` session, and the third test of 1.1 holds it.

- [x] 1.1 RED: in `services/notes/tests/device_channel.rs`, add `site_owner_upload_registers_blob_only` (an upload with the session kind `site_owner`, the origin `site:site-a` and a declared chunk size of 16 MiB is registered, and no revision and no `notes.file.changed.v1` event follow), `site_owner_cannot_send_change_batches` (a change batch and a manifest with that claim answer `409` `notes.channel_mismatch`) and `site_owner_without_site_origin_is_refused` (no origin, `platform`, `site:` and `site:Site_A` answer `401` `notes.request.unauthenticated`, and no blob is registered). Confirm that the first two pass at once, and that the third fails on its status assertion (`201`, expected `401`), because the blob routes do not read the origin claim.
- [x] 1.2 GREEN: on the blob routes, refuse a `site_owner` claim whose origin is not a `WriteOrigin` that starts with `site:`. Verify 1.1, and verify that the other tests of `device_channel.rs` and `write_content.rs` pass unchanged.
