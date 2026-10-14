## 1. Connections and the one-channel rule (workspace tasks 5.2, 5.3)

- [x] 1.1 RED: add `services/notes/tests/connections.rs::second_channel_for_one_vault_is_refused`, which asserts `409` with `notes.connection.vault_already_connected` for a `plugin` connection after a `sync` connection with the same vault key, and `two_owners_may_connect_one_remote_vault`, which asserts that two owners each get a connection for one remote vault id. Run them and confirm that both fail because the route does not exist (`404`).
- [x] 1.2 GREEN: add `notes.connections`, the uniqueness of `(owner, vault_key)` among the connections that are not removed, and `POST /v1/notes/connections`. Verify both tests of 1.1.

## 2. Disconnect (workspace tasks 5.4, 5.5)

- [x] 2.1 RED: add `services/notes/tests/connections.rs::disconnect_removes_unit_tree_state_and_secrets`, which asserts that disconnect writes one `notes.vault.disconnected.v1` event, stops the sync unit and starts the cleanup unit through a fake systemd bus, and that the tree, the client state and the ciphertext are gone. Run it and confirm that it fails because disconnect does not exist (`404`).
- [x] 2.2 GREEN: implement disconnect, the outbox, the `SystemdBus` trait with its D-Bus implementation, and ship `deploy/systemd/ratatoskr-notes-cleanup@.service`. Verify 2.1.

## 3. Secret custody and the login unit (workspace tasks 5.6 to 5.9)

- [x] 3.1 RED: `services/notes/tests/secret_custody.rs::marker_secrets_appear_nowhere_in_notes`, `wrong_e2e_password_fails_with_its_code` and `stored_secret_is_ciphertext_without_mfa`, as workspace task 5.6 words them. Confirm that all three fail because the connect flow does not exist.
- [x] 3.2 GREEN: the encryption with the key file and the connect flow with redacted errors and the four stable login codes (workspace task 5.7). Verify the three tests of 3.1.
- [x] 3.3 RED: `services/notes/tests/secret_custody.rs::login_unit_gets_secrets_only_as_credentials` and `no_secret_in_any_cmdline`, as workspace task 5.8 words them. Confirm that both fail because the login unit does not exist.
- [x] 3.4 GREEN: ship `deploy/systemd/ratatoskr-notes-login@.service` and the credential files (workspace task 5.9). Verify both tests of 3.3.

## 4. Documentation (workspace task 5.10)

- [x] 4.1 Write the README and `DEVELOPMENT.md` sections on connections, the three template units and secret custody. No test: documentation.

## 5. Review follow-ups (workspace tasks 5.12 to 5.19)

- [x] 5.1 RED: in `services/notes/tests/secret_custody.rs`, add `each_login_failure_has_its_code` (the stub `ob` refuses the login, needs an MFA code, or finds no vault: `notes.sync.login_failed`, `notes.sync.mfa_required`, `notes.sync.vault_not_found`), `unsafe_key_file_is_refused` (mode 0644, 31 bytes, a symbolic link), `key_file_of_another_owner_is_refused` (Linux only, as root) and `leading_dash_is_refused` (the email, the vault id and the MFA code), as workspace task 5.12 words them. Confirm that the symbolic link, the owner and the leading `-` cases fail.
- [x] 5.2 GREEN: open the key file once without following a link, and check the mode and the owner on the open file; refuse a leading `-` in the email, the vault id and the MFA code (workspace task 5.13). Verify 5.1.
- [x] 5.3 RED: add `start_removes_stale_credentials` and `cancelled_connect_releases_the_vault`, as workspace task 5.14 words them, and make `disconnect_removes_unit_tree_state_and_secrets` expect that a disconnect also stops the login unit. Confirm that the three fail.
- [x] 5.4 GREEN: remove the credentials root at start, finish a connect whose request is dropped in a task that the request does not own, and stop the login unit at a disconnect and after a failed login (workspace task 5.15). Verify 5.3.
- [x] 5.5 RED: in `services/notes/tests/connections.rs`, add `shutdown_during_login_fails_the_connection` (the stop of the service while a login runs stops the login unit, runs the cleanup unit, deletes the ciphertext and marks the connection `failed` in the grace window), `next_start_fails_an_interrupted_login` (when the grace window ends first, the next start does these steps for each `sync` connection whose login did not finish, and leaves a finished login and a `plugin` connection alone), `connect_after_delete_starts_no_login` (a disconnect between the insert and the start of the login: no login unit starts) and `logins_are_bounded` (a connect over the limit answers `503` with `notes.service.unavailable` at once), as workspace task 5.16 words them. Confirm that the four fail by assertion.
- [x] 5.6 GREEN: track the connect tasks and fail them in the grace window, fail each interrupted login at start before the listeners accept, check the row state before the login starts and again after it, and limit concurrent logins with a semaphore, `RATATOSKR__CONNECTIONS__MAX_LOGINS` (workspace task 5.17). Verify 5.5.
- [x] 5.7 RED: in `services/notes/tests/connections.rs`, add `serve_shuts_down_logins_on_stop` (the real `serve()` with the fake bus and the compose database: a stop signal while a login is held fails the connection, stops the login unit and runs the cleanup unit), `serve_reconciles_before_it_accepts` (an interrupted login is failed before `serve()` accepts the first request) and `delete_during_login_answers_not_found` (a disconnect while the login unit runs: the connect answers `404` with `notes.connection.not_found`, not `422` with `notes.sync.login_failed`), as workspace task 5.18 words them. `serve()` takes the bus, the credentials directory and the stop signal, but does not use them yet. Confirm that the three fail by assertion.
- [x] 5.8 GREEN: connect the bus, the credentials directory and the stop signal of `serve()`, and keep SIGTERM and SIGINT in production; answer `404` when a disconnect stopped the login of its connection; state in `DEVELOPMENT.md` that a database made before the `logged_in_at` column needs its `notes` schema dropped and made again (workspace task 5.19). Verify 5.7.

## 6. Sync channel and blob root (workspace tasks 6.1 to 6.10)

The numbers of this group are the numbers of the workspace tasks.

- [x] 6.1 RED: `services/notes/tests/sync_supervisor.rs::connect_starts_one_unit_in_pull_only_mode`, as workspace task 6.1 words it: with a stub `ob` that records its calls, a connect calls `sync-config --mode pull-only --configs ""` with the device name `ratatoskr-<id>` and starts `ratatoskr-notes-sync@<id>.service`, and the next start of the service starts the same unit again. Confirm that it fails because the supervisor does not exist.
- [x] 6.2 GREEN: implement the supervisor through the systemd D-Bus API, and ship `deploy/systemd/ratatoskr-notes-sync@.service` with a provisional `MemoryMax=` until spike 3.1 measures it. The polkit rule is in the deploy role of `ratatoskr-workspace`, which must add the sync unit to `ratatoskr_template_units`. Verify 6.1.
- [x] 6.3 RED: `services/notes/tests/sync_watcher.rs::settled_change_becomes_one_revision`, as workspace task 6.3 words it. Confirm that it fails because the watcher does not exist.
- [x] 6.4 GREEN: implement the inotify watcher, the settle window and the 15-minute rescan. Verify 6.3.
- [ ] 6.5 RED: workspace task 6.5. It waits for the pull-only session that spike 3.3 records.
- [ ] 6.6 GREEN: workspace task 6.6. It waits for 6.5.
- [x] 6.7 RED: `services/notes/tests/blob_root.rs::new_blob_is_group_readable_and_not_world_readable`, as workspace task 6.7 words it. Confirm that it fails because the service writes with the default umask.
- [x] 6.8 GREEN: write each blob with mode 0640 and each directory with mode 2750, and keep the setgid bit on new directories. Verify 6.7, and document the read group in the README.
- [x] 6.9 RED: `services/notes/tests/sync_supervisor.rs::auth_failure_stops_the_unit_and_waits_for_the_owner`, as workspace task 6.9 words it. Confirm that it fails because the connection stays `active`.
- [x] 6.10 GREEN: read the journal of the sync unit for the line `Failed to authenticate:` only, stop the unit after 5 minutes of repeats, and set the state. Verify 6.9.

## 6 (continued). Review follow-ups of group 6 and the owner decisions of 2026-10-14 (workspace tasks 6.11 to 6.18)

Owner decision A: the sync units log to the journal namespace `ratatoskr-notes-sync`, and the
service reads only that namespace. Owner decision B: the working tree of a connection is
`/mnt/nvme/ratatoskr/notes/trees/<id>` (owner `ratatoskr-notes-sync`, group
`ratatoskr-notes-trees`, mode 2750) and its client state is `/mnt/nvme/ratatoskr/notes/clients/<id>`
(mode 0700).

- [x] 6.11 RED: in `services/notes/tests/sync_supervisor.rs`, add `unreadable_journal_is_reported_at_start` (the fake journal cannot be read: the start of the service logs the code `notes.sync.journal_unreadable` and the reason that `journalctl` gives) and `ended_follower_is_started_again` (the journal follower ends while a connection is `active`: the service follows the journal again after a backoff, and an authentication failure for 5 minutes after that still expires the login and stops the unit once). Confirm that both fail by assertion.
- [x] 6.12 GREEN: at start, check that the service can read the journal namespace `ratatoskr-notes-sync`, and log `notes.sync.journal_unreadable` with the stderr of `journalctl` when it cannot; keep the stderr of the follower in the log. Replace the follower of each unit with one follower of the namespace, which sends each line to its connection by the trusted field `_SYSTEMD_UNIT`, and start it again after a backoff of 1 second to 1 minute until the service stops. Add `LogNamespace=ratatoskr-notes-sync` to the sync unit. Also review item L5: mark the journal line in the sync unit PROVISIONAL. Verify 6.11 and 6.9.
- [x] 6.13 RED: add `services/notes/tests/sync_watcher.rs::linked_folder_does_not_leak_outside_content` (Linux only): `ob` moves a watched folder out of the tree and puts a symbolic link to it in its place, and a file is then written in the moved folder. No revision and no blob may come from the content outside the tree. Confirm that it fails by assertion.
- [ ] 6.14 GREEN: open each file of a tree with `openat2` and `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS` on Linux (on other platforms, open each component with `O_NOFOLLOW`), take its state with `fstat` on the open file, compare that state with the settled state, and copy the bytes from the same file. Also review item L3: after a folder event, scan only that folder, and run each scan on the blocking pool. Verify 6.13 and 6.3.
- [ ] 6.15 RED: add `services/notes/tests/blob_setgid.rs::strict_umask_cannot_drop_setgid` (Linux only): the blob root belongs to a group that the service user is not a member of, and the umask is 0077. `BlobStore::open` must refuse the root, or each new directory must keep mode 2750. Confirm that it fails by assertion.
- [ ] 6.16 GREEN: `BlobStore::open` refuses to open the blob root when the umask is stricter than 0027, so the start of the service fails before a `chmod` can remove the setgid bit. Also review item L4: sync the blob directory after the link, and delete stale `.part` files when the store opens. Verify 6.15 and 6.7.
- [ ] 6.17 RED: add `services/notes/tests/unit_files.rs` for owner decisions A and B: the sync unit has `LogNamespace=ratatoskr-notes-sync`; the sync and login units have `UMask=0027`; each unit uses the tree `/mnt/nvme/ratatoskr/notes/trees/<id>` and the client state `/mnt/nvme/ratatoskr/notes/clients/<id>` in its commands, its environment and its `ReadWritePaths=`; and `systemd::tree_dir` and `systemd::client_dir` give the same paths. Confirm that it fails by assertion.
- [ ] 6.18 GREEN: apply decisions A and B to the three units, the fake bus, the tests and the documentation, and record what the deploy role of `ratatoskr-workspace` must add. Verify 6.17 and every test of the units.
