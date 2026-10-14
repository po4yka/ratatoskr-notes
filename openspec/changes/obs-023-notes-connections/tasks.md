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
