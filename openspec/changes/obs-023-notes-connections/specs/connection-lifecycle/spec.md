## ADDED Requirements

### Requirement: A connection is created on the domain listener and is unique per owner and vault key

`POST /v1/notes/connections` SHALL create a connection for the owner that the `x-ratatoskr-user-id`
claim names, with the channel, the vault key and the vault display name of the body, and SHALL
answer `202` with the connection id. It SHALL refuse a second connection for the same owner and
vault key while the first one is not removed, as the workspace spec `notes-vault-connections`
requires, and SHALL accept the same vault key for a different owner.

#### Scenario: A plugin connection after a sync connection with the same vault key

- **WHEN** an owner has a `sync` connection for a vault key and sends a `plugin` connection for the same vault key
- **THEN** the answer is `409` with `notes.connection.vault_already_connected`, the existing connection id and `channel = sync`, and the owner still has one connection, verified by `services/notes/tests/connections.rs::second_channel_for_one_vault_is_refused`

#### Scenario: Two owners connect one remote vault

- **WHEN** two owners each send a `sync` connection with the same remote vault id
- **THEN** both answers are `202` with two different connection ids, verified by `services/notes/tests/connections.rs::two_owners_may_connect_one_remote_vault`

### Requirement: Disconnect removes the unit, the tree, the client state and the secrets

`DELETE /v1/notes/connections/{id}` SHALL answer `404` for a connection of another owner. For a
`sync` connection it SHALL stop `ratatoskr-notes-sync@<id>.service` and start
`ratatoskr-notes-cleanup@<id>.service`, which deletes the working tree and the client state. It
SHALL delete the stored ciphertext, mark the connection removed and write exactly one
`notes.vault.disconnected.v1` event to the outbox in one transaction, and then answer `204`.

#### Scenario: A sync connection is removed

- **WHEN** an owner removes a `sync` connection that has a working tree, client state and stored ciphertext
- **THEN** the fake systemd bus records one stop of the sync unit and one start of the cleanup unit, the working tree, the client state and the ciphertext are gone, and the outbox holds one `notes.vault.disconnected.v1` event for the connection, verified by `services/notes/tests/connections.rs::disconnect_removes_unit_tree_state_and_secrets`
