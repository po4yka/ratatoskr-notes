## Context

The workspace design `add-notes-vault-connections` D2, D3, D4, D10 and D11 decides the
cross-repository shape. This file records only the decisions that are local to this repository.

## Decisions

### 1. The owner comes from the claim that Edge mints

Edge forwards the owner as the header `x-ratatoskr-user-id`, a Platform user UUID, as it does for
the other domain upstreams. A request without a valid header answers `401` with
`notes.request.unauthenticated`. The device-kind claim of the `plugin` and `cli` channels is
workspace task 9.2, and its checks are workspace tasks 8.3 and 8.4; this change does not read it.

### 2. A connection is unique among the connections that are not removed

The workspace task says "active connections". A connection whose login is still running is not
yet active, but a second channel for the same vault would already deliver each change twice. The
stricter reading applies: a partial unique index on `(owner_id, vault_key)` covers every state
except `removed`. The states are `connecting`, `active`, `login_expired` and `removed`. A new
connection starts as `connecting`; the connect flow (workspace tasks 5.7 and 6.2) makes it
`active`.

A refused insert reads the existing connection and answers `409` with the code
`notes.connection.vault_already_connected`, and the error envelope carries `connection_id` and
`channel` as additive fields. The unique index decides, so two concurrent requests cannot both
succeed.

### 3. Disconnect cleans up first, then commits once

`DELETE /v1/notes/connections/{id}`:

1. reads the connection of this owner; another owner's connection, or a removed one, answers
   `404` with `notes.connection.not_found`;
2. for a `sync` connection, stops `ratatoskr-notes-sync@<id>.service` and starts
   `ratatoskr-notes-cleanup@<id>.service`, and waits for the oneshot job to finish;
3. in one transaction, deletes the rows of `notes.connection_secrets`, sets the state `removed`
   only if it is not `removed` yet, and writes one `notes.vault.disconnected.v1` envelope to
   `notes.outbox`;
4. answers `204`.

If step 2 fails, nothing is committed, the answer is `503` with the retryable code
`notes.connection.cleanup_failed`, and the owner can repeat the request. The cleanup unit is
idempotent. Two concurrent requests can both run step 2, but only the one whose update changes the
state writes the event, so the event is written once.

`plugin` and `cli` connections have no unit, no working tree and no client state, so step 2 does
nothing for them.

### 4. "Publishes" means the transactional outbox

The event is written to `notes.outbox` in the transaction that removes the connection, and the
test reads it there, as the tests of the other fleet services do. The relay that moves outbox rows
to JetStream under the `NOTES` identity is part of workspace task 7.6 ("the outbox"). The envelope
carries the owner as `tenant_id` `user:<id>`, the aggregate `notes_connection:<id>`, the producer
`ratatoskr-notes`, and correlates to itself as `event:<event id>`, because no operation owns a
disconnect.

### 5. systemd is behind one trait

`SystemdBus` has two methods, `start_unit` and `stop_unit`, and each waits for the job and returns
its result. A unit name is a value of `UnitName`, which can only name one of the three template
units for one connection id, so no caller can ask systemd for another unit. The production
implementation talks to `org.freedesktop.systemd1` on the system bus with `zbus`. It connects on
first use, so a developer machine without systemd starts the service, and only a `sync`
disconnect fails there. A stop of a unit that systemd does not know (`NoSuchUnit`) counts as done.

The test double records each call. When it is asked to start the cleanup unit, it runs the
`ExecStart=` lines of the shipped `deploy/systemd/ratatoskr-notes-cleanup@.service`, with `%i`
replaced, the connections root moved into a temporary directory, and `ob` replaced by a stub that
records its arguments. The test therefore checks the shipped unit, not a copy of its behaviour.

### 6. The cleanup unit

The unit runs as `ratatoskr-notes-sync`, the owner of the working tree and of `ob-home`, with
`XDG_CONFIG_HOME` set to `ob-home` as in design D3. It runs `ob sync-unlink --path <tree>` with
the `-` prefix, because a connection whose setup never finished has no Sync configuration and `ob`
then exits 3. It then deletes the working tree and `ob-home` with `rm -rf`. The `ob` path is
`/usr/bin/ob`, where a global `npm` install puts it on Debian; workspace task 14.7 pins the
package and confirms the path.

### 7. Error codes of this repository

`notes.connection.vault_already_connected` and `notes.connection.not_found` are named by the
workspace documents. `notes.request.unauthenticated`, `notes.request.invalid`,
`notes.connection.cleanup_failed` and `notes.service.unavailable` follow the pattern of the other
domain services. They are local codes and no other repository branches on them.

## Risks / Trade-offs

- The D-Bus implementation has no automated test in this change: neither macOS nor the CI runner
  gives the test an unprivileged system bus with the three units. Workspace task 6.2 adds the
  polkit rule test, and the Molecule case of task 14.6 runs the units on a host.
- An outbox row waits for the relay of task 7.6 before it reaches the bus.
