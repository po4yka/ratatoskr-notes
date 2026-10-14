## Why

The workspace change `add-notes-vault-connections` (OBS-023) gives this repository its first
behaviour: an owner connects an Obsidian vault through exactly one channel, and removes the
connection again. The cross-repository rules are the workspace spec `notes-vault-connections`,
requirements "A vault connection has exactly one channel", "The login runs as a oneshot unit that
receives secrets as credentials", "Secrets are kept as ciphertext and a failed login has a stable
code" and "Connection events name the connection and never its secrets". This change implements
the part of them that belongs to group 5 of the workspace task list, and does not restate them.

## What Changes

- Add the table `notes.connections` with the channel, the vault key, the display name, the owner
  and the state, and the uniqueness of `(owner, vault key)` among the connections that are not
  removed.
- Add `POST /v1/notes/connections` on the domain listener. A second connection for the same owner
  and vault key is refused with `409` and `notes.connection.vault_already_connected`, which names
  the existing connection and its channel.
- Add `DELETE /v1/notes/connections/{id}`. For a `sync` connection it stops the sync unit and runs
  the cleanup unit through the systemd D-Bus API. For every channel it deletes the stored
  ciphertext, marks the connection removed, and writes one `notes.vault.disconnected.v1` event to
  the outbox in the same transaction.
- Ship `deploy/systemd/ratatoskr-notes-cleanup@.service`, which runs `ob sync-unlink` and deletes
  the working tree and the client state of the connection.
- Later, in the same change: secret custody, the connect flow and the login unit (workspace tasks
  5.6 to 5.9), and the documentation (5.10).
- Group 6 (workspace tasks 6.1 to 6.10): ship `deploy/systemd/ratatoskr-notes-sync@.service`, which
  runs `ob sync --continuous` in pull-only mode; the Sync supervisor, which starts the unit after
  the first login and at each start of the service, and marks a connection `login_expired` when the
  journal of its unit shows `Failed to authenticate:` for 5 minutes; the inotify watcher of a
  working tree with its settle window and rescan; and the blob store, whose blobs only the read
  group `ratatoskr-notes-blobs` can read. Workspace tasks 6.5 and 6.6 wait for the pull-only
  session that spike 3.3 records.
- Review follow-ups of group 6 (workspace tasks 6.11 to 6.18) and the owner decisions of
  2026-10-14: the sync units log to the journal namespace `ratatoskr-notes-sync`, which the service
  checks at start and follows with one follower that starts again after a backoff; the watcher reads
  a working tree only beneath its root and never through a symbolic link; the blob store refuses a
  umask stricter than 0027; and the working trees and the client states move to the split roots
  `/mnt/nvme/ratatoskr/notes/trees/<id>` and `/mnt/nvme/ratatoskr/notes/clients/<id>`.

## Capabilities

### New Capabilities

- `connection-lifecycle`: the routes that create and remove a connection on the domain listener,
  their answers, and what a removal leaves behind in this repository.

### Modified Capabilities

None.

## Impact

`schema.sql` (connections, secrets, outbox), `crates/persistence` (connection queries),
`services/notes` (the routes, the systemd trait and its D-Bus implementation, the supervisor, the
watcher and the blob store), `deploy/systemd/ratatoskr-notes-cleanup@.service`,
`ratatoskr-notes-login@.service` and `ratatoskr-notes-sync@.service`. The routes are reached only through Edge, which
adds them in workspace task 9.2.
