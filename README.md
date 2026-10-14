# Ratatoskr Notes

`ratatoskr-notes` is the notes bounded context of Ratatoskr. It connects the owner's Obsidian
vaults, keeps a complete and current copy of each vault, records a revision for each change of a
file, and indexes each Markdown file. Knowledge reads the note bytes and the file events to search
and analyse the notes.

> **Status:** in development. The service starts, applies the `notes` schema, serves its operator
> plane, connects and removes vaults, and runs the first Obsidian login of a `sync` connection. The
> Sync supervisor, the index and the device channels are planned in the workspace change
> `add-notes-vault-connections` (OBS-023), groups 6 to 8.

> [!IMPORTANT]
> **Ratatoskr is in development.** No database holds data that has to survive a schema change.
> While this status holds, these two rules replace what the documents below plan:
>
> - the API and the database keep their first version. There is no `v2` and no later major
>   version.
> - the database has no migrations. One schema definition exists, and a schema change edits it in
>   place.
>
> Only the repository owner changes this status.

## Role in Ratatoskr

A **vault connection** connects one Obsidian vault of one owner through exactly one channel:

| Channel | Runs where | Use for |
|---|---|---|
| `sync` | On the host: `ob sync --continuous` under a systemd template unit, one per connection | A vault that uses Obsidian Sync |
| `plugin` | The Ratatoskr Obsidian plugin on the owner's device | A vault without Sync |
| `cli` | `ratatoskr-export-agent` on macOS | A vault without Sync, without the plugin |

Notes owns the connections, the encrypted Obsidian secrets of a `sync` connection, the files, the
revisions and their bytes, and the index. It does not own identity, device pairing, analysis or
search.

The design is in `ratatoskr-workspace`: ADR-0010, `docs/NOTES_AND_SITES.md`, and the change
`openspec/changes/add-notes-vault-connections/`.

## Interfaces

| Interface | Address | Reached by |
|---|---|---|
| Domain API `/v1/notes` | `127.0.0.1:8099` (`RATATOSKR__API__LISTEN_ADDRESS`) | Edge only |
| Operator plane `/health/live`, `/health/ready`, `/metrics`, `/version` | `127.0.0.1:9471` (`RATATOSKR__ADMIN__BIND`) | The host only |
| Database | schema `notes`, role `ratatoskr_notes`, database `ratatoskr` | This service only |

The domain API serves `POST /v1/notes/connections` and `DELETE /v1/notes/connections/{id}`.

## Connections

`POST /v1/notes/connections` takes `channel`, `vault_key` and `vault_display_name`. The owner is
the claim that Edge mints. One vault of one owner has one connection that is not removed: a second
connection for the same vault key answers `409` with `notes.connection.vault_already_connected`,
whatever its channel. Two owners can connect the same remote vault.

A `sync` connection also needs `sync_login`: `email`, `password`, and the optional `mfa_code` and
`e2e_password`. Its vault key is the remote vault id. The service runs the first login before it
answers:

- `202` with the state `connecting` when the login and the setup succeed and the sync unit of the
  connection started;
- `422` with one of the stable codes `notes.sync.login_failed`, `notes.sync.mfa_required`,
  `notes.sync.e2e_password_wrong` or `notes.sync.vault_not_found` when they fail. The connection
  then becomes `failed`, keeps no ciphertext, and does not hold the vault key, so the owner can try
  again.

When the client goes away while the login runs, the connect continues in its own task: it stops
the login unit, runs the cleanup unit, and marks the connection `failed`, as after a failed login.
A SIGTERM does the same for each login that runs, within `RATATOSKR__SHUTDOWN__GRACE_SECONDS`, and
the connect request answers `503`. When the grace window ends first, the connection stays
`connecting` with its ciphertext. The next start of the service then does these steps for it before
it accepts a request. The start does not touch a connection whose login finished.

When the owner removes the connection before its login starts, no login runs, and the connect
answers `404` with `notes.connection.not_found`. At most `RATATOSKR__CONNECTIONS__MAX_LOGINS` logins
run at the same time, 2 by default. A `sync` connect over that limit answers `503` with
`notes.service.unavailable` and creates nothing, and the client can try again later.

`DELETE /v1/notes/connections/{id}` stops the sync unit and a login unit that still runs, runs the
cleanup unit, and then, in one transaction, marks the connection removed, deletes its ciphertext,
and writes `notes.vault.disconnected.v1` to the outbox. When the cleanup fails, nothing is committed and the
owner can send the request again.

## Template units

The service starts and stops the units through the systemd D-Bus API as the user
`ratatoskr-notes`. The instance name of each unit is the connection id. Each unit runs `ob` as
`ratatoskr-notes-sync`, which owns the client state (`ob-home`) and the working tree under
`/mnt/nvme/ratatoskr/notes/connections/<id>/`.

| Unit | Type | Started | Does |
|---|---|---|---|
| `ratatoskr-notes-login@.service` | oneshot | at the connect of a `sync` connection | `ob login`, then `ob sync-setup`. The exit status names the failure: 10 login failed, 11 MFA code needed, 12 end-to-end password wrong, 13 no such remote vault. |
| `ratatoskr-notes-sync@.service` | long-running | by the Sync supervisor, after the first login and at each start of the service | `ob sync-config --mode pull-only --configs ""` with the device name `ratatoskr-<id>`, then `ob sync --continuous`. It is stopped at a disconnect. `MemoryMax=512M` is provisional until spike 3.1 measures it. |
| `ratatoskr-notes-cleanup@.service` | oneshot | at a disconnect, and after a failed login | `ob sync-unlink`, then removes the working tree and `ob-home`. |

The three units are in `deploy/systemd/`. None of them is enabled: only the service starts them.
The sync unit runs in pull-only mode, so Notes never writes into a vault.

An expired or revoked Obsidian login does not stop `ob sync --continuous`: it logs
`Error: Failed to authenticate: ...` and tries again. The service reads that one line from the
journal of each sync unit, and no other. When the line repeats for 5 minutes, the connection
becomes `login_expired` with the code `notes.sync.login_expired`, and the service stops the unit.
It starts no login: the owner removes the connection and connects again. The line comes from
`obsidian-headless` 0.0.14 and is provisional until spike 3.1 sees a real expired token. The
service user must be a member of the group `systemd-journal` to read the journal.

## Blob root and its read group

The bytes of each revision are in the blob root `/mnt/nvme/ratatoskr/blobs/ratatoskr-notes`, at
`sha256/<first two hex digits>/<other 62 hex digits>` of their SHA-256. Knowledge reads them there,
read-only, as the fleet spec `blob-references` allows. No other service may read them.

- The deploy role of `ratatoskr-workspace` creates the system group `ratatoskr-notes-blobs`, makes
  the Knowledge service user its only member, and creates the root with the owner
  `ratatoskr-notes`, that group, and mode 2750.
- The service writes each blob with mode 0640 and each new directory with mode 2750, whatever its
  umask. The setgid bit gives each new file and directory the group of the root, so the read group
  can read a blob and no other user can. The service unit runs the service with `UMask=0027`.
- A later consumer, for example `ratatoskr-sites`, joins the group in its own change.

`services/notes/tests/blob_root.rs` checks the modes and the group of a new blob.

## Secret custody

The Obsidian account password and the end-to-end password stay in this service. The MFA code is a
one-time code and is never stored.

- **At rest:** each password is sealed with XChaCha20-Poly1305 under the key in
  `/etc/ratatoskr/notes-secrets.key` (32 bytes, mode 0400, owner `ratatoskr-notes`), with a random
  nonce and the connection id and the kind as associated data. The service opens the key file
  without following a symbolic link, and refuses it when it is open to other users or has another
  owner. `notes.connection_secrets` holds
  only the ciphertext. A failed login or a disconnect deletes it.
- **To the login:** the service writes the email, the two passwords, the MFA code and the vault id
  to files in `/run/ratatoskr-notes/credentials/<id>/` (directory 0700, files 0600), starts the
  login unit, and deletes the files when the unit stops, whatever its result. At start, the
  service removes `/run/ratatoskr-notes/credentials`, so that no files of a login that a crash
  interrupted stay there. The unit loads them
  with `LoadCredential=`. `ob` reads each password on standard input from
  `$CREDENTIALS_DIRECTORY`. No password is in the command line of a process. Only the email, the
  vault id and the MFA code are in the arguments of `ob`, and the service refuses each of them
  when it starts with `-`, so that `ob` cannot read it as an option.
- **In output:** no secret is in a log line, an event, an error envelope or the journal of a unit.
  Each secret is a `SecretString`, and a refused request body is not logged.

`services/notes/tests/secret_custody.rs` checks each of these points with marker secrets.

## Project status

- [x] Service skeleton: strict configuration, logging, operator plane, embedded schema, CI gate.
- [x] Connections and the one-channel rule (tasks 5.2 to 5.5).
- [x] Secret custody and the login unit (tasks 5.6 to 5.9), and its review follow-ups (tasks 5.12
  to 5.15).
- [ ] Sync supervisor and blob root (group 6).
- [ ] Index, file events and the read API (group 7).
- [ ] Device channel routes (group 8).

## Development

See [DEVELOPMENT.md](DEVELOPMENT.md). The local development database is a convenience of
`compose.yaml`; on the host the service uses the shared database `ratatoskr`.

## Licence

BSD-3-Clause. See [LICENSE](LICENSE).
