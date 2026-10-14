# Ratatoskr Notes

`ratatoskr-notes` is the notes bounded context of Ratatoskr. It connects the owner's Obsidian
vaults, keeps a complete and current copy of each vault, records a revision for each change of a
file, and indexes each Markdown file. Knowledge reads the note bytes and the file events to search
and analyse the notes.

> **Status:** in development. The service starts, applies the `notes` schema, serves its operator
> plane, connects and removes vaults with the connect operation, runs the Obsidian login and the
> Sync supervisor of a `sync` connection, records each file revision with its event and its index,
> relays its outbox to NATS, serves the read routes, and takes the change batches, the uploads and
> the manifests of the `plugin` and `cli` device channels.

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

The domain API serves `POST /v1/notes/connections` and `DELETE /v1/notes/connections/{id}`,
`PUT /v1/notes/connections/{id}/excluded-folders`, and the read routes `GET /v1/notes/connections`,
`GET /v1/notes/connections/{id}/files`, `GET /v1/notes/connections/{id}/files/{path}` (the bytes,
also at `?revision=<sha256>`) and `GET /v1/notes/connections/{id}/index/{path}` (the index with its
links and backlinks). The device channels use `POST /v1/notes/connections/{id}/changes`,
`POST /v1/notes/connections/{id}/manifest` and the upload routes under
`/v1/notes/connections/{id}/blobs` (see Device channels). Every route answers only for the owner in
the claim of Edge.

## Connections

`POST /v1/notes/connections` takes `channel`, `vault_key` and `vault_display_name`. The owner is
the claim that Edge mints. One vault of one owner has one connection that is not removed: a second
connection for the same vault key answers `409` with `notes.connection.vault_already_connected`,
whatever its channel. Two owners can connect the same remote vault.

Each connect is an operation of kind `notes.connection.connect` that Edge mints. Edge sends its id
in `x-ratatoskr-operation-id`; a connect without it answers `400` `notes.request.invalid`. The
connect answers `202` at once with the connection id, the channel, the state `connecting` and the
operation (its id, its kind and the lane `ratatoskr-notes`). Notes is the only reporter of the
operation: it writes each report as `platform.operation.reported.v1` to its outbox.

A `sync` connection also needs `sync_login`: `email`, `password`, and the optional `mfa_code` and
`e2e_password`. Its vault key is the remote vault id. After the answer, a task runs the first
login, starts the sync unit and watches the working tree, and reports the phases `login`, `setup`,
`first_sync` and `indexing`. When the first full scan of the tree is indexed, one transaction makes
the connection `active` and writes `notes.vault.connected.v1` and the report `succeeded`
(`partially_succeeded` with the warning `notes.sync.journal_unreadable` when the service cannot
read the journal of the sync units). A failed login reports `failed` with one of the stable codes
`notes.sync.login_failed`, `notes.sync.mfa_required`, `notes.sync.e2e_password_wrong` or
`notes.sync.vault_not_found`. The connection then becomes `failed`, keeps no ciphertext, and does
not hold the vault key, so the owner can try again. An expired login before the first scan reports
`notes.sync.login_expired`.

A SIGTERM stops each login that runs, runs the cleanup unit and fails the connection, within
`RATATOSKR__SHUTDOWN__GRACE_SECONDS`, and the operation fails with `notes.service.unavailable`.
When the grace window ends first, the connection stays `connecting` with its ciphertext. The next
start of the service then does these steps for it, and fails its operation, before it accepts a
request. The start does not touch a connection whose login finished.

When the owner removes the connection before or while its login runs, the operation fails with
`notes.connection.not_found`. At most `RATATOSKR__CONNECTIONS__MAX_LOGINS` logins run at the same
time, 2 by default. A `sync` connect over that limit answers `503` with `notes.service.unavailable`
and creates nothing, and the client can try again later. A `plugin` or `cli` connect reports
`first_sync`; the first applied manifest of its device completes it. When no manifest is applied
within 30 minutes of the connect, the connection becomes `failed` and the operation fails with
`notes.channel.manifest_missing`. The wait continues after a restart of the service. Each report
correlates to `operation:<operation id>`.

A `plugin` or `cli` connect comes from a paired device. Edge names the device in
`x-ratatoskr-device-id` and its kind in `x-ratatoskr-device-kind`. Without both claims the connect
answers `401` with `notes.request.unauthenticated`. The kind must be `obsidian_plugin` for
`plugin` and `export_agent` for `cli`; another kind answers `409` with `notes.channel_mismatch`.
The connection records its device, the only one whose changes it takes (workspace design D5).
A connect from a second device of the same channel with the vault key of that connection answers
`409` with `notes.channel_mismatch` and the hint to Obsidian Sync.

`DELETE /v1/notes/connections/{id}` stops the sync unit and a login unit that still runs, runs the
cleanup unit, and then, in one transaction, marks the connection removed, deletes its ciphertext,
and writes `notes.vault.disconnected.v1` to the outbox. When the cleanup fails, nothing is committed and the
owner can send the request again.

## Device channels

The plugin and the export agent send the changes of a vault in one format (workspace design D5 and
D6). Each request carries the owner claim and the device claims of Edge. A connection of another
owner answers `404` with `notes.connection.not_found`. A device of another kind than the channel,
or another device than the one that created the connection, answers `409` with
`notes.channel_mismatch`.

**Change batch.** `POST /v1/notes/connections/{id}/changes` takes `{"changes": [...]}` with 1 to
256 changes. Each change has these fields:

| Field | Type | Rule |
|---|---|---|
| `sequence` | integer, at least 1 | The device change sequence. It increases by one per change. |
| `kind` | `created`, `updated`, `renamed` or `deleted` | What happened to the file. |
| `path` | string | The path after the change, relative to the vault root. Notes stores it in NFC. |
| `previous_path` | string | The path before the change. Only for `renamed`, and required there. |
| `sha256` | 64 lowercase hex digits | The SHA-256 of the new bytes. Not for `deleted`, and required for the other kinds. |
| `length_bytes` | integer | The length of the new bytes. Not for `deleted`, and required for the other kinds. |

The connection keeps the highest device sequence that it committed. The answer is
`{"committed_sequence": n}`.

- A batch that starts after the next sequence answers `409` with `notes.channel.sequence_gap`
  and `committed_sequence`. Nothing is committed.
- A change at or below the committed sequence is a repeat. It changes nothing, so a batch that is
  sent again gets the same answer.
- More than 256 changes answer `413` with `notes.channel.batch_too_large` and `max_changes`.
- A change whose bytes Notes does not hold answers `409` with `notes.channel.bytes_missing` and
  `missing_sha256`. Nothing is committed. An excluded path and a file over the size limit need no
  bytes.
- Otherwise the changes after the committed sequence, and the new committed sequence, are one
  transaction. A rename keeps the file id and publishes one `renamed` event with `previous_path`.

**Uploads.** Bytes go first, through the shared transfer contract
(`ratatoskr-blob-transfer-contracts`) on the `transfer` route class of Edge:
`POST .../blobs` opens a session (`201`), `PUT .../blobs/{token}/chunks/{index}` takes the raw
bytes of one chunk, `GET .../blobs/{token}/status` answers the resume view, and
`POST .../blobs/{token}/finalize` with `{"resumption_token": ...}` answers `200` with the outcome
`stored` or `digest_mismatch`. Bytes with another SHA-256 than the declared one are deleted, and
no blob is registered. At most 32 sessions are open, each for one hour, and a declaration over the
size limit (64 MiB) answers `400` with `notes.request.invalid`. A restart forgets the open
sessions; the device opens a new one.

**Manifest.** When a device cannot prove that it saw every change, it sends
`POST /v1/notes/connections/{id}/manifest` with `{"entries": [{"path", "sha256",
"length_bytes"}]}`, every file of the vault, at most 100 000 entries. The answer is
`{"needed_sha256": [...], "committed_sequence": n}`. When Notes does not hold the bytes of an
indexed entry, `needed_sha256` lists their hashes and nothing is recorded. The device uploads them
and sends the manifest again. Then one transaction records `deleted` for each file that the
manifest does not name, `renamed` when exactly one missing path and exactly one new path have the
same hash, `updated` for each changed hash, and `created` for each new path. A manifest does not
change the committed sequence.

## Template units

The service starts and stops the units through the systemd D-Bus API as the user
`ratatoskr-notes`. The instance name of each unit is the connection id. Each unit runs `ob` as
`ratatoskr-notes-sync`, which owns the working tree `/mnt/nvme/ratatoskr/notes/trees/<id>` and
the client state `/mnt/nvme/ratatoskr/notes/clients/<id>` (owner decision B of 2026-10-14).

- The trees root has the group `ratatoskr-notes-trees`, whose only member is `ratatoskr-notes`, and
  mode 2750. The sync and login units run with `UMask=0027`, so each file of a tree gets that group
  from the setgid bit and mode 0640 (2750 for a directory). The service reads the tree through that
  group, and no other user can.
- The client state holds the Sync token and the end-to-end key in plain text. The clients root is
  mode 0700, and the login unit creates the client state of a connection with mode 0700.

| Unit | Type | Started | Does |
|---|---|---|---|
| `ratatoskr-notes-login@.service` | oneshot | at the connect of a `sync` connection | `ob login`, then `ob sync-setup`. The exit status names the failure: 10 login failed, 11 MFA code needed, 12 end-to-end password wrong, 13 no such remote vault. |
| `ratatoskr-notes-sync@.service` | long-running | by the Sync supervisor, after the first login and at each start of the service | `ob sync-config --mode pull-only --configs ""` with the device name `ratatoskr-<id>`, then `ob sync --continuous`. It is stopped at a disconnect. `MemoryMax=512M` is provisional until spike 3.1 measures it. |
| `ratatoskr-notes-cleanup@.service` | oneshot | at a disconnect, and after a failed login | `ob sync-unlink`, then removes the working tree and the client state. |

The three units are in `deploy/systemd/`. None of them is enabled: only the service starts them.
The sync unit runs in pull-only mode, so Notes never writes into a vault.

An expired or revoked Obsidian login does not stop `ob sync --continuous`: it logs
`Error: Failed to authenticate: ...` and tries again. The sync units log to the journal namespace
`ratatoskr-notes-sync` (`LogNamespace=`), not to the system journal. The service follows that
namespace with one `journalctl` process, sends each line to its connection by `_SYSTEMD_UNIT`, and
reads that one line and no other. When the line repeats for 5 minutes, the connection becomes
`login_expired` with the code `notes.sync.login_expired`, and the service stops the unit. It starts
no login: the owner removes the connection and connects again. The line comes from
`obsidian-headless` 0.0.14 and is provisional until spike 3.1 sees a real expired token.

The service user is not in the group `systemd-journal`. It reads only the namespace of the sync
units. The deploy role gives it this access with an ACL on the directory of the namespace. At start
the service checks that it can read the namespace. When it cannot, it logs
`notes.sync.journal_unreadable` with the reason that `journalctl` gives, and an expired login is
not found. When the follower ends, the service starts a new one after 1 second. It doubles the
wait, up to 1 minute, while the follower keeps ending. To read the journal of a sync unit, run
`journalctl --namespace=ratatoskr-notes-sync --unit=ratatoskr-notes-sync@<id>.service`.

## Blob root and its read group

The bytes of each revision are in the blob root `/mnt/nvme/ratatoskr/blobs/ratatoskr-notes`, at
`sha256/<first two hex digits>/<other 62 hex digits>` of their SHA-256. Knowledge reads them there,
read-only, as the fleet spec `blob-references` allows. No other service may read them.

- The deploy role of `ratatoskr-workspace` creates the system group `ratatoskr-notes-blobs`, makes
  the Knowledge service user its only member, and creates the root with the owner
  `ratatoskr-notes`, that group, and mode 2750.
- The service writes each blob with mode 0640 and each new directory with mode 2750. The setgid bit
  gives each new file and directory the group of the root, so the read group can read a blob and
  no other user can. The service unit runs the service with `UMask=0027`. The service is not in
  the read group, and a `chmod` by it would remove the setgid bit. So the start of the service fails
  when the umask is stricter than 0027 (`RATATOSKR__BLOBS__ROOT` names the root).
- A later consumer, for example `ratatoskr-sites`, joins the group in its own change.

`services/notes/tests/blob_root.rs` checks the modes and the group of a new blob, and
`services/notes/tests/blob_setgid.rs` checks that a strict umask cannot remove the setgid bit.

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
- [x] Sync supervisor, the sync unit, the watcher of the working tree and the blob root with its
  read group (group 6, tasks 6.1 to 6.4 and 6.7 to 6.10). `MemoryMax=` of the sync unit and the
  line of an expired login are provisional until spike 3.1.
- [x] Review follow-ups of group 6 and the owner decisions of 2026-10-14 (tasks 6.11 to 6.18): the
  journal namespace of the sync units with one follower that starts again, the reads of a working
  tree beneath its root, the umask rule of the blob store, and the split trees and clients roots.
  The journal namespace and its ACL wait for a check on a real Debian 12 host.
- [ ] The pull-only session of spike 3.3 and its replay test (tasks 6.5 and 6.6).
- [x] Index, file events and the read API (group 7), and the connect operation of workspace design
  D10 (notes tasks 7.9 and 7.10).
- [x] Device channel routes (group 8, tasks 8.1 to 8.8): change batches, uploads and the
  manifest.
- [x] Review follow-ups of group 7 (tasks 8.9 to 8.16): a case-only rename, `.obsidian/` and
  `.trash/` by path key, the end of a device connect, and the correlation of the reports.

## Development

See [DEVELOPMENT.md](DEVELOPMENT.md). The local development database is a convenience of
`compose.yaml`; on the host the service uses the shared database `ratatoskr`.

## Licence

BSD-3-Clause. See [LICENSE](LICENSE).
