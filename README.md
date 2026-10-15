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
`/v1/notes/connections/{id}/blobs` (see Device channels). `GET /v1/notes/conflicts/{id}` reads a
conflict of a write, and `PUT /v1/notes/connections/{id}/write-mode` sets the write mode of a
connection. The device of a `plugin` or `cli` connection reads its writes at
`GET /v1/notes/connections/{id}/device-operations` and below it (see Writes). Every route answers only for the owner in the claim of Edge.

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
within 30 minutes of the connect, the connection becomes `failed`, and in the same transaction the
operation gets its terminal report `failed` with the code `notes.channel.manifest_missing`. That
code is in the report of the operation, never in an HTTP answer. The wait continues after a restart
of the service. Each report correlates to `operation:<operation id>`. A terminal report `failed` is
written in the transaction that marks the connection `failed`, `removed` or `login_expired`, so a
stop of the process cannot separate them.

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
owner, or one that failed, answers `404` with `notes.connection.not_found`. A connection that its
owner removed answers `410` with `notes.connection.removed` to its own device, so that the device
stops and does not connect again. A device of another kind than the channel,
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
- A change may name only bytes of its own connection: bytes that the device uploaded through an
  upload session of that connection (`notes.connection_blobs`), or a revision of one of its files.
  Other bytes, also bytes that another connection stored, answer `409` with
  `notes.channel.bytes_missing` and `missing_sha256`, and the device uploads them itself. Nothing is committed. An excluded path and a file over the size limit need no
  bytes. An empty file needs no upload either: the transfer contract refuses 0 bytes, so Notes
  puts the blob of no bytes in its store itself.
- Otherwise the changes after the committed sequence, and the new committed sequence, are one
  transaction. A rename keeps the file id and publishes one `renamed` event with `previous_path`.

**Uploads.** Bytes go first, through the shared transfer contract
(`ratatoskr-blob-transfer-contracts`) on the `transfer` route class of Edge:
`POST .../blobs` opens a session (`201`), `PUT .../blobs/{token}/chunks/{index}` takes the raw
bytes of one chunk, `GET .../blobs/{token}/status` answers the resume view, and
`POST .../blobs/{token}/finalize` with `{"resumption_token": ...}` answers `200` with the outcome
`stored` or `digest_mismatch`. Bytes with another SHA-256 than the declared one are deleted, and
no blob is registered. At most 4 sessions of one connection and 64 of the service are open, each
for one hour (more answer `503` with `notes.service.unavailable`), and a declaration over the
size limit (64 MiB) answers `400` with `notes.request.invalid`. A restart forgets the open
sessions; the device opens a new one.

**Manifest.** When a device cannot prove that it saw every change, it sends
`POST /v1/notes/connections/{id}/manifest` with `{"entries": [{"path", "sha256",
"length_bytes"}]}`, every file of the vault, at most 100 000 entries. The answer is
`{"needed_sha256": [...], "committed_sequence": n}`. When Notes does not hold the bytes of an
indexed entry that its connection may name, `needed_sha256` lists their hashes and nothing is
recorded. The device uploads them
and sends the manifest again. Then one transaction records `deleted` for each file that the
manifest does not name, `renamed` when exactly one missing path and exactly one new path have the
same hash, `updated` for each changed hash, and `created` for each new path. A manifest does not
change the committed sequence.

## Writes

The write path of workspace change `add-notes-write-back` (OBS-024). Edge publishes each write as
the command `notes.file.write_requested.v1` (contract `notes/write-request.v1`) with the operation
`notes.file.write`. Notes reads the commands through the durable `ratatoskr_notes_write_requests`
on the stream `ratatoskr_commands`, with the filter `cmd.notes.file.write_requested.v1`. Edge
creates the durable; notes only looks it up and compares its filter, its explicit acknowledgement
and its ack wait of 30 s. When `RATATOSKR__BUS__URL` is set, `/health/ready` answers `503` until
that check passed, and again when a later check fails. Each command runs in a task of its own: a defect that
panics in the write path fails that one operation with `notes.write.internal`, terminates the
message and leaves the consumer running. When the consumer ends, for any reason, `/health/ready`
answers `503`.

**Content.** A command never carries file bytes. The owner's session uploads the content first,
through the upload routes of the connection (see Device channels), and the command names the
notes-owned `BlobRef`. Edge names the session in `x-ratatoskr-session-kind` (any kind except
`device`) and sends no device id. A session upload registers the bytes for the connection and is no
revision. A session of another owner gets `404` `notes.connection.not_found`. A change batch or a
manifest from a session answers `409` `notes.channel_mismatch`.

**Checks when a write arrives.** In this order, each refusal fails the operation and changes
nothing:

| Check | Code |
|---|---|
| The command is a valid `notes/write-request.v1` of a `user:` tenant, and its aggregate is `operation:<operation id>` | `notes.request.invalid` |
| The tenant holds the connection, and the connection is `connecting` or `active` | `notes.connection.not_found` (also for a connection of another owner) |
| The write mode of the connection is on (it starts off) | `notes.write.disabled` |
| The idempotency key of the connection, when an earlier write has it, comes with the same origin and operation | `notes.request.invalid` |
| A `resolve_conflict` names an open conflict of the connection | `notes.conflict.not_found` |
| A `resolve_conflict` has the side that its choice takes (see Resolution) | `notes.request.invalid` |
| The connection uploaded the bytes of the `BlobRef`, or holds them as a revision, with the declared length | `notes.write.content_missing` |

A command whose key repeats an earlier write with the same origin and operation queues nothing:
its operation reports the state of that write, also its end.

**Queue and base.** The open writes of one connection with one case-folded NFC path key are the
queue of that file, in the order of acceptance. Only the head is delivered. When a write reaches
the head, notes compares its base with the current revision: a `create` needs a path key that no
present file holds, every other operation needs the base revision, and a `rename` or a `move` also
needs a free target path key. A mismatch fails the write with a conflict, and the next write becomes
the head. A delivered write is pending: its bytes are a blob that the write names. Only the
acknowledgement of the channel makes the revision current and writes `notes.file.changed.v1`. A
refusal of the channel leaves the previous revision current; a base mismatch reported by the
channel is a conflict.

**Link rewrite.** A `rename` or a `move` rewrites the links that it changes, with the rules of
Obsidian's "Automatically update internal links" (spec `notes-link-rewrite`). When notes accepts
it, `ratatoskr_notes_index::rewrite_links` computes the new text of each note whose links change,
from the current revisions: each link resolves before and after the rename to the list of files it
can name, and a link whose list changed is written again for the file it named, as the name alone
when that name is unique, else as the path from the vault root. This also writes again a link to
another file whose name the move made ambiguous. Aliases, headings, block references, embeds,
Markdown link texts, titles and angle brackets stay; a space in a Markdown link is `%20`; links in
code, HTML comments and math do not change; links in the frontmatter are not rewritten. A wikilink in
the text of a Markdown link is rewritten inside that text (Obsidian 1.14.4 corrupts such a note). Notes does
not read the vault setting `newLinkFormat`, and writes links as its default `shortest` does. Each
linking note gets an `update` on its current revision, and the renamed note gets its new bytes on
the rename itself. The rename and its rewrites are one write group: the group waits until each of
its writes is the head of every queue it is in, then every base is checked; one stale base fails
the whole group with one conflict whose `stale_paths` names every stale file, and nothing is
delivered. Each write of a delivered group becomes current when the channel acknowledges it. The
operation ends with the last of them: `succeeded` with one result per file, or, when the channel
applied only a part, `failed` with `notes.write.partially_applied` and one conflict that names every
file that was not applied.
On `sync`, the writer renames the file and writes the renamed note's own new bytes at the target,
and writes each rewrite only after the rename landed; a refused rename fails its rewrites that did
not land, so no link names a path whose file did not move.
The golden fixtures recorded with the real Obsidian app are the oracle (see DEVELOPMENT.md).

**Acknowledgement and echo.** On the `sync` channel, a change of the working tree with the path and
the hash of the delivered write at the head of its queue is the echo of that write and its
acknowledgement: the content hash of a `create`, an `update` or a resolution, or the removal of the
path of a `delete`. The write's revision becomes current, and one `notes.file.changed.v1` names it.
After the sync writer landed a write in the working tree, the next change of its path that the
watcher reports is its acknowledgement, also with other bytes: the vault rewrote the file after the
write landed, for example Obsidian formatted it. Before the write landed, a change with other bytes
is a change of the vault, and the write then fails as a conflict.
When a channel acknowledges a write with another hash than the write named (a plugin formatted the
file on save), the acknowledged bytes become current and are published once, the expected bytes
never are, and the operation succeeds with that hash and the warning `notes.write.transformed`.

**Sync delivery.** Every two seconds, and for each `sync` connection with a delivered write, the
service compares each delivered write with the working tree under a lock of its path. A file with
the hash that the write produces is the acknowledgement. A file with the base of the write (no file
for a `create`) gets the write: new bytes go to a temporary file `.ratatoskr-write-<id>.part` in the
same folder, which is flushed to disk and renamed over the file, so a reader sees the old or the new
bytes and never a mix (the watcher and the index leave such temporary files out); a `delete` removes the file, and a `rename` or a `move` renames it. The
Headless client sends the change to Obsidian Sync, and the watcher reports it back as the echo. Any
other file means that the tree changed after the queue check: nothing is written, the bytes of the
tree go to the blob store, and the write fails with a conflict whose current side is those bytes.
Every file operation runs relative to a folder that was opened from the root of the tree one name
at a time without following a link; a symbolic link (or a file where a folder must be) anywhere in
the path fails the write with `notes.write.unsafe_path`, so no write, delete, rename or read leaves
the tree. A `set_property` on a file with its base changes the one frontmatter key in notes itself: the
value is written as compact JSON, which is valid YAML (`status: "done"`), a missing key is added at
the end of the frontmatter, a file without frontmatter gets one, and every other byte stays. Notes
stores the new bytes, writes them atomically and acknowledges the write with their hash at once.
A file that is not UTF-8, or whose frontmatter has no end, fails with `notes.write.unsupported`.
The service writes the working tree through the group `ratatoskr-notes-trees` (see Template
units).

**Device queue.** The delivered writes of a `plugin` or `cli` connection, in the order of
delivery, are the queue that its device reads through Edge, with the device claims. Only the
device that the connection registered, of the kind of its channel, reads or answers it; any other
device, also one of the owner, gets `404` `notes.connection.not_found`. A `rename` or a `move` is in
the queue of its path and of its target, and is delivered only at the head of both, so a later write
of the new path reaches the device after it.

- `GET /v1/notes/connections/{id}/device-operations?after=<cursor>` answers
  `{"operations": [...], "next_cursor": "<cursor>"}` with at most 256 operations after the cursor
  (from the start when it is absent). The cursor is the delivery sequence of the connection: each
write takes the next number when it becomes delivering, so a write that is delivered after a read
always comes after that read's `next_cursor`. Each operation has `operation_id`, `cursor`, `kind`
  (`create`, `update`, `rename`, `move`, `delete` or `set_property`; a conflict resolution is an
  `update`, or a `create` of a deleted file), `path`, `target_path`, `base_hash` (the hash that the
  local file must have; `null` for a `create`), `content` (the notes-owned `BlobRef` of the bytes
  to write, or of the JSON value of a `set_property`), `property_key`, `property_change` (`set` or
  `remove`) and `expected_sha256` (the hash of the file after the operation as written, when notes
  knows it). A `rename` or a `move` whose links notes rewrites also has `rewrites`: for each
  linking note, `path`, `base_hash` and `expected_sha256`; the rewrites themselves are no
  operations of the queue, because Obsidian rewrites the links when the device renames the file.
  An operation stays in the queue until its result arrives.
- `GET .../device-operations/{operation_id}/content` streams the bytes of `content`.
- `POST .../device-operations/{operation_id}/result` takes `{"outcome": "applied", "sha256": "<hash
  of the file at the path, or at the target of a rename>"}` (no hash after a `delete`; a rename
  with rewrites adds `"rewrites": [{"path": ..., "outcome": "applied", "sha256": ...} or
  {"path": ..., "outcome": "base_mismatch", "local_sha256": ...}]`, and a linking note that the
  list does not name counts as not applied),
  `{"outcome": "base_mismatch", "local_sha256": "<hash>"}`, or `{"outcome": "failed", "code":
  "<typed code>"}`, and answers `{"operation_id": ..., "state": "succeeded" | "failed" |
  "delivering"}`. An `applied` hash names bytes that the device uploaded to the connection first;
  otherwise the answer is `409` `notes.channel.bytes_missing` with the hash, and the device sends
  the result again after the upload. A result for an operation that ended answers its end again.
  An unknown operation answers `404` `notes.device_operation.not_found`.

**Reports.** Notes is the only reporter of `notes.file.write`. Each report goes to the outbox as
`platform.operation.reported.v1`:

| Status | Stage | When |
|---|---|---|
| `running` | `queued` | The write waits in the queue of its file. |
| `running` | `awaiting_device` | The write is in the queue of the device of a `plugin` or `cli` connection, which applies it when it connects. |
| `running` | `awaiting_obsidian` | The write of a `sync` connection waits for the working tree and Obsidian Sync. |
| `succeeded` | | The channel acknowledged the write. The one result has the kind `notes.file`, the target `notes_file:<file id>`, and the `BlobRef` of the new revision (none after a delete). |
| `succeeded` | | A rename with link rewrites: one `notes.file` result for each file of its group. |
| `failed` | | A refusal above, a refusal of the channel with its own code, or the non-retryable `notes.write.conflict` with the error extension `conflict_id`. A rename whose group the channel applied only in part fails with the non-retryable `notes.write.partially_applied` and `conflict_id`. |

**Conflicts.** A conflict is a record with three sides by hash: the base that the writer saw,
the current revision of the vault, and the writer's content (a side that does not exist is left
out). For `text/markdown` and `text/plain`, when notes holds the bytes of all three sides as UTF-8
text of at most 1 MiB and 20 000 lines each, notes computes a line-based three-way merge (diff3). A
larger side is not read, and the conflict has no proposal. At these caps one merge peaks at about
11 MiB, and at most two merges run at the same time. The merged bytes go
to the blob root, and the proposal names their hash and `has_overlapping_hunks`; a region that both
sides changed differently holds both between the markers `<<<<<<< current`, `=======` and
`>>>>>>> writer`. Notes never applies a proposal on its own. The transaction that fails the write records
the conflict and the report. After its commit, outside the lock of the connection row, notes
computes the proposal, and a second short transaction stores it and writes
`notes.conflict.raised.v1` (contract `notes/conflict.v1`, aggregate
`notes_conflict:<conflict id>`, correlated to the operation) with the conflict id, the operation,
the connection, the path, the origin, the media type, the three hashes and the proposal. The event
carries hashes and never file content. A conflict that a stop of the process left unannounced is
announced at the next start.

**Resolution.** A conflict stays open until a `resolve_conflict` write ends it. The command names
the conflict and a choice, and no `base_hash`: the resolution writes the file of the conflict on the
revision that the conflict recorded as current. It waits in the queue of that file like any other
write. At the head, notes checks that the conflict is still open (else `notes.conflict.not_found`)
and that the file still has that revision. When the vault changed the file again, the resolution
raises a second conflict (the old current revision as base, the new one as current, the side of the
choice as the writer's content), fails with `notes.write.conflict`, and changes nothing; the first
conflict stays open.

| Choice | Content that the resolution writes | Refused with `notes.request.invalid` when |
|---|---|---|
| `keep_current` | Nothing: the current revision stays. The resolution ends at the head without a delivery. | The vault holds no current revision of the file. |
| `keep_writer` | The writer's content of the conflict. | The conflict has no writer's content (a `delete`, a `rename`). |
| `accept_merge` | The merge proposal of the conflict, also one with overlapping hunks. | The conflict has no merge proposal. |
| `custom` | The bytes of `blob`, which the owner uploaded to the connection first. | Never: the contract requires `blob`. |

When the channel acknowledges the resolution (or at once for `keep_current`), notes marks the
conflict `resolved` and writes `notes.conflict.resolved.v1` with the conflict id, the operation and
the origin of the resolution, the connection, the path and the revision after it.

`GET /v1/notes/conflicts/{conflict_id}` answers a conflict of a connection of the owner:
`conflict_id`, `operation_id`, `connection_id`, `path`, `origin`, `media_type`, `state` (`open` or
`resolved`), `base_hash`, `current_hash`, `writer_hash`, `merge_proposal`, `stale_paths` (a conflict
of a write group: every stale or unapplied file, sorted; `path` is the first), and, for Markdown and
plain text, `texts` with the text of each side (`base`, `current`, `writer`, `merged`) that notes
holds. A side larger than 1 MiB, the cap of a merge, is not read: `texts_over_limit` names it. A
base or current side that is a revision of the file stays readable at
`GET .../files/{path}?revision=<hash>`. Any other id answers `404` `notes.conflict.not_found`.

**Write mode.** Every connection starts with write mode off. Edge proxies
`PUT /v1/notes/connections/{id}/write-mode` with the body `{"write_mode": "on"}` or
`{"write_mode": "off"}` and the operation `notes.connection.write_mode`, which it mints and sends in
`x-ratatoskr-operation-id`. Notes is the only reporter of that operation (lane `ratatoskr-notes`).
It sets the mode under the lock of the connection row and answers `202` with the connection id, the
mode and the operation. Off fails each write that waits in a queue and was not delivered with
`notes.write.disabled`; a delivered write finishes, because the vault may already hold it. On a
`sync` connection a write is delivered only when it landed in the working tree: off also fails a
delivering write that did not land, and the sync writer never lands it. A write that landed
finishes with its echo or the next pass; when the client goes back to `pull-only` before it sent the
file to Obsidian Sync, the other devices do not get it (provisional until spike 3.3). A
connection of another owner, or one that is not `connecting` or `active`, answers `404`
`notes.connection.not_found` and writes no report: Edge fails the operation with the code it
relays.
`GET /v1/notes/connections` names the mode of each connection in `write_mode` (`on` or `off`).

## Template units

The service starts and stops the units through the systemd D-Bus API as the user
`ratatoskr-notes`. The instance name of each unit is the connection id. Each unit runs `ob` as
`ratatoskr-notes-sync`, which owns the working tree `/mnt/nvme/ratatoskr/notes/trees/<id>` and
the client state `/mnt/nvme/ratatoskr/notes/clients/<id>` (owner decision B of 2026-10-14).

- The trees root has the group `ratatoskr-notes-trees`, whose members are `ratatoskr-notes` and
  `ratatoskr-notes-sync`, the setgid bit and mode 2770 (changed from 2750 for write-back, OBS-024).
  The sync and login units run with `UMask=0007`, so each file of a tree gets that group from the
  setgid bit and mode 0660 (2770 for a directory). The service creates its files with mode 0660 and
  its folders with mode 2770 whatever its own umask. The service reads and writes the tree through
  that group, `ob` replaces what the service wrote, and no other user can enter the tree.
- The client state holds the Sync token and the end-to-end key in plain text. The clients root is
  mode 0700, and the login unit creates the client state of a connection with mode 0700.

| Unit | Type | Started | Does |
|---|---|---|---|
| `ratatoskr-notes-login@.service` | oneshot | at the connect of a `sync` connection | `ob login`, then `ob sync-setup`. The exit status names the failure: 10 login failed, 11 MFA code needed, 12 end-to-end password wrong, 13 no such remote vault. |
| `ratatoskr-notes-sync-pull@.service` | long-running | by the Sync supervisor while the write mode is off: after the first login, at each start of the service, and when the write mode goes off | `ob sync-config --mode pull-only --conflict-strategy conflict --configs ""` with the device name `ratatoskr-<id>`, then `ob sync --continuous`. It is stopped at a disconnect and when the write mode goes on. `MemoryMax=512M` is provisional until spike 3.1 measures it. |
| `ratatoskr-notes-sync-bidi@.service` | long-running | by the Sync supervisor while the write mode is on: at each start of the service, and when the write mode goes on | The same with `--mode bidirectional`. It is stopped at a disconnect and when the write mode goes off. |
| `ratatoskr-notes-cleanup@.service` | oneshot | at a disconnect, and after a failed login | `ob sync-unlink`, then removes the working tree and the client state. |

The four units are in `deploy/systemd/`. None of them is enabled: only the service starts them.
Each write mode has its own sync unit, with the mode and the conflict strategy as literal
arguments: no unit reads an `EnvironmentFile=` or expands a variable, so no file that another user
writes can set `PATH`, `NODE_OPTIONS` or `LD_PRELOAD` for `ob`. Before the service starts the sync
unit of one mode, it stops the unit of the other, so the two never run together. A write-mode
change of a `sync` connection runs after the `202`: on starts the `bidirectional` unit in place of
the `pull-only` unit, then sets the mode; off sets the mode, then starts the `pull-only` unit in
place of the `bidirectional` unit. The operation ends `succeeded` after that, or `failed` with the retryable
`notes.sync.reconfigure_failed` when the unit does not start again; a failed "on" leaves the mode off
and the unit `pull-only`. The modes and the conflict strategy are the documented flags of `ob`
0.0.14 and are provisional until spike 3.3 measures how `ob` treats a local write.

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
`journalctl --namespace=ratatoskr-notes-sync --unit='ratatoskr-notes-sync-*@<id>.service'`.

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
