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
except `failed` and `removed`. The states are `connecting`, `active`, `login_expired`, `failed`
and `removed`. A new connection starts as `connecting`; the connect flow (workspace tasks 5.7 and
6.2) makes it `active`. A `sync` connection whose first login fails becomes `failed` (decision 8).
It never delivered a change, so it does not hold its vault key, and the owner can connect again.

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

`SystemdBus` has three methods. `start_unit` and `stop_unit` each wait for the job and return its
result. `exit_status` reads `ExecMainCode` and `ExecMainStatus` of a unit, which name the exit
status of the command that failed in a oneshot unit (decision 8). A unit name is a value of `UnitName`, which can only name one of the three template
units for one connection id, so no caller can ask systemd for another unit. The production
implementation talks to `org.freedesktop.systemd1` on the system bus with `zbus`. It connects on
first use, so a developer machine without systemd starts the service, and only a `sync`
disconnect fails there. A stop of a unit that systemd does not know (`NoSuchUnit`) counts as done.

The test double records each call. When it is asked to start the cleanup unit, it runs the
`ExecStart=` lines of the shipped `deploy/systemd/ratatoskr-notes-cleanup@.service`, with `%i`
replaced, the trees root and the clients root moved into a temporary directory, and `ob` replaced by a stub that
records its arguments. The test therefore checks the shipped unit, not a copy of its behaviour.

### 6. The cleanup unit

The unit runs as `ratatoskr-notes-sync`, the owner of the working tree and of the client state,
with `XDG_CONFIG_HOME` set to the client state as in design D3. Owner decision B of 2026-10-14 puts
the working tree at `/mnt/nvme/ratatoskr/notes/trees/<id>` (group `ratatoskr-notes-trees`, whose
only member is `ratatoskr-notes`, mode 2750) and the client state at
`/mnt/nvme/ratatoskr/notes/clients/<id>` (mode 0700); `systemd::tree_dir` and `systemd::client_dir`
build these paths, and `tests/unit_files.rs` compares them with the units. The sync and login units
run with `UMask=0027`. So each file of a tree gets the group of the setgid trees root and mode 0640.
The service reads the tree through that group. The login unit sets the umask 0077 for
`ob login`, which creates the client state. It runs `ob sync-unlink --path <tree>` with
the `-` prefix, because a connection whose setup never finished has no Sync configuration and `ob`
then exits 3. It then deletes the working tree and the client state with `rm -rf`, so it may write
the two roots. The `ob` path is
`/usr/bin/ob`, where a global `npm` install puts it on Debian; workspace task 14.7 pins the
package and confirms the path.

### 7. Error codes of this repository

`notes.connection.vault_already_connected` and `notes.connection.not_found` are named by the
workspace documents. `notes.request.unauthenticated`, `notes.request.invalid`,
`notes.connection.cleanup_failed` and `notes.service.unavailable` follow the pattern of the other
domain services. They are local codes and no other repository branches on them.

### 8. The connect flow of a `sync` connection

The body of `POST /v1/notes/connections` carries the object `sync_login` with `email`,
`password`, and the optional `mfa_code` and `e2e_password`. It is required for `sync` and refused
for `plugin` and `cli`. Each secret is a `SecretString`, so its `Debug` output is `[REDACTED]`, and
a refused body is not logged, because a JSON error can quote a value. The vault key of a `sync`
connection is the remote vault id: ASCII letters, digits, `-` and `_`.

The flow is:

1. read the key file (`RATATOSKR__SECRETS__KEY_FILE`, default `/etc/ratatoskr/notes-secrets.key`,
   32 bytes, refused when the group or other users can read it);
2. seal the account password and the end-to-end password with XChaCha20-Poly1305 under a fresh
   random 24-byte nonce, with the associated data `ratatoskr-notes:<connection id>:<kind>`;
3. insert the connection and its ciphertext in one transaction;
4. write the credential files of the login under `/run/ratatoskr-notes/credentials/<id>/`
   (directory 0700, each file 0600, owner `ratatoskr-notes`): `email`, `password`, `mfa`, `vault`
   and `e2e`. `mfa` and `e2e` are empty when the owner gave no value, because systemd 252 fails the
   start of a unit whose `LoadCredential=` file is missing;
5. start `ratatoskr-notes-login@<id>.service` and wait for it, then delete the credential files,
   whatever the result. A guard deletes them when it is dropped, so a cancelled request deletes
   them too;
6. answer `202` when the unit succeeds. When it fails, run the cleanup unit, mark the connection
   `failed` and delete its ciphertext in one transaction, and answer `422` with the code that the
   exit status of the unit names.

| Exit status of the login unit | Code |
|---|---|
| 11: `ob login` ended without a token, because the account needs an MFA code | `notes.sync.mfa_required` |
| 12: `ob sync-setup` exited 2 | `notes.sync.e2e_password_wrong` |
| 13: `ob sync-setup` exited 3 | `notes.sync.vault_not_found` |
| 10, any other status, or no status | `notes.sync.login_failed` |

The login unit runs as `ratatoskr-notes-sync` and loads the five files with `LoadCredential=`.
Each value reaches `ob` from `$CREDENTIALS_DIRECTORY`: the two passwords on standard input, one
for each step, and the email, the vault id and the MFA code in argv, which the workspace spec
allows. The unit writes `$` as `$$` in its commands, so that `sh` and not systemd expands the
variables, and the fake systemd bus of the tests runs these same lines.

The request waits for the login, as a disconnect waits for the cleanup. The workspace design D10
makes the connect an operation that reports its phases. That report needs the supervisor and the
operation reports of group 6, and when they exist the failure code goes to the report as well. A
failure of the bus answers `503` with `notes.service.unavailable`. When the cleanup after a failed
login fails, the connection stays `connecting` with its ciphertext, the answer is `503` with
`notes.connection.cleanup_failed`, and the owner removes the connection with a disconnect.

### 9. The Sync supervisor and the sync unit

`deploy/systemd/ratatoskr-notes-sync@.service` runs `ob sync --continuous --path <tree>` as
`ratatoskr-notes-sync`, with `HOME` and `XDG_CONFIG_HOME` set to the client state, as the login unit does.
Its `ExecStartPre=` runs `ob sync-config --path <tree> --mode pull-only --configs "" --device-name
ratatoskr-<id>`, so each start of the unit sets pull-only mode, no configuration sync and the device
name again. The supervisor does not run `ob` itself. `Restart=on-failure` with `RestartSec=30` and
no start limit restarts a sync that fails. The unit uses only `/usr/bin/ob`, `HOME` and
`XDG_CONFIG_HOME`, so the template unit check of the workspace accepts it.

`MemoryMax=512M` is PROVISIONAL. Spike 3.1 measures the resident memory of one continuous sync of a
1 GiB vault, and the value then becomes that measurement plus 50 % (workspace design, Risks).

The supervisor starts the unit when the login unit of a connect succeeded, before it records the
login. A disconnect that runs while the unit starts makes the record fail, and the cleanup that
then runs again stops the sync unit too. A sync unit that does not start fails the connect with
`503` and the cleanup of a failed login. At each start of the service, after the interrupted logins
are failed and before the listeners accept, the supervisor starts the unit of each `sync`
connection whose login finished and that is `connecting` or `active`: the unit is not enabled, so a
restart of the host does not start it.

### 10. The watcher of the working tree and the blob store

`watcher::watch` watches one working tree. On Linux it reads inotify through the tokio reactor
(the `inotify` crate), with one watch for each directory. It also reads the whole tree at its
start and every 15 minutes. A file is settled when its size and modification time have not
changed for 2 seconds after the watcher last saw them change. A settled state that differs from
the last revision of the path becomes a revision: the bytes are copied to the blob root and hashed
in one pass, and when the file changed during the copy it waits for a new window. A full inotify
queue (`IN_Q_OVERFLOW`) makes the watcher read the whole tree. A new directory makes it read that
directory only, and a removed directory makes it check the files that it knew under it. Each read
of the tree runs on the blocking pool. The watcher sends `TreeChange::Revision` and
`TreeChange::Removed` to a channel. It reads regular files only, and never through a symbolic link
or outside the tree: `ob` writes the tree, and it can put a link where a watched directory was. The
watcher opens the root of the tree once, at its start. It opens the parent of the tree by path, and
then the tree with `O_NOFOLLOW`, so a link in place of the tree stops the watcher. It keeps that
root open. On Linux it opens each file and each directory beneath that root with `openat2` and
`RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS`. On other platforms it opens each component with
`O_NOFOLLOW`. The state of a file is the `fstat` of the open file. The bytes of a revision come from
the same open file. That file must have the settled state before and after the copy. Other
platforms have no inotify, and there the watcher only rescans; the tests of the watcher therefore
run on Linux.

`BlobStore` writes `<root>/sha256/<2 hex>/<62 hex>` through `<root>/staging/` and a hard link, as
the other content-addressed stores of the fleet do. The service does not start a watcher yet: the
revisions, the outbox and the excluded folders of workspace task 7.6 consume its changes.

### 11. The read group of the blob root

`BlobStore` creates each blob with mode 0640 and each directory with mode 2750, and sets the mode
again only when the umask removed a bit. On Linux a directory that is made in a setgid directory
gets the setgid bit and the group from the kernel. The store does not run `chmod` on it then,
because a `chmod` by an owner that is not in the group removes the setgid bit; `ratatoskr-notes`
is not in `ratatoskr-notes-blobs`. So `BlobStore::open`, which the service calls at its start,
refuses a umask stricter than 0027: with it, the create would remove a bit of 2750 and the store
would run that `chmod`. Linux gives the umask in `/proc/self/status` without changing it; other
platforms are not a host, and there the store does not check. This repository ships no unit for
the service, so `UMask=0027` belongs to the service unit that the deploy role of
`ratatoskr-workspace` installs. `BlobStore::open` also deletes each staged `.part` copy that a stop
left behind, and the store writes the directory of a new blob to disk after the link.

### 12. An expired Sync login

The sync units log to the journal namespace `ratatoskr-notes-sync` (`LogNamespace=`, owner decision
A of 2026-10-14). The supervisor follows that namespace through `SystemdBus::follow_journal`, with
one follower for all units. Each follower is one `journalctl` process, and the host has no spare
memory for one process per connection. The production bus runs `journalctl
--namespace=ratatoskr-notes-sync --follow --lines=0 --output=json
--output-fields=MESSAGE,_SYSTEMD_UNIT` and reads the unit, the monotonic time and the message of
each entry. `_SYSTEMD_UNIT` is a trusted field: journald sets it, and `ob` cannot. The supervisor
counts the lines of a unit only after it started that unit. It starts the follower before the first
start of a unit, so no line of that start is lost. Only one line counts: a message that starts with
`Failed to authenticate:`, also after `Error: `. `ob` 0.0.14 throws
`new Error("Failed to authenticate: " + msg)` when the Sync server refuses its token, and
`getServer()` logs the error with `console.error`, so the journal line is
`Error: Failed to authenticate: <server message>`. Every other line is dropped unread.

A run of failure lines starts at the first line, and a pause of more than 10 minutes between two
lines starts a new run: `ob` tries every 30 seconds, but after each failed connection it waits up to
5 minutes. When a run has lasted 5 minutes by the times of its lines, the supervisor marks the
connection `login_expired` if it is `connecting` or `active`, logs `notes.sync.login_expired`, stops
the unit once, and stops reading. It starts no login. A connection that the owner removed meanwhile
is left to the disconnect. The rule uses the times of the lines, not the clock of the service, so
the test drives it with lines that a stub `ob` logs under paused time.

The line and the 5-minute rule are PROVISIONAL: they come from the code of `obsidian-headless`
0.0.14, not from an expired token. Spike 3.1 confirms them, and the deploy role pins the version,
because a release can change the line.

The service user needs read access to the namespace only, not to the system journal. The deploy
role gives the group `ratatoskr-notes` an ACL on `/var/log/journal/<machine id>.ratatoskr-notes-sync`
(DEVELOPMENT.md holds the tmpfiles.d entry). At start the supervisor runs `journalctl
--namespace=ratatoskr-notes-sync --lines=0`, which exits 1 when it can open no file of the
namespace, and logs `notes.sync.journal_unreadable` with the stderr of `journalctl`. The follower
keeps its stderr too, and logs each line with the same code. When the follower ends, the supervisor
logs the code and starts a new follower after 1 second. Each next wait is twice the last wait, up
to 1 minute. After a follower that ran for 1 minute, the wait starts at 1 second again. The run of
failures of a unit stays across a new follower. The lines between two followers are lost, but `ob`
repeats the line every 30 seconds, so a gap delays the expiry and does not hide it.

## Risks / Trade-offs

- The D-Bus implementation has no automated test in this change: neither macOS nor the CI runner
  gives the test an unprivileged system bus with the three units. Workspace task 6.2 adds the
  polkit rule test, and the Molecule case of task 14.6 runs the units on a host.
- An outbox row waits for the relay of task 7.6 before it reaches the bus.
- The mapping of `ob` results to the four codes comes from the code of `obsidian-headless` 0.0.14,
  not from a run against Obsidian. When the account needs an MFA code that the owner did not give,
  `ob login` asks for it on standard input, which is already at its end, and the process ends
  without a token. The unit reports that as `notes.sync.mfa_required`. Spike 3.1 and the Molecule
  case of task 14.6 confirm it on the host.
- The service creates `/run/ratatoskr-notes/credentials` with mode 0700 when it is missing, but
  only root can create `/run/ratatoskr-notes`. The service unit that ships with group 6 needs
  `RuntimeDirectory=ratatoskr-notes`.
- The login unit and the cleanup unit may write the two roots, `/mnt/nvme/ratatoskr/notes/trees`
  and `/mnt/nvme/ratatoskr/notes/clients`. The directories of a new connection do not exist when the
  login unit starts, and a `ReadWritePaths=` entry must exist. A removal removes an entry of each
  root. Every directory there belongs to `ratatoskr-notes-sync`. The deploy role must create the
  trees root with that owner, the group `ratatoskr-notes-trees` and mode 2750. It must create the
  clients root with that owner and mode 0700. It must add the three units to
  `ratatoskr_template_units` (workspace task 14.7).
- The setgid bit and the group of a tree come from the trees root, and the mode of each file from
  `UMask=0027`. `ob` can make a file with an explicit mode that is stricter than 0640. The service
  cannot read that file. The watcher then skips it and logs nothing about its name.
- `no_secret_in_any_cmdline` runs on Linux only. A run on macOS ignores it, so the Linux CI job is
  the check.
- A reconnect of a `login_expired` connection is a new `POST`, and the connection still holds its
  vault key, so the answer is `409`. The owner removes the connection first. Group 6 keeps that
  rule: the supervisor starts no login, and no route reuses a connection.
- The journal namespace and its ACL are not checked on a real Debian 12 host yet (systemd 252).
  The test double reads no journal file, so the check of a real host is the Molecule case of
  workspace task 14.6 or a manual check on the host.
