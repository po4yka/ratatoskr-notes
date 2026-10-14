# Developing Ratatoskr Notes

This file tells you how to build, test and run `ratatoskr-notes` on a developer machine. Read
AGENTS.md first: it holds the rules that this file does not repeat.

## Toolchain

`rust-toolchain.toml` pins Rust 1.97.0 with `rustfmt` and `clippy`. `rustup` installs it on the
first `cargo` command in this directory.

## Local dependencies

The tests need PostgreSQL 17. `compose.yaml` starts it:

```bash
docker compose up -d --wait
```

The tests create one database per test and drop it at the end. They read
`NOTES_TEST_DATABASE_URL`, and the default is `postgres://notes:notes@127.0.0.1:5432/notes`, the
database that `compose.yaml` starts. If the postgres of another Ratatoskr repository uses port
5432, stop it first or set `NOTES_TEST_DATABASE_URL`.

The test of the outbox relay (`tests/file_events.rs`) needs a NATS server with JetStream. It reads
`NOTES_TEST_NATS_URL`, and the default is `nats://127.0.0.1:4222`. CI starts one; on a laptop:

```bash
docker run -d --rm --name notes-nats -p 127.0.0.1:4222:4222 nats:2.15.0-alpine -js
```

The test makes the stream `notes_test_events` for the subjects `evt.notes.>` and
`evt.platform.operation.reported.v1` on that server. Do not point it at a server that holds the
stream `ratatoskr_events`, which takes the same subjects.

### Rust — also the CI gate

The gate is `.github/workflows/ci.yml`; this list and that job's `run:` steps are one list, and a step in ci.yml fails when they differ. Run them in this order before you declare a change done:

```bash
cargo fetch --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo build --workspace --locked
cargo test --workspace --locked
cargo build --workspace --locked --release
```

`cargo deny check` is not in this list. It runs in its own `deny` job in ci.yml, so that a new
RustSec advisory cannot hide a clippy or test failure. It reads RustSec advisories, licences,
duplicate versions and the source policy (`deny.toml`). Install it once with
`cargo install cargo-deny`, and run it before you declare a change done.

Notes on the steps:

- `cargo clippy` applies the size limits from `clippy.toml`: a function has at most 100 code
  lines, a signature at most 7 arguments, a block at most 5 levels. An exception is
  `#[expect(clippy::too_many_lines, reason = "...")]` at the site, never a higher number.
- The file-length limit (850 lines in a tracked `.rs` file) runs as the awk step between clippy
  and build in ci.yml. Run it locally with:
  `git ls-files -z "*.rs" | xargs -0 -r wc -l | awk '$2 != "total" && $1 > 850 { print; bad = 1 } END { exit bad }'`
- The tests need the database above. They fail when it is unreachable; they do not skip.
- `openspec validate --all --strict` checks the OpenSpec changes and specs of this repository.

## Tests of the template units

The tests do not use a real systemd. `services/notes/tests/support/systemd.rs` is a fake systemd
bus. When the service starts a login, sync or cleanup unit, the fake reads the shipped file in
`deploy/systemd/` and runs its `ExecStartPre=` and `ExecStart=` lines with `%i` replaced, `$$` read as `$`, the host
paths moved into a temporary directory, and `/usr/bin/ob` replaced by a stub. It reads
`Environment=`, `EnvironmentFile=` and `LoadCredential=`, and gives the unit a
`$CREDENTIALS_DIRECTORY` that it removes after the run. A test therefore checks the shipped unit
file. Keep each command of a unit on one line, write `$$` for each shell `$`, and use no backslash.
`support/journal.rs` is the journal of the fake sync units: a stub `ob` logs lines with the
monotonic time of the fake host, in a paused tokio runtime on a thread of its own, so that minutes
of `ob` retries take no real time. The stub `ob` records `ob sync --continuous` and returns at once, so the start job of the sync unit
is done when its command started, as for `Type=exec`. A test can hold each login unit before its
first command until it releases it. A stop of a login
unit that runs ends its start job with `canceled` and kills its command, as systemd does.

`sync_watcher.rs::settled_change_becomes_one_revision` and
`sync_watcher.rs::linked_folder_does_not_leak_outside_content` run under paused tokio time and read
inotify, so they run on Linux only. On macOS they are ignored, and the watcher there only rescans.
`sync_watcher.rs::linked_tree_root_leaks_no_outside_content` needs only the first read of the
tree, so it runs on every platform.

`blob_setgid.rs::strict_umask_cannot_drop_setgid` changes the umask of the process and the
credentials of one thread, so it runs on Linux only, alone in its file. As root it prepares the blob
root itself; as another user it prepares the root with `sudo -n`.

`secret_custody.rs::no_secret_in_any_cmdline` reads `/proc/*/cmdline` as the user `nobody` while
the login runs, so it runs on Linux only. On macOS it is ignored.
`secret_custody.rs::key_file_of_another_owner_is_refused` gives the key file to `nobody`, so it
also runs on Linux only. Only a run as root checks the owner: for another user, the mode alone
already keeps the service from reading the file. When the test runs as root, the
reader drops to `nobody` itself; otherwise it starts with `sudo -n -u nobody`, which the GitHub
runner allows. To run it on a Mac, use a Linux container in the network namespace of the compose
database, so that the default database URL reaches it:

```bash
docker run --rm --network container:ratatoskr-notes-postgres -v "$PWD":/src -w /src \
  -e CARGO_TARGET_DIR=/src/target/linux \
  rust:1.97 cargo test --locked -p ratatoskr-notes --test secret_custody
```

## Running the service

```bash
export RATATOSKR__DATABASE__URL="postgres://notes:notes@127.0.0.1:5432/notes"
cargo run -p ratatoskr-notes
```

The service applies `schema.sql` on its first start, then serves the operator plane on
`127.0.0.1:9471` and the domain API on `127.0.0.1:8099`. `cargo run -p ratatoskr-notes -- check-config`
validates the environment and binds nothing (exit 0 valid, exit 78 invalid).

| Variable | Default | Meaning |
|---|---|---|
| `RATATOSKR__DATABASE__URL` | none, required | SECRET. The `PostgreSQL` URL. |
| `RATATOSKR__DATABASE__MAX_CONNECTIONS` | `4` | The pool bound, 1 to 16. |
| `RATATOSKR__DATABASE__ACQUIRE_TIMEOUT_SECONDS` | `5` | 1 to 60. |
| `RATATOSKR__CONNECTIONS__MAX_LOGINS` | `2` | How many first logins of `sync` connections run at the same time, 1 to 16. A `sync` connect over the limit answers `503`. |
| `RATATOSKR__ADMIN__BIND` | `127.0.0.1:9471` | The operator listener. |
| `RATATOSKR__API__LISTEN_ADDRESS` | `127.0.0.1:8099` | The domain API listener. It must differ from the operator listener. |
| `RATATOSKR__BLOBS__ROOT` | `/mnt/nvme/ratatoskr/blobs/ratatoskr-notes` | The notes blob root. The start fails when the umask is stricter than 0027, because the store would then remove the setgid bit of a new directory. |
| `RATATOSKR__BUS__URL` | none | The NATS server of the outbox relay, `nats://` or `tls://`. Without it no relay runs, and the events stay in the outbox. |
| `RATATOSKR__BUS__NKEY_SEED_PATH` | none | An absolute path: the nkey seed of the `NOTES` identity, `/etc/ratatoskr/notes.nkey` on the host. The relay reads it at each connect. |
| `RATATOSKR__FILES__MAX_FILE_BYTES` | `67108864` | The largest file that is indexed, 64 MiB, 1 to 1073741824. A larger file publishes no event. |
| `RATATOSKR__SECRETS__KEY_FILE` | `/etc/ratatoskr/notes-secrets.key` | The 32-byte key of the stored Obsidian passwords: a regular file, not a symbolic link, mode 0400 or 0600, owned by the user of the service. Read at each `sync` connect. |
| `RATATOSKR__SHUTDOWN__GRACE_SECONDS` | `10` | How long in-flight requests and running logins may take after SIGTERM, 1 to 60. The service fails each running login in this window; the next start fails a login that it did not reach. |
| `RATATOSKR__TELEMETRY__LOG_FORMAT` | `text` | `text` or `json`. |
| `RATATOSKR__TELEMETRY__LOG_FILTER` | `info` | A tracing `EnvFilter` directive string. |

An unknown `RATATOSKR__` key stops the start with exit 78.

## Schema

`schema.sql` at the repository root is the whole schema. The binary embeds it and applies it to a
database that has no `notes` schema. There are no migrations: a schema change edits `schema.sql`
in place, and the next test database has it.
A database made before a part of `schema.sql` does not get that part: the column `logged_in_at`,
the columns `excluded_folders`, `last_sequence` and `device_sequence` of `notes.connections`, the
column `position` of `notes.outbox`, the tables `notes.files`, `notes.revisions` and `notes.connection_blobs`, or the table
`notes.path_collisions` with the columns `revision`, `media_type` and `length_bytes`. Drop
its `notes` schema (`drop schema notes cascade`), and the next start of the service makes it
again from `schema.sql` (AGENTS.md, Development status: no migrations).

## Deploy files

| File | Purpose |
|---|---|
| `deploy/postgres/role.sql` | Creates the role `ratatoskr_notes` in the shared database. |
| `deploy/nats/identity.conf` | A copy of the `NOTES` stanza of Platform's NATS authorization file. |
| `deploy/systemd/notes.conf.example` | The environment file of the service on the host. |
| `deploy/systemd/ratatoskr-notes-login@.service` | The oneshot unit that runs the first `ob login` and `ob sync-setup` of a `sync` connection. It reads the secrets with `LoadCredential=` from `/run/ratatoskr-notes/credentials/<id>/`. |
| `deploy/systemd/ratatoskr-notes-sync@.service` | The continuous `ob sync` of a `sync` connection, in pull-only mode. The service starts it after the first login and at each of its own starts. |
| `deploy/systemd/ratatoskr-notes-cleanup@.service` | The oneshot unit that removes the Sync client, the working tree and the client state of a removed `sync` connection, and of a connection whose login failed. |

The host needs, before the first `sync` connect: the user `ratatoskr-notes-sync`; the group
`ratatoskr-notes-trees` with `ratatoskr-notes` as its only member; the trees root
`/mnt/nvme/ratatoskr/notes/trees` with the owner `ratatoskr-notes-sync`, the group
`ratatoskr-notes-trees` and mode 2750; the clients root `/mnt/nvme/ratatoskr/notes/clients` with
the owner `ratatoskr-notes-sync` and mode 0700; `/run/ratatoskr-notes`
owned by `ratatoskr-notes` (`RuntimeDirectory=` of the service unit); the key file; and `ob` at
`/usr/bin/ob`. The service unit also needs `UMask=0027` for the blob root. The service reads the
journal namespace `ratatoskr-notes-sync` of the sync units with `/usr/bin/journalctl`. So the host
needs this tmpfiles.d entry. It gives the group `ratatoskr-notes` read access to that namespace and
to no other journal (`%m` is the machine id):

```text
d  /var/log/journal/%m.ratatoskr-notes-sync 2755 root systemd-journal - -
A+ /var/log/journal/%m.ratatoskr-notes-sync - - - - group:ratatoskr-notes:r-x,d:group:ratatoskr-notes:r-x
```

The entry is not checked on a real Debian 12 host yet (systemd 252). Check there that
`sudo -u ratatoskr-notes journalctl --namespace=ratatoskr-notes-sync --lines=0` exits 0 after a
sync unit has run. The deploy role of `ratatoskr-workspace` installs the units and the polkit rule that
lets `ratatoskr-notes` start and stop them.

## Workflow

1. Start from an OpenSpec change (`/opsx:propose`), as AGENTS.md describes.
2. For each behaviour, write the failing test, run it, and confirm the failure reason.
3. Write the implementation and run the test again.
4. Run the command list above and `cargo deny check`.

## The Rust skills in this repository

`.agents/skills/` holds the vendored Rust skills and `.claude/skills/` links to them. Do not edit
them here. AGENTS.md explains where a correction goes.
