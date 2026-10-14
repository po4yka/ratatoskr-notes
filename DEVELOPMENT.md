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
bus. When the service starts a login or cleanup unit, the fake reads the shipped file in
`deploy/systemd/` and runs its `ExecStart=` lines with `%i` replaced, `$$` read as `$`, the host
paths moved into a temporary directory, and `/usr/bin/ob` replaced by a stub. It reads
`Environment=`, `EnvironmentFile=` and `LoadCredential=`, and gives the unit a
`$CREDENTIALS_DIRECTORY` that it removes after the run. A test therefore checks the shipped unit
file. Keep each command of a unit on one line, write `$$` for each shell `$`, and use no backslash.

`secret_custody.rs::no_secret_in_any_cmdline` reads `/proc/*/cmdline` as the user `nobody` while
the login runs, so it runs on Linux only. On macOS it is ignored. When the test runs as root, the
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
| `RATATOSKR__ADMIN__BIND` | `127.0.0.1:9471` | The operator listener. |
| `RATATOSKR__API__LISTEN_ADDRESS` | `127.0.0.1:8099` | The domain API listener. It must differ from the operator listener. |
| `RATATOSKR__SECRETS__KEY_FILE` | `/etc/ratatoskr/notes-secrets.key` | The 32-byte key of the stored Obsidian passwords, mode 0400 or 0600. Read at each `sync` connect. |
| `RATATOSKR__SHUTDOWN__GRACE_SECONDS` | `10` | How long in-flight requests may run after SIGTERM, 1 to 60. |
| `RATATOSKR__TELEMETRY__LOG_FORMAT` | `text` | `text` or `json`. |
| `RATATOSKR__TELEMETRY__LOG_FILTER` | `info` | A tracing `EnvFilter` directive string. |

An unknown `RATATOSKR__` key stops the start with exit 78.

## Schema

`schema.sql` at the repository root is the whole schema. The binary embeds it and applies it to a
database that has no `notes` schema. There are no migrations: a schema change edits `schema.sql`
in place, and the next test database has it.

## Deploy files

| File | Purpose |
|---|---|
| `deploy/postgres/role.sql` | Creates the role `ratatoskr_notes` in the shared database. |
| `deploy/nats/identity.conf` | A copy of the `NOTES` stanza of Platform's NATS authorization file. |
| `deploy/systemd/notes.conf.example` | The environment file of the service on the host. |
| `deploy/systemd/ratatoskr-notes-login@.service` | The oneshot unit that runs the first `ob login` and `ob sync-setup` of a `sync` connection. It reads the secrets with `LoadCredential=` from `/run/ratatoskr-notes/credentials/<id>/`. |
| `deploy/systemd/ratatoskr-notes-cleanup@.service` | The oneshot unit that removes the Sync client, the working tree and the client state of a removed `sync` connection, and of a connection whose login failed. |

The host needs, before the first `sync` connect: the user `ratatoskr-notes-sync`; the directory
`/mnt/nvme/ratatoskr/notes/connections` with that owner and mode 0700; `/run/ratatoskr-notes`
owned by `ratatoskr-notes` (`RuntimeDirectory=` of the service unit); the key file; and `ob` at
`/usr/bin/ob`. The deploy role of `ratatoskr-workspace` installs the units and the polkit rule that
lets `ratatoskr-notes` start and stop them.

## Workflow

1. Start from an OpenSpec change (`/opsx:propose`), as AGENTS.md describes.
2. For each behaviour, write the failing test, run it, and confirm the failure reason.
3. Write the implementation and run the test again.
4. Run the command list above and `cargo deny check`.

## The Rust skills in this repository

`.agents/skills/` holds the vendored Rust skills and `.claude/skills/` links to them. Do not edit
them here. AGENTS.md explains where a correction goes.
