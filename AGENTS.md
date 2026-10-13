# Ratatoskr Notes Agent Instructions

## Scope

These instructions apply to the `ratatoskr-notes` repository.

This repository owns the notes bounded context: connected Obsidian vaults, their files, file
revisions and the bytes of each revision, the index of each Markdown file, and the custody of the
Obsidian secrets that a `sync` connection needs.

## Repository mission

Notes gives Ratatoskr a complete and current copy of every vault that the owner connects. It
answers two questions:

1. Which vaults are connected, through which channel, and in which state is each connection?
2. What is the current content of each file, and which revisions came before it?

Notes never writes into a vault in this phase. The write path is the workspace change
`add-notes-write-back` (OBS-024).

## Current phase

The repository starts with the workspace change `add-notes-vault-connections` (OBS-023). The
service, the `notes` schema, the operator plane and the deploy examples exist. Read README.md under
Project status for the parts that exist today. Do not assume anything beyond what is present in the
checkout.

### Development status

Ratatoskr is in development. No database holds data that has to survive a schema change. While this
status holds, these rules are binding, and they override anything else in this repository that
plans otherwise, including the rest of this file:

- **One version only.** The API, the database, and the contracts keep their first version. Do not
  add a `v2` or a later major version, and do not add version negotiation, deprecation windows, or
  parallel-major routing.
- **No database migrations.** Do not add a migration file, and do not add migration tooling. A
  schema change edits the current schema definition in place, and a test database is created from
  that definition.
- **The product is `Ratatoskr`.** It is not "Ratatoskr Next". Do not write that name in code,
  documentation, identifiers, comments, or commit messages.

Only the repository owner changes this status. Ask before you write anything these rules forbid.

## How a change starts

Every non-trivial change begins as an OpenSpec change rather than as an edit, and each assistant
starts one in its own syntax. Claude Code has the command: `/opsx:propose <what you want to build>`,
or `/opsx:explore` first when the shape is not clear yet. Codex has no project-level command and
triggers the same skill by name, `$openspec-propose`, or lets its description match it. OpenCode has
its own command, `/opsx-propose`. Whichever starts it, the result is `openspec/changes/<id>/` holding
a proposal, the spec deltas, a design and a task list, and you read that plan before any code is
written. `/opsx:apply`, `$openspec-apply-change` or `/opsx-apply` builds it, and `/opsx:archive`,
`$openspec-archive-change` or `/opsx-archive` folds the deltas into `openspec/specs/`.

`openspec/specs/` holds the behaviour that is true today, and it starts empty on purpose. A spec here
grows from a change that needed it. Do NOT convert `docs/REQUIREMENTS.md`, `docs/INTERFACES.md`,
`docs/DOMAIN.md` or `docs/DATA_MODEL.md` into specs in bulk. Those documents stay where they are, as
material an exploration reads. A spec set produced by bulk conversion is large, stale on the day it
lands, and trusted by nobody.

Behaviour that more than one repository can see — the shape of a contract, the meaning of a field, the
order in which repositories must receive a change — belongs in the `ratatoskr-workspace` store, not
here. `openspec/config.yaml` references it, so `openspec instructions` in this repository lists the
store's specs with the exact command that fetches one. Cite that spec from a local proposal instead
of restating it.

### Tests come first

The task list carries one pair per behaviour. The first task adds a test that fails. The second makes
it pass. Never one task that does both.

- Run the new test before you write the implementation, and confirm it fails for the reason the task
  states — not for a compile error or a typo.
- A refactor task comes after the tests are green. It adds no test and changes no behaviour.
- A task that cannot start from a failing test says why in one line. Configuration, documentation and
  generated files are the usual reasons.
- Do not tick a task whose test has not been run.

Nothing can check the order in which the two were written. What CI does check is
`openspec validate --archived`, which fails when a change was archived with a task left unticked, and
the step in `fleet.yml` that fails when a repository holds a manifest and a `ci.yml` that never runs
a test. `ratatoskr-workspace/docs/QUALITY_GATES.md` states that limit rather than implying it is
covered.

## The Rust skill catalogue

`.agents/skills/` holds eighteen Rust skills, and `.claude/skills/` symlinks to them, so all three
assistants read one copy. Codex reads `.agents/skills/`, Claude Code reads `.claude/skills/`, and
OpenCode scans both, so the existing symlink already covers it and nothing belongs under
`.opencode/skills/`. Each is a reference sheet rather than a tutorial: the commands, flags,
thresholds and triage tables for one Rust concern. Your assistant reads the descriptions and opens a
skill only when the task matches one, so the set costs almost nothing until it is needed.

`rust-tdd` is the Rust form of the task pair above. `rust-lints` owns `clippy.toml`, which is where
this repository's size limits live. `rust-security` answers a `RUSTSEC` advisory.
`rust-async-internals` covers `tokio::select!` cancel safety and shutdown. `rust-database` covers
pool budgets and transaction ownership. `rust-compiler-errors` is the entry point when the build
fails and the cause is not obvious.

`rust-database` also carries a section on deploying migrations in compatible phases. The Development
status above overrides it: while that status holds, this product has no migrations at all. Read the
rest of that skill and skip that section.

The eighteen are identical in every Ratatoskr repository whose stack is Rust, and
`ratatoskr-workspace/.github/workflows/drift.yml` fails when one copy stops matching the others. Do
not edit a file under `.agents/skills/`. A correction belongs upstream in `po4yka/rust-skills` and
reaches this repository through `npx skills update`.

The catalogue holds forty-four skills and eighteen are vendored here.
`ratatoskr-workspace/docs/QUALITY_GATES.md` records which were left out and why. They are vendored
under BSD-3-Clause, (c) 2026 Nikita Pochaev, who also owns this repository; each `SKILL.md` keeps its
`license` field, and the full text is in that repository's `LICENSE`.

## Deployment target

**The host is frozen: nothing is deployed to it and nothing on it is changed.** It stays in this document as a set of constraints on the code you write, and for no other reason. Do not offer to deploy, do not measure the running host against `main`, and do not treat what is installed there as the state of this repository. Everything below is a budget, not an environment.

Ratatoskr runs on ONE machine and there is no second one: a Raspberry Pi 5, `aarch64`, 4 cores, 16 GiB RAM, Debian 12 with glibc 2.36 and systemd 252, a 16 KiB page size, root moving to an NVMe SSD and every durable byte under `/mnt/nvme`. One systemd unit supervises `ratatoskr-notes`. Three template units run the Obsidian Headless client (`ob`, Node.js 22) for each `sync` connection: `ratatoskr-notes-sync@<id>.service`, `ratatoskr-notes-login@<id>.service` and `ratatoskr-notes-cleanup@<id>.service`. There is no orchestrator, no pod, no replica, no rolling deployment and no secret store. `ratatoskr-workspace/docs/DEPLOYMENT_TARGET.md` holds the host description, the storage contract and the port table; this section is complete without it.

You may assume:

- exactly one `ratatoskr-notes` process, started by systemd on that host;
- one PostgreSQL cluster (major 17) and one NATS server, shared with other tenants on the box;
- a path under `/mnt/nvme` is durable storage: the blob root `/mnt/nvme/ratatoskr/blobs/ratatoskr-notes` and the working trees and client state under `/mnt/nvme/ratatoskr/notes/connections/<id>/`;
- the operator is one person with `ssh`, not a pipeline with an API.

Stop assuming:

- **a second instance.** Never answer a load or availability problem with a second process. The schema advisory lock, the outbox and the uniqueness of a connection STAY: they make a restart and a redelivery safe with one process.
- **a rolling deployment.** An upgrade is a full stop and start.
- **spare memory or CPU.** The host runs other services and has no disk swap. Each `ob` process costs memory, so each sync unit carries `MemoryMax=`. Every queue, cache, batch, connection pool and worker pool carries an explicit bound.
- **the boot device.** New durable state gets an absolute path under `/mnt/nvme`, named in the deployment unit.
- **that a dependency runs here.** Every crate, image and binary needs a `linux/arm64` build against glibc 2.36 that works on 16 KiB pages. `PrivatePIDs=` needs systemd 257 and is not available.
- **a free port.** The domain API is `8099` and the operator listener is `9471`, both allocated in the port table. A compiled default is for a developer machine only. Never widen a bind to `0.0.0.0`.

The domain API is reached only through Edge on loopback, and it trusts only the claims that Edge mints. The operator listener (`/health/*`, `/metrics`, `/version`), PostgreSQL and NATS are never reachable from outside the host.

## Sources of truth

Use this order:

1. the active task or changeset, and the workspace change `add-notes-vault-connections` with its
   specs;
2. accepted ADRs, in this repository and `ratatoskr-workspace/docs/adr/0010-notes-and-obsidian-vault-connections.md`;
3. `README.md`;
4. the contracts in `ratatoskr-contracts` (`ratatoskr-notes-contracts`);
5. repository tests;
6. implementation details.

`ratatoskr-workspace/docs/NOTES_AND_SITES.md` holds the shared names: subjects, routes, ports,
storage paths and error codes. Use those names and no others.

## Hard bounded-context rules

### Notes owns

- vault connections, their channel, their vault key and their state;
- the encrypted Obsidian account password and end-to-end password of a `sync` connection;
- the working tree and the client state of each `sync` connection;
- files, path keys, revisions and the bytes of each revision in the notes blob root;
- the index of each Markdown file;
- the notes outbox.

### Notes does not own

- user identity, sessions or device pairing (Platform);
- analysis, embeddings or search (Knowledge);
- sites and publications (`ratatoskr-sites`, later changes).

Use the word "vault" alone for an Obsidian vault only in contract names that already use it. In
code and documents, say **vault connection**. `ratatoskr-vault` is the Git backup service.

## Secret custody

These rules come from design D4 of the workspace change. Do not weaken them.

- The account password and the end-to-end password are stored only as ciphertext. The MFA code is
  never stored.
- No secret goes into argv, a log line, an event, an operation report, an error envelope or a
  metrics label.
- A secret reaches the login unit only as a systemd credential (`LoadCredential=`), and `ob` reads
  it on standard input.
- Tests use obvious fake marker values only. Never commit a real credential.

## Testing expectations

- Every behaviour starts from a failing test, as the task pairs above require.
- Tests that touch the database use a disposable database from
  `ratatoskr_notes_persistence::test_support`.
- The systemd D-Bus API is reached only through one trait, so a test uses a fake bus and the real
  implementation uses the same trait.
- A test never reads the wall clock (`fleet.yml` checks it).

## Completion criteria

A task is complete only when:

- the responsibility belongs to Notes;
- its test failed first for the stated reason and passes now;
- the command list in DEVELOPMENT.md passes, and so does `cargo deny check`;
- no secret appears anywhere the secret custody rules forbid;
- the schema change, if any, edits `schema.sql` in place.
