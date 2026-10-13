# Notes architecture

> Status: target architecture, partly implemented. README.md under Project status lists what
> exists. The cross-repository design is `ratatoskr-workspace` ADR-0010, `docs/NOTES_AND_SITES.md`
> and the change `add-notes-vault-connections`, design D1 to D12. This file does not repeat it; it
> shows where each part lives in this repository.

## 1. Components

```
 Edge ──loopback 8099──► ratatoskr-notes ──► PostgreSQL schema `notes`
                              │   │
                              │   └──► outbox ──► NATS (evt.notes.*)
                              │
                              └──D-Bus──► systemd: ratatoskr-notes-sync@<id>
                                                   ratatoskr-notes-login@<id>
                                                   ratatoskr-notes-cleanup@<id>
```

- One process, `ratatoskr-notes`, with two listeners: the domain API on 8099 and the operator plane
  on 9471.
- One schema, `notes`, in the shared database `ratatoskr`. `schema.sql` is the whole schema.
- For each `sync` connection, systemd runs Obsidian Headless (`ob`) as the user
  `ratatoskr-notes-sync`. Notes starts and stops the units through the systemd D-Bus API and one
  polkit rule.

## 2. Packages

| Package | Path | Holds |
|---|---|---|
| `ratatoskr-notes-persistence` | `crates/persistence` | The pool, the embedded schema, the queries, the disposable test database. |
| `ratatoskr-notes` | `services/notes` | Configuration, the process lifecycle, the operator plane and the domain routes. |

## 3. Storage

| What | Where |
|---|---|
| Database | schema `notes`, role `ratatoskr_notes`, database `ratatoskr` |
| Note file bytes | `/mnt/nvme/ratatoskr/blobs/ratatoskr-notes/sha256/<2 hex>/<rest>` |
| Sync working tree | `/mnt/nvme/ratatoskr/notes/connections/<id>/tree` |
| Headless client state | `/mnt/nvme/ratatoskr/notes/connections/<id>/ob-home`, mode 0700, owner `ratatoskr-notes-sync` |
| Obsidian secrets key | `/etc/ratatoskr/notes-secrets.key` |
