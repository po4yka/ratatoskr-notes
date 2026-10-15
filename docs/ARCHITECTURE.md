# Notes architecture

> Status: target architecture, partly implemented. README.md under Project status lists what
> exists. The cross-repository design is `ratatoskr-workspace` ADR-0010, `docs/NOTES_AND_SITES.md`
> and the change `add-notes-vault-connections`, design D1 to D12. This file does not repeat it; it
> shows where each part lives in this repository.

## 1. Components

```
 ratatoskr-sites ──loopback 8099, /internal/ only──┐
 Edge ──loopback 8099──► ratatoskr-notes ──► PostgreSQL schema `notes`
                              │   │
                              │   └──► outbox ──► NATS (evt.notes.*)
                              │
                              └──D-Bus──► systemd: ratatoskr-notes-sync-pull@<id>
                                                   ratatoskr-notes-sync-bidi@<id>
                                                   ratatoskr-notes-login@<id>
                                                   ratatoskr-notes-cleanup@<id>
```

- One process, `ratatoskr-notes`, with two listeners: the domain API on 8099 and the operator plane
  on 9471.
- The domain listener serves two kinds of route. Edge reverse-proxies `/v1/notes/...` with the
  claims of a user. `GET /internal/connections/{connection_id}/files` (workspace change
  `add-site-publications`, design D3) gives the index of every present file of a connection:
  `{"files": [...]}`, each with `path`, `revision` (the notes-owned `BlobRef`), `frontmatter`,
  `tags`, `headings`, `blocks`, `links` and `embeds`, each link with `link`, `display_text`, `form`
  and `target` (the resolved vault path, or `null`). Its only caller is `ratatoskr-sites`, which
  builds site releases with the resolution of notes instead of its own. The route reads no claim.
  Its guard is the peer address: a loopback peer reads it, and any other peer, or a request whose
  peer is not known, gets the empty `404` of an unknown route. Edge's `/v1/notes` row never maps a
  path to `/internal/` (Platform's `gateway.rs::no_public_path_reaches_notes_internal`).
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
| Sync working tree | `/mnt/nvme/ratatoskr/notes/trees/<id>`, mode 2750, owner `ratatoskr-notes-sync`, group `ratatoskr-notes-trees` |
| Headless client state | `/mnt/nvme/ratatoskr/notes/clients/<id>`, mode 0700, owner `ratatoskr-notes-sync` |
| Obsidian secrets key | `/etc/ratatoskr/notes-secrets.key` |
