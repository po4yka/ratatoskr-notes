# Ratatoskr Notes

`ratatoskr-notes` is the notes bounded context of Ratatoskr. It connects the owner's Obsidian
vaults, keeps a complete and current copy of each vault, records a revision for each change of a
file, and indexes each Markdown file. Knowledge reads the note bytes and the file events to search
and analyse the notes.

> **Status:** scaffolding. The service starts, applies the `notes` schema, and serves its operator
> plane. Connections, the Sync supervisor, the index and the device channels are planned in the
> workspace change `add-notes-vault-connections` (OBS-023), groups 5 to 8.

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

The domain API serves `POST /v1/notes/connections`.

## Project status

- [x] Service skeleton: strict configuration, logging, operator plane, embedded schema, CI gate.
- [ ] Connections and the one-channel rule (tasks 5.2 to 5.5).
- [ ] Secret custody and the login unit (tasks 5.6 to 5.9).
- [ ] Sync supervisor and blob root (group 6).
- [ ] Index, file events and the read API (group 7).
- [ ] Device channel routes (group 8).

## Development

See [DEVELOPMENT.md](DEVELOPMENT.md). The local development database is a convenience of
`compose.yaml`; on the host the service uses the shared database `ratatoskr`.

## Licence

BSD-3-Clause. See [LICENSE](LICENSE).
