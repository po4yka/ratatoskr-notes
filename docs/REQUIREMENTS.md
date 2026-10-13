# Notes requirements

The requirements that more than one repository can see are specs in the `ratatoskr-workspace`
store. This file lists them and does not restate them.

| Spec (workspace change `add-notes-vault-connections`) | What it requires of Notes |
|---|---|
| `notes-vault-connections` | One channel per connection; uniqueness of `(owner, vault key)`; the connect operation; secret custody; the login and cleanup units; the pull-only Sync device; the connection events. |
| `notes-device-channels` | The owner and channel checks; one device per device connection; bounded, ordered and idempotent change batches; blob uploads; manifest reconciliation. |
| `notes-file-changes` | `evt.notes.file.changed.v1`: NFC paths, path keys, revisions, exclusions and the gapless connection sequence. |
| `notes-file-index` | The index of a Markdown file, Obsidian's link resolution, and the read API. |
| `postgres-provisioning` | The role `ratatoskr_notes` and the database URL of the deploy example. |
| `fleet-bus-topology` | The `NOTES` identity, its inbox prefix, the notes upstream 8099 and the operator listener 9471. |

Requirements that only this repository can see grow as specs in `openspec/specs/` from the changes
that need them.
