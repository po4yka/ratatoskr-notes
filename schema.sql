-- The Notes database, in one file.
--
-- `ratatoskr-notes` applies this at startup, to a fresh database. There is no migration ledger and
-- no incremental history: no database holds data that has to survive a schema change (development
-- status). A schema change edits this file in place; the next fresh database has it.
--
-- One schema: `notes`, owned by the role `ratatoskr_notes` in the shared database `ratatoskr`.
-- Notes writes only this schema and reads no other. An owner is an unenforced column that holds the
-- Platform user id from the minted claim, never a cross-schema foreign key.
--
-- Conventions, applied to every table and stated once here:
--
--   * Identifiers are UUIDv7 that the application mints. No id column has a DEFAULT, so a missing
--     id is an insert error.
--
--   * Closed vocabularies are `text` with a CHECK, not a PostgreSQL enum. One statement alters a
--     CHECK; an enum value cannot be added in the transaction that uses it.
--
--   * Every timestamp is `timestamptz`.
--
--   * No column holds an Obsidian secret in readable form (SECURITY.md).

create schema notes;

comment on schema notes is
    'Notes-owned state: vault connections, their encrypted secrets, files, revisions, the index, '
    'and the outbox.';
