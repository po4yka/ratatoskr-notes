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

-- ---------------------------------------------------------------------------------------------
-- connections: one Obsidian vault of one owner, reached through exactly one channel
-- ---------------------------------------------------------------------------------------------

create table notes.connections (
    connection_id      uuid        primary key,
    -- The Platform user id from the claim that Edge mints. Unenforced: no cross-schema key.
    owner_id           uuid        not null,
    channel            text        not null,
    -- `sync`: the remote vault id that `ob sync-list-remote` reports. `plugin` and `cli`: the
    -- random id that the device keeps beside the vault (workspace design D2).
    vault_key          text        not null,
    vault_display_name text        not null,
    state              text        not null,
    created_at         timestamptz not null,
    updated_at         timestamptz not null,

    constraint connections_channel_is_known
        check (channel in ('sync', 'plugin', 'cli')),
    constraint connections_vault_key_is_bounded
        check (length(vault_key) between 1 and 255 and vault_key !~ '[[:cntrl:]]'),
    constraint connections_display_name_is_bounded
        check (length(vault_display_name) between 1 and 255 and vault_display_name !~ '[[:cntrl:]]'),
    -- `connecting` until the first full scan is indexed, then `active`; `login_expired` when the
    -- Sync login stopped working; `removed` after a disconnect. A removed row stays, because a
    -- connection id is never reused.
    constraint connections_state_is_known
        check (state in ('connecting', 'active', 'login_expired', 'removed')),
    constraint connections_updated_at_is_not_before_created_at
        check (updated_at >= created_at)
);

comment on table notes.connections is
    'One vault connection: an Obsidian vault of one owner and the one channel that reaches it.';

-- One channel per vault: an owner has at most one connection per vault key that is not removed,
-- whatever the channel (workspace spec notes-vault-connections). A connection whose login still
-- runs counts too, so that two channels never deliver the same change.
create unique index connections_one_per_owner_and_vault_key
    on notes.connections (owner_id, vault_key)
    where state <> 'removed';
