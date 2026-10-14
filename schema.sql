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
    -- When the first login and the setup of a `sync` connection succeeded. Null while the login
    -- runs, and for the other channels. At start, the service fails each `sync` connection that is
    -- `connecting` with no value here: a stop of the service interrupted its login.
    logged_in_at       timestamptz,
    -- The connect operation that Edge minted for this connection (workspace design D10). Notes
    -- reports its phases and its end. Null only for a row that a test writes directly.
    operation_id       uuid,
    -- The device of a `plugin` or `cli` connection: the only one whose changes it takes
    -- (workspace design D5). Null for `sync`.
    device_id          uuid,
    -- The folders that the owner excluded, relative to the vault root. A file under one of them is
    -- not indexed and publishes no event (workspace design D7).
    excluded_folders   text[]      not null default '{}',
    -- The `connection_sequence` of the last `notes.file.changed.v1` of this connection. Each event
    -- takes the next number under the lock of this row, in the transaction that writes it to the
    -- outbox, so the numbers start at 1 and have no gap (workspace design D7).
    last_sequence      bigint      not null default 0,
    -- The highest change sequence of the device of a `plugin` or `cli` connection that Notes
    -- committed. A batch continues at the next number; a batch that starts later is a gap, and a
    -- change at or below it is a repeat that changes nothing (workspace design D5). 0 for `sync`.
    device_sequence    bigint      not null default 0,
    created_at         timestamptz not null,
    updated_at         timestamptz not null,

    constraint connections_channel_is_known
        check (channel in ('sync', 'plugin', 'cli')),
    constraint connections_vault_key_is_bounded
        check (length(vault_key) between 1 and 255 and vault_key !~ '[[:cntrl:]]'),
    constraint connections_display_name_is_bounded
        check (length(vault_display_name) between 1 and 255 and vault_display_name !~ '[[:cntrl:]]'),
    -- `connecting` until the first full scan is indexed, then `active`; `login_expired` when the
    -- Sync login stopped working; `failed` when the first login of a `sync` connection failed, so
    -- the connection never became active; `removed` after a disconnect. A failed or removed row
    -- stays, because a connection id is never reused.
    constraint connections_state_is_known
        check (state in ('connecting', 'active', 'login_expired', 'failed', 'removed')),
    constraint connections_updated_at_is_not_before_created_at
        check (updated_at >= created_at),
    constraint connections_only_sync_logs_in
        check (logged_in_at is null or channel = 'sync'),
    constraint connections_last_sequence_is_not_negative
        check (last_sequence >= 0),
    constraint connections_device_sequence_is_not_negative
        check (device_sequence >= 0),
    constraint connections_excluded_folders_are_bounded
        check (cardinality(excluded_folders) <= 64)
);

comment on table notes.connections is
    'One vault connection: an Obsidian vault of one owner and the one channel that reaches it.';

-- One channel per vault: an owner has at most one connection per vault key that is not removed
-- and did not fail, whatever the channel (workspace spec notes-vault-connections). A connection
-- whose login still runs counts too, so that two channels never deliver the same change.
create unique index connections_one_per_owner_and_vault_key
    on notes.connections (owner_id, vault_key)
    where state not in ('failed', 'removed');

-- ---------------------------------------------------------------------------------------------
-- connection_secrets: the encrypted Obsidian passwords of a `sync` connection
-- ---------------------------------------------------------------------------------------------

-- XChaCha20-Poly1305 under the key in /etc/ratatoskr/notes-secrets.key (workspace design D4): 32
-- random bytes, mode 0400, owner ratatoskr-notes. The associated data is the UTF-8 text
-- `ratatoskr-notes:<connection id>:<secret kind>`, so a ciphertext opens only in its own row. The
-- account password and the end-to-end password only: the MFA code is never stored, and the CHECK
-- refuses any other kind. A disconnect and a failed login delete the rows.
create table notes.connection_secrets (
    connection_id uuid        not null references notes.connections (connection_id),
    secret_kind   text        not null,
    nonce         bytea       not null,
    ciphertext    bytea       not null,
    created_at    timestamptz not null,

    primary key (connection_id, secret_kind),

    constraint connection_secrets_kind_is_known
        check (secret_kind in ('account_password', 'e2e_password')),
    -- XChaCha20 takes a 24-byte nonce.
    constraint connection_secrets_nonce_is_24_bytes
        check (length(nonce) = 24),
    -- Poly1305 appends a 16-byte tag, so a ciphertext of a non-empty secret is longer than 16.
    constraint connection_secrets_ciphertext_holds_a_tag
        check (length(ciphertext) > 16)
);

comment on table notes.connection_secrets is
    'Ciphertext of the Obsidian account password and end-to-end password; never the MFA code.';

-- ---------------------------------------------------------------------------------------------
-- files: one path of a vault connection, stable across its revisions
-- ---------------------------------------------------------------------------------------------

create table notes.files (
    file_id       uuid        primary key,
    connection_id uuid        not null references notes.connections (connection_id),
    -- The path relative to the vault root, in Unicode NFC.
    path          text        not null,
    -- The path folded with Unicode default case folding. Two paths with one key are a collision
    -- (workspace design D7); the second one gets no row here.
    path_key      text        not null,
    -- `present`, or `deleted` after the file left the vault or became excluded. A file that comes
    -- back keeps its id.
    state         text        not null,
    -- The lowercase hex SHA-256 of the current bytes; of the last bytes for a deleted file.
    revision      text        not null,
    media_type    text        not null,
    length_bytes  bigint      not null,
    created_at    timestamptz not null,
    updated_at    timestamptz not null,

    constraint files_one_per_path_key unique (connection_id, path_key),
    constraint files_state_is_known check (state in ('present', 'deleted')),
    constraint files_revision_is_sha256 check (revision ~ '^[0-9a-f]{64}$'),
    constraint files_path_is_bounded check (length(path) between 1 and 1024),
    constraint files_length_is_not_negative check (length_bytes >= 0)
);

comment on table notes.files is
    'One file of a vault connection by its NFC path and its case-folded path key.';

-- ---------------------------------------------------------------------------------------------
-- revisions: the bytes of a file at one SHA-256, in the notes blob root
-- ---------------------------------------------------------------------------------------------

create table notes.revisions (
    file_id      uuid        not null references notes.files (file_id),
    revision     text        not null,
    media_type   text        not null,
    length_bytes bigint      not null,
    -- The index of a Markdown revision (frontmatter, tags, headings, block ids, aliases, links and
    -- embeds as the note writes them; workspace design D8). Null for other files.
    note_index   jsonb,
    created_at   timestamptz not null,

    primary key (file_id, revision),

    constraint revisions_revision_is_sha256 check (revision ~ '^[0-9a-f]{64}$'),
    constraint revisions_length_is_not_negative check (length_bytes >= 0)
);

comment on table notes.revisions is
    'Each revision of a file; the bytes are in the notes blob root under the SHA-256.';

-- ---------------------------------------------------------------------------------------------
-- connection_blobs: the bytes that a device uploaded through an upload session of a connection
-- ---------------------------------------------------------------------------------------------

-- A batch or a manifest of a `plugin` or `cli` connection may name only bytes of that connection:
-- a row here, a revision of one of its files, or the bytes of no bytes. A hash alone is not a
-- right to read the bytes that another connection stored (workspace spec notes-device-channels).
create table notes.connection_blobs (
    connection_id uuid        not null references notes.connections (connection_id),
    revision      text        not null,
    length_bytes  bigint      not null,
    received_at   timestamptz not null,

    primary key (connection_id, revision),

    constraint connection_blobs_revision_is_sha256 check (revision ~ '^[0-9a-f]{64}$'),
    constraint connection_blobs_length_is_not_negative check (length_bytes >= 0)
);

comment on table notes.connection_blobs is
    'The bytes that a device uploaded for one connection; the bytes are in the notes blob root.';

-- ---------------------------------------------------------------------------------------------
-- path_collisions: a path whose key another present file of the connection holds
-- ---------------------------------------------------------------------------------------------

create table notes.path_collisions (
    connection_id uuid        not null references notes.connections (connection_id),
    -- The NFC path that got no revision.
    path          text        not null,
    path_key      text        not null,
    -- The facts of the bytes of that path, so that the path gets its revision when the file that
    -- holds its key leaves, as after a rename that changes only the case.
    revision      text        not null,
    media_type    text        not null,
    length_bytes  bigint      not null,
    observed_at   timestamptz not null,

    primary key (connection_id, path),

    constraint path_collisions_revision_is_sha256 check (revision ~ '^[0-9a-f]{64}$'),
    constraint path_collisions_length_is_not_negative check (length_bytes >= 0)
);

comment on table notes.path_collisions is
    'The condition notes.file.path_collision of a connection, with the path that got no revision.';

-- ---------------------------------------------------------------------------------------------
-- outbox: events Notes owes the bus, written in the same transaction as their cause
-- ---------------------------------------------------------------------------------------------

create table notes.outbox (
    event_id       uuid        primary key,
    -- The order in which the relay publishes. The events of one connection take this number under
    -- the lock of the connection row, so it follows their `connection_sequence`.
    position       bigint      generated always as identity,
    event_type     text        not null,
    aggregate_type text        not null,
    -- An entity reference, for example `notes_connection:<uuid>`.
    aggregate_id   text        not null,
    -- The whole event envelope. It carries ids, the channel and the display name, never a secret
    -- and never file content.
    payload        jsonb       not null,
    created_at     timestamptz not null,
    published_at   timestamptz,

    -- A CLOSED list: a type that Notes does not produce yet cannot be inserted.
    constraint outbox_event_type_is_known
        check (event_type in ('notes.vault.connected.v1', 'notes.vault.disconnected.v1',
                              'notes.file.changed.v1', 'platform.operation.reported.v1')),
    constraint outbox_aggregate_type_is_bounded
        check (length(aggregate_type) between 1 and 64)
);

comment on table notes.outbox is
    'Transactional outbox. The relay to JetStream under the NOTES identity reads it.';

create index outbox_unpublished_idx on notes.outbox (position) where published_at is null;
