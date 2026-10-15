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
    -- Whether Ratatoskr may write into the vault (OBS-024 design D8). Every connection starts
    -- `off`; the owner turns it on.
    write_mode         text        not null default 'off',
    -- The delivery sequence of the last write of this connection that became `delivering`. Each
    -- delivery takes the next number under the lock of this row, so the numbers of one connection
    -- grow in the order of delivery; the device of a `plugin` or `cli` connection reads its queue
    -- by them (OBS-024 design D5).
    last_delivery      bigint      not null default 0,
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
        check (cardinality(excluded_folders) <= 64),
    constraint connections_write_mode_is_known
        check (write_mode in ('off', 'on')),
    constraint connections_last_delivery_is_not_negative
        check (last_delivery >= 0)
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
-- The removal of a connection deletes its rows; the bytes stay in the blob root.
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
-- writes: the writes that Ratatoskr asked for, and the queue of each file (OBS-024)
-- ---------------------------------------------------------------------------------------------

-- The queue of a file is the open writes (`queued` or `delivering`) of one connection with one
-- path key, in the order of `position`, and the open `rename` and `move` writes whose target has
-- that key. Only the head of a queue is delivered, and a `rename` or a `move` only when it is the
-- head of both queues; the next write waits until the channel acknowledged or refused the head
-- (OBS-024 design D1). A write keeps its row after it ended. The delivering writes of a `plugin` or
-- `cli` connection, in the order of `delivery_sequence`, are the queue that its device reads (design
-- D5); `delivery_sequence` is the cursor of that queue. It is not `position`: a write is delivered
-- only after the writes before it on its path, so the order of delivery differs from the order of
-- acceptance across paths.
create table notes.writes (
    write_id       uuid        primary key,
    connection_id  uuid        not null references notes.connections (connection_id),
    -- The order of acceptance.
    position       bigint      generated always as identity,
    -- The operation that Edge minted for the write (kind `notes.file.write`). A retry under
    -- another operation is in `write_operations`.
    operation_id   uuid        not null,
    -- The idempotency record: the hex SHA-256 key that the writer chose, and the hex SHA-256 of
    -- the origin and the operation of the command. One key of a connection names one write; the
    -- same key with another digest is refused (OBS-024 spec notes-write-command).
    idempotency_key text       not null,
    request_digest  text       not null,
    -- `platform` or `site:<site id>`.
    origin         text        not null,
    kind           text        not null,
    -- The NFC path that the write changes, and its case-folded key, which names the queue.
    path           text        not null,
    path_key       text        not null,
    -- The new path of a `rename` or a `move`, and its case-folded key.
    target_path    text,
    target_path_key text,
    -- The revision that the writer saw.
    base_hash      text,
    -- The SHA-256 and the length of the uploaded bytes that the write puts at the path.
    content_hash   text,
    content_length bigint,
    -- The conflict that a `resolve_conflict` ends, and its choice. No foreign key: the table
    -- `conflicts` names its write and comes after this one.
    resolves       uuid,
    choice         text,
    -- The frontmatter property of a `set_property`, and whether it is `set` (to the JSON value
    -- whose bytes `content_hash` names) or `remove`.
    property_key    text,
    property_change text,
    -- `queued`, then `delivering`, then `succeeded` or `failed`.
    state          text        not null,
    -- The number of the delivery of this write in its connection, from `connections.last_delivery`;
    -- null until the write is delivering.
    delivery_sequence bigint,
    -- Whether the sync writer landed the write in the working tree of its `sync` connection. From
    -- then on, the next change of its path that the watcher reports is its acknowledgement, also
    -- with other bytes (OBS-024 design D6).
    landed         boolean     not null default false,
    -- The SHA-256 of the file after a write that succeeded; null after a delete.
    revision       text,
    -- The error code of a write that failed.
    error_code     text,
    -- The conflict that a write which failed with `notes.write.conflict` raised.
    conflict_id    uuid,
    -- The write group of a `rename` or a `move` and the link rewrites it needs (OBS-024 spec
    -- notes-link-rewrite): the `write_id` of the rename, on the rename and on each rewrite, an
    -- `update` of a linking note. Null for a write outside a group. A group is delivered only when
    -- it is the head of every queue it touches, and it ends as one operation.
    group_id       uuid,
    -- The SHA-256 of the file that the channel reported when it refused the write with a base
    -- mismatch; a conflict of a write group names it as the current side.
    local_hash     text,
    created_at     timestamptz not null,
    updated_at     timestamptz not null,

    constraint writes_kind_is_known
        check (kind in ('create', 'update', 'rename', 'move', 'delete', 'set_property',
                        'resolve_conflict')),
    constraint writes_state_is_known
        check (state in ('queued', 'delivering', 'succeeded', 'failed')),
    constraint writes_origin_is_known
        check (origin ~ '^(platform|site:[a-z0-9-]{3,40})$'),
    constraint writes_base_hash_is_sha256 check (base_hash ~ '^[0-9a-f]{64}$'),
    constraint writes_content_hash_is_sha256 check (content_hash ~ '^[0-9a-f]{64}$'),
    constraint writes_revision_is_sha256 check (revision ~ '^[0-9a-f]{64}$'),
    constraint writes_local_hash_is_sha256 check (local_hash ~ '^[0-9a-f]{64}$'),
    constraint writes_content_length_is_not_negative check (content_length >= 0),
    constraint writes_path_is_bounded check (length(path) between 1 and 1024),
    constraint writes_failure_has_a_code check ((state = 'failed') = (error_code is not null)),
    constraint writes_resolution_names_its_conflict
        check ((kind = 'resolve_conflict') = (resolves is not null and choice is not null)),
    constraint writes_choice_is_known
        check (choice in ('keep_current', 'keep_writer', 'accept_merge', 'custom')),
    constraint writes_one_per_delivery unique (connection_id, delivery_sequence),
    constraint writes_delivery_is_numbered
        check (state <> 'delivering' or delivery_sequence is not null),
    constraint writes_target_has_its_key
        check ((target_path is null) = (target_path_key is null)),
    constraint writes_property_names_its_change
        check ((kind = 'set_property') = (property_key is not null and property_change is not null)),
    constraint writes_property_change_is_known check (property_change in ('set', 'remove')),
    constraint writes_one_per_idempotency_key unique (connection_id, idempotency_key),
    constraint writes_idempotency_key_is_sha256 check (idempotency_key ~ '^[0-9a-f]{64}$'),
    constraint writes_request_digest_is_sha256 check (request_digest ~ '^[0-9a-f]{64}$')
);

comment on table notes.writes is
    'Each write of a vault file that Ratatoskr asked for; the open ones form one queue per file.';

create index writes_open_idx on notes.writes (connection_id, path_key, position)
    where state in ('queued', 'delivering');

create index writes_group_idx on notes.writes (connection_id, group_id, position)
    where group_id is not null;

create index writes_open_target_idx on notes.writes (connection_id, target_path_key, position)
    where state in ('queued', 'delivering') and target_path_key is not null;

-- The queue that the device of a `plugin` or `cli` connection reads.
create index writes_delivering_idx on notes.writes (connection_id, delivery_sequence)
    where state = 'delivering';

-- Every operation that waits for a write: the first one, and each one that repeated its
-- idempotency key. Each of them reports the outcome of the write.
create table notes.write_operations (
    operation_id uuid        primary key,
    write_id     uuid        not null references notes.writes (write_id),
    created_at   timestamptz not null
);

comment on table notes.write_operations is
    'The operations that report the outcome of one write: the first, and each retry of its key.';

create index write_operations_write_idx on notes.write_operations (write_id);

-- ---------------------------------------------------------------------------------------------
-- conflicts: a write that met a revision its writer did not see (OBS-024 design D2)
-- ---------------------------------------------------------------------------------------------

-- The three sides are hashes; their bytes are in the blob root. A conflict stays open until a
-- `resolve_conflict` write ends it.
create table notes.conflicts (
    conflict_id       uuid        primary key,
    connection_id     uuid        not null references notes.connections (connection_id),
    -- The write that raised the conflict, and its operation.
    write_id          uuid        not null references notes.writes (write_id),
    operation_id      uuid        not null,
    path              text        not null,
    path_key          text        not null,
    origin            text        not null,
    media_type        text        not null,
    -- The revision that the writer saw, the revision of the vault, and the writer's content. A
    -- side that does not exist is null: a `create` has no base, a deleted file no current
    -- revision, a `delete` or a `rename` no content.
    base_hash         text,
    current_hash      text,
    writer_hash       text,
    -- The three-way merge proposal of a Markdown or plain-text file whose three sides notes holds:
    -- the hash of the merged bytes in the blob root, and whether the merge has overlapping hunks.
    merged_hash       text,
    has_overlapping_hunks boolean,
    -- Whether `notes.conflict.raised.v1` is in the outbox. The transaction that fails the write
    -- records the conflict unannounced; the merge proposal is computed after its commit, outside
    -- the lock of the connection row, and a second short transaction stores it and writes the
    -- event. The start of the service announces a conflict that a stop left unannounced. A
    -- resolution marks its conflict announced, so a conflict resolved before its announcement
    -- gets no raised event.
    announced         boolean     not null,
    -- Every file of a write group that was stale or not applied, sorted, when the conflict ends a
    -- rename and its link rewrites; `path` is the first of them. Null for a conflict of one file.
    stale_paths       text[],
    -- `open`, or `resolved` after a resolution that the channel acknowledged.
    state             text        not null,
    created_at        timestamptz not null,
    resolved_at       timestamptz,

    constraint conflicts_state_is_known check (state in ('open', 'resolved')),
    constraint conflicts_base_hash_is_sha256 check (base_hash ~ '^[0-9a-f]{64}$'),
    constraint conflicts_current_hash_is_sha256 check (current_hash ~ '^[0-9a-f]{64}$'),
    constraint conflicts_writer_hash_is_sha256 check (writer_hash ~ '^[0-9a-f]{64}$'),
    constraint conflicts_resolution_has_a_time check ((state = 'resolved') = (resolved_at is not null)),
    constraint conflicts_merged_hash_is_sha256 check (merged_hash ~ '^[0-9a-f]{64}$'),
    constraint conflicts_proposal_is_whole
        check ((merged_hash is null) = (has_overlapping_hunks is null))
);

comment on table notes.conflicts is
    'A write that met a revision its writer did not see: the three sides, by hash.';

create index conflicts_connection_idx on notes.conflicts (connection_id, created_at);

create index conflicts_unannounced_idx on notes.conflicts (conflict_id)
    where not announced and state = 'open';

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
                              'notes.file.changed.v1', 'notes.conflict.raised.v1',
                              'notes.conflict.resolved.v1', 'platform.operation.reported.v1')),
    constraint outbox_aggregate_type_is_bounded
        check (length(aggregate_type) between 1 and 64)
);

comment on table notes.outbox is
    'Transactional outbox. The relay to JetStream under the NOTES identity reads it.';

create index outbox_unpublished_idx on notes.outbox (position) where published_at is null;

-- One end per connect operation: at most one terminal report (`succeeded`, `partially_succeeded`
-- or `failed`) of an operation is in the outbox. A second end, for example a connect that finds
-- its connection already `login_expired`, is not written (workspace design D10).
create unique index outbox_one_terminal_report
    on notes.outbox ((payload -> 'payload' ->> 'operation_id'))
    where event_type = 'platform.operation.reported.v1'
      and payload -> 'payload' ->> 'status' in ('succeeded', 'partially_succeeded', 'failed');
