-- Run as a PostgreSQL administrator after Platform's `01-database-and-roles.sql` has created the database `ratatoskr`, and before the first notes start. Set the password separately with `\password ratatoskr_notes`; no credential belongs in this repository.
-- `create` on the database is all the schema needs: the service creates the `notes` schema on its first start and owns everything in it. The role gets nothing in the `public` schema.

do $$
begin
    if not exists (select 1 from pg_roles where rolname = 'ratatoskr_notes') then
        create role ratatoskr_notes login;
    end if;
end
$$;

alter role ratatoskr_notes nosuperuser nocreatedb nocreaterole noreplication nobypassrls;
grant connect, create on database ratatoskr to ratatoskr_notes;

\connect ratatoskr
revoke all on schema public from ratatoskr_notes;
