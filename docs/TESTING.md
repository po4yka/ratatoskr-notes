# Notes testing

## Rules

- Every behaviour starts from a failing test. The task list pairs a RED task with a GREEN task.
- A test that needs PostgreSQL creates a disposable database with
  `ratatoskr_notes_persistence::test_support::TestDatabase` and drops it at the end. It fails when
  the database is unreachable; it does not skip.
- HTTP behaviour is tested through the real router with `tower::ServiceExt::oneshot`, with the
  headers that Edge mints.
- systemd is reached through one trait. A test uses a fake bus that records each call; the real
  implementation uses the same trait.
- A test never reads the wall clock. `fleet.yml` fails on `SystemTime::now`, `Utc::now` and the
  like in a test file.
- Secrets in tests are obvious fake markers. Never use a real credential.

## Running

```bash
docker compose up -d --wait
cargo test --workspace --locked
```

`DEVELOPMENT.md` holds the whole gate.
