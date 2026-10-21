## Context
See proposal.md for motivation. The existing adapter lazily connects to the system bus, subscribes before StartUnit/StopUnit, matches the returned job path, accepts NoSuchUnit on stop, and reads current exit properties without a proxy cache.

## Goals / Non-Goals
Preserve this protocol behavior while replacing the unmaintained dependency family. Do not change the public API, events, schema, units, polkit, host settings, credentials or deployment. The published TLS patch remains intact.

## Decisions
Use zbus 6.0.0, the current stable release, with Tokio, proxy and tracing features and default features disabled. Its minimum Rust 1.89 fits the pinned 1.97 toolchain. Adapt owned object-path imports and fluent builder validation as required by the official migration guide. Keep error mapping unchanged if the actual error variant remains compatible.

Use a private session bus for characterization tests, never the host system bus. Spawn an isolated dbus-daemon child and retain a kill-on-drop process guard. Inject only the test connection through private test-module access to the existing OnceCell; do not add a production configuration option. Test immediate completion, unrelated job signals, missing-unit stop and changing uncached exit properties. Establish these tests on the old dependency before migration. Since behavior is preserved, characterization tests should pass on the baseline; do not manufacture failures. Any actual behavioral defect needs a separate red/green test pair.

## Risks / Trade-offs
Major API changes can change builder errors or signal order: use real wire-level tests, not only mocks or compile checks. A missing local bus fixture is a validation failure, not a reason to skip tests. Stop for revised approval if architectural changes become necessary.

## Migration Plan
Update manifest and lockfile with official checksum verification, preserve TLS patches, run focused tests and unchanged DEVELOPMENT.md gates, then obtain independent review and fresh exact-head hosted CI. Update the existing security PR only after review. No deployment is part of this change. A failed validation blocks merge; do not weaken policy or add ignores.
