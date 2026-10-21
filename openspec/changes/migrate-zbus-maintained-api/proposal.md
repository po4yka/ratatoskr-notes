## Why
The dependency policy fails on unmaintained zvariant, zbus_names and zvariant_derive. These maintenance notices are separate from the TLS fixes and do not prove exploitation.

## What Changes
- Update the systemd client to maintained zbus 6 with the minimum required features.
- Adapt object-path imports, proxy builder validation and error matching only as required.
- Add isolated D-Bus characterization tests for job ordering, matching, missing units and current exit properties.
- Preserve all public behavior, contracts, units, access settings and quality gates.

## Capabilities
### New Capabilities
None. This change preserves behavior and adds regression coverage.
### Modified Capabilities
None. The change uses skip_specs because no specification-level requirement changes.

## Impact
Cargo.toml, Cargo.lock, services/notes/src/systemd.rs and focused tests. No schema, deployment, polkit, secret or access change. Keep the TLS security patches. Rust 1.97 exceeds the documented zbus 6 minimum of 1.89.
