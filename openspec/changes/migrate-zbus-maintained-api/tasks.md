## 1. Verify the plan and sources
- [x] 1.1 Read the official zbus 6 release and migration guide and verify its published checksum and Rust requirement. Source review has no failing application test.
- [x] 1.2 Validate the planning artifacts with the pinned OpenSpec CLI. Documentation validation has no failing application test.

## 2. Preserve the systemd protocol
- [x] 2.1 Add private-bus characterization tests for fast completion, unrelated jobs, NoSuchUnit and uncached exit status; run them on zbus 5 before migration. These tests preserve existing behavior and should pass on the baseline, so no artificial failing assertion is required.
- [x] 2.2 Update the zbus manifest and lockfile, and verify official checksums for newly resolved packages. Dependency configuration has no failing application test.
- [ ] 2.3 Adapt only the required object-path and builder APIs after baseline tests pass; rerun all protocol tests. This is a behavior-preserving refactor.

## 3. Verify and review
- [ ] 3.1 Run the unchanged DEVELOPMENT.md gate and cargo-deny; record actual results without exceptions or ignored tests.
- [ ] 3.2 Obtain independent full-diff review, publish the reviewed change to the existing PR, and wait for fresh exact-head required CI before merge; verify main CI afterward. No deployment.
