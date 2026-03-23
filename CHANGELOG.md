# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Add built-in `file`, `syslog`, and `tcp` source connector implementations under `src/sources`
- Add source-side unit coverage for file wildcard ordering, syslog header handling, and TCP source lifecycle/framing behavior

### Changed

- Expand the crate boundary from sink-only runtimes to core connector runtimes for both sources and sinks
- Keep builtin source definitions and source factory registration in the same crate as the concrete source implementations

## [0.1.1] - 2026-03-10

### Added

- Add `arrow-file-std` sink support for writing standard Arrow IPC files readable by Arrow `FileReader`
- Add builtin connector definition `arrow_file_std_sink`
- Add test coverage for standard Arrow file writing and multi-batch round trips
- Add README configuration examples for both framed and standard Arrow file sinks

### Changed

- Clarify in README that `arrow-file` is the existing custom length-prefixed framed Arrow IPC format
- Keep `arrow-file` backward compatible while introducing `arrow-file-std` for standards-based file exchange

### Fixed

- Box the large `NetWriter` field in `arrow_ipc` connection state to satisfy `cargo clippy --all-targets --all-features -- -D warnings`

## [0.1.0] - 2026-03-10

### Added

- Add `README.md` with crate overview, module layout, builtin connector coverage, and minimal usage guidance
- Add `CHANGELOG.md` to track future project changes
- Add `LICENSE` with the Apache License 2.0 text
- Add standalone crate package metadata and explicit dependency versions in `Cargo.toml` so the crate can build outside the `wp-motor` workspace
- Add regression tests for syslog sink spec parsing, covering default `port`/`protocol`, `app_name` fallback, and invalid parameter validation
- Add registry tests to lock duplicate source and sink factory registration behavior

### Changed

- Decouple `wp-core-connectors` from `wp-conf` by using connector definitions and provider traits directly from `wp-connector-api`
- Replace the syslog sink's dependency on `wp-conf` config models with local runtime spec parsing from `ResolvedSinkSpec`
- Keep connector runtime registration and builtin definitions self-contained for external consumers

### Fixed

- Return explicit errors for `arrow-ipc` sends during disconnect/backoff windows instead of silently reporting success
- Reject unsupported raw input paths on Arrow sinks rather than dropping data silently
- Resolve file and arrow-file output paths against `SinkBuildCtx.work_root` and shard filenames for multi-replica builds
- Preserve the first registered factory for duplicate connector kinds and emit diagnostics instead of silently overriding
- Handle raw byte payloads in TCP and syslog sinks consistently with string payload handling
