# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.4] - 2026-06-13

### Changed

- 所有 sink def 的 `allow_override` 增加 `"protocol"` 参数

## [0.3.3] - 2026-06-12

### Added

- Arrow output for file and TCP sinks via `protocol = "arrow"` parameter in `FileFactory` and `TcpFactory`.
- Arrow file sinks support append mode and optional `sync` fsync for durability.
- Arrow TCP sink supports automatic reconnect with exponential backoff.

### Changed

- Arrow sink configuration consolidated under `protocol` dispatch (`"arrow"` / `"txt"`).

### Fixed

- Invalid `protocol` values now produce a clear configuration error instead of silently defaulting to text mode.

### Removed

- `arrow-file`, `arrow-file-std`, and `arrow-ipc` factory kinds. Use `kind = "file"` or `kind = "tcp"` with `protocol = "arrow"` instead.

## [0.3.2] - 2026-06-11

### Added

- Add `wf-connector-api` dependency for Arrow-native `BatchSource` trait support
- Add `sources/batch/` module with `BatchSource` implementations for CEP engines:
  - `FileBatchSource` — wraps any `wp_connector_api::DataSource` and converts NDJSON output to Arrow `RecordBatch`
  - `TcpBatchSource` — supports both NDJSON and Arrow IPC wire formats
  - `ndjson_to_record_batch()` — NDJSON → Arrow columnar conversion (Utf8 / Int64 / Float64 / Boolean / Timestamp)
  - `decode_arrow_ipc()` — Arrow IPC stream decoder
  - Shared `payload_to_string/bytes` extractors and `wp_error_to_wf` error mapping
- Add `SimpleFileSource` — lightweight line-by-line file reader implementing `DataSource`
- Add comprehensive tests (13 cases): NDJSON type handling, file lifecycle, TCP Arrow IPC round-trip, empty/null/missing/invalid inputs

## [0.3.0] - 2026-05-03

### Changed

- Migrate all sink constructor and config-parsing functions from `anyhow::Result` to
  `SinkResult` (`StructError<SinkReason>`), eliminating `anyhow::Error` from sink
  production code paths
- Enable `orion-error`'s `anyhow` feature so `anyhow::Error` implements
  `UnstructuredSource`, allowing `.source_err()` on `anyhow::Result<T>` directly
- Replace `SinkReason::resource_error().to_err().with_detail(e.to_string())` with
  `.source_err(SinkReason::Sink, "...")` (for `anyhow`/`io` errors) or
  `.source_raw_err(SinkReason::Sink, "...")` (for third-party `StdError` types),
  preserving original error sources instead of stringifying them
- Replace `anyhow::bail!`/`anyhow::anyhow!` in config validation functions
  (`from_resolved`, `parse_fields_from_params`, `parse_target`,
  `syslog_conf_from_spec`) with `SinkReason::core_conf().to_err().with_detail(...)`,
  returning structured config errors directly
- Remove redundant `.map_err(|e| SinkReason::core_conf().to_err().with_detail(...))`
  wrappers from factory `validate_spec`/`build` methods now that parsing functions
  return `SinkResult` natively

### Removed

- Remove `type AnyResult<T> = anyhow::Result<T>` type aliases from all sink modules

## [0.2.0] - 2026-04-30

### Added

- Add built-in `file`, `syslog`, and `tcp` source connector implementations under `src/sources`
- Add source-side unit coverage for file wildcard ordering, syslog header handling, and TCP source lifecycle/framing behavior

### Changed

- Expand the crate boundary from sink-only runtimes to core connector runtimes for both sources and sinks
- Keep builtin source definitions and source factory registration in the same crate as the concrete source implementations
- Migrate from deprecated `orion_conf::ErrorOwe` to `ErrorOweBase` with explicit
  reason types; replace `ErrorWith::with()`/`want()` with `with_context()`/`doing()`
- Fix `ErrorOweBase` and `ToStructError` import paths for orion-error 0.7.2
- Bump orion-error to 0.7, orion_conf to 0.6, wp-connector-api to 0.9

## [0.1.3] - 2026-03-29

### Changed

- Lower the default TCP reader batch queue capacity from `64` to `32` so reader-side backpressure is applied earlier under sustained input load

### Fixed

- Bound per-connection TCP pending backlog by bytes to stop reader-side buffered messages from growing without limit under downstream backpressure
- Add TCP regression coverage for pending-byte capped draining behavior

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

[Unreleased]: https://github.com/wp-labs/wp-core-connectors/compare/v0.3.2...HEAD
[0.3.2]: https://github.com/wp-labs/wp-core-connectors/compare/v0.3.0...v0.3.2
[0.3.0]: https://github.com/wp-labs/wp-core-connectors/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/wp-labs/wp-core-connectors/releases/tag/v0.2.0
[0.1.3]: https://github.com/wp-labs/wp-core-connectors/releases/tag/v0.1.3
[0.1.1]: https://github.com/wp-labs/wp-core-connectors/releases/tag/v0.1.1
[0.1.0]: https://github.com/wp-labs/wp-core-connectors/releases/tag/v0.1.0
