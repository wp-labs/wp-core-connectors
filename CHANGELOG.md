# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.5.5] - 2026-06-21

### Changed

- `TcpArrowSink` 暴露 `encode_batch_payload`、`send_payload` 为 `pub`，新增 `encode_batch_payload_with_tag` 方法支持多流 tag 复用同一连接

## [0.5.4] - 2026-06-19


## [0.5.3] - 2026-06-18

### Added

- Sink 支持 `data_format` 参数（`arrow_ipc` / `arrow_framed`）：`TcpArrowSink`、`ArrowFileSink` 均接入，`TcpFactory`、`FileFactory` 严格校验未知值
- `encode_ipc_frame` / `encode_ipc_frame_multi` 共享编码函数集中到 `sinks/arrow_conv/mod.rs`，TCP 和 File Arrow sink 共用
- `ArrowFileSink::with_format` 构造器：framed 模式下 batch 缓存在内存，`stop()` 时编码为单个 wp_arrow frame 原子写入
- `ArrowFileWriter` 内部枚举分派 stream / framed 两种写入模式
- `encode_batch_ipc_stream` 移入 `arrow_conv` 模块，`TcpArrowSink::encode_batch_payload` 统一按格式分派

### Changed

- `ArrowFileSink` 重构：`ensure_writer` → `ensure_schema` + `write_stream_batch` 辅助方法
- `stop()` 中 framed 路径统一使用 `encode_ipc_frame_multi`，消除空/非空分支重复
- `TcpArrowSink` 中本地 `encode_ipc_frame` / `encode_batch_ipc_stream` 移入共享 `arrow_conv` 模块

### Fixed

- `ArrowFileSink::stop()` framed 模式空 batches 不执行 `fsync` 的问题

### Documentation

- `ArrowFileSink::with_format` 文档标注 framed 模式数据仅在 `stop()` 时写盘，进程崩溃可能丢数据

## [0.5.2] - 2026-06-18

### Documentation

- README（中英双语）与 `TCP_SOURCE_DESIGN.md` 完善 `data_format` 使用文档（WireFormat 变体映射、校验、共享解码层、TOML 配置示例）

## [0.5.1] - 2026-06-18

### Added

- Source 支持 `data_format` 参数（`ndjson` / `arrow_ipc` / `arrow_framed`）：`FileSourceSpec`、`TcpSourceSpec`、builtin 定义（`file_src`/`tcp_src`）均接入，严格校验未知值
- `WireFormat` 枚举集中到 `sources/batch/arrow.rs`，TCP/File batch source 共享解码逻辑；移除了此前重复的 `payload_to_bytes`
- `BinaryFileSource`（生产）与 `SimpleBinaryFileSource`（测试）：整文件二进制读取，支持 Arrow IPC / framed 格式文件
- `decode_arrow_ipc_batches` / `decode_arrow_framed_batches`：Arrow 字节 → `RecordBatch`
- `ArrowFramed` 解码：`TcpBatchSource` 从仅支持 NDJSON/ArrowStream 扩展为支持三种格式

### Changed

- `TcpBatchSource` / `FileBatchSource::convert_batch` 按 `WireFormat` 分派解码路径
- `FileSourceFactory::build` 非 NDJSON 格式自动路由到 `BinaryFileSource`；Arrow 文件不做文件内字节范围分片（`instances` 不生效）

### Documentation

- README（中英双语）新增 "Source 数据格式（`data_format`）" 章节（含 `WireFormat` 变体映射、严格校验、共享解码层、schema 处理、TOML 配置示例）
- `TCP_SOURCE_DESIGN.md` 补充 `data_format` 解码说明与配置示例

## [0.5.0] - 2026-06-13

### Added

- Arrow 类型映射全覆盖：35 个 DataType 变体全部显式映射（Port→Int32, Hex/Base64→Binary, 其余→Utf8）
- `format_utf8_value`：Obj/Array 序列化为 JSON
- `to_raw_bytes`：Hex 值提取原始字节用于 Binary 列
- `infer_schema_from_record` 过滤 Ignore 字段
- Arrow source 测试：typed columns roundtrip、empty stream

### Changed

- `arrow_conv.rs` 拆分为 `arrow_conv/{mod,schema,batch}.rs`

## [0.4.1] - 2026-06-19

### Fixed

- Syslog UDP sink `sink_str_batch` 修复：UDP 每条消息必须是独立数据报，先前合并多消息为一次 `send()` 超过 65535 字节导致 `EMSGSIZE` 错误；现按传输类型分叉——TCP 保持合并 buffer 一次 `write()`，UDP 逐条 `send()`


## [0.4.0] - 2026-06-19

### Added

- Syslog TCP source 支持 `instances` 多实例：`SyslogSourceSpec` 新增 `instances` 字段（默认 1，范围 [1, 16]）；factory TCP 分支改为循环建 N 个 `TcpSyslogSource` reader，共享 `connection_registry`，acceptor 传 N 个 `reg_tx` 实现连接 round-robin 分发
- Syslog source builtin def 增加 `instances` 参数（默认 `1`）到 `default_params` 和 `allow_override`

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

## [0.3.6] - 2026-06-13

### Added

- Arrow sink 支持类型化字段：schema 从 DataRecord 自动推断真实类型（Bool→Boolean, Digit→Int64, Float→Float64, Time→Timestamp, 其余→Utf8）

## [0.3.5] - 2026-06-13

### Changed

- Arrow sink 不再依赖外部传入 `fields`，schema 从首条 DataRecord 自动推断

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


[Unreleased]: https://github.com/wp-labs/wp-core-connectors/compare/v0.5.5...HEAD
[0.5.5]: https://github.com/wp-labs/wp-core-connectors/compare/v0.5.4...v0.5.5
[0.5.4]: https://github.com/wp-labs/wp-core-connectors/compare/v0.5.3...v0.5.4
[0.5.3]: https://github.com/wp-labs/wp-core-connectors/compare/v0.5.2...v0.5.3
[0.5.2]: https://github.com/wp-labs/wp-core-connectors/compare/v0.5.1...v0.5.2
[0.5.1]: https://github.com/wp-labs/wp-core-connectors/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/wp-labs/wp-core-connectors/compare/v0.3.6...v0.5.0
[0.4.1]: https://github.com/wp-labs/wp-core-connectors/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/wp-labs/wp-core-connectors/compare/v0.2.0...v0.4.0
[0.3.6]: https://github.com/wp-labs/wp-core-connectors/compare/v0.3.5...v0.3.6
[0.3.5]: https://github.com/wp-labs/wp-core-connectors/compare/v0.3.4...v0.3.5
[0.3.4]: https://github.com/wp-labs/wp-core-connectors/compare/v0.3.3...v0.3.4
[0.3.3]: https://github.com/wp-labs/wp-core-connectors/compare/v0.3.2...v0.3.3
[0.3.2]: https://github.com/wp-labs/wp-core-connectors/compare/v0.3.0...v0.3.2
[0.3.0]: https://github.com/wp-labs/wp-core-connectors/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/wp-labs/wp-core-connectors/releases/tag/v0.2.0
[0.1.3]: https://github.com/wp-labs/wp-core-connectors/releases/tag/v0.1.3
[0.1.1]: https://github.com/wp-labs/wp-core-connectors/releases/tag/v0.1.1
[0.1.0]: https://github.com/wp-labs/wp-core-connectors/releases/tag/v0.1.0
