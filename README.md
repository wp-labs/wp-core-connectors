# wp-core-connectors

`wp-core-connectors` is the shared connector runtime crate for WarpParse core pipelines. It provides:

- global source/sink factory registries
- builtin connector definitions for configuration discovery
- sink runtime implementations for file, Arrow IPC, syslog, TCP, and test utilities
- startup helpers for initializing and logging registered connector kinds

## What This Crate Contains

### Registry

`src/registry.rs` exposes process-wide registries for `SinkFactory` and `SourceFactory` implementations from `wp-connector-api`.

Key entry points:

- `register_sink_factory`
- `register_source_factory`
- `get_sink_factory`
- `get_source_factory`
- `registered_sink_defs`
- `registered_source_defs`

Duplicate registrations are ignored and logged with caller location metadata.

### Builtin Connector Definitions

`src/builtin.rs` publishes builtin `ConnectorDef` values that can be surfaced by tooling or used as defaults.

Builtin sink definitions:

- `arrow-ipc`
- `arrow-file`
- `blackhole`
- `file`
- `syslog`
- `tcp`
- `test_rescue`

Builtin source definitions:

- `file`
- `syslog`
- `tcp`

Note: this crate currently includes concrete sink factory/runtime implementations. Source definitions are available here, while source runtime factories can be registered externally through the same registry API.

### Builtin Sink Implementations

The `src/sinks/` module currently includes:

- `arrow_ipc` for streaming `DataRecord` batches over TCP as Arrow IPC frames
- `arrow_file` for appending Arrow IPC payloads to files
- `blackhole` for discard/testing sinks
- `file` for formatted text output (`json`, `csv`, `show`, `kv`, `raw`, `proto-text`)
- `syslog` for RFC3164-style UDP/TCP syslog emission
- `tcp` for raw line-framed or length-framed TCP output

Supporting transport and protocol helpers live in:

- `src/net/transport/`
- `src/protocol/syslog/`

## Minimal Usage

```rust
use wp_core_connectors::registry;
use wp_core_connectors::sinks::arrow_file::ArrowFileFactory;
use wp_core_connectors::sinks::arrow_ipc::ArrowIpcFactory;
use wp_core_connectors::sinks::blackhole_factory::BlackHoleFactory;
use wp_core_connectors::sinks::file_factory::FileFactory;
use wp_core_connectors::sinks::syslog::SyslogFactory;
use wp_core_connectors::sinks::tcp::TcpFactory;
use wp_core_connectors::startup;

fn register_sinks() {
    registry::register_sink_factory(ArrowIpcFactory);
    registry::register_sink_factory(ArrowFileFactory);
    registry::register_sink_factory(BlackHoleFactory);
    registry::register_sink_factory(FileFactory);
    registry::register_sink_factory(SyslogFactory);
    registry::register_sink_factory(TcpFactory);
}

fn register_sources() {}

fn init() {
    startup::init_runtime_registries(register_sinks, register_sources);

    let sink_defs = registry::registered_sink_defs();
    let source_defs = registry::registered_source_defs();

    assert!(!sink_defs.is_empty());
    let _ = source_defs;
}
```

If you only need the builtin catalog, use `wp_core_connectors::builtin::{builtin_sink_defs, builtin_source_defs}`.

## Project Layout

```text
src/
  builtin.rs        Builtin connector definitions
  lib.rs            Public module exports
  net/              Network transport helpers
  protocol/         Protocol helpers, including syslog encoding
  registry.rs       Shared source/sink factory registries
  sinks/            Builtin sink implementations and factories
  startup.rs        Initialization and registry logging helpers
```

## Workspace Note

This crate inherits package metadata such as `version`, `edition`, and `license` from a workspace root manifest. Build and publish flows are expected to run from that workspace context.

## License

Licensed under [Apache License 2.0](./LICENSE).

---

# wp-core-connectors（中文）

`wp-core-connectors` 是 WarpParse 核心流水线里的连接器运行时 crate，主要负责：

- 维护全局 Source/Sink 工厂注册表
- 提供内置连接器定义，便于配置发现和默认值生成
- 提供若干内置 Sink 实现
- 提供启动期注册与诊断日志辅助函数

## 当前能力

- 注册表：统一注册和查询 `SinkFactory` / `SourceFactory`
- 内置 Sink 实现：`arrow-ipc`、`arrow-file`、`blackhole`、`file`、`syslog`、`tcp`
- 内置 Source 定义：`file`、`syslog`、`tcp`
- 文本文件输出格式：`json`、`csv`、`show`、`kv`、`raw`、`proto-text`

需要注意的是：当前仓库里已经实现的是 sink runtime/factory；source 侧在这里主要提供 builtin 定义，具体 runtime 工厂可以通过同一套注册表接口由外部注册。

## 许可证

本项目使用 [Apache License 2.0](./LICENSE)。
