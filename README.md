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
- `arrow-file` (custom length-prefixed framed Arrow IPC payloads)
- `arrow-file-std` (standard Arrow IPC file format)
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
- `arrow_file` for appending custom framed Arrow IPC payloads to files
- `arrow_file_std` for writing standard Arrow IPC files readable by Arrow `FileReader`
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
use wp_core_connectors::sinks::arrow_file_std::ArrowFileStdFactory;
use wp_core_connectors::sinks::arrow_ipc::ArrowIpcFactory;
use wp_core_connectors::sinks::blackhole_factory::BlackHoleFactory;
use wp_core_connectors::sinks::file_factory::FileFactory;
use wp_core_connectors::sinks::syslog::SyslogFactory;
use wp_core_connectors::sinks::tcp::TcpFactory;
use wp_core_connectors::startup;

fn register_sinks() {
    registry::register_sink_factory(ArrowIpcFactory);
    registry::register_sink_factory(ArrowFileFactory);
    registry::register_sink_factory(ArrowFileStdFactory);
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

## Configuration Examples

Use `arrow-file` when you want an append-friendly internal runtime format. Use `arrow-file-std` when you want a standard Arrow file that other Arrow tools can read directly.

### Framed Arrow IPC file

```toml
version = "2.0"

[sink_group]
name = "/sink/arrow_frames"
oml = ["logs"]

[[sink_group.sinks]]
name = "arrow_frames"
connect = "arrow_file_sink"
params = { 
  base = "./data/out_dat",
  file = "events.arrow",
  tag = "default",
  fields = [
    { name = "name", type = "chars" },
    { name = "count", type = "digit" }
  ]
}
```

This writes the current custom on-disk format:

- 4-byte big-endian frame length
- framed payload produced by `wp_arrow::ipc::encode_ipc`

This format is good for:

- append-heavy runtime output
- internal replay/diagnostics
- consumers that already understand the framed protocol

### Standard Arrow file

```toml
version = "2.0"

[sink_group]
name = "/sink/arrow_std"
oml = ["logs"]

[[sink_group.sinks]]
name = "arrow_std"
connect = "arrow_file_std_sink"
params = {
  base = "./data/out_dat",
  file = "events.arrow",
  fields = [
    { name = "name", type = "chars" },
    { name = "count", type = "digit" }
  ]
}
```

This writes a standard Arrow IPC file and is the better choice for:

- interchange with external Arrow tooling
- consumers using Arrow `FileReader`
- offline analysis and export workflows

### Recommendation

- Prefer `arrow_file_std_sink` for external file exchange.
- Keep `arrow_file_sink` for internal streaming/debug artifacts where append-friendly framing matters.

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
- 内置 Sink 实现：`arrow-ipc`、`arrow-file`、`arrow-file-std`、`blackhole`、`file`、`syslog`、`tcp`
- 内置 Source 定义：`file`、`syslog`、`tcp`
- 文本文件输出格式：`json`、`csv`、`show`、`kv`、`raw`、`proto-text`

需要注意的是：当前仓库里已经实现的是 sink runtime/factory；source 侧在这里主要提供 builtin 定义，具体 runtime 工厂可以通过同一套注册表接口由外部注册。

## 配置示例

建议把两种格式分开使用：

- `arrow_file_sink`：内部流式落盘、诊断、回放
- `arrow_file_std_sink`：标准 Arrow 文件交换、离线分析、跨工具消费

### 1. Framed Arrow IPC 文件

```toml
version = "2.0"

[sink_group]
name = "/sink/arrow_frames"
oml = ["logs"]

[[sink_group.sinks]]
name = "arrow_frames"
connect = "arrow_file_sink"
params = {
  base = "./data/out_dat",
  file = "events.arrow",
  tag = "default",
  fields = [
    { name = "name", type = "chars" },
    { name = "count", type = "digit" }
  ]
}
```

这会写出当前自定义格式：每帧前有 4 字节长度头，后面跟 `encode_ipc()` 产出的 Arrow IPC payload。

### 2. 标准 Arrow 文件

```toml
version = "2.0"

[sink_group]
name = "/sink/arrow_std"
oml = ["logs"]

[[sink_group.sinks]]
name = "arrow_std"
connect = "arrow_file_std_sink"
params = {
  base = "./data/out_dat",
  file = "events.arrow",
  fields = [
    { name = "name", type = "chars" },
    { name = "count", type = "digit" }
  ]
}
```

这会写出标准 Arrow IPC file，适合 `FileReader` 和其它 Arrow 生态工具直接读取。

## 许可证

本项目使用 [Apache License 2.0](./LICENSE)。
