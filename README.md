# wp-core-connectors

[![Crates.io](https://img.shields.io/crates/v/wp-core-connectors.svg)](https://crates.io/crates/wp-core-connectors)
[![CI](https://img.shields.io/github/actions/workflow/status/wp-labs/wp-core-connectors/ci.yml?branch=main)](https://github.com/wp-labs/wp-core-connectors/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/wp-labs/wp-core-connectors/graph/badge.svg?token=6SVCXBHB6B)](https://codecov.io/gh/wp-labs/wp-core-connectors)
[![Crates.io downloads](https://img.shields.io/crates/d/wp-core-connectors)](https://crates.io/crates/wp-core-connectors)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust Edition](https://img.shields.io/badge/edition-2024-orange.svg)](https://doc.rust-lang.org/edition-guide/rust-2024/index.html)

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

### Source payload format (`data_format`)

The builtin `file` and `tcp` sources accept a `data_format` parameter that selects how each batch is decoded into Arrow `RecordBatch`es:

| `data_format` | Meaning | `WireFormat` variant | Suitable source |
|---------------|---------|----------------------|-------------------|
| `ndjson` (default) | Newline-delimited JSON (line-oriented) | `WireFormat::Ndjson` | `file`, `tcp` |
| `arrow_ipc` | Arrow IPC Stream (`StreamReader`-compatible) | `WireFormat::ArrowStream` | `file`, `tcp` |
| `arrow_framed` | wp_arrow frame: `[4B tag_len][tag][Arrow IPC Stream]` | `WireFormat::ArrowFramed` | `file`, `tcp` |

> Note: the variant for `arrow_ipc` is named `ArrowStream` because it decodes an Arrow IPC *streaming* format (length-prefix-free, `StreamReader`-compatible).

**Validation.** `data_format` is validated strictly at spec parse time
(`FileSourceSpec` / `TcpSourceSpec`); an unknown value is rejected with a clear
error rather than silently degrading to NDJSON.

**Shared decode layer.** `WireFormat` and the Arrow decode routines are
centralised in `src/sources/batch/arrow.rs` and shared by both `TcpBatchSource`
and `FileBatchSource` (this also removed a previously duplicated
`payload_to_bytes`):

- `WireFormat::from_data_format()` — lenient parse used for defaults (`None`/unknown → `Ndjson`)
- `decode_arrow_ipc_batches` / `decode_arrow_framed_batches` — Arrow bytes → `RecordBatch`

**Schema handling.** For NDJSON inputs the `FileBatchSource` / `TcpBatchSource`
adapters build typed columns from a provided schema; for Arrow inputs the
schema is taken from the stream itself. Arrow **file** inputs use the binary
whole-file reader (`BinaryFileSource` / `SimpleBinaryFileSource`) — line-based
splitting is avoided because it would corrupt a binary Arrow stream, so
`instances` (intra-file byte-range sharding) does not apply to Arrow.

```toml
# file source reading an Arrow IPC stream
[[sources]]
key = "arrow_in"
connect = "file_src"
params = {
  base = "./data/in_dat",
  file = "events.arrow",
  data_format = "arrow_ipc"      # ndjson | arrow_ipc | arrow_framed
}

# tcp source decoding wp_arrow frames
[[sources]]
key = "tcp_in"
connect = "tcp_src"
params = {
  addr = "0.0.0.0", port = 9000,
  data_format = "arrow_framed"
}
```

### TCP Secure Transport (TLS)

Both the `tcp` source (server side) and the `tcp` sink (client side) accept a nested
`tls = { … }` block to enable encrypted transport.

| Field | Type | Meaning |
|-------|------|---------|
| `enabled` | bool | Enable TLS; default `false` |
| `cert` | str | Certificate chain (PEM file path). Required for source; for sink only when using mTLS |
| `key` | str | Private key (PEM file path), paired with `cert` |
| `ca` | str | CA certificate (PEM file path). Source: client CA (enables mTLS); sink: verify server cert |
| `server_name` | str | SNI / server name (sink only; defaults to the target host) |
| `insecure` | bool | Sink only; `true` skips certificate verification (dangerous, test-only) |

Source (server) rules: `cert` + `key` are required; `ca` is optional and enables mTLS
(client certificate verification).

Sink (client) rules: provide `ca` to verify the server, or `insecure = true` to skip
verification; `cert` + `key` are optional (client mTLS certificate); `server_name` is
optional (SNI).

```toml
# TCP source (server) with TLS; optional ca enables mTLS
[[sources]]
key = "tcp_tls_in"
connect = "tcp_src"
params = {
  addr = "0.0.0.0", port = 9000,
  framing = "line",
  tls = { enabled = true, cert = "certs/server.pem", key = "certs/server.key", ca = "certs/client-ca.pem" }
}

# TCP sink (client) with TLS, verifying the server certificate
[[sink_group.sinks]]
name = "tcp_tls_out"
connect = "tcp_sink"
params = {
  addr = "127.0.0.1", port = 9000,
  framing = "line",
  tls = { enabled = true, ca = "certs/ca.pem", server_name = "localhost" }
}
```

> Certificates are PEM files. `tls.cert` and `tls.key` must be paired. Source `ca`
> (mTLS) and sink `insecure` / `cert` / `key` are optional.

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

### Source 数据格式（`data_format`）

内置的 `file`、`tcp` source 支持 `data_format` 参数，选择每个批次如何被解码为 Arrow `RecordBatch`：

| `data_format` | 含义 | `WireFormat` 变体 | 适用 source |
|---------------|------|------------------|------------|
| `ndjson`（默认） | 换行分隔的 JSON（行式） | `WireFormat::Ndjson` | `file`、`tcp` |
| `arrow_ipc` | Arrow IPC Stream（`StreamReader` 可读） | `WireFormat::ArrowStream` | `file`、`tcp` |
| `arrow_framed` | wp_arrow 帧：`[4B tag_len][tag][Arrow IPC Stream]` | `WireFormat::ArrowFramed` | `file`、`tcp` |

> 说明：`arrow_ipc` 对应的变体名为 `ArrowStream`，因为它解码的是 Arrow IPC *streaming* 格式（无长度前缀，`StreamReader` 可读）。

**校验。** `data_format` 在 spec 解析期（`FileSourceSpec` / `TcpSourceSpec`）被严格校验，未知值会返回明确错误，而不是静默退化为 NDJSON。

**共享解码层。** `WireFormat` 与 Arrow 解码函数集中在 `src/sources/batch/arrow.rs`，由 `TcpBatchSource` 和 `FileBatchSource` 共享（同时移除了此前重复的 `payload_to_bytes`）：

- `WireFormat::from_data_format()`——宽松解析，用于默认值（`None`/未知值 → `Ndjson`）
- `decode_arrow_ipc_batches` / `decode_arrow_framed_batches`——Arrow 字节 → `RecordBatch`

**Schema 处理。** NDJSON 输入由 `FileBatchSource` / `TcpBatchSource` 适配器按传入的 schema 构建类型化列；Arrow 输入的 schema 直接取自流本身。Arrow **文件**输入使用整文件二进制读取器（`BinaryFileSource` / `SimpleBinaryFileSource`）——不按行切分，否则会损坏二进制 Arrow 流，因此 `instances`（文件内字节范围分片）对 Arrow 不生效。

```toml
# 文件 source 读取 Arrow IPC 流
[[sources]]
key = "arrow_in"
connect = "file_src"
params = {
  base = "./data/in_dat",
  file = "events.arrow",
  data_format = "arrow_ipc"      # ndjson | arrow_ipc | arrow_framed
}

# tcp source 解码 wp_arrow 帧
[[sources]]
key = "tcp_in"
connect = "tcp_src"
params = {
  addr = "0.0.0.0", port = 9000,
  data_format = "arrow_framed"
}
```

### TCP 安全传输（TLS）

内置 `tcp` source（服务端）与 `tcp` sink（客户端）都支持用 `tls = { … }` 子块启用 TLS 加密传输。

| 字段 | 类型 | 说明 |
|------|------|------|
| `enabled` | bool | 是否启用 TLS，默认 `false` |
| `cert` | str | 证书链（PEM 文件路径）。source 必填；sink 仅在 mTLS 时填 |
| `key` | str | 私钥（PEM 文件路径），与 `cert` 成对出现 |
| `ca` | str | CA 证书（PEM 文件路径）。source：客户端 CA（开启 mTLS）；sink：校验服务端证书 |
| `server_name` | str | SNI / 服务端名（仅 sink；缺省取目标主机名） |
| `insecure` | bool | 仅 sink；`true` 跳过证书校验（危险，仅测试用） |

服务端（source）规则：`cert` + `key` 必填；`ca` 可选，填了即开启 mTLS（校验客户端证书）。

客户端（sink）规则：提供 `ca` 校验服务端，或 `insecure = true` 跳过校验；`cert` + `key` 可选（客户端 mTLS 证书）；`server_name` 可选（SNI）。

```toml
# TCP source（服务端）启用 TLS；ca 可选 → mTLS
[[sources]]
key = "tcp_tls_in"
connect = "tcp_src"
params = {
  addr = "0.0.0.0", port = 9000,
  framing = "line",
  tls = { enabled = true, cert = "certs/server.pem", key = "certs/server.key", ca = "certs/client-ca.pem" }
}

# TCP sink（客户端）启用 TLS，校验服务端证书
[[sink_group.sinks]]
name = "tcp_tls_out"
connect = "tcp_sink"
params = {
  addr = "127.0.0.1", port = 9000,
  framing = "line",
  tls = { enabled = true, ca = "certs/ca.pem", server_name = "localhost" }
}
```

> 证书为 PEM 格式；`tls.cert` 与 `tls.key` 必须成对；source 的 `ca`（mTLS）与 sink 的
> `insecure` / `cert` / `key` 均为可选。

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
