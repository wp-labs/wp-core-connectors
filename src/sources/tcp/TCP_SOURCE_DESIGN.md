# TcpSource 设计目标与实现文档

## 设计目标

### 1. 高性能 TCP 数据接收
- **零拷贝取数**: 直接读入 `BytesMut`，帧切出后以 `Bytes` 承载（`RawData::Bytes`），避免额外拷贝
- **分离式架构**: 连接接受（Acceptor）与数据读取（TcpSource 实例）分离；`instances` 可配置
- **配置驱动**: 监听地址、接收缓冲、帧模式、TLS、codec（压缩/加密）均可配置

### 2. 生产级可靠性
- **连接生命周期管理**: 自动接受、注销断开/出错的连接
- **错误隔离**: 单连接读/codec 错误只注销该连接，不拖垮整个 source
- **资源限制**: 最大并发连接数（`max_connections`）、单帧上限、批/待处理字节上限
- **公平性**: 多连接轮转，任一连接的半帧不阻塞其它连接

### 3. 多协议帧支持
- **行帧模式（Line）**: 以 `\n` 为分隔符
- **长度帧模式（Len）**: RFC6587 风格 `<len><SP><payload>`（长度上限 + 溢出安全）
- **自动模式（Auto）**: 优先按长度前缀，回退到按行

## 当前实现

### 架构设计（Acceptor + 多实例 TcpSource）

```
                    ┌─────────────────────────────────────────┐
   TcpListenerLoop  │ bind(addr)；accept（1ms 超时）           │
   (Acceptor,       │ TLS 握手（可选，10s 超时）                │
    worker/)        │ registry.insert(id)  ← 共享计数           │
                    │ 按轮转分发给某个 TcpSource 实例（mpsc）    │
                    └───────────────────┬─────────────────────┘
                                        │ ConnectionRegistration { id, stream, peer }
        ┌───────────────────────────────┼───────────────────────────────┐
        ▼                               ▼                               ▼
 ┌──────────────┐               ┌──────────────┐               ┌──────────────┐
 │ TcpSource #1 │               │ TcpSource #2 │     ...       │ TcpSource #N │
 │ DataSource   │               │ DataSource   │               │ DataSource   │
 └──────┬───────┘               └──────┬───────┘               └──────┬───────┘
        │  connections: HashMap<u64, TcpConnection>                     │
        │  connection_order: VecDeque<u64>（轮转，保证公平）             │
        └─────────────────────────────┬────────────────────────────────┘
                                      ▼
                       ┌───────────────────────────────┐
                       │ TcpConnection                 │
                       │  ConnStream(Plain|Tls)        │
                       │  + BatchBuilder(buffer/帧/批) │
                       └───────────────────────────────┘
```

- **Acceptor**（`worker/listener_loop.rs`）：`bind` 后 `select!` 在 `listener.accept()` 与停止信号上；accept **立即返回**，TLS 握手（10s 超时）与分发在**独立 task** 中完成（受握手并发上限约束），故慢/恶意握手不阻塞其它连接的 accept，也不延迟 stop。连接 id 写入**共享** `registry`（供 `max_connections` 限流），再经 mpsc 分发给某个实例；id 由**进程内单调计数器**（`AtomicU64`）生成，避免时间戳纳秒碰撞。每个实例 `close()` 只移除**自身拥有**的 id（含已下发未处理的注册），不整体清空共享 registry。
- **TcpSource**（`source.rs`）：实现 `DataSource`；持有 `connections` 与 `connection_order`。
- **TcpConnection / BatchBuilder**（`conn/connection.rs`）：单连接读取、分帧、批/待处理上限。
- **TcpSourceFactory**（`factory.rs`）：解析参数，构建 N 个 `TcpSource` + 1 个 Acceptor。

> 不变量：一个 `TcpSource` 的所有连接来自**同一个 acceptor**，故 transport 单一（全明文或全 TLS）；TLS 由 listener 决定、逐连接不变，实例内不存在明文/TLS 混流。

### 读取：快路径 + 慢路径（修复 #367 队头阻塞）

- **快路径 `drain_ready`**：对 `connection_order` 中每条连接各做一次**非阻塞** `try_read_batch`；任一切出完整帧立即返回。半帧连接被放回队尾，**不阻塞其它连接**。
- **慢路径 `wait_any_ready`**：所有连接都没有完整帧时，对每条连接**并发**发起阻塞读，用自实现的 `FirstReady`（≈ `select_all`）取最先就绪的一条；`select!` 同时监听**新连接到达**。长记录的半帧只挂起它自己的 future。
- 服务后 `rotate_connection` 把该连接移到队尾（与快路径一致），避免固定顺序偏好导致饿死。
- `conn_rx_closed` 在注册通道关闭后置位，避免忙轮询。

### 单连接数据流

1. `BoundedBufMut` 把单次 syscall 读**夹到** `MAX_READ_BYTES`（256 KiB），缓冲恒定有界（防 macOS `read()` EINVAL）。明文与 TLS 走同一夹读路径。
2. 每次读取前先 drain 上次残留的完整帧（F1），避免对端静默时这些帧滞留。
3. codec（可选）：按 `magic(2)+version+kind+len(4BE)` 切帧 → 解密/解压 → 明文缓冲；再按 framing 切消息。
4. framing：`Line`（`\n`）/ `Len`（`<len><SP><payload>`）/ `Auto`。
5. 批上限 `DEFAULT_BATCH_CAPACITY`(128) 与 `MAX_BATCH_BYTES`(64 KiB)；溢出事件进 `pending`（`MAX_PENDING_BYTES` = 256 KiB）。
6. **EOF**：抽走残余完整帧；**Line** 一律、**Auto** 在残余非长度前缀起点时，把末尾无换行的最后一条也交付（F3）；`Len`（及 `Auto` 的 len 残余）无法补全，丢弃。

### 错误处理与隔离

- **per-connection** 读错误 / codec 解码错误：记录日志并**注销该连接**，`receive()` **不返回 `Err`**，不影响其它连接。
- **熔断**：连续单连接错误且期间无任何成功产出（≥ `MAX_CONSECUTIVE_CONN_ERRORS`）→ `receive()` 返回 `Err`，暴露系统性故障（如 codec 误配、上游全坏）；任一次成功产出即重置计数。
- 仅**结构性**错误（acceptor 注册通道关闭 / 已无连接）才 `receive() -> Err`。
- **畸形 / 超大长度前缀**：`Len` 框架下前缀非法（溢出或 ≥ `MAX_FRAME_BYTES`）时**立即**注销该连接；兜底：待分帧缓冲超过 `MAX_FRAME_BYTES` + 前缀余量仍分不出帧也注销，避免缓冲无界增长。长度前缀解析用 `checked_*`（32 位平台不溢出）。
- 当前**无连接空闲超时 / 心跳**（连接随对端关闭或读错误注销）。

### 配置集成

#### 工厂模式配置解析（`factory.rs`）

| 参数 | 默认 | 说明 |
|------|------|------|
| `addr` | `0.0.0.0` | 监听地址 |
| `port` | `9000` | 监听端口 |
| `framing` | `auto` | `line` \| `len` \| `auto` |
| `tcp_recv_bytes` | `256000` | 接收缓冲 / 单连接初始容量 |
| `instances` | `1` | TcpSource 实例数 |
| `tls` | — | `{ enabled, cert, key, ... }` |
| `compression` | — | `{ enabled, algo = gzip\|zstd, level }` |
| `encryption` | — | `{ enabled, algo = aes-256-gcm\|sm4-gcm, key }` |
| `data_format` | `ndjson` | `ndjson` \| `arrow_ipc` \| `arrow_framed` |

#### 使用示例配置

```toml
# topology/sources/wpsrc.toml
[[sources]]
enable = true
key = "tcp_1"
connect = "tcp_src"
params = {
    port = 19001,                    # 监听端口
    addr = "127.0.0.1",             # 监听地址
    framing = "auto",               # line | len | auto
    tcp_recv_bytes = 8192,          # 缓冲区大小
    data_format = "ndjson"          # ndjson | arrow_ipc | arrow_framed
}
```

#### 数据格式（`data_format`）与 Arrow 解码

`data_format` 决定 `TcpSource` 接收到的字节如何被解码为 Arrow `RecordBatch`。该参数在 `TcpSourceSpec::from_params()` 中被严格校验（未知值会报错，而不是静默退化为 NDJSON）。

| `data_format` | 解码方式 |
|---------------|---------|
| `ndjson`（默认） | 逐行解析为 JSON，按传入 schema 构建类型化列 |
| `arrow_ipc` | 整段字节作为 Arrow IPC Stream（`StreamReader`）解码 |
| `arrow_framed` | 解析 wp_arrow 帧 `[4B tag_len][tag][Arrow IPC Stream]` 后解码 |

枚举与解码函数集中在 `src/sources/batch/arrow.rs`：

- `WireFormat` 枚举 + `from_data_format()`（宽松解析）
- `decode_arrow_ipc_batches` / `decode_arrow_framed_batches`

解码由 `TcpBatchSource` 适配器在 `convert_batch` 中按 `WireFormat` 分派完成。Arrow 路径直接从流取 schema，不依赖外部传入的 schema。

## 实现特点

### ✅ 已实现

1. **Acceptor + 多实例 TcpSource**：连接管理与数据读取分离，`instances` 可 > 1
2. **快/慢两路读取**：任意连接的半帧不阻塞其它连接（#367）
3. **多帧支持**：Line / Len / Auto；Len 有长度上限且溢出安全
4. **可选 codec**：压缩（gzip/zstd）与加密（aes-256-gcm/sm4-gcm）往返
5. **有界读**：单次 syscall ≤ `MAX_READ_BYTES`；批/待处理字节上限
6. **TLS**：明文或 TLS（逐源单一 transport）
7. **错误隔离**：per-connection 错误只注销该连接；EOF 尾行交付
8. **DataSource 接口**：完整实现 `receive` / `start` / `close`

### ⚠️ 当前限制

1. **`try_receive()` 有限**：受异步/同步桥接限制，恒返回 `None`（`can_try_receive()` 为 `false`）
2. **无连接空闲超时 / 心跳**：连接仅在关闭或读错误时注销；且 `max_connections` **硬编码 1000**（`factory.rs`），二者均不可配
3. **单帧上限 10 MB**：超过按该连接错误处理
4. **两套解析实现并存**：`framing.rs`（`drain_auto_all`/`extract_octet_counted`，syslog 在用）与 `framing/extractor.rs`（`FramingExtractor`，tcp 源在用）；各自有上限但逻辑未合并
5. **实例未经 `close()` 被直接 drop**：其在共享 `registry` 中的 id 会残留（约定引擎调用 `close`）

## 测试验证

### 覆盖范围
- 帧提取（Line/Len/Auto）、有界读与缓冲上限、超长/溢出前缀
- 快/慢路径：半帧不阻塞、公平轮转、新连接唤醒、持续就绪不饿死
- 错误隔离、EOF 尾行交付、连接注销
- codec 收发往返、TLS 读取
- 网络测试**默认运行**（本地 loopback `127.0.0.1`，无需 `WP_NET_TESTS`）

### 性能测试
- `WP_TCP_BENCH=1` 触发 `throughput_bench`（`conn/connection.rs` / `throughput_bench.rs`）

## 已知限制与遗留项（记录，未处理）

> 以下为本轮修复后仍存在、但判定「不那么重要」的项，先记录备查。

- **A. 空闲超时 / `max_connections` 可配**：见上「当前限制 2」。慢/僵尸连接可长期占槽；`max_connections` 不可配。
- **B. 熔断阈值与行为**：`MAX_CONSECUTIVE_CONN_ERRORS = 32` 为常量；熔断后再出现「空闲但健康」的连接时会先 `Err` 直到其产出数据（引擎按 `Err` 处理即可）。（`source.rs`）
- **C. close/drop**：见上「当前限制 5」。
- **D. 两套解析实现**：见上「当前限制 4」。
- **E. Acceptor 常量**：accept 错误退避固定 10ms；`MAX_CONCURRENT_HANDSHAKES = 1024`。（`worker/listener_loop.rs`）
- **F. 小清理（cosmetic）**：`run()` 中 `if let Err(e) = self.handle_accepted(...)` 恒不可达（`handle_accepted` 恒返回 `Ok`）；`framing.rs::drain_by_line` 为 `pub` 但全仓无引用。
- **G. 整机验证缺口**：本地 `wp-core-connectors 0.10.1` 需 `wp-connector-api 0.14`，当前引擎（wp-motor v1.27）为 `0.13`，端到端 A/B 需引擎升级（wp-motor v1.28+）。
- **H. `tcp_recv_bytes` 语义**：R3 后单次读一律按 `MAX_READ_BYTES`(256 KiB) 夹取；`tcp_recv_bytes` 只决定初始容量、且会被抬到 ≥ `MAX_READ_BYTES`（默认 `256000 ≈ 256 KiB` 无变化）。
- **观测（未来）**：连接数、吞吐量、错误率等指标。

---

**文档状态**: 与当前实现同步
**最后更新**: 2026-10-14
**维护者**: TcpSource 开发团队
