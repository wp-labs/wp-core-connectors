use std::sync::Arc;
use std::time::Duration;

use crate::builtin;
use async_trait::async_trait;
use orion_error::conversion::{SourceErr, ToStructError};
use wp_connector_api::SinkReason;
use wp_connector_api::SinkResult;
use wp_connector_api::{
    AsyncCtrl, AsyncRawDataSink, AsyncRecordSink, ConnectorDef, SinkBuildCtx, SinkDefProvider,
    SinkFactory, SinkHandle, SinkSpec as ResolvedSinkSpec,
};
use wp_data_fmt::RecordFormatter; // for fmt_record

use crate::net::transport::{BackoffMode, NetSendPolicy, NetWriter, net_backoff_adaptive};

use super::arrow_conv::{
    data_record_to_batch, data_records_to_batch, encode_batch_ipc_stream, encode_ipc_frame,
    infer_schema_from_record,
};
use wp_model_core::model::DataRecord;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Framing {
    Line,
    Len,
}

#[derive(Clone, Debug)]
struct TcpSinkSpec {
    addr: String,
    port: u16,
    framing: Framing,
}

impl TcpSinkSpec {
    fn from_resolved(spec: &ResolvedSinkSpec) -> SinkResult<Self> {
        let addr = match spec.params.get("addr").and_then(|v| v.as_str()) {
            Some(s) => s.to_string(),
            None => {
                return Err(SinkReason::core_conf()
                    .to_err()
                    .with_detail("tcp.addr must be a string"));
            }
        };
        let port = match spec.params.get("port").and_then(|v| v.as_i64()) {
            Some(p) if (1..=65535).contains(&p) => p as u16,
            Some(_) => {
                return Err(SinkReason::core_conf()
                    .to_err()
                    .with_detail("tcp.port must be in 1..=65535"));
            }
            None => 9000,
        };
        let framing = spec
            .params
            .get("framing")
            .and_then(|v| v.as_str())
            .unwrap_or("line");
        let framing = match framing.to_ascii_lowercase().as_str() {
            "len" | "length" => Framing::Len,
            "line" => Framing::Line,
            _ => {
                return Err(SinkReason::core_conf()
                    .to_err()
                    .with_detail("tcp.framing must be 'line' or 'len'"));
            }
        };
        Self::ensure_bool(spec, "max_backoff")?;
        Self::ensure_bool(spec, "sendq_backpressure")?;
        Ok(Self {
            addr,
            port,
            framing,
        })
    }

    fn ensure_bool(spec: &ResolvedSinkSpec, key: &str) -> SinkResult<()> {
        if let Some(v) = spec.params.get(key)
            && v.as_bool().is_none()
        {
            return Err(SinkReason::core_conf()
                .to_err()
                .with_detail(format!("tcp.{key} must be a boolean")));
        }
        Ok(())
    }

    fn target_addr(&self) -> String {
        format!("{}:{}", self.addr, self.port)
    }
}

// Max seconds to wait for kernel TCP send-queue to drain at shutdown
const TCP_DRAIN_MAX_SECS: u64 = 10;

pub struct TcpSink {
    writer: NetWriter,
    framing: Framing,
    sent_cnt: u64,
}

impl TcpSink {
    async fn connect(spec: &TcpSinkSpec, rate_limit_rps: usize) -> SinkResult<Self> {
        let target = spec.target_addr();
        // 根据限速目标决定策略：
        // - rate_limit_rps == 0（无限速）：启用背压能力（ForceOn）——仅在水位/包型需要时退让；
        // - rate_limit_rps > 0（限速）：关闭背压能力（ForceOff），避免与源端限速叠加造成双重退让。
        let mode = if rate_limit_rps == 0 {
            BackoffMode::ForceOn
        } else {
            BackoffMode::ForceOff
        };
        let writer = NetWriter::connect_tcp_with_policy(
            &target,
            NetSendPolicy {
                rate_limit_rps,
                backoff_mode: mode,
                adaptive: net_backoff_adaptive(),
            },
        )
        .await
        .source_err(SinkReason::Sink, "tcp sink connect tcp")?;
        log::info!("tcp sink connected: target={}", target);
        Ok(Self {
            writer,
            framing: spec.framing,
            sent_cnt: 0,
        })
    }
}

#[async_trait]
impl AsyncCtrl for TcpSink {
    async fn stop(&mut self) -> SinkResult<()> {
        // Gracefully shutdown write side so peer can drain
        self.writer.shutdown().await?;
        // Best-effort kernel send-queue drain, capped 10s
        self.writer
            .drain_until_empty(std::time::Duration::from_secs(TCP_DRAIN_MAX_SECS))
            .await;
        Ok(())
    }
    async fn reconnect(&mut self) -> SinkResult<()> {
        Ok(())
    }
}

#[async_trait]
impl AsyncRecordSink for TcpSink {
    async fn sink_record(&mut self, data: &wp_model_core::model::DataRecord) -> SinkResult<()> {
        // 复用 Raw 格式化，随后走 raw 路径
        let raw = wp_data_fmt::Raw::new().fmt_record(data);
        AsyncRawDataSink::sink_str(self, raw.as_str()).await
    }

    async fn sink_records(
        &mut self,
        data: Vec<std::sync::Arc<wp_model_core::model::DataRecord>>,
    ) -> SinkResult<()> {
        for record in data {
            self.sink_record(&record).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl AsyncRawDataSink for TcpSink {
    async fn sink_str(&mut self, data: &str) -> SinkResult<()> {
        let payload = build_payload_bytes(data.as_bytes(), self.framing);
        if self.sent_cnt == 0 {
            log::info!(
                "tcp sink first-send: framing={:?} msg_len={} preview='{}'",
                self.framing,
                payload.len(),
                &data.chars().take(64).collect::<String>()
            );
        }
        self.writer.write(&payload).await?;
        self.sent_cnt = self.sent_cnt.saturating_add(1);
        Ok(())
    }
    async fn sink_bytes(&mut self, data: &[u8]) -> SinkResult<()> {
        let payload = build_payload_bytes(data, self.framing);
        if self.sent_cnt == 0 {
            log::info!(
                "tcp sink first-send(bytes): framing={:?} msg_len={}",
                self.framing,
                payload.len(),
            );
        }
        self.writer.write(&payload).await?;
        self.sent_cnt = self.sent_cnt.saturating_add(1);
        Ok(())
    }

    async fn sink_str_batch(&mut self, data: Vec<&str>) -> SinkResult<()> {
        if data.is_empty() {
            return Ok(());
        }

        // 批量处理：根据 framing 模式决定如何合并数据
        match self.framing {
            Framing::Line => {
                // Line 模式：合并所有字符串，确保每个都有换行符
                let mut total_len = 0;
                for str_data in &data {
                    total_len += str_data.len();
                    if str_data.as_bytes().last().is_none_or(|&b| b != b'\n') {
                        total_len += 1;
                    }
                }

                let mut buffer = Vec::with_capacity(total_len);
                for str_data in &data {
                    buffer.extend_from_slice(str_data.as_bytes());
                    if str_data.as_bytes().last().is_none_or(|&b| b != b'\n') {
                        buffer.push(b'\n');
                    }
                }

                // 一次性发送所有数据
                self.writer.write(&buffer).await?;
                self.sent_cnt = self.sent_cnt.saturating_add(1);
            }
            Framing::Len => {
                // Length 模式：每个消息需要独立的长度前缀，不能简单合并
                // 但仍然可以批量发送
                let mut buffers = Vec::with_capacity(data.len());
                for str_data in &data {
                    buffers.push(build_payload_bytes(str_data.as_bytes(), self.framing));
                }

                // 合并所有消息
                let total_len: usize = buffers.iter().map(|b| b.len()).sum();
                let mut combined = Vec::with_capacity(total_len);
                for buffer in buffers {
                    combined.extend_from_slice(&buffer);
                }

                self.writer.write(&combined).await?;
                self.sent_cnt = self.sent_cnt.saturating_add(data.len() as u64);
            }
        }

        Ok(())
    }

    async fn sink_bytes_batch(&mut self, data: Vec<&[u8]>) -> SinkResult<()> {
        if data.is_empty() {
            return Ok(());
        }

        match self.framing {
            Framing::Line => {
                let mut total_len = 0;
                for bytes_data in &data {
                    total_len += bytes_data.len();
                    if bytes_data.last().is_none_or(|&b| b != b'\n') {
                        total_len += 1;
                    }
                }

                let mut buffer = Vec::with_capacity(total_len);
                for bytes_data in &data {
                    buffer.extend_from_slice(bytes_data);
                    if bytes_data.last().is_none_or(|&b| b != b'\n') {
                        buffer.push(b'\n');
                    }
                }

                self.writer.write(&buffer).await?;
                self.sent_cnt = self.sent_cnt.saturating_add(1);
            }
            Framing::Len => {
                let mut combined = Vec::new();
                for bytes_data in &data {
                    combined.extend_from_slice(&build_payload_bytes(bytes_data, self.framing));
                }
                self.writer.write(&combined).await?;
                self.sent_cnt = self.sent_cnt.saturating_add(data.len() as u64);
            }
        }
        Ok(())
    }
}

// 小工具：将 Vec<u8> 适配为 fmt::Write
fn buf_writer(buf: &mut Vec<u8>) -> impl std::fmt::Write + '_ {
    struct W<'a>(&'a mut Vec<u8>);
    impl<'a> std::fmt::Write for W<'a> {
        fn write_str(&mut self, s: &str) -> std::fmt::Result {
            self.0.extend_from_slice(s.as_bytes());
            Ok(())
        }
    }
    W(buf)
}

pub struct TcpFactory;

#[async_trait]
impl SinkFactory for TcpFactory {
    fn kind(&self) -> &'static str {
        "tcp"
    }

    fn validate_spec(&self, spec: &ResolvedSinkSpec) -> SinkResult<()> {
        let protocol = spec
            .params
            .get("protocol")
            .and_then(|v| v.as_str())
            .unwrap_or("txt");
        match protocol {
            "arrow" => {
                // Validate that addr is present (port is optional with default)
                let _ = spec
                    .params
                    .get("addr")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        SinkReason::core_conf()
                            .to_err()
                            .with_detail("tcp_arrow: missing required param 'addr'")
                    })?;
                // data_format selects the on-wire Arrow encoding:
                //   "arrow_framed" -> wp_arrow frame `[4B tag_len BE][tag][ipc]`,
                //                     length-prefixed (RFC6587) for peer framing=len
                //   "arrow_ipc" / absent -> bare Arrow IPC Stream (legacy)
                if let Some(df) = spec.params.get("data_format").and_then(|v| v.as_str())
                    && !matches!(df, "arrow_framed" | "arrow_ipc")
                {
                    return Err(SinkReason::core_conf().to_err().with_detail(format!(
                        "tcp_arrow.data_format must be 'arrow_framed' or 'arrow_ipc' (got '{df}')"
                    )));
                }
                Ok(())
            }
            "txt" | "" => {
                TcpSinkSpec::from_resolved(spec)?;
                Ok(())
            }
            other => Err(SinkReason::core_conf().to_err().with_detail(format!(
                "unsupported tcp protocol: '{other}'; expected 'txt' or 'arrow'"
            ))),
        }
    }

    async fn build(&self, spec: &ResolvedSinkSpec, ctx: &SinkBuildCtx) -> SinkResult<SinkHandle> {
        let protocol = spec
            .params
            .get("protocol")
            .and_then(|v| v.as_str())
            .unwrap_or("txt");

        let sink: Box<dyn wp_connector_api::AsyncSink> = match protocol {
            "arrow" => {
                let runtime = TcpArrowSink::connect(spec, ctx.rate_limit_rps).await?;
                Box::new(runtime)
            }
            "txt" | "" => {
                let resolved = TcpSinkSpec::from_resolved(spec)?;
                let runtime = TcpSink::connect(&resolved, ctx.rate_limit_rps).await?;
                Box::new(runtime)
            }
            other => {
                return Err(SinkReason::core_conf().to_err().with_detail(format!(
                    "unsupported tcp protocol: '{other}'; expected 'txt' or 'arrow'"
                )));
            }
        };

        Ok(SinkHandle::new(sink))
    }
}

impl SinkDefProvider for TcpFactory {
    fn sink_def(&self) -> ConnectorDef {
        builtin::sink_def("tcp_sink").expect("builtin sink def missing: tcp_sink")
    }
}

// No external ACK mode; keep sink simple

// --- pure helper for payload framing ---
fn build_payload_bytes(data: &[u8], framing: Framing) -> Vec<u8> {
    match framing {
        Framing::Line => {
            if data.last() == Some(&b'\n') {
                data.to_vec()
            } else {
                let mut buf = Vec::with_capacity(data.len() + 1);
                buf.extend_from_slice(data);
                buf.push(b'\n');
                buf
            }
        }
        Framing::Len => {
            let mut buf = Vec::with_capacity(16 + data.len());
            let _ = std::fmt::Write::write_fmt(
                &mut buf_writer(&mut buf),
                format_args!("{} ", data.len()),
            );
            buf.extend_from_slice(data);
            buf
        }
    }
}

// ---------------------------------------------------------------------------
// TcpArrowSink — Arrow IPC Stream over TCP
// ---------------------------------------------------------------------------

const BACKOFF_INITIAL: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

enum ConnState {
    Connected {
        writer: Box<NetWriter>,
    },
    Disconnected {
        next_attempt: tokio::time::Instant,
        backoff: Duration,
    },
    Stopped,
}

pub struct TcpArrowSink {
    conn: ConnState,
    host: String,
    port: u16,
    rate_limit_rps: usize,
    schema: tokio::sync::Mutex<Option<Arc<arrow::datatypes::Schema>>>,
    sent_cnt: u64,
    /// When true, emit wp_arrow frames (`[tag_len][tag][ipc]`) with RFC6587
    /// length-prefixing — selected by `data_format = "arrow_framed"`.
    framed: bool,
    /// Stream tag embedded in each wp_arrow frame (only used when `framed`).
    tag: String,
}

impl TcpArrowSink {
    pub async fn connect(spec: &ResolvedSinkSpec, rate_limit_rps: usize) -> SinkResult<Self> {
        let addr = spec
            .params
            .get("addr")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                SinkReason::core_conf()
                    .to_err()
                    .with_detail("tcp_arrow: missing required param 'addr'")
            })?;
        let port = spec
            .params
            .get("port")
            .and_then(|v| v.as_i64())
            .unwrap_or(9000) as u16;

        let data_format = spec
            .params
            .get("data_format")
            .and_then(|v| v.as_str())
            .unwrap_or("arrow_ipc")
            .to_ascii_lowercase();
        let framed = data_format == "arrow_framed";
        let tag = spec
            .params
            .get("tag")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let target = format!("{addr}:{port}");
        let mode = if rate_limit_rps == 0 {
            BackoffMode::ForceOn
        } else {
            BackoffMode::ForceOff
        };
        let writer = NetWriter::connect_tcp_with_policy(
            &target,
            NetSendPolicy {
                rate_limit_rps,
                backoff_mode: mode,
                adaptive: net_backoff_adaptive(),
            },
        )
        .await
        .source_err(SinkReason::Sink, "tcp_arrow connect tcp")?;

        log::info!("tcp_arrow sink connected: target={target}");

        Ok(Self {
            conn: ConnState::Connected {
                writer: Box::new(writer),
            },
            host: addr.to_string(),
            port,
            rate_limit_rps,
            schema: tokio::sync::Mutex::new(None),
            sent_cnt: 0,
            framed,
            tag,
        })
    }

    /// Lazily get or infer the Arrow schema from the first DataRecord.
    async fn get_or_infer_schema(
        &self,
        record: &DataRecord,
    ) -> SinkResult<Arc<arrow::datatypes::Schema>> {
        let mut guard = self.schema.lock().await;
        if guard.is_none() {
            *guard = Some(Arc::new(infer_schema_from_record(record)));
        }
        Ok(Arc::clone(guard.as_ref().unwrap()))
    }

    async fn connect_writer(&self) -> SinkResult<NetWriter> {
        let target = format!("{}:{}", self.host, self.port);
        let mode = if self.rate_limit_rps == 0 {
            BackoffMode::ForceOn
        } else {
            BackoffMode::ForceOff
        };
        NetWriter::connect_tcp_with_policy(
            &target,
            NetSendPolicy {
                rate_limit_rps: self.rate_limit_rps,
                backoff_mode: mode,
                adaptive: net_backoff_adaptive(),
            },
        )
        .await
        .source_err(SinkReason::Sink, "tcp_arrow reconnect tcp")
    }

    fn enter_disconnected(&mut self) {
        self.conn = ConnState::Disconnected {
            next_attempt: tokio::time::Instant::now() + BACKOFF_INITIAL,
            backoff: BACKOFF_INITIAL,
        };
        log::warn!("tcp_arrow sink disconnected, will retry");
    }

    async fn try_reconnect(&mut self) {
        match self.connect_writer().await {
            Ok(writer) => {
                self.conn = ConnState::Connected {
                    writer: Box::new(writer),
                };
                log::info!("tcp_arrow sink reconnected: {}:{}", self.host, self.port,);
            }
            Err(e) => {
                if let ConnState::Disconnected {
                    ref mut next_attempt,
                    ref mut backoff,
                } = self.conn
                {
                    *backoff = (*backoff * 2).min(BACKOFF_MAX);
                    *next_attempt = tokio::time::Instant::now() + *backoff;
                }
                log::debug!("tcp_arrow sink reconnect failed: {e}");
            }
        }
    }

    /// Encode a batch into the on-wire payload, honouring `data_format`:
    /// - `arrow_framed`: wp_arrow frame `[tag_len][tag][ipc]`, RFC6587 length-prefixed
    ///   so the peer's `framing = "len"` can delineate messages.
    /// - otherwise (`arrow_ipc`/absent): a bare Arrow IPC Stream (legacy behaviour).
    pub fn encode_batch_payload(
        &self,
        batch: &arrow::record_batch::RecordBatch,
    ) -> SinkResult<Vec<u8>> {
        if self.framed {
            let frame = encode_ipc_frame(&self.tag, batch)?;
            Ok(build_payload_bytes(&frame, Framing::Len))
        } else {
            encode_batch_ipc_stream(batch)
        }
    }

    /// Like [`encode_batch_payload`], but uses the given `tag` instead of `self.tag`.
    ///
    /// This allows callers that multiplex multiple streams over a single TCP
    /// connection (e.g. `conn_events`, `auth_events`, etc.) to tag each batch
    /// independently so the receiver can route to the correct window.
    pub fn encode_batch_payload_with_tag(
        &self,
        tag: &str,
        batch: &arrow::record_batch::RecordBatch,
    ) -> SinkResult<Vec<u8>> {
        if self.framed {
            let frame = encode_ipc_frame(tag, batch)?;
            Ok(build_payload_bytes(&frame, Framing::Len))
        } else {
            encode_batch_ipc_stream(batch)
        }
    }

    pub async fn send_payload(&mut self, payload: &[u8]) -> SinkResult<()> {
        match &mut self.conn {
            ConnState::Connected { writer } => match writer.write(payload).await {
                Ok(()) => Ok(()),
                Err(e) => {
                    log::warn!("tcp_arrow send error: {e}");
                    self.enter_disconnected();
                    Err(e)
                }
            },
            ConnState::Disconnected { next_attempt, .. } => {
                if tokio::time::Instant::now() >= *next_attempt {
                    self.try_reconnect().await;
                    if let ConnState::Connected { writer } = &mut self.conn {
                        match writer.write(payload).await {
                            Ok(()) => Ok(()),
                            Err(e) => {
                                log::warn!("tcp_arrow send error after reconnect: {e}");
                                self.enter_disconnected();
                                Err(e)
                            }
                        }
                    } else {
                        Err(SinkReason::Sink
                            .to_err()
                            .with_detail("tcp_arrow sink reconnect did not restore connection"))
                    }
                } else {
                    Err(SinkReason::Sink
                        .to_err()
                        .with_detail("tcp_arrow sink waiting for reconnect backoff"))
                }
            }
            ConnState::Stopped => Err(SinkReason::Sink
                .to_err()
                .with_detail("tcp_arrow sink stopped")),
        }
    }
}

#[async_trait]
impl AsyncRecordSink for TcpArrowSink {
    async fn sink_record(&mut self, data: &wp_model_core::model::DataRecord) -> SinkResult<()> {
        let schema = self.get_or_infer_schema(data).await?;
        let batch = data_record_to_batch(data, &schema)?;
        let payload = self.encode_batch_payload(&batch)?;

        self.send_payload(&payload).await?;
        self.sent_cnt = self.sent_cnt.saturating_add(1);

        if self.sent_cnt == 1 {
            log::info!(
                "tcp_arrow sink first-send: cols={} payload_bytes={}",
                schema.fields().len(),
                payload.len(),
            );
        }
        Ok(())
    }

    async fn sink_records(
        &mut self,
        data: Vec<Arc<wp_model_core::model::DataRecord>>,
    ) -> SinkResult<()> {
        if data.is_empty() {
            return Ok(());
        }
        let row_count = data.len();
        let schema = self.get_or_infer_schema(&data[0]).await?;
        let batch = data_records_to_batch(&data, &schema)?;
        let payload = self.encode_batch_payload(&batch)?;

        self.send_payload(&payload).await?;

        if self.sent_cnt == 0 {
            log::info!(
                "tcp_arrow sink first-send: rows={} cols={} payload_bytes={}",
                row_count,
                schema.fields().len(),
                payload.len(),
            );
        }
        self.sent_cnt = self.sent_cnt.saturating_add(1);
        Ok(())
    }
}

#[async_trait]
impl AsyncRawDataSink for TcpArrowSink {
    async fn sink_str(&mut self, _data: &str) -> SinkResult<()> {
        Err(SinkReason::Sink
            .to_err()
            .with_detail("tcp_arrow sink only accepts records"))
    }

    async fn sink_bytes(&mut self, _data: &[u8]) -> SinkResult<()> {
        Err(SinkReason::Sink
            .to_err()
            .with_detail("tcp_arrow sink only accepts records"))
    }

    async fn sink_str_batch(&mut self, _data: Vec<&str>) -> SinkResult<()> {
        Err(SinkReason::Sink
            .to_err()
            .with_detail("tcp_arrow sink only accepts records"))
    }

    async fn sink_bytes_batch(&mut self, _data: Vec<&[u8]>) -> SinkResult<()> {
        Err(SinkReason::Sink
            .to_err()
            .with_detail("tcp_arrow sink only accepts records"))
    }
}

#[async_trait]
impl AsyncCtrl for TcpArrowSink {
    async fn stop(&mut self) -> SinkResult<()> {
        let old = std::mem::replace(&mut self.conn, ConnState::Stopped);
        if let ConnState::Connected { mut writer } = old {
            let _ = writer.shutdown().await;
            writer
                .drain_until_empty(std::time::Duration::from_secs(10))
                .await;
        }
        Ok(())
    }

    async fn reconnect(&mut self) -> SinkResult<()> {
        self.conn = ConnState::Disconnected {
            next_attempt: tokio::time::Instant::now(),
            backoff: BACKOFF_INITIAL,
        };
        self.try_reconnect().await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;
    use wp_connector_api::{AsyncRawDataSink, SinkFactory};

    #[tokio::test(flavor = "multi_thread")]
    async fn tcp_sink_sends_line() -> anyhow::Result<()> {
        if std::env::var("WP_NET_TESTS").unwrap_or_default() != "1" {
            return Ok(());
        }
        // server
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let srv = tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 16];
            let n = s.read(&mut buf).await.unwrap();
            String::from_utf8_lossy(&buf[..n]).into_owned()
        });
        // sink
        let fac = TcpFactory;
        let mut params = toml::map::Map::new();
        params.insert("addr".into(), toml::Value::String("127.0.0.1".into()));
        params.insert("port".into(), toml::Value::Integer(port as i64));
        params.insert("framing".into(), toml::Value::String("line".into()));
        let spec = wp_connector_api::SinkSpec {
            group: String::new(),
            name: "t".into(),
            kind: "tcp".into(),
            connector_id: String::new(),
            params: wp_connector_api::parammap_from_toml_map(params),
            filter: None,
        };
        let ctx = wp_connector_api::SinkBuildCtx::new(std::env::current_dir().unwrap());
        let mut h = fac
            .build(&spec, &ctx)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        AsyncRawDataSink::sink_str(h.sink.as_mut(), "abc")
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let body = srv.await.unwrap();
        assert_eq!(body, "abc\n");
        Ok(())
    }

    #[test]
    fn payload_builder_line_and_len() {
        let p1 = build_payload_bytes(b"abc", Framing::Line);
        assert_eq!(p1, b"abc\n");
        let p2 = build_payload_bytes(b"hello", Framing::Len);
        assert_eq!(p2, b"5 hello");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn tcp_sink_sends_len() -> anyhow::Result<()> {
        if std::env::var("WP_NET_TESTS").unwrap_or_default() != "1" {
            return Ok(());
        }
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let srv = tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 32];
            let n = s.read(&mut buf).await.unwrap();
            buf[..n].to_vec()
        });
        let fac = TcpFactory;
        let mut params = toml::map::Map::new();
        params.insert("addr".into(), toml::Value::String("127.0.0.1".into()));
        params.insert("port".into(), toml::Value::Integer(port as i64));
        params.insert("framing".into(), toml::Value::String("len".into()));
        let spec = wp_connector_api::SinkSpec {
            group: String::new(),
            name: "t".into(),
            kind: "tcp".into(),
            connector_id: String::new(),
            params: wp_connector_api::parammap_from_toml_map(params),
            filter: None,
        };
        let ctx = wp_connector_api::SinkBuildCtx::new(std::env::current_dir().unwrap());
        let mut h = fac
            .build(&spec, &ctx)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        AsyncRawDataSink::sink_str(h.sink.as_mut(), "hello")
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let body = srv.await.unwrap();
        assert_eq!(body, b"5 hello");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn tcp_arrow_roundtrip_stream() -> anyhow::Result<()> {
        // TcpArrowSink sends Arrow IPC Stream → receiver decodes via read_arrow_stream_batches
        use crate::sources::batch::tcp::read_arrow_stream_batches;

        use wp_model_core::model::{Field, FieldStorage};

        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let port = listener.local_addr()?.port();

        // Receiver: accept connection, read Arrow IPC Stream
        let srv = std::thread::spawn(move || {
            let (stream, _) = listener.accept()?;
            stream.set_read_timeout(Some(std::time::Duration::from_secs(5)))?;
            let reader = std::io::BufReader::new(&stream);
            let batches: Vec<_> = read_arrow_stream_batches(reader)
                .map_err(|e| anyhow::anyhow!("{e}"))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            Ok::<_, anyhow::Error>(batches)
        });

        // Sender: build TcpArrowSink and send records
        let fac = TcpFactory;
        let mut params = toml::map::Map::new();
        params.insert("protocol".into(), toml::Value::String("arrow".into()));
        params.insert("addr".into(), toml::Value::String("127.0.0.1".into()));
        params.insert("port".into(), toml::Value::Integer(port as i64));
        let spec = wp_connector_api::SinkSpec {
            group: String::new(),
            name: "t".into(),
            kind: "tcp".into(),
            connector_id: String::new(),
            params: wp_connector_api::parammap_from_toml_map(params),
            filter: None,
        };
        let ctx = wp_connector_api::SinkBuildCtx::new(std::env::current_dir().unwrap());
        let mut h = fac
            .build(&spec, &ctx)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        let rec1 = Arc::new(DataRecord::from(vec![
            FieldStorage::from(Field::from_chars("name", "alice")),
            FieldStorage::from(Field::from_digit("count", 42)),
        ]));
        let rec2 = Arc::new(DataRecord::from(vec![
            FieldStorage::from(Field::from_chars("name", "bob")),
            FieldStorage::from(Field::from_digit("count", 7)),
        ]));
        // Single sink_records call with 2 records → one IPC stream, 2 rows
        h.sink
            .as_mut()
            .sink_records(vec![rec1, rec2])
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        drop(h);

        let batches = srv.join().unwrap()?;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2, "2 records in one batch");
        assert_eq!(batches[0].num_columns(), 2);
        Ok(())
    }

    // -- encode_ipc_frame → decode_arrow_framed_batches roundtrip ---------

    use crate::sinks::arrow_conv::encode_ipc_frame_multi;
    use crate::sources::batch::arrow::decode_arrow_framed_batches;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use bytes::Bytes;
    use wp_connector_api::SourceEvent;
    use wp_model_core::raw::RawData;

    #[test]
    fn encode_ipc_frame_roundtrip() {
        // Encode with shared arrow_conv::encode_ipc_frame, decode with
        // sources::batch::arrow::decode_arrow_framed_batches.
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["a", "b"]))],
        )
        .unwrap();

        // 1. Sink side: encode as framed
        let frame = encode_ipc_frame("my_tag", &batch).unwrap();

        // 2. Sink side: wrap with RFC6587 length prefix (Framing::Len)
        let wire_payload = build_payload_bytes(&frame, Framing::Len);

        // 3. Source side: framing layer strips RFC6587 prefix
        //    Format is "{N} " followed by N bytes. Parse the length prefix.
        let prefix_end = wire_payload
            .iter()
            .position(|&b| b == b' ')
            .expect("RFC6587 payload must have space delimiter");
        let _len: usize = std::str::from_utf8(&wire_payload[..prefix_end])
            .unwrap()
            .parse()
            .unwrap();
        let framed_payload = &wire_payload[prefix_end + 1..];

        // 4. Source side: decode the frame
        let event = SourceEvent::new(
            1,
            "key",
            RawData::Bytes(Bytes::from(framed_payload.to_vec())),
            Default::default(),
        );
        let batches = decode_arrow_framed_batches(&vec![event]).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[test]
    fn encode_ipc_frame_multi_roundtrip() {
        // encode_ipc_frame_multi encodes multiple batches into one frame.
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, false)]));
        let b1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(vec!["a"]))])
            .unwrap();
        let b2 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["b", "c"]))],
        )
        .unwrap();

        let frame = encode_ipc_frame_multi("multi", &[b1, b2]).unwrap();

        let event = SourceEvent::new(
            1,
            "key",
            RawData::Bytes(Bytes::from(frame)),
            Default::default(),
        );
        let batches = decode_arrow_framed_batches(&vec![event]).unwrap();
        assert_eq!(
            batches.len(),
            2,
            "two batches decoded from multi-batch IPC stream"
        );
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[1].num_rows(), 2);
    }

    #[test]
    fn encode_ipc_frame_empty_tag() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["data"]))],
        )
        .unwrap();

        let frame = encode_ipc_frame("", &batch).unwrap();
        // Should start with tag_len = 0, no tag bytes, then IPC stream
        assert_eq!(&frame[0..4], &0u32.to_be_bytes());

        let event = SourceEvent::new(
            1,
            "key",
            RawData::Bytes(Bytes::from(frame)),
            Default::default(),
        );
        let batches = decode_arrow_framed_batches(&vec![event]).unwrap();
        assert_eq!(batches[0].num_rows(), 1);
    }
}
