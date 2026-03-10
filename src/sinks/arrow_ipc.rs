use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use orion_conf::ErrorOwe;
#[cfg(test)]
use serde_json::json;
use wp_arrow::convert::records_to_batch;
use wp_arrow::ipc::encode_ipc;
use wp_arrow::schema::{FieldDef, parse_wp_type};
use wp_connector_api::{
    AsyncCtrl, AsyncRawDataSink, AsyncRecordSink, ConnectorDef, ParamMap, SinkBuildCtx,
    SinkDefProvider, SinkFactory, SinkHandle, SinkSpec as ResolvedSinkSpec,
};
use wp_connector_api::{SinkError, SinkReason, SinkResult};
use wp_model_core::model::DataRecord;

use crate::net::transport::{BackoffMode, NetSendPolicy, NetWriter, net_backoff_adaptive};

fn parse_target(s: &str) -> anyhow::Result<(String, u16)> {
    let addr = s
        .strip_prefix("tcp://")
        .ok_or_else(|| anyhow::anyhow!("target must start with tcp://, got: {s}"))?;
    let (host, port) = addr
        .rsplit_once(':')
        .ok_or_else(|| anyhow::anyhow!("tcp:// target must be tcp://host:port"))?;
    let port: u16 = port
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid port in tcp:// target"))?;
    if host.is_empty() {
        anyhow::bail!("tcp:// target must include a host");
    }
    Ok((host.to_string(), port))
}

fn parse_fields_from_params(params: &ParamMap) -> anyhow::Result<Vec<FieldDef>> {
    let fields_val = params
        .get("fields")
        .ok_or_else(|| anyhow::anyhow!("missing required param: fields"))?;
    let arr = fields_val
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("fields must be an array"))?;

    let mut defs = Vec::with_capacity(arr.len());
    for item in arr {
        let name = item
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("each field must have a string 'name'"))?;
        let type_str = item
            .get("type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("each field must have a string 'type'"))?;
        let nullable = item
            .get("nullable")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let wp_type = parse_wp_type(type_str)
            .map_err(|e| anyhow::anyhow!("field '{}' has invalid type: {}", name, e))?;
        defs.push(FieldDef::new(name, wp_type).with_nullable(nullable));
    }
    Ok(defs)
}

const BACKOFF_INITIAL: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

enum ConnState {
    Connected {
        writer: NetWriter,
    },
    Disconnected {
        next_attempt: tokio::time::Instant,
        backoff: Duration,
    },
    Stopped,
}

pub struct ArrowIpcSink {
    conn: ConnState,
    host: String,
    port: u16,
    rate_limit_rps: usize,
    tag: String,
    field_defs: Vec<FieldDef>,
    sent_cnt: u64,
}

impl ArrowIpcSink {
    async fn connect_writer(addr: &str, rate_limit_rps: usize) -> anyhow::Result<NetWriter> {
        let mode = if rate_limit_rps == 0 {
            BackoffMode::ForceOn
        } else {
            BackoffMode::ForceOff
        };
        NetWriter::connect_tcp_with_policy(
            addr,
            NetSendPolicy {
                rate_limit_rps,
                backoff_mode: mode,
                adaptive: net_backoff_adaptive(),
            },
        )
        .await
    }

    async fn connect(
        host: &str,
        port: u16,
        rate_limit_rps: usize,
        tag: String,
        field_defs: Vec<FieldDef>,
    ) -> anyhow::Result<Self> {
        let addr = format!("{host}:{port}");
        let writer = Self::connect_writer(&addr, rate_limit_rps).await?;

        log::info!("arrow_ipc sink connected: tcp://{addr}");

        Ok(Self {
            conn: ConnState::Connected { writer },
            host: host.to_string(),
            port,
            rate_limit_rps,
            tag,
            field_defs,
            sent_cnt: 0,
        })
    }

    fn enter_disconnected(&mut self) {
        self.conn = ConnState::Disconnected {
            next_attempt: tokio::time::Instant::now() + BACKOFF_INITIAL,
            backoff: BACKOFF_INITIAL,
        };
        log::warn!("arrow_ipc sink disconnected, will retry");
    }

    async fn try_reconnect(&mut self) {
        let addr = format!("{}:{}", self.host, self.port);
        match Self::connect_writer(&addr, self.rate_limit_rps).await {
            Ok(writer) => {
                self.conn = ConnState::Connected { writer };
                log::info!("arrow_ipc sink reconnected: tcp://{addr}");
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
                log::debug!("arrow_ipc sink reconnect failed: {e}");
            }
        }
    }

    async fn send_frame(&mut self, payload: &[u8]) -> SinkResult<()> {
        let mut frame = Vec::with_capacity(4 + payload.len());
        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        frame.extend_from_slice(payload);

        match &mut self.conn {
            ConnState::Connected { writer } => writer.write(&frame).await,
            ConnState::Disconnected { .. } | ConnState::Stopped => Err(SinkError::from(
                SinkReason::sink("arrow_ipc sink not connected"),
            )),
        }
    }

    async fn send_batch(&mut self, records: &[DataRecord]) -> SinkResult<()> {
        let batch = records_to_batch(records, &self.field_defs)
            .map_err(|e| anyhow::anyhow!("{e}"))
            .owe_res()?;

        let payload = encode_ipc(&self.tag, &batch)
            .map_err(|e| anyhow::anyhow!("{e}"))
            .owe_res()?;

        let send_result = match self.conn {
            ConnState::Connected { .. } => {
                if let Err(e) = self.send_frame(&payload).await {
                    log::warn!("arrow_ipc send error: {e}");
                    self.enter_disconnected();
                    Err(e)
                } else {
                    Ok(())
                }
            }
            ConnState::Disconnected { next_attempt, .. } => {
                if tokio::time::Instant::now() >= next_attempt {
                    self.try_reconnect().await;
                    if matches!(self.conn, ConnState::Connected { .. })
                        && let Err(e) = self.send_frame(&payload).await
                    {
                        log::warn!("arrow_ipc send error after reconnect: {e}");
                        self.enter_disconnected();
                        Err(e)
                    } else if matches!(self.conn, ConnState::Connected { .. }) {
                        Ok(())
                    } else {
                        Err(SinkError::from(SinkReason::sink(
                            "arrow_ipc sink reconnect did not restore connection",
                        )))
                    }
                } else {
                    Err(SinkError::from(SinkReason::sink(
                        "arrow_ipc sink waiting for reconnect backoff",
                    )))
                }
            }
            ConnState::Stopped => Err(SinkError::from(SinkReason::sink("arrow_ipc sink stopped"))),
        };

        if send_result.is_ok() {
            self.sent_cnt = self.sent_cnt.saturating_add(1);
            log::info!(
                "arrow_ipc sink first-send: tag={} rows={} payload_bytes={}",
                self.tag,
                records.len(),
                payload.len(),
            );
        }
        send_result
    }
}

#[async_trait]
impl AsyncCtrl for ArrowIpcSink {
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

#[async_trait]
impl AsyncRecordSink for ArrowIpcSink {
    async fn sink_record(&mut self, data: &DataRecord) -> SinkResult<()> {
        self.send_batch(std::slice::from_ref(data)).await
    }

    async fn sink_records(&mut self, data: Vec<Arc<DataRecord>>) -> SinkResult<()> {
        let records: Vec<DataRecord> = data.iter().map(|a| a.as_ref().clone()).collect();
        self.send_batch(&records).await
    }
}

#[async_trait]
impl AsyncRawDataSink for ArrowIpcSink {
    async fn sink_str(&mut self, _data: &str) -> SinkResult<()> {
        Err(SinkError::from(SinkReason::sink(
            "arrow_ipc sink only accepts records",
        )))
    }

    async fn sink_bytes(&mut self, _data: &[u8]) -> SinkResult<()> {
        Err(SinkError::from(SinkReason::sink(
            "arrow_ipc sink only accepts records",
        )))
    }

    async fn sink_str_batch(&mut self, _data: Vec<&str>) -> SinkResult<()> {
        Err(SinkError::from(SinkReason::sink(
            "arrow_ipc sink only accepts records",
        )))
    }

    async fn sink_bytes_batch(&mut self, _data: Vec<&[u8]>) -> SinkResult<()> {
        Err(SinkError::from(SinkReason::sink(
            "arrow_ipc sink only accepts records",
        )))
    }
}

pub struct ArrowIpcFactory;

#[async_trait]
impl SinkFactory for ArrowIpcFactory {
    fn kind(&self) -> &'static str {
        "arrow-ipc"
    }

    fn validate_spec(&self, spec: &ResolvedSinkSpec) -> SinkResult<()> {
        let target_str = spec
            .params
            .get("target")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("missing required param: target"))
            .owe_conf()?;
        parse_target(target_str).owe_conf()?;

        spec.params
            .get("tag")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("missing required param: tag"))
            .owe_conf()?;

        parse_fields_from_params(&spec.params).owe_conf()?;
        Ok(())
    }

    async fn build(&self, spec: &ResolvedSinkSpec, ctx: &SinkBuildCtx) -> SinkResult<SinkHandle> {
        let target_str = spec
            .params
            .get("target")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("missing required param: target"))
            .owe_conf()?;
        let (host, port) = parse_target(target_str).owe_conf()?;

        let tag = spec
            .params
            .get("tag")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("missing required param: tag"))
            .owe_conf()?
            .to_string();

        let field_defs = parse_fields_from_params(&spec.params).owe_conf()?;

        let sink = ArrowIpcSink::connect(&host, port, ctx.rate_limit_rps, tag, field_defs)
            .await
            .owe_res()?;
        Ok(SinkHandle::new(Box::new(sink)))
    }
}

impl SinkDefProvider for ArrowIpcFactory {
    fn sink_def(&self) -> ConnectorDef {
        crate::builtin::sink_def("arrow_ipc_sink")
            .expect("builtin sink def missing: arrow_ipc_sink")
    }

    fn sink_defs(&self) -> Vec<ConnectorDef> {
        crate::builtin::sink_defs_by_kind(self.kind())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    #[test]
    fn parse_target_tcp() {
        let (host, port) = parse_target("tcp://127.0.0.1:9800").unwrap();
        assert_eq!(host, "127.0.0.1");
        assert_eq!(port, 9800);
    }

    #[test]
    fn parse_target_invalid_scheme() {
        assert!(parse_target("http://example.com").is_err());
    }

    #[test]
    fn parse_target_unix_rejected() {
        assert!(parse_target("unix:///var/run/test.sock").is_err());
    }

    #[test]
    fn parse_target_tcp_missing_port() {
        assert!(parse_target("tcp://127.0.0.1").is_err());
    }

    #[test]
    fn parse_target_tcp_empty_host() {
        assert!(parse_target("tcp://:9800").is_err());
    }

    #[test]
    fn parse_fields_from_json() {
        let mut params = ParamMap::new();
        params.insert(
            "fields".into(),
            json!([
                { "name": "sip", "type": "ip" },
                { "name": "dport", "type": "digit" },
                { "name": "action", "type": "chars", "nullable": false },
            ]),
        );
        let defs = parse_fields_from_params(&params).unwrap();
        assert_eq!(defs.len(), 3);
        assert_eq!(defs[0].name, "sip");
        assert!(defs[0].nullable);
        assert_eq!(defs[1].name, "dport");
        assert!(!defs[2].nullable);
    }

    #[test]
    fn parse_fields_invalid_type() {
        let mut params = ParamMap::new();
        params.insert(
            "fields".into(),
            json!([{ "name": "x", "type": "unknown_type" }]),
        );
        assert!(parse_fields_from_params(&params).is_err());
    }

    #[test]
    fn parse_fields_missing_name() {
        let mut params = ParamMap::new();
        params.insert("fields".into(), json!([{ "type": "chars" }]));
        assert!(parse_fields_from_params(&params).is_err());
    }

    #[test]
    fn parse_fields_missing_type() {
        let mut params = ParamMap::new();
        params.insert("fields".into(), json!([{ "name": "x" }]));
        assert!(parse_fields_from_params(&params).is_err());
    }

    async fn read_one_frame(reader: &mut (impl AsyncReadExt + Unpin)) -> Option<Vec<u8>> {
        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf).await.ok()?;
        let frame_len = u32::from_be_bytes(len_buf) as usize;
        let mut payload = vec![0u8; frame_len];
        reader.read_exact(&mut payload).await.ok()?;
        Some(payload)
    }

    async fn bind_test_listener() -> Option<TcpListener> {
        match TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => Some(listener),
            Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => None,
            Err(e) => panic!("bind test listener failed: {e}"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sink_records_roundtrip_tcp() {
        use wp_arrow::ipc::decode_ipc;
        use wp_model_core::model::{Field, FieldStorage};

        let Some(listener) = bind_test_listener().await else {
            return;
        };
        let port = listener.local_addr().unwrap().port();

        let srv = tokio::spawn(async move {
            let (mut conn, _) = listener.accept().await.unwrap();
            read_one_frame(&mut conn).await.unwrap()
        });

        let field_defs = vec![
            FieldDef::new("name", wp_arrow::schema::WpDataType::Chars),
            FieldDef::new("count", wp_arrow::schema::WpDataType::Digit),
        ];

        let mut sink = ArrowIpcSink::connect("127.0.0.1", port, 0, "test-tag".into(), field_defs)
            .await
            .unwrap();

        let rec = DataRecord::from(vec![
            FieldStorage::from(Field::from_chars("name", "alice")),
            FieldStorage::from(Field::from_digit("count", 42)),
        ]);
        sink.send_batch(&[rec]).await.unwrap();

        let payload = srv.await.unwrap();
        let frame = decode_ipc(&payload).unwrap();
        assert_eq!(frame.tag, "test-tag");
        assert_eq!(frame.batch.num_rows(), 1);
        assert_eq!(frame.batch.num_columns(), 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sink_records_multiple_batches() {
        use wp_arrow::ipc::decode_ipc;
        use wp_model_core::model::{Field, FieldStorage};

        let Some(listener) = bind_test_listener().await else {
            return;
        };
        let port = listener.local_addr().unwrap().port();

        let srv = tokio::spawn(async move {
            let (mut conn, _) = listener.accept().await.unwrap();
            let mut tags = Vec::new();
            for _ in 0..3 {
                let payload = read_one_frame(&mut conn).await.unwrap();
                let frame = decode_ipc(&payload).unwrap();
                tags.push(frame.tag);
            }
            tags
        });

        let field_defs = vec![FieldDef::new("v", wp_arrow::schema::WpDataType::Chars)];
        let mut sink = ArrowIpcSink::connect("127.0.0.1", port, 0, "multi".into(), field_defs)
            .await
            .unwrap();

        for _ in 0..3 {
            let rec = DataRecord::from(vec![FieldStorage::from(Field::from_chars("v", "x"))]);
            sink.send_batch(&[rec]).await.unwrap();
        }

        let tags = srv.await.unwrap();
        assert_eq!(tags, vec!["multi", "multi", "multi"]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sink_empty_records() {
        use wp_arrow::ipc::decode_ipc;

        let Some(listener) = bind_test_listener().await else {
            return;
        };
        let port = listener.local_addr().unwrap().port();

        let srv = tokio::spawn(async move {
            let (mut conn, _) = listener.accept().await.unwrap();
            read_one_frame(&mut conn).await.unwrap()
        });

        let field_defs = vec![FieldDef::new("x", wp_arrow::schema::WpDataType::Chars)];
        let mut sink = ArrowIpcSink::connect("127.0.0.1", port, 0, "empty".into(), field_defs)
            .await
            .unwrap();

        sink.send_batch(&[]).await.unwrap();

        let payload = srv.await.unwrap();
        let frame = decode_ipc(&payload).unwrap();
        assert_eq!(frame.batch.num_rows(), 0);
    }

    #[test]
    fn backoff_doubles_then_caps() {
        let mut backoff = BACKOFF_INITIAL;
        let expected = [
            Duration::from_secs(1),
            Duration::from_secs(2),
            Duration::from_secs(4),
            Duration::from_secs(8),
            Duration::from_secs(16),
            Duration::from_secs(30),
            Duration::from_secs(30),
        ];
        for &exp in &expected {
            assert_eq!(backoff, exp);
            backoff = (backoff * 2).min(BACKOFF_MAX);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn send_batch_returns_err_while_disconnected() {
        let mut sink = ArrowIpcSink {
            conn: ConnState::Disconnected {
                next_attempt: tokio::time::Instant::now() + Duration::from_secs(30),
                backoff: BACKOFF_INITIAL,
            },
            host: "127.0.0.1".into(),
            port: 9800,
            rate_limit_rps: 0,
            tag: "test".into(),
            field_defs: Vec::new(),
            sent_cnt: 0,
        };

        sink.send_batch(&[]).await.expect_err("send should fail");
    }
}
