use std::sync::Arc;

use async_trait::async_trait;
use orion_conf::ErrorOwe;
#[cfg(test)]
use serde_json::json;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use wp_arrow::convert::records_to_batch;
use wp_arrow::ipc::encode_ipc;
use wp_arrow::schema::{FieldDef, parse_wp_type};
use wp_connector_api::SinkResult;
use wp_connector_api::{
    AsyncCtrl, AsyncRawDataSink, AsyncRecordSink, ConnectorDef, ParamMap, SinkBuildCtx,
    SinkDefProvider, SinkErrorOwe, SinkFactory, SinkHandle, SinkSpec as ResolvedSinkSpec,
};
use wp_model_core::model::DataRecord;

use crate::sinks::file::resolve_output_path;

#[derive(Clone, Debug)]
struct ArrowFileSpec {
    base: String,
    file_name: String,
    tag: String,
    field_defs: Vec<FieldDef>,
    sync: bool,
}

impl ArrowFileSpec {
    fn from_resolved(spec: &ResolvedSinkSpec) -> anyhow::Result<Self> {
        let base = spec
            .params
            .get("base")
            .and_then(|v| v.as_str())
            .unwrap_or("./data/out_dat")
            .to_string();
        let file_name = spec
            .params
            .get("file")
            .and_then(|v| v.as_str())
            .unwrap_or("default.arrow")
            .to_string();
        let tag = spec
            .params
            .get("tag")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("missing required param: tag"))?
            .to_string();
        let sync = spec
            .params
            .get("sync")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let field_defs = parse_fields_from_params(&spec.params)?;
        Ok(Self {
            base,
            file_name,
            tag,
            field_defs,
            sync,
        })
    }

    fn resolve_path(&self, _ctx: &SinkBuildCtx) -> String {
        resolve_output_path(&self.base, &self.file_name, _ctx)
            .display()
            .to_string()
    }
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

pub struct ArrowFileSink {
    out_io: tokio::fs::File,
    tag: String,
    field_defs: Vec<FieldDef>,
    sync: bool,
    sent_cnt: u64,
}

impl ArrowFileSink {
    async fn new(
        path: &str,
        tag: String,
        field_defs: Vec<FieldDef>,
        sync: bool,
    ) -> anyhow::Result<Self> {
        if let Some(parent) = std::path::Path::new(path).parent()
            && !parent.exists()
        {
            std::fs::create_dir_all(parent)?;
        }
        let out_io = OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)
            .await?;
        Ok(Self {
            out_io,
            tag,
            field_defs,
            sync,
            sent_cnt: 0,
        })
    }

    async fn send_batch(&mut self, records: &[DataRecord]) -> SinkResult<()> {
        let batch = records_to_batch(records, &self.field_defs)
            .map_err(|e| anyhow::anyhow!("{e}"))
            .owe_res()?;
        let payload = encode_ipc(&self.tag, &batch)
            .map_err(|e| anyhow::anyhow!("{e}"))
            .owe_res()?;

        self.out_io
            .write_all(&(payload.len() as u32).to_be_bytes())
            .await
            .owe_sink("arrow_file write header fail")?;
        self.out_io
            .write_all(&payload)
            .await
            .owe_sink("arrow_file write payload fail")?;

        if self.sync {
            self.out_io
                .sync_all()
                .await
                .owe_sink("arrow_file sync fail")?;
        }

        self.sent_cnt = self.sent_cnt.saturating_add(1);
        if self.sent_cnt == 1 {
            log::info!(
                "arrow_file sink first-send: tag={} rows={} payload_bytes={}",
                self.tag,
                records.len(),
                payload.len()
            );
        }
        Ok(())
    }
}

#[async_trait]
impl AsyncCtrl for ArrowFileSink {
    async fn stop(&mut self) -> SinkResult<()> {
        self.out_io
            .flush()
            .await
            .owe_sink("arrow_file flush fail")?;
        if self.sync {
            self.out_io
                .sync_all()
                .await
                .owe_sink("arrow_file sync on stop fail")?;
        }
        Ok(())
    }

    async fn reconnect(&mut self) -> SinkResult<()> {
        Ok(())
    }
}

#[async_trait]
impl AsyncRecordSink for ArrowFileSink {
    async fn sink_record(&mut self, data: &DataRecord) -> SinkResult<()> {
        self.send_batch(std::slice::from_ref(data)).await
    }

    async fn sink_records(&mut self, data: Vec<Arc<DataRecord>>) -> SinkResult<()> {
        let records: Vec<DataRecord> = data.iter().map(|a| a.as_ref().clone()).collect();
        self.send_batch(&records).await
    }
}

#[async_trait]
impl AsyncRawDataSink for ArrowFileSink {
    async fn sink_str(&mut self, _data: &str) -> SinkResult<()> {
        Err(wp_connector_api::SinkError::from(
            wp_connector_api::SinkReason::sink("arrow_file sink only accepts records"),
        ))
    }

    async fn sink_bytes(&mut self, _data: &[u8]) -> SinkResult<()> {
        Err(wp_connector_api::SinkError::from(
            wp_connector_api::SinkReason::sink("arrow_file sink only accepts records"),
        ))
    }

    async fn sink_str_batch(&mut self, _data: Vec<&str>) -> SinkResult<()> {
        Err(wp_connector_api::SinkError::from(
            wp_connector_api::SinkReason::sink("arrow_file sink only accepts records"),
        ))
    }

    async fn sink_bytes_batch(&mut self, _data: Vec<&[u8]>) -> SinkResult<()> {
        Err(wp_connector_api::SinkError::from(
            wp_connector_api::SinkReason::sink("arrow_file sink only accepts records"),
        ))
    }
}

pub struct ArrowFileFactory;

#[async_trait]
impl SinkFactory for ArrowFileFactory {
    fn kind(&self) -> &'static str {
        "arrow-file"
    }

    fn validate_spec(&self, spec: &ResolvedSinkSpec) -> SinkResult<()> {
        ArrowFileSpec::from_resolved(spec).owe_conf()?;
        Ok(())
    }

    async fn build(&self, spec: &ResolvedSinkSpec, ctx: &SinkBuildCtx) -> SinkResult<SinkHandle> {
        let resolved = ArrowFileSpec::from_resolved(spec).owe_conf()?;
        let path = resolved.resolve_path(ctx);
        let sink = ArrowFileSink::new(&path, resolved.tag, resolved.field_defs, resolved.sync)
            .await
            .owe_res()?;
        Ok(SinkHandle::new(Box::new(sink)))
    }
}

impl SinkDefProvider for ArrowFileFactory {
    fn sink_def(&self) -> ConnectorDef {
        crate::builtin::sink_def("arrow_file_sink")
            .expect("builtin sink def missing: arrow_file_sink")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn tmp_path(ext: &str) -> std::path::PathBuf {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("wp_arrow_file_{ts}.{ext}"))
    }

    fn read_frames(path: &std::path::Path) -> Vec<Vec<u8>> {
        let body = std::fs::read(path).unwrap();
        let mut out = Vec::new();
        let mut off = 0usize;
        while off + 4 <= body.len() {
            let len = u32::from_be_bytes(body[off..off + 4].try_into().unwrap()) as usize;
            off += 4;
            assert!(
                off + len <= body.len(),
                "invalid frame len={}, off={}, body_len={}, head={:02x?}",
                len,
                off,
                body.len(),
                &body[..body.len().min(32)]
            );
            out.push(body[off..off + len].to_vec());
            off += len;
        }
        out
    }

    #[test]
    fn parse_fields_from_json() {
        let mut params = ParamMap::new();
        params.insert(
            "fields".into(),
            json!([
                { "name": "sip", "type": "ip" },
                { "name": "dport", "type": "digit" }
            ]),
        );
        let defs = parse_fields_from_params(&params).unwrap();
        assert_eq!(defs.len(), 2);
        assert_eq!(defs[0].name, "sip");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sink_records_roundtrip_file() {
        use wp_arrow::ipc::decode_ipc;
        use wp_model_core::model::{Field, FieldStorage};

        let path = tmp_path("arrow");
        let field_defs = vec![
            FieldDef::new("name", wp_arrow::schema::WpDataType::Chars),
            FieldDef::new("count", wp_arrow::schema::WpDataType::Digit),
        ];
        let mut sink = ArrowFileSink::new(
            path.to_string_lossy().as_ref(),
            "test-tag".into(),
            field_defs,
            false,
        )
        .await
        .unwrap();

        let rec = DataRecord::from(vec![
            FieldStorage::from(Field::from_chars("name", "alice")),
            FieldStorage::from(Field::from_digit("count", 42)),
        ]);
        sink.send_batch(&[rec]).await.unwrap();
        sink.stop().await.unwrap();

        let frames = read_frames(&path);
        assert_eq!(frames.len(), 1);
        let frame = decode_ipc(&frames[0]).unwrap();
        assert_eq!(frame.tag, "test-tag");
        assert_eq!(frame.batch.num_rows(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sink_records_multiple_batches() {
        use wp_arrow::ipc::decode_ipc;
        use wp_model_core::model::{Field, FieldStorage};

        let path = tmp_path("arrow");
        let field_defs = vec![FieldDef::new("v", wp_arrow::schema::WpDataType::Chars)];
        let mut sink = ArrowFileSink::new(
            path.to_string_lossy().as_ref(),
            "multi".into(),
            field_defs,
            false,
        )
        .await
        .unwrap();

        for _ in 0..3 {
            let rec = DataRecord::from(vec![FieldStorage::from(Field::from_chars("v", "x"))]);
            sink.send_batch(&[rec]).await.unwrap();
        }
        sink.stop().await.unwrap();

        let frames = read_frames(&path);
        assert_eq!(frames.len(), 3);
        for frame_bytes in frames {
            let frame = decode_ipc(&frame_bytes).unwrap();
            assert_eq!(frame.tag, "multi");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn raw_payloads_are_rejected() {
        let path = tmp_path("arrow");
        let field_defs = vec![FieldDef::new("v", wp_arrow::schema::WpDataType::Chars)];
        let mut sink = ArrowFileSink::new(
            path.to_string_lossy().as_ref(),
            "multi".into(),
            field_defs,
            false,
        )
        .await
        .unwrap();

        assert!(sink.sink_str("raw").await.is_err());
        assert!(sink.sink_bytes(b"raw").await.is_err());
    }
}
