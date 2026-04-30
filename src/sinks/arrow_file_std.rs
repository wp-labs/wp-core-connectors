use std::io::BufWriter;
use std::sync::Arc;

use arrow::ipc::writer::FileWriter;
use async_trait::async_trait;
use orion_error::UvsReason;
use orion_error::compat_traits::ErrorOweBase;
#[cfg(test)]
use serde_json::json;
use wp_arrow::convert::records_to_batch;
use wp_arrow::schema::{FieldDef, parse_wp_type, to_arrow_schema};
use wp_connector_api::SinkResult;
use wp_connector_api::{
    AsyncCtrl, AsyncRawDataSink, AsyncRecordSink, ConnectorDef, ParamMap, SinkBuildCtx,
    SinkFactory, SinkHandle, SinkReason, SinkSpec as ResolvedSinkSpec,
};
use wp_model_core::model::DataRecord;

use crate::sinks::file::resolve_output_path;

type StdArrowWriter = FileWriter<BufWriter<std::fs::File>>;

fn sink_err<E>(msg: &'static str, err: E) -> wp_connector_api::SinkError
where
    E: std::fmt::Display,
{
    wp_connector_api::SinkError::from(SinkReason::sink(msg)).with_detail(err.to_string())
}

#[derive(Clone, Debug)]
struct ArrowFileStdSpec {
    base: String,
    file_name: String,
    field_defs: Vec<FieldDef>,
    sync: bool,
}

impl ArrowFileStdSpec {
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
        let sync = spec
            .params
            .get("sync")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let field_defs = parse_fields_from_params(&spec.params)?;
        Ok(Self {
            base,
            file_name,
            field_defs,
            sync,
        })
    }

    fn resolve_path(&self, ctx: &SinkBuildCtx) -> String {
        resolve_output_path(&self.base, &self.file_name, ctx)
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

pub struct ArrowFileStdSink {
    path: String,
    writer: Option<StdArrowWriter>,
    field_defs: Vec<FieldDef>,
    sync: bool,
    sent_cnt: u64,
}

impl Drop for ArrowFileStdSink {
    fn drop(&mut self) {
        if let Some(mut writer) = self.writer.take() {
            let _ = writer.finish();
            if self.sync {
                let _ = writer.get_mut().get_mut().sync_all();
            }
        }
    }
}

impl ArrowFileStdSink {
    fn new(path: &str, field_defs: Vec<FieldDef>, sync: bool) -> anyhow::Result<Self> {
        if let Some(parent) = std::path::Path::new(path).parent()
            && !parent.exists()
        {
            std::fs::create_dir_all(parent)?;
        }

        let schema = to_arrow_schema(&field_defs).map_err(|e| anyhow::anyhow!("{e}"))?;
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        let writer =
            FileWriter::try_new_buffered(file, &schema).map_err(|e| anyhow::anyhow!("{e}"))?;

        Ok(Self {
            path: path.to_string(),
            writer: Some(writer),
            field_defs,
            sync,
            sent_cnt: 0,
        })
    }

    fn writer_mut(&mut self) -> SinkResult<&mut StdArrowWriter> {
        self.writer.as_mut().ok_or_else(|| {
            wp_connector_api::SinkError::from(SinkReason::sink("arrow_file_std sink stopped"))
        })
    }

    async fn send_batch(&mut self, records: &[DataRecord]) -> SinkResult<()> {
        let batch = records_to_batch(records, &self.field_defs)
            .map_err(|e| anyhow::anyhow!("{e}"))
            .owe(SinkReason::from(UvsReason::resource_error()))?;
        let sync = self.sync;

        {
            let writer = self.writer_mut()?;
            writer
                .write(&batch)
                .map_err(|e| sink_err("arrow_file_std write batch fail", e))?;
            writer
                .flush()
                .map_err(|e| sink_err("arrow_file_std flush fail", e))?;
            if sync {
                writer
                    .get_mut()
                    .get_mut()
                    .sync_all()
                    .map_err(|e| sink_err("arrow_file_std sync fail", e))?;
            }
        }

        self.sent_cnt = self.sent_cnt.saturating_add(1);
        if self.sent_cnt == 1 {
            log::info!(
                "arrow_file_std sink first-send: path={} rows={}",
                self.path,
                records.len(),
            );
        }
        Ok(())
    }
}

#[async_trait]
impl AsyncCtrl for ArrowFileStdSink {
    async fn stop(&mut self) -> SinkResult<()> {
        if let Some(mut writer) = self.writer.take() {
            writer
                .finish()
                .map_err(|e| sink_err("arrow_file_std finish fail", e))?;
            if self.sync {
                writer
                    .get_mut()
                    .get_mut()
                    .sync_all()
                    .map_err(|e| sink_err("arrow_file_std sync on stop fail", e))?;
            }
        }
        Ok(())
    }

    async fn reconnect(&mut self) -> SinkResult<()> {
        Ok(())
    }
}

#[async_trait]
impl AsyncRecordSink for ArrowFileStdSink {
    async fn sink_record(&mut self, data: &DataRecord) -> SinkResult<()> {
        self.send_batch(std::slice::from_ref(data)).await
    }

    async fn sink_records(&mut self, data: Vec<Arc<DataRecord>>) -> SinkResult<()> {
        let records: Vec<DataRecord> = data.iter().map(|a| a.as_ref().clone()).collect();
        self.send_batch(&records).await
    }
}

#[async_trait]
impl AsyncRawDataSink for ArrowFileStdSink {
    async fn sink_str(&mut self, _data: &str) -> SinkResult<()> {
        Err(wp_connector_api::SinkError::from(SinkReason::sink(
            "arrow_file_std sink only accepts records",
        )))
    }

    async fn sink_bytes(&mut self, _data: &[u8]) -> SinkResult<()> {
        Err(wp_connector_api::SinkError::from(SinkReason::sink(
            "arrow_file_std sink only accepts records",
        )))
    }

    async fn sink_str_batch(&mut self, _data: Vec<&str>) -> SinkResult<()> {
        Err(wp_connector_api::SinkError::from(SinkReason::sink(
            "arrow_file_std sink only accepts records",
        )))
    }

    async fn sink_bytes_batch(&mut self, _data: Vec<&[u8]>) -> SinkResult<()> {
        Err(wp_connector_api::SinkError::from(SinkReason::sink(
            "arrow_file_std sink only accepts records",
        )))
    }
}

pub struct ArrowFileStdFactory;

#[async_trait]
impl SinkFactory for ArrowFileStdFactory {
    fn kind(&self) -> &'static str {
        "arrow-file-std"
    }

    fn validate_spec(&self, spec: &ResolvedSinkSpec) -> SinkResult<()> {
        ArrowFileStdSpec::from_resolved(spec).owe(SinkReason::from(UvsReason::core_conf()))?;
        Ok(())
    }

    async fn build(&self, spec: &ResolvedSinkSpec, ctx: &SinkBuildCtx) -> SinkResult<SinkHandle> {
        let resolved =
            ArrowFileStdSpec::from_resolved(spec).owe(SinkReason::from(UvsReason::core_conf()))?;
        let path = resolved.resolve_path(ctx);
        let sink = ArrowFileStdSink::new(&path, resolved.field_defs, resolved.sync)
            .map_err(|e| sink_err("arrow_file_std open fail", e))?;
        Ok(SinkHandle::new(Box::new(sink)))
    }
}

impl wp_connector_api::SinkDefProvider for ArrowFileStdFactory {
    fn sink_def(&self) -> ConnectorDef {
        crate::builtin::sink_def("arrow_file_std_sink")
            .expect("builtin sink def missing: arrow_file_std_sink")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::ipc::reader::FileReader;
    use std::time::{SystemTime, UNIX_EPOCH};
    use wp_model_core::model::{Field, FieldStorage};

    fn tmp_path(ext: &str) -> std::path::PathBuf {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("wp_arrow_file_std_{ts}.{ext}"))
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
    async fn sink_records_roundtrip_standard_file() {
        let path = tmp_path("arrow");
        let field_defs = vec![
            FieldDef::new("name", wp_arrow::schema::WpDataType::Chars),
            FieldDef::new("count", wp_arrow::schema::WpDataType::Digit),
        ];
        let mut sink =
            ArrowFileStdSink::new(path.to_string_lossy().as_ref(), field_defs, false).unwrap();

        let rec = DataRecord::from(vec![
            FieldStorage::from(Field::from_chars("name", "alice")),
            FieldStorage::from(Field::from_digit("count", 42)),
        ]);
        sink.send_batch(&[rec]).await.unwrap();
        sink.stop().await.unwrap();

        let reader = std::fs::File::open(&path).unwrap();
        let batches: Vec<_> = FileReader::try_new(reader, None)
            .unwrap()
            .map(|batch| batch.unwrap())
            .collect();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sink_records_multiple_batches() {
        let path = tmp_path("arrow");
        let field_defs = vec![FieldDef::new("v", wp_arrow::schema::WpDataType::Chars)];
        let mut sink =
            ArrowFileStdSink::new(path.to_string_lossy().as_ref(), field_defs, false).unwrap();

        for value in ["a", "b", "c"] {
            let rec = DataRecord::from(vec![FieldStorage::from(Field::from_chars("v", value))]);
            sink.send_batch(&[rec]).await.unwrap();
        }
        sink.stop().await.unwrap();

        let reader = std::fs::File::open(&path).unwrap();
        let batches: Vec<_> = FileReader::try_new(reader, None)
            .unwrap()
            .map(|batch| batch.unwrap())
            .collect();
        assert_eq!(batches.len(), 3);
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn raw_payloads_are_rejected() {
        let path = tmp_path("arrow");
        let field_defs = vec![FieldDef::new("v", wp_arrow::schema::WpDataType::Chars)];
        let mut sink =
            ArrowFileStdSink::new(path.to_string_lossy().as_ref(), field_defs, false).unwrap();

        assert!(sink.sink_str("raw").await.is_err());
        assert!(sink.sink_bytes(b"raw").await.is_err());
    }
}
