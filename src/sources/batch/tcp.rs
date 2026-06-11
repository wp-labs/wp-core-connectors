//! `BatchSource` adapter for TCP-based sources.
//!
//! Wraps any `wp_connector_api::DataSource` and decodes Arrow IPC frames
//! or NDJSON lines into `RecordBatch`es.

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use std::sync::Arc;
use wf_connector_api::{BatchSource, SourceError, SourceReason, SourceResult};
use wp_connector_api::{DataSource, SourceBatch, SourceError as WpError};

use super::ndjson::ndjson_to_record_batch;
use super::payload::{payload_to_bytes, payload_to_string};

/// Format expected on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TcpWireFormat {
    Ndjson,
    ArrowIpc,
}

/// A TCP source that produces Arrow `RecordBatch`es.
pub struct TcpBatchSource {
    key: String,
    inner: Box<dyn DataSource>,
    schema: Arc<Schema>,
    format: TcpWireFormat,
}

impl TcpBatchSource {
    pub fn new(
        key: impl Into<String>,
        source: Box<dyn DataSource>,
        schema: Arc<Schema>,
        format: TcpWireFormat,
    ) -> Self {
        Self {
            key: key.into(),
            inner: source,
            schema,
            format,
        }
    }

    fn convert_batch(&self, events: SourceBatch) -> SourceResult<Vec<RecordBatch>> {
        if events.is_empty() {
            return Ok(vec![]);
        }
        match self.format {
            TcpWireFormat::Ndjson => {
                let lines: Vec<String> = events
                    .iter()
                    .map(|e| payload_to_string(&e.payload))
                    .collect();
                match ndjson_to_record_batch(&lines, &self.schema) {
                    Ok(Some(batch)) => Ok(vec![batch]),
                    Ok(None) => Ok(vec![]),
                    Err(e) => Err(SourceReason::Decode.err_detail(e)),
                }
            }
            TcpWireFormat::ArrowIpc => {
                let mut batches = Vec::new();
                for event in &events {
                    let bytes = payload_to_bytes(&event.payload);
                    let batch =
                        decode_arrow_ipc(&bytes).map_err(|e| SourceReason::Decode.err_detail(e))?;
                    batches.push(batch);
                }
                Ok(batches)
            }
        }
    }

    fn wp_error_to_wf(err: WpError) -> SourceError {
        super::error::wp_error_to_wf(err)
    }
}

/// Decode a single Arrow IPC message into a RecordBatch.
pub fn decode_arrow_ipc(data: &[u8]) -> Result<RecordBatch, String> {
    use arrow::ipc::reader::StreamReader;
    use std::io::Cursor;

    let cursor = Cursor::new(data);
    let mut reader =
        StreamReader::try_new(cursor, None).map_err(|e| format!("arrow ipc reader: {e}"))?;
    reader
        .next()
        .transpose()
        .map_err(|e| format!("arrow ipc decode: {e}"))?
        .ok_or_else(|| "empty arrow ipc message".to_string())
}

#[async_trait]
impl BatchSource for TcpBatchSource {
    async fn start(&mut self) -> SourceResult<()> {
        self.inner.close().await.ok();
        Ok(())
    }

    async fn receive_batch(&mut self) -> SourceResult<Vec<RecordBatch>> {
        match self.inner.receive().await {
            Ok(batch) => self.convert_batch(batch),
            Err(e) => Err(Self::wp_error_to_wf(e)),
        }
    }

    async fn close(&mut self) -> SourceResult<()> {
        self.inner.close().await.ok();
        Ok(())
    }

    fn identifier(&self) -> &str {
        &self.key
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field};
    use arrow::ipc::writer::StreamWriter;

    #[test]
    fn decode_arrow_ipc_round_trip() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("sip", DataType::Utf8, false),
            Field::new("dport", DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["10.0.0.1", "10.0.0.2"])),
                Arc::new(Int64Array::from(vec![443i64, 80])),
            ],
        )
        .unwrap();

        let mut buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        let decoded = decode_arrow_ipc(&buf).unwrap();
        assert_eq!(decoded.num_rows(), 2);
        assert_eq!(decoded.num_columns(), 2);
    }
}
