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
use super::payload::payload_to_string;

/// Format expected on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TcpWireFormat {
    Ndjson,
    /// Arrow IPC Stream format (no length prefix, `StreamReader`-compatible).
    /// Suitable for continuous streaming connections.
    ArrowStream,
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
            TcpWireFormat::ArrowStream => Err(SourceError::from(
                SourceReason::Connect.err_detail("ArrowStream format is not supported by TcpBatchSource; use read_arrow_stream_batches()"),
            )),
        }
    }

    fn wp_error_to_wf(err: WpError) -> SourceError {
        super::error::wp_error_to_wf(err)
    }
}

/// Read Arrow IPC Stream batches from a blocking reader.
///
/// Uses `arrow::ipc::reader::StreamReader` to decode the continuous stream
/// frame by frame — no length prefix, no buffering the entire connection.
/// Suitable for long-lived TCP connections.
///
/// Returns an error if the stream header (schema) cannot be read.
pub fn read_arrow_stream_batches(
    reader: impl std::io::Read,
) -> Result<impl Iterator<Item = Result<RecordBatch, String>>, String> {
    use arrow::ipc::reader::StreamReader;

    let stream_reader =
        StreamReader::try_new(reader, None).map_err(|e| format!("arrow stream reader: {e}"))?;
    Ok(StreamBatchIter {
        inner: Some(stream_reader),
        errored: false,
    })
}

struct StreamBatchIter<R: std::io::Read> {
    inner: Option<arrow::ipc::reader::StreamReader<R>>,
    errored: bool,
}

impl<R: std::io::Read> Iterator for StreamBatchIter<R> {
    type Item = Result<RecordBatch, String>;

    fn next(&mut self) -> Option<Self::Item> {
        let reader = self.inner.as_mut()?;
        match reader.next() {
            Some(Ok(batch)) => Some(Ok(batch)),
            Some(Err(e)) => {
                self.errored = true;
                self.inner = None;
                Some(Err(format!("arrow stream decode: {e}")))
            }
            None => {
                self.inner = None;
                None
            }
        }
    }
}

impl<R: std::io::Read> StreamBatchIter<R> {
    /// Check whether the stream ended normally or was interrupted.
    ///
    /// Returns `Ok(())` if the stream ended cleanly (EOF after EOS marker),
    /// or `Err(...)` if a decode error occurred mid-stream.
    #[allow(dead_code)]
    pub fn finish(self) -> Result<(), String> {
        if self.errored {
            Err("arrow stream ended with decode error".into())
        } else {
            Ok(())
        }
    }
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
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field};
    use arrow::ipc::writer::StreamWriter;

    #[test]
    fn read_arrow_stream_batches_round_trip() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, false)]));

        // Single IPC stream with two batches (StreamWriter allows writing
        // multiple batches before finish).
        let mut buf = Vec::new();
        {
            let mut w = StreamWriter::try_new(&mut buf, &schema).unwrap();
            w.write(
                &RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(StringArray::from(vec!["a", "b"]))],
                )
                .unwrap(),
            )
            .unwrap();
            w.write(
                &RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(vec!["c"]))])
                    .unwrap(),
            )
            .unwrap();
            w.finish().unwrap();
        }

        let cursor = std::io::Cursor::new(buf);
        let batches: Vec<_> = read_arrow_stream_batches(cursor)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[1].num_rows(), 1);
    }

    #[test]
    fn read_arrow_stream_bad_input_returns_err() {
        let bad = b"not an arrow stream";
        let cursor = std::io::Cursor::new(&bad[..]);
        assert!(read_arrow_stream_batches(cursor).is_err());
    }

    #[test]
    fn read_arrow_stream_typed_columns() {
        use arrow::array::{BooleanArray, Float64Array, Int64Array};

        let schema = Arc::new(Schema::new(vec![
            Field::new("flag", DataType::Boolean, true),
            Field::new("count", DataType::Int64, true),
            Field::new("score", DataType::Float64, true),
        ]));

        let mut buf = Vec::new();
        {
            let mut w = StreamWriter::try_new(&mut buf, &schema).unwrap();
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(BooleanArray::from(vec![true, false])),
                    Arc::new(Int64Array::from(vec![42, 99])),
                    Arc::new(Float64Array::from(vec![1.5, 2.5])),
                ],
            )
            .unwrap();
            w.write(&batch).unwrap();
            w.finish().unwrap();
        }

        let cursor = std::io::Cursor::new(buf);
        let batches: Vec<_> = read_arrow_stream_batches(cursor)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[0].num_columns(), 3);

        // Verify types
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("col 0 should be Boolean");
        batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("col 1 should be Int64");
        batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("col 2 should be Float64");
    }

    #[test]
    fn read_arrow_stream_empty_stream() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, false)]));
        let mut buf = Vec::new();
        {
            // StreamWriter with no batches written — just schema
            let mut w = StreamWriter::try_new(&mut buf, &schema).unwrap();
            w.finish().unwrap();
        }
        let cursor = std::io::Cursor::new(buf);
        let batches: Vec<_> = read_arrow_stream_batches(cursor)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(batches.len(), 0);
    }
}
