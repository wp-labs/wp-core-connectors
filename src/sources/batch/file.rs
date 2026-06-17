//! `BatchSource` adapter for file-based sources.
//!
//! `FileBatchSource` wraps any `wp_connector_api::DataSource` and converts
//! its output into Arrow `RecordBatch`es. The payload format is selected via
//! [`WireFormat`]: NDJSON (line-oriented) or Arrow IPC / framed (binary).

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::Arc;
use wf_connector_api::{BatchSource, SourceError, SourceReason, SourceResult};
use wp_connector_api::{DataSource, SourceBatch, SourceError as WpError, SourceReason as WpReason};

use super::arrow::{WireFormat, decode_arrow_framed_batches, decode_arrow_ipc_batches};
use super::ndjson::ndjson_to_record_batch;
use super::payload::payload_to_string;

/// A file source that produces Arrow `RecordBatch`es.
///
/// Internally wraps a `wp_connector_api::DataSource` and converts each
/// `SourceBatch` into one or more `RecordBatch`es, dispatching on `format`.
pub struct FileBatchSource {
    key: String,
    inner: Box<dyn DataSource>,
    schema: Arc<Schema>,
    format: WireFormat,
}

impl FileBatchSource {
    /// Create from an existing `DataSource`.
    pub fn new(
        key: impl Into<String>,
        source: Box<dyn DataSource>,
        schema: Arc<Schema>,
        format: WireFormat,
    ) -> Self {
        Self {
            key: key.into(),
            inner: source,
            schema,
            format,
        }
    }

    /// Create from a file path, using the built-in `SimpleFileSource`.
    ///
    /// Defaults to [`WireFormat::Ndjson`]; see [`FileBatchSource::from_path_format`]
    /// for Arrow IPC / framed inputs.
    pub async fn from_path(
        key: impl Into<String>,
        path: impl AsRef<std::path::Path>,
        schema: Arc<Schema>,
    ) -> std::io::Result<Self> {
        Self::from_path_format(key, path, schema, WireFormat::Ndjson).await
    }

    /// Create from a file path with an explicit [`WireFormat`].
    ///
    /// NDJSON uses the line-oriented `SimpleFileSource`; Arrow formats use the
    /// binary `SimpleBinaryFileSource` that reads the whole file as bytes.
    pub async fn from_path_format(
        key: impl Into<String>,
        path: impl AsRef<std::path::Path>,
        schema: Arc<Schema>,
        format: WireFormat,
    ) -> std::io::Result<Self> {
        let source: Box<dyn DataSource> = match format {
            WireFormat::Ndjson => Box::new(SimpleFileSource::open(path).await?),
            WireFormat::ArrowStream | WireFormat::ArrowFramed => {
                Box::new(SimpleBinaryFileSource::open(path).await?)
            }
        };
        Ok(Self::new(key, source, schema, format))
    }

    fn convert_batch(&self, events: SourceBatch) -> SourceResult<Vec<RecordBatch>> {
        if events.is_empty() {
            return Ok(vec![]);
        }
        match self.format {
            WireFormat::Ndjson => {
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
            WireFormat::ArrowStream => decode_arrow_ipc_batches(&events),
            WireFormat::ArrowFramed => decode_arrow_framed_batches(&events),
        }
    }

    fn wp_error_to_wf(err: WpError) -> SourceError {
        super::error::wp_error_to_wf(err)
    }
}

#[async_trait]
impl BatchSource for FileBatchSource {
    async fn start(&mut self) -> SourceResult<()> {
        self.inner.close().await.ok(); // idempotent, ensures clean state
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

// -- SimpleFileSource --------------------------------------------------------

use std::io;
use std::path::Path;
use tokio::io::{AsyncBufReadExt, BufReader};
use wp_connector_api::{SourceEvent, Tags};
use wp_model_core::raw::RawData;

/// Lightweight line-by-line file source. Each line is one NDJSON record.
pub struct SimpleFileSource {
    lines: tokio::io::Lines<BufReader<tokio::fs::File>>,
    key: String,
    eof: bool,
}

impl SimpleFileSource {
    pub async fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let p = path.as_ref();
        let file = tokio::fs::File::open(p).await?;
        let reader = BufReader::new(file);
        Ok(Self {
            lines: reader.lines(),
            key: p.display().to_string(),
            eof: false,
        })
    }
}

#[async_trait]
impl DataSource for SimpleFileSource {
    async fn receive(&mut self) -> Result<SourceBatch, WpError> {
        if self.eof {
            return Err(WpError::from(WpReason::EOF));
        }
        let mut batch = Vec::new();
        for _ in 0..128 {
            match self.lines.next_line().await {
                Ok(Some(line)) => {
                    batch.push(SourceEvent::new(
                        0,
                        &self.key,
                        RawData::from_string(line),
                        Arc::new(Tags::new()),
                    ));
                }
                Ok(None) => {
                    self.eof = true;
                    break;
                }
                Err(e) => {
                    return Err(WpReason::Other.err_detail(e.to_string()));
                }
            }
        }
        if batch.is_empty() && self.eof {
            return Err(WpError::from(WpReason::EOF));
        }
        Ok(batch)
    }

    fn try_receive(&mut self) -> Option<SourceBatch> {
        None
    }
    fn identifier(&self) -> String {
        self.key.clone()
    }
}

// -- SimpleBinaryFileSource ------------------------------------------------

/// Whole-file binary source. Reads the entire file as a single `RawData::Bytes`
/// payload, suitable for Arrow IPC / framed formats. Emits one batch then EOF.
pub struct SimpleBinaryFileSource {
    key: String,
    data: Option<Bytes>,
}

impl SimpleBinaryFileSource {
    pub async fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let p = path.as_ref();
        let data = tokio::fs::read(p).await?;
        Ok(Self {
            key: p.display().to_string(),
            data: Some(Bytes::from(data)),
        })
    }
}

#[async_trait]
impl DataSource for SimpleBinaryFileSource {
    async fn receive(&mut self) -> Result<SourceBatch, WpError> {
        match self.data.take() {
            Some(bytes) => {
                if bytes.is_empty() {
                    return Err(WpError::from(WpReason::EOF));
                }
                let event =
                    SourceEvent::new(0, &self.key, RawData::Bytes(bytes), Arc::new(Tags::new()));
                Ok(vec![event])
            }
            None => Err(WpError::from(WpReason::EOF)),
        }
    }

    fn try_receive(&mut self) -> Option<SourceBatch> {
        None
    }

    fn identifier(&self) -> String {
        self.key.clone()
    }
}

// -- Tests -------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn file_batch_source_identifier() {
        let schema = Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, true)]));
        let src = FileBatchSource::new(
            "test_key",
            Box::new(SimpleFileSource::open("Cargo.toml").await.unwrap()),
            schema,
            WireFormat::Ndjson,
        );
        assert_eq!(src.identifier(), "test_key");
    }

    #[tokio::test]
    async fn file_batch_source_lifecycle() {
        let mut tmp = NamedTempFile::new().unwrap();
        writeln!(tmp, r#"{{"msg":"hello"}}"#).unwrap();
        writeln!(tmp, r#"{{"msg":"world"}}"#).unwrap();
        let path = tmp.path().to_path_buf();

        let schema = Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, true)]));
        let mut src = FileBatchSource::new(
            "test",
            Box::new(SimpleFileSource::open(&path).await.unwrap()),
            schema,
            WireFormat::Ndjson,
        );

        src.start().await.unwrap();
        let batches = src.receive_batch().await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);

        let result = src.receive_batch().await;
        assert!(result.is_err()); // EOF
        src.close().await.unwrap();
        src.close().await.unwrap(); // idempotent
    }

    #[tokio::test]
    async fn file_batch_source_empty_file() {
        let tmp = NamedTempFile::new().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, true)]));
        let mut src = FileBatchSource::new(
            "empty",
            Box::new(SimpleFileSource::open(tmp.path()).await.unwrap()),
            schema,
            WireFormat::Ndjson,
        );
        src.start().await.unwrap();
        assert!(src.receive_batch().await.is_err());
    }

    #[tokio::test]
    async fn from_path_constructor() {
        let mut tmp = NamedTempFile::new().unwrap();
        writeln!(tmp, r#"{{"msg":"hi"}}"#).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, true)]));
        let mut src = FileBatchSource::from_path("fp", tmp.path(), schema)
            .await
            .unwrap();
        src.start().await.unwrap();
        let batches = src.receive_batch().await.unwrap();
        assert_eq!(batches[0].num_rows(), 1);
    }

    // -- Arrow formats -----------------------------------------------------

    fn write_ipc_file(schema: &Arc<Schema>, values: &[&str]) -> NamedTempFile {
        use arrow::array::StringArray;
        use arrow::ipc::writer::StreamWriter;
        use std::io::Write;
        let mut buf = Vec::new();
        {
            let mut w = StreamWriter::try_new(&mut buf, schema).unwrap();
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(StringArray::from(values.to_vec()))],
            )
            .unwrap();
            w.write(&batch).unwrap();
            w.finish().unwrap();
        }
        let mut tmp = NamedTempFile::new().unwrap();
        tmp.write_all(&buf).unwrap();
        tmp.flush().unwrap();
        tmp
    }

    #[tokio::test]
    async fn file_arrow_ipc_roundtrip() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, false)]));
        let tmp = write_ipc_file(&schema, &["hello", "world"]);

        let mut src =
            FileBatchSource::from_path_format("ipc", tmp.path(), schema, WireFormat::ArrowStream)
                .await
                .unwrap();
        src.start().await.unwrap();
        let batches = src.receive_batch().await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);

        // second read is EOF
        assert!(src.receive_batch().await.is_err());
    }

    #[tokio::test]
    async fn file_arrow_framed_roundtrip() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, false)]));
        let ipc_buf = {
            use arrow::array::StringArray;
            use arrow::ipc::writer::StreamWriter;
            let mut buf = Vec::new();
            let mut w = StreamWriter::try_new(&mut buf, &schema).unwrap();
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(StringArray::from(vec!["a", "b"]))],
            )
            .unwrap();
            w.write(&batch).unwrap();
            w.finish().unwrap();
            buf
        };

        // Build wp_arrow frame: [4B tag_len][tag][Arrow IPC Stream]
        let tag = b"t";
        let mut frame = Vec::new();
        frame.extend_from_slice(&(tag.len() as u32).to_be_bytes());
        frame.extend_from_slice(tag);
        frame.extend_from_slice(&ipc_buf);

        let mut tmp = NamedTempFile::new().unwrap();
        use std::io::Write;
        tmp.write_all(&frame).unwrap();
        tmp.flush().unwrap();

        let mut src = FileBatchSource::from_path_format(
            "framed",
            tmp.path(),
            schema,
            WireFormat::ArrowFramed,
        )
        .await
        .unwrap();
        src.start().await.unwrap();
        let batches = src.receive_batch().await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
    }

    // -- SimpleBinaryFileSource -------------------------------------------

    #[tokio::test]
    async fn binary_source_empty_file_is_eof() {
        let tmp = NamedTempFile::new().unwrap();
        let mut src = SimpleBinaryFileSource::open(tmp.path()).await.unwrap();
        assert!(src.receive().await.is_err()); // empty file -> EOF
    }

    #[tokio::test]
    async fn binary_source_second_read_is_eof() {
        let mut tmp = NamedTempFile::new().unwrap();
        tmp.write_all(b"hello").unwrap();
        tmp.flush().unwrap();
        let mut src = SimpleBinaryFileSource::open(tmp.path()).await.unwrap();
        let first = src.receive().await.unwrap();
        assert_eq!(first.len(), 1);
        assert!(src.receive().await.is_err()); // drained -> EOF
    }

    // -- FileBatchSource robustness --------------------------------------

    #[tokio::test]
    async fn file_arrow_corrupt_data_returns_decode_err() {
        // A file whose bytes are not a valid IPC stream must surface a decode
        // error through the BatchSource adapter, never panic.
        let mut tmp = NamedTempFile::new().unwrap();
        tmp.write_all(b"not an arrow stream at all").unwrap();
        tmp.flush().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, false)]));
        let mut src =
            FileBatchSource::from_path_format("bad", tmp.path(), schema, WireFormat::ArrowStream)
                .await
                .unwrap();
        src.start().await.unwrap();
        assert!(src.receive_batch().await.is_err());
    }
}
