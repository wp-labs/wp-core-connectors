//! `BatchSource` adapter for file-based sources.
//!
//! Wraps the existing `FileSource` (which implements
//! `wp_connector_api::DataSource`) and converts its NDJSON output
//! into Arrow `RecordBatch`es via [`super::ndjson::ndjson_to_record_batch`].

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use std::sync::Arc;
use wf_connector_api::{BatchSource, SourceError, SourceReason, SourceResult};

#[cfg(test)]
use std::{io, path::Path};
#[cfg(test)]
use tokio::io::{AsyncBufReadExt, BufReader};
#[cfg(test)]
use wp_connector_api::{SourceEvent, Tags};
use wp_connector_api::{
    DataSource, SourceBatch, SourceError as WpError, SourceReason as WpReason,
};
use wp_model_core::raw::RawData;

use super::ndjson::ndjson_to_record_batch;

/// A file source that produces Arrow `RecordBatch`es from NDJSON files.
///
/// Internally wraps a `wp_connector_api::DataSource` file source and
/// converts each `SourceBatch` into one or more `RecordBatch`es.
pub struct FileBatchSource {
    key: String,
    inner: Box<dyn DataSource>,
    schema: Arc<Schema>,
    started: bool,
}

impl FileBatchSource {
    /// Create a new `FileBatchSource` from an existing file `DataSource`.
    ///
    /// The `schema` defines the Arrow column types for NDJSON → RecordBatch
    /// conversion.
    pub fn new(
        key: impl Into<String>,
        source: Box<dyn DataSource>,
        schema: Arc<Schema>,
    ) -> Self {
        Self {
            key: key.into(),
            inner: source,
            schema,
            started: false,
        }
    }

    /// Convert a batch of `SourceEvent`s into a single Arrow `RecordBatch`.
    fn convert_batch(&self, events: SourceBatch) -> SourceResult<Vec<RecordBatch>> {
        if events.is_empty() {
            return Ok(vec![]);
        }
        let lines: Vec<String> = events
            .iter()
            .map(|e| match &e.payload {
                RawData::String(s) => s.clone(),
                RawData::Bytes(b) => String::from_utf8_lossy(b).to_string(),
                RawData::ArcBytes(b) => String::from_utf8_lossy(b).to_string(),
            })
            .collect();

        match ndjson_to_record_batch(&lines, &self.schema) {
            Ok(Some(batch)) => Ok(vec![batch]),
            Ok(None) => Ok(vec![]),
            Err(e) => Err(SourceReason::Decode.err_detail(e)),
        }
    }

    fn wp_error_to_wf(err: WpError) -> SourceError {
        if matches!(err.reason(), WpReason::EOF) {
            SourceError::from(SourceReason::EOF)
        } else {
            SourceReason::Connect.err_detail(err.to_string())
        }
    }
}

#[async_trait]
impl BatchSource for FileBatchSource {
    async fn start(&mut self) -> SourceResult<()> {
        if self.started { return Ok(()); }
        self.started = true;
        Ok(())
    }

    async fn receive_batch(&mut self) -> SourceResult<Vec<RecordBatch>> {
        match self.inner.receive().await {
            Ok(batch) => self.convert_batch(batch),
            Err(e) => Err(Self::wp_error_to_wf(e)),
        }
    }

    async fn close(&mut self) -> SourceResult<()> {
        self.inner
            .close()
            .await
            .map_err(|e| SourceReason::Connect.err_detail(e.to_string()))?;
        self.started = false;
        Ok(())
    }

    fn identifier(&self) -> &str {
        &self.key
    }
}

// -- Simple file source wrapper (when factory not available) -----------------

#[cfg(test)]
struct SimpleFileSource {
    lines: tokio::io::Lines<BufReader<tokio::fs::File>>,
    key: String,
    eof: bool,
}

#[cfg(test)]
impl SimpleFileSource {
    async fn open(path: impl AsRef<Path>) -> io::Result<Self> {
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

#[cfg(test)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};
    use tempfile::NamedTempFile;
    use std::io::Write;

    #[tokio::test]
    async fn file_batch_source_identifier() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("msg", DataType::Utf8, true),
        ]));
        let src = FileBatchSource::new(
            "test_key",
            Box::new(SimpleFileSource::open("Cargo.toml").await.unwrap()),
            schema,
        );
        assert_eq!(src.identifier(), "test_key");
    }

    #[tokio::test]
    async fn file_batch_source_lifecycle() {
        let mut tmp = NamedTempFile::new().unwrap();
        writeln!(tmp, r#"{{"msg":"hello"}}"#).unwrap();
        writeln!(tmp, r#"{{"msg":"world"}}"#).unwrap();
        let path = tmp.path().to_path_buf();

        let schema = Arc::new(Schema::new(vec![
            Field::new("msg", DataType::Utf8, true),
        ]));
        let mut src = FileBatchSource::new(
            "test",
            Box::new(SimpleFileSource::open(&path).await.unwrap()),
            schema,
        );

        // Start
        src.start().await.unwrap();

        // Receive — should get a batch with 2 rows
        let batches = src.receive_batch().await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);

        // After EOF, should get EOF error
        let result = src.receive_batch().await;
        assert!(result.is_err());

        // Close is idempotent
        src.close().await.unwrap();
        src.close().await.unwrap();
    }

    #[tokio::test]
    async fn file_batch_source_empty_file() {
        let tmp = NamedTempFile::new().unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("msg", DataType::Utf8, true),
        ]));
        let mut src = FileBatchSource::new(
            "empty",
            Box::new(SimpleFileSource::open(tmp.path()).await.unwrap()),
            schema,
        );
        src.start().await.unwrap();
        let result = src.receive_batch().await;
        assert!(result.is_err()); // EOF immediately
    }
}
