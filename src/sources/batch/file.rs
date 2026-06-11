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
use wp_connector_api::{
    DataSource, SourceBatch, SourceError as WpError, SourceReason as WpReason, SourceEvent, Tags,
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

use std::io;
use std::path::Path;
use tokio::io::{AsyncBufReadExt, BufReader};

struct SimpleFileSource {
    lines: tokio::io::Lines<BufReader<tokio::fs::File>>,
    key: String,
    eof: bool,
}

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
