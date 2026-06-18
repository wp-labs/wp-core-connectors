//! Arrow IPC serialization helpers for sink `protocol: arrow`.
//!
//! - `schema` — Schema inference from `DataRecord` fields
//! - `batch`  — `DataRecord` → `RecordBatch` conversion with typed column builders

pub mod batch;
pub mod schema;

pub use batch::{data_record_to_batch, data_records_to_batch};
pub use schema::{infer_arrow_schema, infer_schema_from_record};

use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use orion_error::conversion::{SourceRawErr, ToStructError};
use wp_connector_api::{SinkReason, SinkResult};

// ---------------------------------------------------------------------------
// Shared error helper
// ---------------------------------------------------------------------------

pub(crate) fn sink_err<E>(msg: &'static str, err: E) -> wp_connector_api::SinkError
where
    E: std::fmt::Display,
{
    SinkReason::Sink
        .to_err()
        .with_detail(format!("{msg}: {err}"))
}

// ---------------------------------------------------------------------------
// Shared Arrow IPC encoding helpers (used by both TCP and file sinks)
// ---------------------------------------------------------------------------

/// Encode a single `RecordBatch` as Arrow IPC Stream bytes (self-contained:
/// schema + record batch + end-of-stream marker).
pub(crate) fn encode_batch_ipc_stream(batch: &RecordBatch) -> SinkResult<Vec<u8>> {
    let schema = batch.schema();
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &schema)
            .source_raw_err(SinkReason::Sink, "arrow create stream writer")?;
        writer
            .write(batch)
            .map_err(|e| sink_err("arrow encode batch", e))?;
        writer
            .finish()
            .map_err(|e| sink_err("arrow finish stream", e))?;
    }
    Ok(buf)
}

/// Encode a `RecordBatch` as a wp_arrow IPC frame: `[4B tag_len BE][tag][Arrow IPC stream]`.
///
/// Mirrors `wp_arrow::ipc::encode_ipc` so a peer using `data_format = "arrow_framed"`
/// (which decodes via `decode_arrow_framed_batches` in `sources/batch/arrow.rs`) can
/// recover both the batch and the stream `tag`. Inlined here to avoid pulling a
/// `wp-arrow` dependency into this connector crate.
pub(crate) fn encode_ipc_frame(tag: &str, batch: &RecordBatch) -> SinkResult<Vec<u8>> {
    let tag_bytes = tag.as_bytes();
    let schema = batch.schema();
    let mut buf = Vec::with_capacity(4 + tag_bytes.len() + 1024);
    buf.extend_from_slice(&(tag_bytes.len() as u32).to_be_bytes());
    buf.extend_from_slice(tag_bytes);
    {
        let mut writer = StreamWriter::try_new(&mut buf, &schema)
            .source_raw_err(SinkReason::Sink, "arrow create framed stream writer")?;
        writer
            .write(batch)
            .map_err(|e| sink_err("arrow encode framed batch", e))?;
        writer
            .finish()
            .map_err(|e| sink_err("arrow finish framed stream", e))?;
    }
    Ok(buf)
}

/// Encode multiple `RecordBatch`es as a single Arrow IPC stream wrapped in a
/// wp_arrow frame: `[4B tag_len BE][tag][Arrow IPC stream with all batches]`.
///
/// Used by `ArrowFileSink` in `arrow_framed` mode, where the entire file is one frame.
pub(crate) fn encode_ipc_frame_multi(tag: &str, batches: &[RecordBatch]) -> SinkResult<Vec<u8>> {
    if batches.is_empty() {
        // Empty payload: still emit the frame header with no IPC body.
        let tag_bytes = tag.as_bytes();
        let mut buf = Vec::with_capacity(4 + tag_bytes.len());
        buf.extend_from_slice(&(tag_bytes.len() as u32).to_be_bytes());
        buf.extend_from_slice(tag_bytes);
        return Ok(buf);
    }
    let tag_bytes = tag.as_bytes();
    let schema = batches[0].schema();
    let mut buf = Vec::with_capacity(4 + tag_bytes.len() + 1024);
    buf.extend_from_slice(&(tag_bytes.len() as u32).to_be_bytes());
    buf.extend_from_slice(tag_bytes);
    {
        let mut writer = StreamWriter::try_new(&mut buf, &schema)
            .source_raw_err(SinkReason::Sink, "arrow create framed multi stream writer")?;
        for batch in batches {
            writer
                .write(batch)
                .map_err(|e| sink_err("arrow encode framed multi batch", e))?;
        }
        writer
            .finish()
            .map_err(|e| sink_err("arrow finish framed multi stream", e))?;
    }
    Ok(buf)
}
