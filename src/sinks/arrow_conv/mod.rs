//! Arrow IPC serialization helpers for sink `protocol: arrow`.
//!
//! - `schema` — Schema inference from `DataRecord` fields
//! - `batch`  — `DataRecord` → `RecordBatch` conversion with typed column builders

pub use wp_connector_utils::arrow::{
    data_record_to_batch, data_records_to_batch, infer_arrow_schema, infer_schema_from_record,
};

use arrow::record_batch::RecordBatch;
use orion_error::conversion::ToStructError;
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
// Shared Arrow IPC encoding helpers — delegate to wp_connector_utils
// ---------------------------------------------------------------------------

/// Encode a single `RecordBatch` as Arrow IPC Stream bytes.
pub(crate) fn encode_batch_ipc_stream(batch: &RecordBatch) -> SinkResult<Vec<u8>> {
    wp_connector_utils::arrow::encode_batch_ipc_stream(batch)
}

/// Encode a `RecordBatch` as a wp_arrow IPC frame: `[4B tag_len BE][tag][Arrow IPC stream]`.
pub(crate) fn encode_ipc_frame(tag: &str, batch: &RecordBatch) -> SinkResult<Vec<u8>> {
    wp_connector_utils::arrow::encode_ipc_frame(tag, batch)
}

/// Encode multiple `RecordBatch`es as a single wp_arrow frame.
pub(crate) fn encode_ipc_frame_multi(tag: &str, batches: &[RecordBatch]) -> SinkResult<Vec<u8>> {
    wp_connector_utils::arrow::encode_ipc_frame_multi(tag, batches)
}
