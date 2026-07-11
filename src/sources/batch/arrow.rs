//! Shared Arrow decode helpers for batch sources.
//!
//! Both `TcpBatchSource` and `FileBatchSource` decode their payloads into
//! Arrow `RecordBatch`es. [`WireFormat`] and the core decode logic are
//! re-exported from `wp_connector_utils::arrow`.

pub use wp_connector_utils::arrow::WireFormat;

use arrow::record_batch::RecordBatch;
use wf_connector_api::{SourceReason, SourceResult};
use wp_connector_api::SourceBatch;

use super::payload::payload_to_bytes;

/// Decode raw Arrow IPC Stream bytes from `SourceEvent`s into `RecordBatch`es.
///
/// Each event's payload is decoded independently via
/// [`wp_connector_utils::arrow::decode_arrow_ipc_batches`]; results are
/// concatenated.
pub fn decode_arrow_ipc_batches(events: &SourceBatch) -> SourceResult<Vec<RecordBatch>> {
    let mut batches = Vec::new();
    for event in events {
        let payload = payload_to_bytes(&event.payload);
        let decoded = wp_connector_utils::arrow::decode_arrow_ipc_batches(&payload)
            .map_err(|e| SourceReason::Decode.err_detail(e))?;
        batches.extend(decoded);
    }
    Ok(batches)
}

/// Decode wp_arrow frames into `RecordBatch`es.
///
/// Each event's payload is decoded independently via
/// [`wp_connector_utils::arrow::decode_arrow_framed_batches`]; results are
/// concatenated.
pub fn decode_arrow_framed_batches(events: &SourceBatch) -> SourceResult<Vec<RecordBatch>> {
    let mut batches = Vec::new();
    for event in events {
        let payload = payload_to_bytes(&event.payload);
        let decoded = wp_connector_utils::arrow::decode_arrow_framed_batches(&payload)
            .map_err(|e| SourceReason::Decode.err_detail(e))?;
        batches.extend(decoded);
    }
    Ok(batches)
}
