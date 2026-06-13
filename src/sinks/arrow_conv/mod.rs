//! Arrow IPC serialization helpers for sink `protocol: arrow`.
//!
//! - `schema` — Schema inference from `DataRecord` fields
//! - `batch`  — `DataRecord` → `RecordBatch` conversion with typed column builders

pub mod batch;
pub mod schema;

pub use batch::{data_record_to_batch, data_records_to_batch};
pub use schema::{infer_arrow_schema, infer_schema_from_record};

use orion_error::conversion::ToStructError;
use wp_connector_api::SinkReason;

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
