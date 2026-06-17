//! `wf_connector_api::BatchSource` implementations.
//!
//! These adapters wrap existing `wp_connector_api::DataSource` sources
//! and convert their `SourceEvent { payload: RawData }` output into
//! Arrow `RecordBatch`es suitable for CEP engines like warp-fusion.

pub mod arrow;
pub mod error;
pub mod file;
pub mod ndjson;
pub mod payload;
pub mod tcp;
