//! Shared error mapping between `wp_connector_api` and `wf_connector_api`.

use wf_connector_api::{SourceError, SourceReason};
use wp_connector_api::{SourceError as WpError, SourceReason as WpReason};

/// Map a `wp_connector_api::SourceError` to `wf_connector_api::SourceError`.
pub fn wp_error_to_wf(err: WpError) -> SourceError {
    match err.reason() {
        WpReason::EOF => SourceError::from(SourceReason::EOF),
        WpReason::SupplierError | WpReason::Disconnect => {
            SourceReason::Connect.err_detail(err.to_string())
        }
        _ => SourceReason::Decode.err_detail(err.to_string()),
    }
}
