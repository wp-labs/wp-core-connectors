//! Payload extraction helpers shared across batch source implementations.

use wp_model_core::raw::RawData;

/// Extract a UTF-8 string from any RawData variant.
pub fn payload_to_string(payload: &RawData) -> String {
    match payload {
        RawData::String(s) => s.clone(),
        RawData::Bytes(b) => String::from_utf8_lossy(b).to_string(),
        RawData::ArcBytes(b) => String::from_utf8_lossy(b).to_string(),
    }
}

/// Extract raw bytes from any RawData variant.
pub fn payload_to_bytes(payload: &RawData) -> Vec<u8> {
    match payload {
        RawData::String(s) => s.as_bytes().to_vec(),
        RawData::Bytes(b) => b.to_vec(),
        RawData::ArcBytes(b) => b.to_vec(),
    }
}
