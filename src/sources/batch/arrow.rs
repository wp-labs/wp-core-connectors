//! Shared Arrow decode helpers for batch sources.
//!
//! Both `TcpBatchSource` and `FileBatchSource` decode their payloads into
//! Arrow `RecordBatch`es. This module centralises the [`WireFormat`] enum
//! (parsed from the `data_format` spec parameter) and the Arrow IPC / framed
//! decode routines so each source only has to dispatch on the format.

use arrow::record_batch::RecordBatch;
use wf_connector_api::{SourceReason, SourceResult};
use wp_connector_api::SourceBatch;

use super::payload::payload_to_bytes;

/// On-the-wire / on-disk payload format expected by a batch source.
///
/// Parsed from the `data_format` spec parameter via [`WireFormat::from_data_format`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireFormat {
    /// Newline-delimited JSON.
    Ndjson,
    /// Arrow IPC Stream format (no length prefix, `StreamReader`-compatible).
    /// Suitable for continuous streaming connections.
    ArrowStream,
    /// wp_arrow frame: `[4B tag_len][tag][Arrow IPC Stream]`.
    /// Produced by wparse `encode_ipc`.
    ArrowFramed,
}

impl WireFormat {
    /// Parse from the `data_format` spec parameter.
    ///
    /// `None` and any unrecognised value fall back to [`WireFormat::Ndjson`].
    pub fn from_data_format(value: Option<&str>) -> Self {
        match value.unwrap_or("ndjson") {
            "arrow_framed" => WireFormat::ArrowFramed,
            "arrow_ipc" => WireFormat::ArrowStream,
            _ => WireFormat::Ndjson,
        }
    }
}

/// Decode raw Arrow IPC Stream bytes from `SourceEvent`s into `RecordBatch`es.
///
/// Each event's payload is decoded independently; batches from all events are
/// concatenated into the result.
pub fn decode_arrow_ipc_batches(events: &SourceBatch) -> SourceResult<Vec<RecordBatch>> {
    use arrow::ipc::reader::StreamReader;
    let mut batches = Vec::new();
    for event in events {
        let payload = payload_to_bytes(&event.payload);
        let cursor = std::io::Cursor::new(payload);
        let reader = StreamReader::try_new(cursor, None)
            .map_err(|e| SourceReason::Decode.err_detail(format!("arrow ipc: {e}")))?;
        for batch in reader {
            let batch = batch
                .map_err(|e| SourceReason::Decode.err_detail(format!("arrow ipc batch: {e}")))?;
            batches.push(batch);
        }
    }
    Ok(batches)
}

/// Decode wp_arrow frames into `RecordBatch`es.
///
/// Frame layout: `[4B tag_len (big-endian u32)][tag][Arrow IPC Stream]`.
/// The `tag` is currently only used to locate the IPC payload and is otherwise
/// discarded. Payloads too short for the header (or whose declared `tag_len`
/// exceeds the payload) are silently skipped rather than aborting the batch.
pub fn decode_arrow_framed_batches(events: &SourceBatch) -> SourceResult<Vec<RecordBatch>> {
    use arrow::ipc::reader::StreamReader;
    let mut batches = Vec::new();
    for event in events {
        let payload = payload_to_bytes(&event.payload);
        if payload.len() < 4 {
            continue;
        }
        let tag_len = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
        let ipc_start = 4 + tag_len;
        if ipc_start > payload.len() {
            continue;
        }
        let ipc_bytes = &payload[ipc_start..];
        let cursor = std::io::Cursor::new(ipc_bytes);
        let reader = StreamReader::try_new(cursor, None)
            .map_err(|e| SourceReason::Decode.err_detail(format!("arrow framed: {e}")))?;
        for batch in reader {
            let batch = batch
                .map_err(|e| SourceReason::Decode.err_detail(format!("arrow framed batch: {e}")))?;
            batches.push(batch);
        }
    }
    Ok(batches)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::StreamWriter;
    use bytes::Bytes;
    use std::sync::Arc;
    use wp_connector_api::SourceEvent;
    use wp_model_core::raw::RawData;

    // -- WireFormat parsing ------------------------------------------------

    #[test]
    fn wire_format_defaults_to_ndjson() {
        assert_eq!(WireFormat::from_data_format(None), WireFormat::Ndjson);
    }

    #[test]
    fn wire_format_parses_arrow_framed() {
        assert_eq!(
            WireFormat::from_data_format(Some("arrow_framed")),
            WireFormat::ArrowFramed
        );
    }

    #[test]
    fn wire_format_parses_arrow_ipc() {
        assert_eq!(
            WireFormat::from_data_format(Some("arrow_ipc")),
            WireFormat::ArrowStream
        );
    }

    #[test]
    fn wire_format_unknown_falls_back_to_ndjson() {
        assert_eq!(
            WireFormat::from_data_format(Some("nonsense")),
            WireFormat::Ndjson
        );
    }

    fn make_ipc_bytes(schema: &Arc<Schema>, values: &[&str]) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut w = StreamWriter::try_new(&mut buf, schema).unwrap();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(values.to_vec()))],
        )
        .unwrap();
        w.write(&batch).unwrap();
        w.finish().unwrap();
        buf
    }

    fn event_from_bytes(bytes: impl Into<Bytes>) -> SourceEvent {
        SourceEvent::new(
            1,
            "key".to_string(),
            RawData::Bytes(bytes.into()),
            Default::default(),
        )
    }

    // -- decode_arrow_ipc_batches ------------------------------------------

    #[test]
    fn decode_arrow_ipc_roundtrip() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, false)]));
        let ipc = make_ipc_bytes(&schema, &["hello"]);
        let event = event_from_bytes(ipc);
        let batches = decode_arrow_ipc_batches(&vec![event]).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[test]
    fn decode_arrow_ipc_multiple_batches_concat() {
        // Two IPC streams in two events -> all batches concatenated.
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, false)]));
        let ev1 = event_from_bytes(make_ipc_bytes(&schema, &["a", "b"]));
        let ev2 = event_from_bytes(make_ipc_bytes(&schema, &["c"]));
        let batches = decode_arrow_ipc_batches(&vec![ev1, ev2]).unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[1].num_rows(), 1);
    }

    #[test]
    fn decode_arrow_ipc_invalid_returns_err() {
        // Corrupt bytes are not a valid IPC stream; must surface a Decode
        // error rather than panic.
        let event = event_from_bytes(Bytes::from_static(b"ARROW?? not really"));
        let result = decode_arrow_ipc_batches(&vec![event]);
        assert!(result.is_err(), "expected decode error for corrupt input");
        let err = result.unwrap_err();
        assert!(
            format!("{err:?}").to_lowercase().contains("decode"),
            "error should be classified as Decode: {err:?}"
        );
    }

    // -- decode_arrow_framed_batches ---------------------------------------

    #[test]
    fn decode_arrow_framed_roundtrip() {
        let tag = b"my_tag";
        let tag_len = (tag.len() as u32).to_be_bytes();

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, false)]));
        let ipc_buf = make_ipc_bytes(&schema, &["hello"]);

        // Build wp_arrow frame: [4B tag_len][tag][Arrow IPC Stream]
        let mut frame = Vec::new();
        frame.extend_from_slice(&tag_len);
        frame.extend_from_slice(tag);
        frame.extend_from_slice(&ipc_buf);

        let event = event_from_bytes(frame);
        let batches = decode_arrow_framed_batches(&vec![event]).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[test]
    fn decode_arrow_framed_too_short_payload_is_skipped() {
        // Payload too short for the tag_len header.
        let event = event_from_bytes(Bytes::from(vec![0, 0]));
        let batches = decode_arrow_framed_batches(&vec![event]).unwrap();
        assert!(batches.is_empty());
    }

    #[test]
    fn decode_arrow_framed_tag_len_exceeds_payload_is_skipped() {
        // Declares a huge tag_len that runs past the payload end.
        let event = event_from_bytes(Bytes::from(vec![0xff, 0xff, 0xff, 0xff, 0x00]));
        let batches = decode_arrow_framed_batches(&vec![event]).unwrap();
        assert!(batches.is_empty());
    }

    #[test]
    fn decode_arrow_framed_empty_tag() {
        // tag_len = 0: IPC payload starts immediately after the 4-byte header.
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, false)]));
        let ipc = make_ipc_bytes(&schema, &["hi"]);

        let mut frame = Vec::new();
        frame.extend_from_slice(&0u32.to_be_bytes()); // tag_len = 0
        frame.extend_from_slice(&ipc);

        let event = event_from_bytes(frame);
        let batches = decode_arrow_framed_batches(&vec![event]).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[test]
    fn decode_arrow_framed_multiple_events_concat() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, false)]));
        let make_framed = |tag: &str| {
            let ipc = make_ipc_bytes(&schema, &["row"]);
            let mut frame = Vec::new();
            frame.extend_from_slice(&(tag.len() as u32).to_be_bytes());
            frame.extend_from_slice(tag.as_bytes());
            frame.extend_from_slice(&ipc);
            event_from_bytes(frame)
        };
        let batches =
            decode_arrow_framed_batches(&vec![make_framed("a"), make_framed("bb")]).unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[1].num_rows(), 1);
    }

    #[test]
    fn decode_arrow_framed_invalid_ipc_returns_err() {
        // Valid header (tag_len = 1) but corrupt IPC body -> Decode error.
        let mut frame = Vec::new();
        frame.extend_from_slice(&1u32.to_be_bytes());
        frame.extend_from_slice(b"t");
        frame.extend_from_slice(b"definitely not arrow");
        let event = event_from_bytes(frame);
        let result = decode_arrow_framed_batches(&vec![event]);
        assert!(
            result.is_err(),
            "expected decode error for corrupt framed body"
        );
    }
}
