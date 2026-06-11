//! NDJSON → Arrow RecordBatch conversion.
//!
//! Used by `BatchSource` adapters to convert raw text lines into
//! columnar Arrow batches matching a given schema.

use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

/// Parse a batch of NDJSON lines into a single Arrow [`RecordBatch`].
///
/// Returns `None` if the input is empty. Returns an error if any line
/// is not valid JSON or a field value cannot be converted to the
/// expected Arrow type.
pub fn ndjson_to_record_batch(
    lines: &[String],
    schema: &Schema,
) -> Result<Option<RecordBatch>, String> {
    if lines.is_empty() {
        return Ok(None);
    }

    let mut columns: HashMap<String, Vec<serde_json::Value>> = HashMap::new();
    for field in schema.fields() {
        columns.insert(field.name().clone(), Vec::with_capacity(lines.len()));
    }

    for line in lines {
        let obj: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(line).map_err(|e| format!("invalid JSON: {e}"))?;
        for field in schema.fields() {
            let val = obj
                .get(field.name())
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            columns.get_mut(field.name()).unwrap().push(val);
        }
    }

    let arrays: Result<Vec<ArrayRef>, String> = schema
        .fields()
        .iter()
        .map(|field| build_array(field, columns.get(field.name()).unwrap()))
        .collect();

    let batch = RecordBatch::try_new(Arc::new(schema.clone()), arrays?)
        .map_err(|e| format!("arrow error: {e}"))?;
    Ok(Some(batch))
}

fn build_array(field: &Field, values: &[serde_json::Value]) -> Result<ArrayRef, String> {
    match field.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 => {
            let arr: StringArray = values
                .iter()
                .map(|v| match v {
                    serde_json::Value::String(s) => Some(s.clone()),
                    serde_json::Value::Null => None,
                    other => Some(other.to_string()),
                })
                .collect::<Vec<Option<String>>>()
                .into_iter()
                .collect();
            Ok(Arc::new(arr))
        }
        DataType::Int64 => {
            let arr: Int64Array = values
                .iter()
                .map(|v| match v {
                    serde_json::Value::Number(n) => n.as_i64(),
                    serde_json::Value::String(s) => s.parse::<i64>().ok(),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .into();
            Ok(Arc::new(arr))
        }
        DataType::Float64 => {
            let arr: Float64Array = values
                .iter()
                .map(|v| match v {
                    serde_json::Value::Number(n) => n.as_f64(),
                    serde_json::Value::String(s) => s.parse::<f64>().ok(),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .into();
            Ok(Arc::new(arr))
        }
        DataType::Boolean => {
            let arr: BooleanArray = values
                .iter()
                .map(|v| match v {
                    serde_json::Value::Bool(b) => Some(*b),
                    serde_json::Value::String(s) => s.parse::<bool>().ok(),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .into();
            Ok(Arc::new(arr))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, None) => {
            let arr: TimestampNanosecondArray = values
                .iter()
                .map(|v| match v {
                    serde_json::Value::String(s) => chrono::DateTime::parse_from_rfc3339(s)
                        .ok()
                        .map(|dt| dt.timestamp_nanos_opt().unwrap_or(0)),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .into();
            Ok(Arc::new(arr))
        }
        other => Err(format!("unsupported Arrow type for NDJSON: {other:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::new("sip", DataType::Utf8, true),
            Field::new("dport", DataType::Int64, true),
            Field::new("score", DataType::Float64, true),
            Field::new("active", DataType::Boolean, true),
            Field::new(
                "event_time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
        ])
    }

    #[test]
    fn parse_simple_ndjson() {
        let schema = Schema::new(vec![
            Field::new("sip", DataType::Utf8, true),
            Field::new("dport", DataType::Int64, true),
        ]);
        let lines = vec![
            r#"{"sip":"10.0.0.1","dport":"443"}"#.to_string(),
            r#"{"sip":"10.0.0.2","dport":"80"}"#.to_string(),
        ];
        let batch = ndjson_to_record_batch(&lines, &schema).unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn empty_lines_returns_none() {
        let schema = test_schema();
        let result = ndjson_to_record_batch(&[], &schema).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn null_fields_become_null() {
        let schema = test_schema();
        let lines = vec![
            r#"{"sip":null,"dport":null,"score":null,"active":null,"event_time":null}"#.to_string(),
        ];
        let batch = ndjson_to_record_batch(&lines, &schema).unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 5);
    }

    #[test]
    fn float_and_bool_types() {
        let schema = Schema::new(vec![
            Field::new("score", DataType::Float64, true),
            Field::new("active", DataType::Boolean, true),
        ]);
        let lines = vec![
            r#"{"score":70.5,"active":true}"#.to_string(),
            r#"{"score":0.0,"active":false}"#.to_string(),
            r#"{"score":"99.9","active":"true"}"#.to_string(), // string→number, string→bool
        ];
        let batch = ndjson_to_record_batch(&lines, &schema).unwrap().unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn timestamp_from_rfc3339() {
        let schema = Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )]);
        let lines = vec![
            r#"{"ts":"2026-01-01T00:00:00Z"}"#.to_string(),
            r#"{"ts":"2026-01-01T00:00:01Z"}"#.to_string(),
        ];
        let batch = ndjson_to_record_batch(&lines, &schema).unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn missing_field_defaults_to_null() {
        let schema = Schema::new(vec![
            Field::new("sip", DataType::Utf8, true),
            Field::new("dport", DataType::Int64, true),
        ]);
        let lines = vec![
            r#"{"sip":"10.0.0.1"}"#.to_string(), // dport missing
        ];
        let batch = ndjson_to_record_batch(&lines, &schema).unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn invalid_json_returns_error() {
        let schema = test_schema();
        let lines = vec!["not json".to_string()];
        let result = ndjson_to_record_batch(&lines, &schema);
        assert!(result.is_err());
    }

    #[test]
    fn extra_fields_are_ignored() {
        let schema = Schema::new(vec![Field::new("sip", DataType::Utf8, true)]);
        let lines = vec![r#"{"sip":"10.0.0.1","extra_field":"ignored","another":42}"#.to_string()];
        let batch = ndjson_to_record_batch(&lines, &schema).unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 1);
    }
}
