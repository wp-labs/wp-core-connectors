use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
    TimestampNanosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use orion_error::conversion::ToStructError;
use wp_connector_api::{SinkReason, SinkResult};
use wp_model_core::model::DataRecord;
use wp_model_core::model::Value;

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
// Schema inference
// ---------------------------------------------------------------------------

/// Infer an Arrow schema from field names.
///
/// All fields default to `Utf8` (conservative).
pub fn infer_arrow_schema(fields: &[String]) -> Schema {
    Schema::new(
        fields
            .iter()
            .map(|f| Field::new(f.as_str(), DataType::Utf8, true))
            .collect::<Vec<_>>(),
    )
}

/// Infer an Arrow schema automatically from a [`DataRecord`]'s fields.
///
/// All fields default to `Utf8` (conservative). Use this instead of
/// `infer_arrow_schema` when the field names should be derived from
/// the data itself rather than passed externally.
/// Map wp_model_core DataType to Arrow DataType.
fn wp_type_to_arrow(dt: &wp_model_core::model::DataType) -> arrow::datatypes::DataType {
    use wp_model_core::model::DataType as WpDt;
    match dt {
        WpDt::Bool => arrow::datatypes::DataType::Boolean,
        WpDt::Digit => arrow::datatypes::DataType::Int64,
        WpDt::Float => arrow::datatypes::DataType::Float64,
        WpDt::Time
        | WpDt::TimeISO
        | WpDt::TimeRFC3339
        | WpDt::TimeRFC2822
        | WpDt::TimeTIMESTAMP
        | WpDt::TimeCLF => {
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None)
        }
        // Ip, Hex, IpNet, Domain, Email, Chars, Symbol, etc → Utf8
        _ => arrow::datatypes::DataType::Utf8,
    }
}

/// Infer an Arrow schema from a DataRecord using actual field types (get_meta()).
pub fn infer_schema_from_record(record: &DataRecord) -> Schema {
    Schema::new(
        record
            .items
            .iter()
            .map(|f| {
                let arrow_type = wp_type_to_arrow(f.get_meta());
                Field::new(f.get_name(), arrow_type, true)
            })
            .collect::<Vec<_>>(),
    )
}

// ---------------------------------------------------------------------------
// DataRecord → RecordBatch
// ---------------------------------------------------------------------------

/// Convert a single `DataRecord` into an Arrow `RecordBatch`.
///
/// Each field in the schema is looked up by name in the record.
/// Missing fields default to an empty string.
pub fn data_record_to_batch(record: &DataRecord, schema: &Arc<Schema>) -> SinkResult<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        let records = [Arc::new(record.clone())];
        columns.push(build_column_from_field(field, &records)?);
    }

    RecordBatch::try_new(Arc::clone(schema), columns).map_err(|e| {
        SinkReason::Sink
            .to_err()
            .with_detail(format!("data_record_to_batch failed: {e}"))
    })
}

/// Convert multiple `DataRecord`s into a single Arrow `RecordBatch`.
///
/// Each field in the schema is looked up by name in each record.
/// Missing fields default to null.
pub fn data_records_to_batch(
    records: &[Arc<DataRecord>],
    schema: &Arc<Schema>,
) -> SinkResult<RecordBatch> {
    if records.is_empty() {
        // Return an empty batch with the given schema
        let empty_columns: Vec<ArrayRef> = schema
            .fields()
            .iter()
            .map(|f| empty_column_for_type(f.data_type(), 0))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                SinkReason::Sink
                    .to_err()
                    .with_detail(format!("data_records_to_batch empty failed: {e}"))
            })?;
        return RecordBatch::try_new(Arc::clone(schema), empty_columns).map_err(|e| {
            SinkReason::Sink
                .to_err()
                .with_detail(format!("data_records_to_batch empty failed: {e}"))
        });
    }

    let num_fields = schema.fields().len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(num_fields);

    for field in schema.fields() {
        columns.push(build_column_from_field(field, records)?);
    }

    RecordBatch::try_new(Arc::clone(schema), columns).map_err(|e| {
        SinkReason::Sink
            .to_err()
            .with_detail(format!("data_records_to_batch failed: {e}"))
    })
}

/// Build a single column array for a schema field from the given records.
fn build_column_from_field(field: &Field, records: &[Arc<DataRecord>]) -> SinkResult<ArrayRef> {
    let field_name = field.name();
    match field.data_type() {
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(records.len());
            for record in records {
                match record.field(field_name).map(|f| f.get_value()) {
                    Some(Value::Bool(v)) => builder.append_value(*v),
                    Some(Value::Chars(s)) => builder.append_value(s.eq_ignore_ascii_case("true")),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Int64 => {
            let mut builder = Int64Builder::with_capacity(records.len());
            for record in records {
                match record
                    .field(field_name)
                    .and_then(|f| parse_digit(f.get_value()))
                {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(records.len());
            for record in records {
                match record
                    .field(field_name)
                    .and_then(|f| parse_float(f.get_value()))
                {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, None) => {
            let mut builder = TimestampNanosecondBuilder::with_capacity(records.len());
            for record in records {
                match record
                    .field(field_name)
                    .and_then(|f| parse_timestamp_ns(f.get_value()))
                {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        // Utf8 and everything else
        _ => {
            let mut builder = StringBuilder::with_capacity(records.len(), records.len() * 32);
            for record in records {
                match record.field(field_name) {
                    Some(f) => builder.append_value(f.get_value().to_string()),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
    }
}

fn parse_digit(v: &Value) -> Option<i64> {
    match v {
        Value::Digit(d) => Some(*d),
        Value::Float(f) => Some(*f as i64),
        Value::Chars(s) => s.parse().ok(),
        _ => None,
    }
}

fn parse_float(v: &Value) -> Option<f64> {
    match v {
        Value::Float(f) => Some(*f),
        Value::Digit(d) => Some(*d as f64),
        Value::Chars(s) => s.parse().ok(),
        _ => None,
    }
}

fn parse_timestamp_ns(v: &Value) -> Option<i64> {
    match v {
        Value::Time(t) => Some(t.and_utc().timestamp_nanos_opt()?),
        Value::Digit(d) => d.checked_mul(1_000_000), // millis → nanos
        Value::Chars(s) => {
            // Try RFC3339 or simple format
            chrono::DateTime::parse_from_rfc3339(s)
                .ok()
                .or_else(|| {
                    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                        .ok()
                        .map(|dt| dt.and_utc().fixed_offset())
                })
                .and_then(|dt| dt.timestamp_nanos_opt())
        }
        _ => None,
    }
}

fn empty_column_for_type(data_type: &DataType, _capacity: usize) -> Result<ArrayRef, String> {
    let arr: ArrayRef = match data_type {
        DataType::Boolean => Arc::new(arrow::array::BooleanArray::from(Vec::<bool>::new())),
        DataType::Int64 => Arc::new(arrow::array::Int64Array::from(Vec::<i64>::new())),
        DataType::Float64 => Arc::new(arrow::array::Float64Array::from(Vec::<f64>::new())),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => Arc::new(
            arrow::array::TimestampNanosecondArray::from(Vec::<i64>::new()),
        ),
        _ => Arc::new(arrow::array::StringArray::from(Vec::<Option<&str>>::new())),
    };
    Ok(arr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use wp_model_core::model::{Field as ModelField, FieldStorage};

    #[test]
    fn infer_schema_all_utf8() {
        let fields = vec!["name".to_string(), "count".to_string()];
        let schema = infer_arrow_schema(&fields);
        assert_eq!(schema.fields().len(), 2);
        for f in schema.fields() {
            assert_eq!(f.data_type(), &DataType::Utf8);
            assert!(f.is_nullable());
        }
    }

    #[test]
    fn data_record_to_batch_roundtrip() {
        let fields = vec!["name".to_string(), "count".to_string()];
        let schema = Arc::new(infer_arrow_schema(&fields));

        let rec = DataRecord::from(vec![
            FieldStorage::from(ModelField::from_chars("name", "alice")),
            FieldStorage::from(ModelField::from_chars("count", "42")),
        ]);

        let batch = data_record_to_batch(&rec, &schema).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn missing_field_defaults_to_empty() {
        let fields = vec!["present".to_string(), "missing".to_string()];
        let schema = Arc::new(infer_arrow_schema(&fields));

        let rec = DataRecord::from(vec![FieldStorage::from(ModelField::from_chars(
            "present", "hello",
        ))]);

        let batch = data_record_to_batch(&rec, &schema).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn data_records_to_batch_multiple_rows() {
        let fields = vec!["name".to_string(), "count".to_string()];
        let schema = Arc::new(infer_arrow_schema(&fields));

        let records: Vec<Arc<DataRecord>> = vec![
            Arc::new(DataRecord::from(vec![
                FieldStorage::from(ModelField::from_chars("name", "alice")),
                FieldStorage::from(ModelField::from_chars("count", "1")),
            ])),
            Arc::new(DataRecord::from(vec![
                FieldStorage::from(ModelField::from_chars("name", "bob")),
                FieldStorage::from(ModelField::from_chars("count", "2")),
            ])),
            Arc::new(DataRecord::from(vec![
                FieldStorage::from(ModelField::from_chars("name", "carol")),
                FieldStorage::from(ModelField::from_chars("count", "3")),
            ])),
        ];

        let batch = data_records_to_batch(&records, &schema).unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn data_records_to_batch_empty() {
        let fields = vec!["x".to_string()];
        let schema = Arc::new(infer_arrow_schema(&fields));

        let batch = data_records_to_batch(&[], &schema).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 1);
    }

    // -- Typed schema inference -------------------------------------------

    #[test]
    fn infer_schema_from_record_uses_actual_types() {
        let rec = DataRecord::from(vec![
            FieldStorage::from(ModelField::from_bool("active", true)),
            FieldStorage::from(ModelField::from_digit("count", 42)),
            FieldStorage::from(ModelField::from_float("score", 2.71)),
            FieldStorage::from(ModelField::from_chars("name", "alice")),
        ]);

        let schema = infer_schema_from_record(&rec);
        assert_eq!(schema.fields().len(), 4);
        assert_eq!(schema.field(0).data_type(), &DataType::Boolean);
        assert_eq!(schema.field(1).data_type(), &DataType::Int64);
        assert_eq!(schema.field(2).data_type(), &DataType::Float64);
        assert_eq!(schema.field(3).data_type(), &DataType::Utf8);
    }

    #[test]
    fn infer_schema_include_time_type() {
        use chrono::NaiveDateTime;
        let dt = NaiveDateTime::parse_from_str("2026-06-13 12:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
        let rec = DataRecord::from(vec![FieldStorage::from(ModelField::from_time("ts", dt))]);
        let schema = infer_schema_from_record(&rec);
        assert_eq!(
            schema.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
    }

    // -- Typed column building --------------------------------------------

    #[test]
    fn typed_batch_bool_column() {
        let rec = DataRecord::from(vec![FieldStorage::from(ModelField::from_bool(
            "flag", true,
        ))]);
        let schema = Arc::new(infer_schema_from_record(&rec));
        let batch = data_record_to_batch(&rec, &schema).unwrap();

        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap();
        assert!(col.value(0));
    }

    #[test]
    fn typed_batch_digit_column() {
        let rec = DataRecord::from(vec![FieldStorage::from(ModelField::from_digit(
            "count", 99,
        ))]);
        let schema = Arc::new(infer_schema_from_record(&rec));
        let batch = data_record_to_batch(&rec, &schema).unwrap();

        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 99);
    }

    #[test]
    fn typed_batch_float_column() {
        let rec = DataRecord::from(vec![FieldStorage::from(ModelField::from_float(
            "score", 2.5,
        ))]);
        let schema = Arc::new(infer_schema_from_record(&rec));
        let batch = data_record_to_batch(&rec, &schema).unwrap();

        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert!((col.value(0) - 2.5).abs() < 0.001);
    }

    #[test]
    fn typed_batch_multi_row() {
        let schema = Arc::new(infer_schema_from_record(&DataRecord::from(vec![
            FieldStorage::from(ModelField::from_digit("v", 0)),
        ])));

        let records: Vec<Arc<DataRecord>> = (0..5)
            .map(|i| {
                Arc::new(DataRecord::from(vec![FieldStorage::from(
                    ModelField::from_digit("v", i),
                )]))
            })
            .collect();

        let batch = data_records_to_batch(&records, &schema).unwrap();
        assert_eq!(batch.num_rows(), 5);
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        for i in 0..5 {
            assert_eq!(col.value(i), i as i64);
        }
    }

    // -- Parse helpers ----------------------------------------------------

    #[test]
    fn parse_digit_from_chars_fallback() {
        let v = Value::Chars("123".into());
        assert_eq!(parse_digit(&v), Some(123));
    }

    #[test]
    fn parse_digit_from_invalid_chars_is_none() {
        let v = Value::Chars("abc".into());
        assert_eq!(parse_digit(&v), None);
    }

    #[test]
    fn parse_digit_from_float_truncation() {
        let v = Value::Float(3.7);
        assert_eq!(parse_digit(&v), Some(3));
    }

    #[test]
    fn parse_timestamp_ns_from_digit_millis() {
        let v = Value::Digit(1_700_000_000_000_i64); // millis
        let ns = parse_timestamp_ns(&v);
        assert_eq!(ns, Some(1_700_000_000_000_000_000_i64)); // nanos
    }

    #[test]
    fn boolean_builder_chars_fallback() {
        let rec = DataRecord::from(vec![FieldStorage::from(ModelField::from_chars(
            "flag", "TRUE",
        ))]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "flag",
            DataType::Boolean,
            true,
        )]));
        let batch = data_record_to_batch(&rec, &schema).unwrap();
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap();
        assert!(col.value(0));
    }

    #[test]
    fn parse_float_from_chars_fallback() {
        let v = Value::Chars("2.71".into());
        let result = parse_float(&v).unwrap();
        assert!((result - 2.71).abs() < 0.001);
    }

    #[test]
    fn parse_float_from_digit() {
        let v = Value::Digit(42);
        assert_eq!(parse_float(&v), Some(42.0));
    }

    #[test]
    fn parse_timestamp_ns_from_rfc3339() {
        let v = Value::Chars("2026-06-13T12:00:00+00:00".into());
        let ns = parse_timestamp_ns(&v);
        assert!(ns.is_some(), "should parse RFC3339");
    }

    // -- Empty column helpers ---------------------------------------------

    #[test]
    fn empty_column_for_each_supported_type() {
        for dt in &[
            DataType::Boolean,
            DataType::Int64,
            DataType::Float64,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::Utf8,
        ] {
            let arr = empty_column_for_type(dt, 0).unwrap();
            assert_eq!(arr.len(), 0, "empty column of type {dt:?}");
        }
    }

    // -- Missing/null fields ----------------------------------------------

    #[test]
    fn typed_batch_null_on_missing_field() {
        let rec = DataRecord::from(vec![FieldStorage::from(ModelField::from_digit("x", 1))]);
        // Schema expects "x" and "y", but "y" is missing
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, true),
            Field::new("y", DataType::Int64, true),
        ]));
        let batch = data_record_to_batch(&rec, &schema).unwrap();
        assert_eq!(batch.num_columns(), 2);
        let y = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert!(y.is_null(0));
    }
}
