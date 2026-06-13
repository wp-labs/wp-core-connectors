use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use orion_error::conversion::ToStructError;
use wp_connector_api::{SinkReason, SinkResult};
use wp_model_core::model::DataRecord;

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
pub fn infer_schema_from_record(record: &DataRecord) -> Schema {
    Schema::new(
        record
            .items
            .iter()
            .map(|f| Field::new(f.get_name(), DataType::Utf8, true))
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
        let value = record
            .field(field.name())
            .map(|f| f.get_value().to_string())
            .unwrap_or_default();
        let arr = StringArray::from(vec![Some(value.as_str())]);
        columns.push(Arc::new(arr));
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
/// Missing fields default to an empty string.
pub fn data_records_to_batch(
    records: &[Arc<DataRecord>],
    schema: &Arc<Schema>,
) -> SinkResult<RecordBatch> {
    if records.is_empty() {
        // Return an empty batch with the given schema
        let empty_columns: Vec<ArrayRef> = schema
            .fields()
            .iter()
            .map(|_| Arc::new(StringArray::from(Vec::<Option<&str>>::new())) as ArrayRef)
            .collect();
        return RecordBatch::try_new(Arc::clone(schema), empty_columns).map_err(|e| {
            SinkReason::Sink
                .to_err()
                .with_detail(format!("data_records_to_batch empty failed: {e}"))
        });
    }

    let num_fields = schema.fields().len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(num_fields);

    for field in schema.fields() {
        let field_name = field.name();
        let mut builder = StringBuilder::with_capacity(records.len(), records.len() * 32);
        for record in records {
            match record.field(field_name) {
                Some(f) => builder.append_value(f.get_value().to_string()),
                None => builder.append_null(),
            }
        }
        columns.push(Arc::new(builder.finish()) as ArrayRef);
    }

    RecordBatch::try_new(Arc::clone(schema), columns).map_err(|e| {
        SinkReason::Sink
            .to_err()
            .with_detail(format!("data_records_to_batch failed: {e}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
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
}
