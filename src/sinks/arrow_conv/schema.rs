//! Arrow schema inference from `DataRecord` fields.

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use wp_model_core::model::DataRecord;

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

/// Map wp_model_core DataType to Arrow DataType.
fn wp_type_to_arrow(dt: &wp_model_core::model::DataType) -> DataType {
    use wp_model_core::model::DataType as WpDt;
    match dt {
        WpDt::Bool => DataType::Boolean,
        WpDt::Digit => DataType::Int64,
        WpDt::Float => DataType::Float64,
        WpDt::Port => DataType::Int32,
        WpDt::Time
        | WpDt::TimeISO
        | WpDt::TimeRFC3339
        | WpDt::TimeRFC2822
        | WpDt::TimeTIMESTAMP
        | WpDt::TimeCLF => DataType::Timestamp(TimeUnit::Nanosecond, None),
        WpDt::Hex | WpDt::Base64 => DataType::Binary,
        WpDt::Chars
        | WpDt::Symbol
        | WpDt::PeekSymbol
        | WpDt::IP
        | WpDt::IpNet
        | WpDt::Domain
        | WpDt::Email
        | WpDt::Url
        | WpDt::SN
        | WpDt::IdCard
        | WpDt::MobilePhone
        | WpDt::KV
        | WpDt::KvArr
        | WpDt::Json
        | WpDt::ExactJson
        | WpDt::HttpRequest
        | WpDt::HttpStatus
        | WpDt::HttpAgent
        | WpDt::HttpMethod
        | WpDt::Auto
        | WpDt::ProtoText
        | WpDt::Obj
        | WpDt::Ignore => DataType::Utf8,
        WpDt::Array(_) => DataType::Utf8,
    }
}

/// Infer an Arrow schema from a DataRecord using actual field types (get_meta()).
///
/// Fields with `DataType::Ignore` are excluded from the schema.
pub fn infer_schema_from_record(record: &DataRecord) -> Schema {
    Schema::new(
        record
            .items
            .iter()
            .filter(|f| !matches!(f.get_meta(), wp_model_core::model::DataType::Ignore))
            .map(|f| {
                let arrow_type = wp_type_to_arrow(f.get_meta());
                Field::new(f.get_name(), arrow_type, true)
            })
            .collect::<Vec<_>>(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDateTime;
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
        let dt = NaiveDateTime::parse_from_str("2026-06-13 12:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
        let rec = DataRecord::from(vec![FieldStorage::from(ModelField::from_time("ts", dt))]);
        let schema = infer_schema_from_record(&rec);
        assert_eq!(
            schema.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
    }

    #[test]
    fn port_maps_to_int32() {
        use wp_model_core::model::DataType as WpDt;
        assert_eq!(wp_type_to_arrow(&WpDt::Port), DataType::Int32);
    }

    #[test]
    fn ignore_field_excluded_from_schema() {
        let rec = DataRecord::from(vec![FieldStorage::from(ModelField::from_digit("count", 1))]);
        let schema = infer_schema_from_record(&rec);
        assert!(schema.fields().iter().any(|f| f.name() == "count"));
        assert_eq!(schema.fields().len(), 1);
    }
}
