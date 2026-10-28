use async_trait::async_trait;
use orion_error::conversion::{SourceErr, ToStructError};
use wp_connector_api::{
    ConnectorDef, SinkBuildCtx, SinkDefProvider, SinkFactory, SinkHandle, SinkReason, SinkResult,
    SinkSpec,
};

use super::file::{ArrowFileSink, AsyncFileSink, FileSinkSpec, FormattedFileSink};

/// Resolve the output file path from sink spec params.
///
/// Uses `base` (default `"./data/out_dat"`) and `file` (default `"out.dat"`),
/// then resolves relative to the `SinkBuildCtx::work_root`.
fn resolve_file_path(spec: &SinkSpec, ctx: &SinkBuildCtx) -> SinkResult<String> {
    let base = spec
        .params
        .get("base")
        .and_then(|v| v.as_str())
        .unwrap_or("./data/out_dat");
    let file_name = spec
        .params
        .get("file")
        .and_then(|v| v.as_str())
        .unwrap_or("out.dat");
    Ok(super::file::resolve_output_path(base, file_name, ctx)
        .display()
        .to_string())
}

pub struct FileFactory;

#[async_trait]
impl SinkFactory for FileFactory {
    fn kind(&self) -> &'static str {
        "file"
    }

    fn validate_spec(&self, spec: &SinkSpec) -> SinkResult<()> {
        let protocol = spec
            .params
            .get("protocol")
            .and_then(|v| v.as_str())
            .unwrap_or("txt");
        match protocol {
            "arrow" => {
                // Ensure 'file' param is present
                let _ = spec
                    .params
                    .get("file")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        SinkReason::core_conf()
                            .to_err()
                            .with_detail("arrow file sink requires 'file' param")
                    })?;
                // 滚动（max_size/max_rolls/roll_compress）目前只支持 txt 协议
                let rolling_requested = super::file::parse_max_size(spec.params.get("max_size"))?
                    .is_some()
                    || spec.params.contains_key("max_rolls")
                    || super::file::RollCompress::parse(spec.params.get("roll_compress"))?
                        != super::file::RollCompress::None;
                if rolling_requested {
                    return Err(SinkReason::core_conf().to_err().with_detail(
                        "max_size/max_rolls/roll_compress rolling is only supported for protocol = \"txt\"",
                    ));
                }
                // Validate data_format if provided
                if let Some(df) = spec.params.get("data_format").and_then(|v| v.as_str())
                    && !matches!(df, "arrow_framed" | "arrow_ipc")
                {
                    return Err(SinkReason::core_conf().to_err().with_detail(format!(
                        "file_arrow.data_format must be 'arrow_framed' or 'arrow_ipc' (got '{df}')"
                    )));
                }
                Ok(())
            }
            "txt" | "" => {
                // Existing text-based validation
                FileSinkSpec::from_resolved("file", spec)?;
                Ok(())
            }
            other => Err(SinkReason::core_conf().to_err().with_detail(format!(
                "unsupported file protocol: '{other}'; expected 'txt' or 'arrow'"
            ))),
        }
    }

    async fn build(&self, spec: &SinkSpec, ctx: &SinkBuildCtx) -> SinkResult<SinkHandle> {
        let protocol = spec
            .params
            .get("protocol")
            .and_then(|v| v.as_str())
            .unwrap_or("txt");

        let sink: Box<dyn wp_connector_api::AsyncSink> = match protocol {
            "arrow" => {
                let file_path = resolve_file_path(spec, ctx)?;
                let sync = spec
                    .params
                    .get("sync")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                let data_format = spec
                    .params
                    .get("data_format")
                    .and_then(|v| v.as_str())
                    .unwrap_or("arrow_ipc")
                    .to_ascii_lowercase();
                let framed = data_format == "arrow_framed";
                let tag = spec
                    .params
                    .get("tag")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                Box::new(ArrowFileSink::with_format(&file_path, sync, framed, tag)?)
            }
            "txt" | "" => {
                let resolved = FileSinkSpec::from_resolved("file", spec)?;
                let path = resolved.resolve_path(ctx);
                let fmt = resolved.text_fmt();
                let sync = resolved.sync();
                let sink = AsyncFileSink::with_sync(&path, sync)
                    .await
                    .source_err(SinkReason::Sink, "file sink open")?
                    .with_roll(resolved.max_size(), resolved.max_rolls())
                    .with_roll_compress(resolved.roll_compress())
                    .with_codec_config(resolved.codec().clone())?;
                Box::new(FormattedFileSink::new(fmt, sink))
            }
            other => {
                return Err(SinkReason::core_conf().to_err().with_detail(format!(
                    "unsupported file protocol: '{other}'; expected 'txt' or 'arrow'"
                )));
            }
        };

        Ok(SinkHandle::new(sink))
    }
}

impl SinkDefProvider for FileFactory {
    fn sink_def(&self) -> ConnectorDef {
        crate::builtin::sink_def("file_json_sink")
            .expect("builtin sink def missing: file_json_sink")
    }

    fn sink_defs(&self) -> Vec<ConnectorDef> {
        crate::builtin::sink_defs_by_kind(self.kind())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn mk_spec(params: &[(&str, serde_json::Value)]) -> SinkSpec {
        let mut map = wp_connector_api::ParamMap::new();
        for (k, v) in params {
            map.insert((*k).to_string(), v.clone());
        }
        SinkSpec {
            group: "g".into(),
            name: "file_demo".into(),
            kind: "file".into(),
            connector_id: "file_json_sink".into(),
            params: map,
            filter: None,
        }
    }

    #[test]
    fn txt_protocol_accepts_rolling_params() {
        let factory = FileFactory;
        let spec = mk_spec(&[
            ("base", json!("./out")),
            ("file", json!("a.json")),
            ("max_size", json!("10MB")),
            ("max_rolls", json!(5)),
            ("roll_compress", json!("zip")),
        ]);
        assert!(factory.validate_spec(&spec).is_ok());
    }

    #[test]
    fn arrow_protocol_rejects_rolling_params() {
        let factory = FileFactory;
        let base: &[(&str, serde_json::Value)] =
            &[("protocol", json!("arrow")), ("file", json!("out.arrow"))];
        assert!(factory.validate_spec(&mk_spec(base)).is_ok());

        for extra in [
            ("max_size", json!(1024)),
            ("max_rolls", json!(3)),
            ("roll_compress", json!("gzip")),
        ] {
            let mut params = base.to_vec();
            params.push(extra.clone());
            assert!(
                factory.validate_spec(&mk_spec(&params)).is_err(),
                "arrow sink should reject {extra:?}"
            );
        }
    }

    #[test]
    fn txt_protocol_rejects_roll_compress_without_max_size() {
        let factory = FileFactory;
        let spec = mk_spec(&[("file", json!("a.json")), ("roll_compress", json!("gzip"))]);
        assert!(factory.validate_spec(&spec).is_err());
    }
}
