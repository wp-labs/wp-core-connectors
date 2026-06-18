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
                    .source_err(SinkReason::Sink, "file sink open")?;
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
