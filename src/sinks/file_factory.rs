use async_trait::async_trait;
use orion_conf::ErrorOwe;
use wp_connector_api::{
    ConnectorDef, SinkBuildCtx, SinkDefProvider, SinkFactory, SinkHandle, SinkResult, SinkSpec,
};

use super::file::{AsyncFileSink, FileSinkSpec, FormattedFileSink};

pub struct FileFactory;

#[async_trait]
impl SinkFactory for FileFactory {
    fn kind(&self) -> &'static str {
        "file"
    }

    fn validate_spec(&self, spec: &SinkSpec) -> SinkResult<()> {
        FileSinkSpec::from_resolved("file", spec).owe_conf()?;
        Ok(())
    }

    async fn build(&self, spec: &SinkSpec, ctx: &SinkBuildCtx) -> SinkResult<SinkHandle> {
        let resolved = FileSinkSpec::from_resolved("file", spec).owe_conf()?;
        let path = resolved.resolve_path(ctx);
        let fmt = resolved.text_fmt();
        let sync = resolved.sync();
        let sink = AsyncFileSink::with_sync(&path, sync).await.owe_res()?;
        Ok(SinkHandle::new(Box::new(FormattedFileSink::new(fmt, sink))))
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
