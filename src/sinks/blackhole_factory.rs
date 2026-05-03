use super::blackhole::BlackHoleSink;
use async_trait::async_trait;
use orion_error::conversion::ToStructError;
use wp_connector_api::{
    ConnectorDef, ParamMap, SinkBuildCtx, SinkDefProvider, SinkFactory, SinkHandle, SinkReason,
    SinkResult,
};

pub struct BlackHoleFactory;

struct BlackHoleSpec {
    sleep_ms: u64,
}

impl BlackHoleSpec {
    fn from_params(params: &ParamMap) -> SinkResult<Self> {
        if let Some(value) = params.get("sleep_ms")
            && value.as_u64().is_none()
        {
            return Err(SinkReason::core_conf()
                .to_err()
                .with_detail("blackhole.sleep_ms must be an unsigned integer"));
        }
        let sleep_ms = params.get("sleep_ms").and_then(|v| v.as_u64()).unwrap_or(0);
        Ok(Self { sleep_ms })
    }
}

#[async_trait]
impl SinkFactory for BlackHoleFactory {
    fn kind(&self) -> &'static str {
        "blackhole"
    }
    fn validate_spec(&self, spec: &wp_connector_api::SinkSpec) -> SinkResult<()> {
        BlackHoleSpec::from_params(&spec.params)?;
        Ok(())
    }
    async fn build(
        &self,
        spec: &wp_connector_api::SinkSpec,
        _ctx: &SinkBuildCtx,
    ) -> SinkResult<SinkHandle> {
        let resolved = BlackHoleSpec::from_params(&spec.params)?;
        Ok(SinkHandle::new(Box::new(BlackHoleSink::new(
            resolved.sleep_ms,
        ))))
    }
}

impl SinkDefProvider for BlackHoleFactory {
    fn sink_def(&self) -> ConnectorDef {
        crate::builtin::sink_def("blackhole_sink")
            .expect("builtin sink def missing: blackhole_sink")
    }
}
