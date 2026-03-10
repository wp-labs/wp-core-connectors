use super::blackhole::BlackHoleSink;
use async_trait::async_trait;
use wp_connector_api::{ConnectorDef, ParamMap, SinkDefProvider, SinkFactory, SinkResult};

pub struct BlackHoleFactory;

struct BlackHoleSpec {
    sleep_ms: u64,
}

impl BlackHoleSpec {
    fn from_params(params: &ParamMap) -> anyhow::Result<Self> {
        if let Some(value) = params.get("sleep_ms")
            && value.as_u64().is_none()
        {
            anyhow::bail!("blackhole.sleep_ms must be an unsigned integer");
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
        BlackHoleSpec::from_params(&spec.params).map_err(|e| {
            wp_connector_api::SinkError::from(wp_connector_api::SinkReason::sink(e.to_string()))
        })?;
        Ok(())
    }
    async fn build(
        &self,
        spec: &wp_connector_api::SinkSpec,
        _ctx: &wp_connector_api::SinkBuildCtx,
    ) -> SinkResult<wp_connector_api::SinkHandle> {
        let resolved = BlackHoleSpec::from_params(&spec.params).map_err(|e| {
            wp_connector_api::SinkError::from(wp_connector_api::SinkReason::sink(e.to_string()))
        })?;
        Ok(wp_connector_api::SinkHandle::new(Box::new(
            BlackHoleSink::new(resolved.sleep_ms),
        )))
    }
}

impl SinkDefProvider for BlackHoleFactory {
    fn sink_def(&self) -> ConnectorDef {
        crate::builtin::sink_def("blackhole_sink")
            .expect("builtin sink def missing: blackhole_sink")
    }
}
