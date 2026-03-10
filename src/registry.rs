//! Runtime registry for connector source/sink factories.
//! This crate-level registry is shared by engine and tooling code.

use once_cell::sync::OnceCell;
use std::collections::HashMap;
use std::panic::Location;
use std::sync::{Arc, RwLock};
use wp_connector_api::{ConnectorDef, SinkFactory, SourceFactory};

type SinkRec = (Arc<dyn SinkFactory>, &'static Location<'static>);
type SrcRec = (Arc<dyn SourceFactory>, &'static Location<'static>);
type SinkReg = RwLock<HashMap<String, SinkRec>>;
type SrcReg = RwLock<HashMap<String, SrcRec>>;

static SINKS: OnceCell<SinkReg> = OnceCell::new();
static SRCS: OnceCell<SrcReg> = OnceCell::new();

fn sink_reg() -> &'static SinkReg {
    SINKS.get_or_init(|| RwLock::new(HashMap::new()))
}

fn src_reg() -> &'static SrcReg {
    SRCS.get_or_init(|| RwLock::new(HashMap::new()))
}

// ---------- Sink ----------
#[track_caller]
pub fn register_sink_factory<F>(f: F)
where
    F: SinkFactory,
{
    let base: Arc<F> = Arc::new(f);
    let ex_arc: Arc<dyn SinkFactory> = base.clone();
    let kind = ex_arc.kind().to_string();
    if let Ok(mut w) = sink_reg().write() {
        if let Some((_, prev_loc)) = w.get(&kind) {
            let new_loc = Location::caller();
            log::warn!(
                "duplicate sink factory registration ignored: kind='{}' existing={}:{} new={}:{}",
                kind,
                prev_loc.file(),
                prev_loc.line(),
                new_loc.file(),
                new_loc.line()
            );
            return;
        }
        w.insert(kind, (ex_arc.clone(), Location::caller()));
    }
}

pub fn get_sink_factory(kind: &str) -> Option<Arc<dyn SinkFactory>> {
    sink_reg()
        .read()
        .ok()
        .and_then(|r| r.get(kind).map(|(f, _)| f.clone() as Arc<dyn SinkFactory>))
}

pub fn list_sink_kinds() -> Vec<String> {
    sink_reg()
        .read()
        .ok()
        .map(|r| r.keys().cloned().collect())
        .unwrap_or_default()
}

/// Collect connector definitions from all registered sink factories.
pub fn registered_sink_defs() -> Vec<ConnectorDef> {
    sink_reg()
        .read()
        .map(|reg| reg.values().flat_map(|(f, _)| f.sink_defs()).collect())
        .unwrap_or_default()
}

// ---------- Source ----------
#[track_caller]
pub fn register_source_factory<F>(f: F)
where
    F: SourceFactory,
{
    let base: Arc<F> = Arc::new(f);
    let ex_arc: Arc<dyn SourceFactory> = base.clone();
    let kind = ex_arc.kind().to_string();
    if let Ok(mut w) = src_reg().write() {
        if let Some((_, prev_loc)) = w.get(&kind) {
            let new_loc = Location::caller();
            log::warn!(
                "duplicate source factory registration ignored: kind='{}' existing={}:{} new={}:{}",
                kind,
                prev_loc.file(),
                prev_loc.line(),
                new_loc.file(),
                new_loc.line()
            );
            return;
        }
        w.insert(kind, (ex_arc.clone(), Location::caller()));
    }
}

pub fn get_source_factory(kind: &str) -> Option<Arc<dyn SourceFactory>> {
    src_reg().read().ok().and_then(|r| {
        r.get(kind)
            .map(|(f, _)| f.clone() as Arc<dyn SourceFactory>)
    })
}

pub fn list_source_kinds() -> Vec<String> {
    src_reg()
        .read()
        .ok()
        .map(|r| r.keys().cloned().collect())
        .unwrap_or_default()
}

/// Collect connector definitions from all registered source factories.
pub fn registered_source_defs() -> Vec<ConnectorDef> {
    src_reg()
        .read()
        .map(|reg| reg.values().flat_map(|(f, _)| f.source_defs()).collect())
        .unwrap_or_default()
}

pub fn sink_diagnostics() -> Vec<(String, &'static Location<'static>)> {
    sink_reg()
        .read()
        .ok()
        .map(|r| r.iter().map(|(k, (_f, loc))| (k.clone(), *loc)).collect())
        .unwrap_or_default()
}

pub fn source_diagnostics() -> Vec<(String, &'static Location<'static>)> {
    src_reg()
        .read()
        .ok()
        .map(|r| r.iter().map(|(k, (_f, loc))| (k.clone(), *loc)).collect())
        .unwrap_or_default()
}

#[cfg(any(test, feature = "dev-tools"))]
pub fn clear_all() {
    if let Ok(mut w) = sink_reg().write() {
        w.clear();
    }
    if let Ok(mut w) = src_reg().write() {
        w.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Mutex;
    use wp_connector_api::{
        AsyncCtrl, AsyncRawDataSink, AsyncRecordSink, ConnectorDef, ConnectorScope, SinkBuildCtx,
        SinkDefProvider, SinkHandle, SinkResult, SourceBuildCtx, SourceDefProvider, SourceResult,
        SourceSvcIns,
    };
    use wp_model_core::model::DataRecord;

    static REGISTRY_TEST_LOCK: Mutex<()> = Mutex::new(());

    struct DummySink;

    #[async_trait]
    impl AsyncCtrl for DummySink {
        async fn stop(&mut self) -> SinkResult<()> {
            Ok(())
        }

        async fn reconnect(&mut self) -> SinkResult<()> {
            Ok(())
        }
    }

    #[async_trait]
    impl AsyncRecordSink for DummySink {
        async fn sink_record(&mut self, _data: &DataRecord) -> SinkResult<()> {
            Ok(())
        }

        async fn sink_records(&mut self, _data: Vec<std::sync::Arc<DataRecord>>) -> SinkResult<()> {
            Ok(())
        }
    }

    #[async_trait]
    impl AsyncRawDataSink for DummySink {
        async fn sink_str(&mut self, _data: &str) -> SinkResult<()> {
            Ok(())
        }

        async fn sink_bytes(&mut self, _data: &[u8]) -> SinkResult<()> {
            Ok(())
        }

        async fn sink_str_batch(&mut self, _data: Vec<&str>) -> SinkResult<()> {
            Ok(())
        }

        async fn sink_bytes_batch(&mut self, _data: Vec<&[u8]>) -> SinkResult<()> {
            Ok(())
        }
    }

    struct DummySinkFactoryA;
    struct DummySinkFactoryB;

    impl SinkDefProvider for DummySinkFactoryA {
        fn sink_def(&self) -> ConnectorDef {
            ConnectorDef {
                id: "dummy_sink_a".into(),
                kind: "dummy".into(),
                scope: ConnectorScope::Sink,
                allow_override: Vec::new(),
                default_params: Default::default(),
                origin: None,
            }
        }
    }

    impl SinkDefProvider for DummySinkFactoryB {
        fn sink_def(&self) -> ConnectorDef {
            ConnectorDef {
                id: "dummy_sink_b".into(),
                kind: "dummy".into(),
                scope: ConnectorScope::Sink,
                allow_override: Vec::new(),
                default_params: Default::default(),
                origin: None,
            }
        }
    }

    #[async_trait]
    impl SinkFactory for DummySinkFactoryA {
        fn kind(&self) -> &'static str {
            "dummy"
        }

        async fn build(
            &self,
            _spec: &wp_connector_api::SinkSpec,
            _ctx: &SinkBuildCtx,
        ) -> SinkResult<SinkHandle> {
            Ok(SinkHandle::new(Box::new(DummySink)))
        }
    }

    #[async_trait]
    impl SinkFactory for DummySinkFactoryB {
        fn kind(&self) -> &'static str {
            "dummy"
        }

        async fn build(
            &self,
            _spec: &wp_connector_api::SinkSpec,
            _ctx: &SinkBuildCtx,
        ) -> SinkResult<SinkHandle> {
            Ok(SinkHandle::new(Box::new(DummySink)))
        }
    }

    struct DummySourceFactoryA;
    struct DummySourceFactoryB;

    impl SourceDefProvider for DummySourceFactoryA {
        fn source_def(&self) -> ConnectorDef {
            ConnectorDef {
                id: "dummy_source_a".into(),
                kind: "dummy-src".into(),
                scope: ConnectorScope::Source,
                allow_override: Vec::new(),
                default_params: Default::default(),
                origin: None,
            }
        }
    }

    impl SourceDefProvider for DummySourceFactoryB {
        fn source_def(&self) -> ConnectorDef {
            ConnectorDef {
                id: "dummy_source_b".into(),
                kind: "dummy-src".into(),
                scope: ConnectorScope::Source,
                allow_override: Vec::new(),
                default_params: Default::default(),
                origin: None,
            }
        }
    }

    #[async_trait]
    impl SourceFactory for DummySourceFactoryA {
        fn kind(&self) -> &'static str {
            "dummy-src"
        }

        async fn build(
            &self,
            _spec: &wp_connector_api::SourceSpec,
            _ctx: &SourceBuildCtx,
        ) -> SourceResult<SourceSvcIns> {
            panic!("unused in registry test")
        }
    }

    #[async_trait]
    impl SourceFactory for DummySourceFactoryB {
        fn kind(&self) -> &'static str {
            "dummy-src"
        }

        async fn build(
            &self,
            _spec: &wp_connector_api::SourceSpec,
            _ctx: &SourceBuildCtx,
        ) -> SourceResult<SourceSvcIns> {
            panic!("unused in registry test")
        }
    }

    #[test]
    fn duplicate_sink_registration_keeps_first_factory() {
        let _guard = REGISTRY_TEST_LOCK.lock().unwrap();
        clear_all();
        register_sink_factory(DummySinkFactoryA);
        register_sink_factory(DummySinkFactoryB);
        let defs = registered_sink_defs();
        assert_eq!(defs.len(), 1);
        assert_eq!(defs[0].id, "dummy_sink_a");
        assert_eq!(list_sink_kinds(), vec!["dummy".to_string()]);
    }

    #[test]
    fn duplicate_source_registration_keeps_first_factory() {
        let _guard = REGISTRY_TEST_LOCK.lock().unwrap();
        clear_all();
        register_source_factory(DummySourceFactoryA);
        register_source_factory(DummySourceFactoryB);
        let defs = registered_source_defs();
        assert_eq!(defs.len(), 1);
        assert_eq!(defs[0].id, "dummy_source_a");
        assert_eq!(list_source_kinds(), vec!["dummy-src".to_string()]);
    }
}
