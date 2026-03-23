//! Syslog source factory implementation
//!
//! This module provides the factory for creating syslog data sources
//! with support for both UDP and TCP protocols.

use super::config::{Protocol, SyslogSourceSpec};
use super::tcp_source::TcpSyslogSource;
use super::udp_source::UdpSyslogSource;
use crate::sources::tcp::{FramingMode, TcpAcceptor, TcpSource, tcp_reader_batch_channel_cap};
use orion_conf::{ErrorWith, UvsFrom};
use orion_error::ErrorOweBase;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use wp_conf_base::ConfParser;
use wp_connector_api::{
    AcceptorHandle, ConnectorDef, SourceBuildCtx, SourceDefProvider, SourceFactory, SourceHandle,
    SourceMeta, SourceReason, SourceResult, SourceSvcIns, Tags,
};

/// Syslog source factory that creates both UDP and TCP syslog sources
pub struct SyslogSourceFactory {}

impl SyslogSourceFactory {
    /// Create a new syslog source factory
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for SyslogSourceFactory {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl SourceFactory for SyslogSourceFactory {
    fn kind(&self) -> &'static str {
        "syslog"
    }

    async fn build(
        &self,
        spec: &wp_connector_api::SourceSpec,
        _ctx: &SourceBuildCtx,
    ) -> SourceResult<SourceSvcIns> {
        let fut = async {
            let config = SyslogSourceSpec::from_params(&spec.params)?;
            let mut base_tags = Tags::from_parse(&spec.tags);
            base_tags.set("access_source", "syslog".to_string());
            base_tags.set("syslog_protocol", format!("{:?}", config.protocol));

            let meta_builder = |name: &str, tagset: &Tags| -> SourceMeta {
                let mut meta = SourceMeta::new(name.to_string(), spec.kind.clone());
                for (k, v) in tagset.iter() {
                    meta.tags.set(k, v);
                }
                meta
            };

            let svc = match config.protocol {
                Protocol::Udp => {
                    info_ctrl!(
                        "syslog UDP factory build: strip_header={}, attach_meta_tags={}, fast_strip={}, udp_recv_buffer={}",
                        config.strip_header,
                        config.attach_meta_tags,
                        config.fast_strip,
                        config.udp_recv_buffer
                    );
                    let tagset = base_tags.clone();
                    let source = UdpSyslogSource::new(
                        spec.name.clone(),
                        config.address(),
                        tagset.clone(),
                        config.strip_header,
                        config.attach_meta_tags,
                        config.fast_strip,
                        config.udp_recv_buffer,
                    )
                    .await?;
                    let meta = meta_builder(&spec.name, &tagset);
                    SourceSvcIns::new()
                        .with_sources(vec![SourceHandle::new(Box::new(source), meta)])
                }
                Protocol::Tcp => {
                    let tags = base_tags.clone();
                    let pool = Arc::new(Mutex::new(HashSet::<u64>::new()));
                    let (reg_tx, reg_rx) = mpsc::channel(tcp_reader_batch_channel_cap());
                    let framing = FramingMode::Auto;

                    let inner = TcpSource::new(
                        spec.name.clone(),
                        tags.clone(),
                        config.address(),
                        config.tcp_recv_bytes,
                        framing,
                        pool.clone(),
                        reg_rx,
                    )?;
                    let acceptor = TcpAcceptor::new(
                        spec.name.clone(),
                        config.address(),
                        1000,
                        pool,
                        vec![reg_tx],
                    );

                    let meta = meta_builder(&spec.name, &tags);
                    let syslog = TcpSyslogSource::new(
                        spec.name.clone(),
                        tags,
                        config.strip_header,
                        config.attach_meta_tags,
                        config.fast_strip,
                        inner,
                    )
                    .await?;

                    SourceSvcIns::new()
                        .with_sources(vec![SourceHandle::new(Box::new(syslog), meta)])
                        .with_acceptor(AcceptorHandle::new(spec.name.clone(), Box::new(acceptor)))
                }
            };

            Ok(svc)
        };

        let fut: anyhow::Result<SourceSvcIns> = fut.await;
        fut.owe(SourceReason::from_conf())
            .with(spec.name.as_str())
            .want("build syslog source service")
    }
}

impl SourceDefProvider for SyslogSourceFactory {
    fn source_def(&self) -> ConnectorDef {
        crate::builtin::source_def("syslog_src").expect("builtin source def missing: syslog_src")
    }
}
