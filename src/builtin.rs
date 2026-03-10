use serde_json::json;
use wp_connector_api::{ConnectorDef, ConnectorScope, ParamMap};

pub fn builtin_sink_defs() -> Vec<ConnectorDef> {
    let mut defs = Vec::new();

    // arrow-ipc
    {
        let mut params = ParamMap::new();
        params.insert("target".into(), json!("tcp://127.0.0.1:9800"));
        params.insert("tag".into(), json!("default"));
        params.insert("fields".into(), json!([]));
        defs.push(ConnectorDef {
            id: "arrow_ipc_sink".into(),
            kind: "arrow-ipc".into(),
            scope: ConnectorScope::Sink,
            allow_override: vec!["target".into(), "tag".into(), "fields".into()],
            default_params: params,
            origin: Some("builtin:arrow_ipc_sink".into()),
        });
    }

    // arrow-tcp alias
    {
        let mut params = ParamMap::new();
        params.insert("target".into(), json!("tcp://127.0.0.1:9800"));
        params.insert("tag".into(), json!("default"));
        params.insert("fields".into(), json!([]));
        defs.push(ConnectorDef {
            id: "arrow_tcp_sink".into(),
            kind: "arrow-ipc".into(),
            scope: ConnectorScope::Sink,
            allow_override: vec!["target".into(), "tag".into(), "fields".into()],
            default_params: params,
            origin: Some("builtin:arrow_tcp_sink".into()),
        });
    }

    // arrow-file
    {
        let mut params = ParamMap::new();
        params.insert("base".into(), json!("./data/out_dat"));
        params.insert("file".into(), json!("default.arrow"));
        params.insert("tag".into(), json!("default"));
        params.insert("fields".into(), json!([]));
        params.insert("sync".into(), json!(false));
        defs.push(ConnectorDef {
            id: "arrow_file_sink".into(),
            kind: "arrow-file".into(),
            scope: ConnectorScope::Sink,
            allow_override: vec![
                "base".into(),
                "file".into(),
                "tag".into(),
                "fields".into(),
                "sync".into(),
            ],
            default_params: params,
            origin: Some("builtin:arrow_file_sink".into()),
        });
    }

    // blackhole
    {
        let mut params = ParamMap::new();
        params.insert("sleep_ms".into(), json!(0));
        defs.push(ConnectorDef {
            id: "blackhole_sink".into(),
            kind: "blackhole".into(),
            scope: ConnectorScope::Sink,
            allow_override: vec!["sleep_ms".into()],
            default_params: params,
            origin: Some("builtin:blackhole".into()),
        });
    }

    // file_json
    {
        let mut params = ParamMap::new();
        params.insert("fmt".into(), json!("json"));
        params.insert("base".into(), json!("./data/out_dat"));
        params.insert("file".into(), json!("default.json"));
        params.insert("sync".into(), json!(false));
        defs.push(ConnectorDef {
            id: "file_json_sink".into(),
            kind: "file".into(),
            scope: ConnectorScope::Sink,
            allow_override: vec!["base".into(), "file".into(), "sync".into()],
            default_params: params,
            origin: Some("builtin:file".into()),
        });
    }

    // file_proto_text
    {
        let mut params = ParamMap::new();
        params.insert("fmt".into(), json!("proto-text"));
        params.insert("base".into(), json!("./data/out_dat"));
        params.insert("file".into(), json!("default.pbtxt"));
        params.insert("sync".into(), json!(false));
        defs.push(ConnectorDef {
            id: "file_proto_text_sink".into(),
            kind: "file".into(),
            scope: ConnectorScope::Sink,
            allow_override: vec!["base".into(), "file".into(), "sync".into()],
            default_params: params,
            origin: Some("builtin:file".into()),
        });
    }

    // file_proto alias
    {
        let mut params = ParamMap::new();
        params.insert("fmt".into(), json!("proto-text"));
        params.insert("base".into(), json!("./data/out_dat"));
        params.insert("file".into(), json!("default.dat"));
        params.insert("sync".into(), json!(false));
        defs.push(ConnectorDef {
            id: "file_proto_sink".into(),
            kind: "file".into(),
            scope: ConnectorScope::Sink,
            allow_override: vec!["base".into(), "file".into(), "sync".into()],
            default_params: params,
            origin: Some("builtin:file".into()),
        });
    }

    // file_kv
    {
        let mut params = ParamMap::new();
        params.insert("fmt".into(), json!("kv"));
        params.insert("base".into(), json!("./data/out_dat"));
        params.insert("file".into(), json!("default.kv"));
        params.insert("sync".into(), json!(false));
        defs.push(ConnectorDef {
            id: "file_kv_sink".into(),
            kind: "file".into(),
            scope: ConnectorScope::Sink,
            allow_override: vec!["base".into(), "file".into(), "sync".into()],
            default_params: params,
            origin: Some("builtin:file".into()),
        });
    }

    // syslog
    {
        let mut params = ParamMap::new();
        params.insert("addr".into(), json!("127.0.0.1"));
        params.insert("port".into(), json!(1514));
        params.insert("protocol".into(), json!("udp"));
        params.insert("strip_header".into(), json!(true));
        params.insert("attach_meta_tags".into(), json!(true));
        params.insert("tcp_recv_bytes".into(), json!(256000));
        defs.push(ConnectorDef {
            id: "syslog_sink".into(),
            kind: "syslog".into(),
            scope: ConnectorScope::Sink,
            allow_override: vec![
                "addr".into(),
                "port".into(),
                "protocol".into(),
                "app_name".into(),
            ],
            default_params: params,
            origin: Some("builtin:syslog_sink".into()),
        });
    }

    // tcp
    {
        let mut params = ParamMap::new();
        params.insert("addr".into(), json!("127.0.0.1"));
        params.insert("port".into(), json!(9000));
        params.insert("framing".into(), json!("line"));
        defs.push(ConnectorDef {
            id: "tcp_sink".into(),
            kind: "tcp".into(),
            scope: ConnectorScope::Sink,
            allow_override: vec!["addr".into(), "port".into(), "framing".into()],
            default_params: params,
            origin: Some("builtin:tcp_sink".into()),
        });
    }

    // test_rescue
    {
        let mut params = ParamMap::new();
        params.insert("fmt".into(), json!("kv"));
        params.insert("base".into(), json!("./data/out_dat"));
        params.insert("file".into(), json!("default.kv"));
        defs.push(ConnectorDef {
            id: "file_rescue_sink".into(),
            kind: "test_rescue".into(),
            scope: ConnectorScope::Sink,
            allow_override: vec!["base".into(), "file".into()],
            default_params: params,
            origin: Some("builtin:test_rescue".into()),
        });
    }

    defs
}

pub fn builtin_source_defs() -> Vec<ConnectorDef> {
    let mut defs = Vec::new();

    // file source
    {
        let mut params = ParamMap::new();
        params.insert("base".into(), json!("./data/in_dat"));
        params.insert("file".into(), json!("gen.dat"));
        params.insert("encode".into(), json!("text"));
        defs.push(ConnectorDef {
            id: "file_src".into(),
            kind: "file".into(),
            scope: ConnectorScope::Source,
            allow_override: vec!["base".into(), "file".into(), "encode".into()],
            default_params: params,
            origin: Some("builtin:file_source".into()),
        });
    }

    // syslog source
    {
        let mut params = ParamMap::new();
        params.insert("addr".into(), json!("0.0.0.0"));
        params.insert("port".into(), json!(514));
        params.insert("protocol".into(), json!("udp"));
        params.insert("tcp_recv_bytes".into(), json!(10_485_760));
        params.insert("udp_recv_buffer".into(), json!(8_388_608));
        params.insert("header_mode".into(), json!("skip"));
        params.insert("fast_strip".into(), json!(false));
        defs.push(ConnectorDef {
            id: "syslog_src".into(),
            kind: "syslog".into(),
            scope: ConnectorScope::Source,
            allow_override: vec![
                "addr".into(),
                "port".into(),
                "protocol".into(),
                "tcp_recv_bytes".into(),
                "udp_recv_buffer".into(),
                "header_mode".into(),
                "fast_strip".into(),
            ],
            default_params: params,
            origin: Some("builtin:syslog_source".into()),
        });
    }

    // tcp source
    {
        let mut params = ParamMap::new();
        params.insert("addr".into(), json!("0.0.0.0"));
        params.insert("port".into(), json!(9000));
        params.insert("framing".into(), json!("auto"));
        params.insert("tcp_recv_bytes".into(), json!(256_000));
        params.insert("instances".into(), json!(1));
        defs.push(ConnectorDef {
            id: "tcp_src".into(),
            kind: "tcp".into(),
            scope: ConnectorScope::Source,
            allow_override: vec![
                "addr".into(),
                "port".into(),
                "framing".into(),
                "tcp_recv_bytes".into(),
                "instances".into(),
            ],
            default_params: params,
            origin: Some("builtin:tcp_source".into()),
        });
    }

    defs
}

pub fn sink_def(id: &str) -> Option<ConnectorDef> {
    builtin_sink_defs().into_iter().find(|d| d.id == id)
}

pub fn source_def(id: &str) -> Option<ConnectorDef> {
    builtin_source_defs().into_iter().find(|d| d.id == id)
}

pub fn sink_defs_by_kind(kind: &str) -> Vec<ConnectorDef> {
    builtin_sink_defs()
        .into_iter()
        .filter(|d| d.kind.as_str() == kind)
        .collect()
}
