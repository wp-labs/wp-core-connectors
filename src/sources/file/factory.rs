use super::source::{
    BinaryFileSource, FileEncoding, FileSource, MultiFileSource, compute_file_ranges,
};
use crate::sources::batch::arrow::WireFormat;
use async_trait::async_trait;
use glob::glob;
use orion_conf::{ErrorWith, ToStructError};
use std::path::Path;
use wp_connector_api::{
    ConnectorDef, SourceBuildCtx, SourceDefProvider, SourceFactory, SourceHandle, SourceMeta,
    SourceReason, SourceResult, SourceSpec as ResolvedSourceSpec, SourceSvcIns, Tags,
};

const FILE_SOURCE_MAX_INSTANCES: usize = 32;

#[derive(Clone, Debug)]
struct FileSourceSpec {
    base: String,
    file: String,
    encoding: FileEncoding,
    instances: usize,
    format: WireFormat,
}

impl FileSourceSpec {
    fn from_resolved(resolved: &ResolvedSourceSpec) -> anyhow::Result<Self> {
        if resolved.params.contains_key("path") {
            anyhow::bail!(
                "'path' is not supported for file source; use 'file' (with optional wildcard) and optional 'base'"
            );
        }
        let base = resolved
            .params
            .get("base")
            .and_then(|v| v.as_str())
            .unwrap_or("./data/in_dat")
            .to_string();
        if has_glob_pattern(&base) {
            anyhow::bail!("'base' does not support wildcard patterns for file source");
        }
        let file = resolved
            .params
            .get("file")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing required 'file' for file source"))?
            .to_string();
        let encoding = match resolved.params.get("encode").and_then(|v| v.as_str()) {
            None | Some("text") => FileEncoding::Text,
            Some("base64") => FileEncoding::Base64,
            Some("hex") => FileEncoding::Hex,
            Some(v) => {
                anyhow::bail!(
                    "Invalid encode value for file source '{}': {}",
                    resolved.name,
                    v
                );
            }
        };
        // Parse data_format (defaulting to NDJSON). Reject unknown values
        // explicitly so a typo doesn't silently degrade to NDJSON.
        let format = match resolved.params.get("data_format").and_then(|v| v.as_str()) {
            None | Some("ndjson") => WireFormat::Ndjson,
            Some("arrow_ipc") => WireFormat::ArrowStream,
            Some("arrow_framed") => WireFormat::ArrowFramed,
            Some(raw) => {
                anyhow::bail!(
                    "file.data_format must be one of: ndjson, arrow_ipc, arrow_framed (got '{raw}')"
                )
            }
        };

        let instances = resolved
            .params
            .get("instances")
            .and_then(|v| v.as_i64())
            .map(|n| n.clamp(1, FILE_SOURCE_MAX_INSTANCES as i64) as usize)
            .unwrap_or(1);
        Ok(Self {
            base,
            file,
            encoding,
            instances,
            format,
        })
    }

    fn resolved_path(&self) -> String {
        Path::new(&self.base).join(&self.file).display().to_string()
    }

    fn expand_paths(&self) -> anyhow::Result<Vec<String>> {
        if !has_glob_pattern(&self.file) {
            return Ok(vec![self.resolved_path()]);
        }

        let pattern = Path::new(&self.base).join(&self.file).display().to_string();
        let mut matches = Vec::new();
        for entry in glob(&pattern)? {
            let path = entry?;
            if path.is_file() {
                matches.push(path);
            }
        }
        matches.sort_by(|left, right| compare_paths_by_file_name(left, right));
        matches.dedup();
        if matches.is_empty() {
            anyhow::bail!("file source wildcard matched no files: {}", pattern);
        }
        Ok(matches
            .into_iter()
            .map(|path| path.display().to_string())
            .collect())
    }
}

pub struct FileSourceFactory;

#[async_trait]
impl SourceFactory for FileSourceFactory {
    fn kind(&self) -> &'static str {
        "file"
    }

    fn validate_spec(&self, resolved: &ResolvedSourceSpec) -> SourceResult<()> {
        let res: anyhow::Result<()> = (|| {
            FileSourceSpec::from_resolved(resolved)?;
            Ok(())
        })();
        res.map_err(|e| {
            SourceReason::core_conf()
                .to_err()
                .with_detail(e.to_string())
        })
        .with_context(resolved.name.as_str())
        .doing("validate file source spec")
    }

    async fn build(
        &self,
        resolved: &ResolvedSourceSpec,
        _ctx: &SourceBuildCtx,
    ) -> SourceResult<SourceSvcIns> {
        let fut = async {
            let spec = FileSourceSpec::from_resolved(resolved)?;
            let tagset = {
                let mut tags = Tags::new();
                for item in &resolved.tags {
                    if let Some((k, v)) = item.split_once("=").or_else(|| item.split_once(":")) {
                        tags.set(k, v);
                    }
                }
                tags
            };
            let matched_paths = spec.expand_paths()?;

            let mut meta = SourceMeta::new(resolved.name.clone(), resolved.kind.clone());
            for (k, v) in tagset.iter() {
                meta.tags.set(k, v);
            }

            // Binary (Arrow) formats cannot be line-split, so each matched file
            // becomes one whole-file `BinaryFileSource`. Inter-file parallelism
            // is achieved via multiple source handles; intra-file byte-range
            // sharding (`instances`) is intentionally not applied.
            if spec.format != WireFormat::Ndjson {
                let multi = matched_paths.len() > 1;
                let mut handles = Vec::with_capacity(matched_paths.len());
                for (idx, path) in matched_paths.into_iter().enumerate() {
                    let key = if !multi {
                        resolved.name.clone()
                    } else {
                        format!("{}-{}", resolved.name, idx + 1)
                    };
                    let source = BinaryFileSource::new(key.clone(), &path, tagset.clone())
                        .await
                        .map_err(|e| anyhow::anyhow!("{}", e))?;
                    let source_meta = if !multi {
                        meta.clone()
                    } else {
                        let mut m = SourceMeta::new(key.clone(), resolved.kind.clone());
                        for (k, v) in tagset.iter() {
                            m.tags.set(k, v);
                        }
                        m
                    };
                    handles.push(SourceHandle::new(Box::new(source), source_meta));
                }
                return Ok(SourceSvcIns::new().with_sources(handles));
            }

            if matched_paths.len() > 1 {
                let source = MultiFileSource::new(
                    resolved.name.clone(),
                    matched_paths,
                    spec.encoding.clone(),
                    tagset,
                    spec.instances,
                );
                return Ok(SourceSvcIns::new()
                    .with_sources(vec![SourceHandle::new(Box::new(source), meta)]));
            }

            let source_path = matched_paths
                .into_iter()
                .next()
                .expect("single file path should exist");
            let ranges = compute_file_ranges(Path::new(&source_path), spec.instances)
                .map_err(|e| anyhow::anyhow!("open {source_path}: {e}"))?;
            let mut handles = Vec::with_capacity(ranges.len());
            let multi = ranges.len() > 1;
            for (idx, (start, end)) in ranges.into_iter().enumerate() {
                let key = if !multi {
                    resolved.name.clone()
                } else {
                    format!("{}-{}", resolved.name, idx + 1)
                };
                let source = FileSource::new(
                    key.clone(),
                    &source_path,
                    spec.encoding.clone(),
                    tagset.clone(),
                    start,
                    end,
                )
                .await
                .map_err(|e| anyhow::anyhow!("{}", e))?;
                let mut source_meta = if !multi {
                    meta.clone()
                } else {
                    SourceMeta::new(key.clone(), resolved.kind.clone())
                };
                if multi {
                    for (k, v) in tagset.iter() {
                        source_meta.tags.set(k, v);
                    }
                }
                handles.push(SourceHandle::new(Box::new(source), source_meta));
            }
            Ok(SourceSvcIns::new().with_sources(handles))
        };

        let fut: anyhow::Result<SourceSvcIns> = fut.await;
        fut.map_err(|e| {
            SourceReason::core_conf()
                .to_err()
                .with_detail(e.to_string())
        })
        .with_context(resolved.name.as_str())
        .doing("build file source service")
    }
}

impl SourceDefProvider for FileSourceFactory {
    fn source_def(&self) -> ConnectorDef {
        crate::builtin::source_def("file_src").expect("builtin source def missing: file_src")
    }
}

fn has_glob_pattern(value: &str) -> bool {
    value.contains('*') || value.contains('?') || value.contains('[')
}

fn compare_paths_by_file_name(left: &Path, right: &Path) -> std::cmp::Ordering {
    let left_name = left
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    let right_name = right
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    left_name
        .cmp(right_name)
        .then_with(|| left.as_os_str().cmp(right.as_os_str()))
}

pub fn register_factory_only() {
    crate::registry::register_source_factory(FileSourceFactory);
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::{NamedTempFile, TempDir};
    use toml::map::Map as TomlMap;
    use wp_connector_api::{SourceBuildCtx, SourceFactory, parammap_from_toml_map};
    use wp_model_core::raw::RawData;

    fn build_spec(file: &str, instances: Option<i64>) -> ResolvedSourceSpec {
        let mut params = TomlMap::new();
        params.insert("base".into(), toml::Value::String("/tmp".into()));
        params.insert("file".into(), toml::Value::String(file.into()));
        if let Some(value) = instances {
            params.insert("instances".into(), toml::Value::Integer(value));
        }
        ResolvedSourceSpec {
            name: "file_test".into(),
            kind: "file".into(),
            connector_id: String::new(),
            params: parammap_from_toml_map(params),
            tags: vec![],
        }
    }

    #[test]
    fn file_spec_instances_defaults_and_clamps() {
        let spec = build_spec("input.log", None);
        let resolved = FileSourceSpec::from_resolved(&spec).expect("default instances");
        assert_eq!(resolved.instances, 1);

        let over = build_spec("input.log", Some((FILE_SOURCE_MAX_INSTANCES + 5) as i64));
        let resolved_over = FileSourceSpec::from_resolved(&over).expect("clamp high");
        assert_eq!(resolved_over.instances, FILE_SOURCE_MAX_INSTANCES);

        let under = build_spec("input.log", Some(0));
        let resolved_under = FileSourceSpec::from_resolved(&under).expect("clamp low");
        assert_eq!(resolved_under.instances, 1);
    }

    #[test]
    fn file_spec_rejects_path_param_and_wildcard_base() {
        let mut params = TomlMap::new();
        params.insert("path".into(), toml::Value::String("/tmp/input.log".into()));
        let path_spec = ResolvedSourceSpec {
            name: "file_test".into(),
            kind: "file".into(),
            connector_id: String::new(),
            params: parammap_from_toml_map(params),
            tags: vec![],
        };
        let path_err =
            FileSourceSpec::from_resolved(&path_spec).expect_err("path should be rejected");
        assert!(path_err.to_string().contains("'path' is not supported"));

        let mut base_params = TomlMap::new();
        base_params.insert("base".into(), toml::Value::String("/tmp/*".into()));
        base_params.insert("file".into(), toml::Value::String("input.log".into()));
        let base_spec = ResolvedSourceSpec {
            name: "file_test".into(),
            kind: "file".into(),
            connector_id: String::new(),
            params: parammap_from_toml_map(base_params),
            tags: vec![],
        };
        let base_err =
            FileSourceSpec::from_resolved(&base_spec).expect_err("wildcard base should fail");
        assert!(
            base_err
                .to_string()
                .contains("'base' does not support wildcard")
        );
    }

    #[test]
    fn compute_file_ranges_aligns_to_line_boundaries() {
        let file = NamedTempFile::new().expect("temp file");
        std::fs::write(file.path(), b"aaaa\nbbbb\nccccc\n").expect("write temp file");

        let ranges = compute_file_ranges(file.path(), 3).expect("compute ranges");
        assert_eq!(ranges, vec![(0, Some(10)), (10, None)]);
    }

    #[tokio::test]
    async fn build_propagates_tags_into_metadata_and_events() {
        let file = NamedTempFile::new().expect("temp file");
        std::fs::write(file.path(), b"hello\nworld\n").expect("write temp file");
        let expected_access = file.path().display().to_string();

        let mut params = TomlMap::new();
        params.insert("base".into(), toml::Value::String("/".into()));
        params.insert(
            "file".into(),
            toml::Value::String(
                file.path()
                    .strip_prefix("/")
                    .expect("strip root")
                    .display()
                    .to_string(),
            ),
        );
        let spec = ResolvedSourceSpec {
            name: "file_tagged".into(),
            kind: "file".into(),
            connector_id: String::new(),
            params: parammap_from_toml_map(params),
            tags: vec!["env:test".into(), "team:platform".into()],
        };
        let ctx = SourceBuildCtx::new(std::path::PathBuf::from("."));
        let fac = FileSourceFactory;
        let mut svc = fac
            .build(&spec, &ctx)
            .await
            .expect("build tagged file source");

        assert_eq!(svc.sources.len(), 1);
        let mut handle = svc.sources.remove(0);
        assert_eq!(handle.metadata.name, "file_tagged");
        assert_eq!(handle.metadata.tags.get("env"), Some("test"));
        assert_eq!(handle.metadata.tags.get("team"), Some("platform"));
        assert_eq!(handle.metadata.tags.len(), 2);

        let (_tx, rx) = async_broadcast::broadcast::<wp_connector_api::ControlEvent>(1);
        handle.source.start(rx).await.expect("start file source");
        let mut batch = handle.source.receive().await.expect("read batch");
        assert!(!batch.is_empty());
        let event = batch.pop().expect("one event");
        assert_eq!(event.tags.get("env"), Some("test"));
        assert_eq!(event.tags.get("team"), Some("platform"));
        assert_eq!(
            event.tags.get("access_source"),
            Some(expected_access.as_str())
        );
        assert_eq!(event.tags.len(), 3);
        handle.source.close().await.expect("close source");
    }

    #[tokio::test]
    async fn wildcard_file_source_reads_files_in_file_name_order() {
        let dir = TempDir::new().expect("temp dir");
        std::fs::write(dir.path().join("b.log"), b"b1\nb2\n").expect("write b");
        std::fs::write(dir.path().join("a.log"), b"a1\na2\na3\na4\n").expect("write a");
        std::fs::write(dir.path().join("c.txt"), b"ignore\n").expect("write c");

        let spec = build_glob_spec(dir.path(), "*.log", Some(2));
        let fac = FileSourceFactory;
        let ctx = SourceBuildCtx::new(std::path::PathBuf::from("."));
        let mut svc = fac.build(&spec, &ctx).await.expect("build wildcard source");
        assert_eq!(
            svc.sources.len(),
            1,
            "wildcard source should stay single-handle"
        );

        let mut handle = svc.sources.remove(0);
        let (_tx, rx) = async_broadcast::broadcast::<wp_connector_api::ControlEvent>(1);
        handle
            .source
            .start(rx)
            .await
            .expect("start wildcard source");

        let mut observed = Vec::new();
        loop {
            match handle.source.receive().await {
                Ok(batch) => {
                    for event in batch {
                        let payload = match event.payload {
                            RawData::Bytes(bytes) => {
                                String::from_utf8(bytes.to_vec()).expect("utf8 payload")
                            }
                            RawData::ArcBytes(bytes) => {
                                String::from_utf8(bytes.as_ref().to_vec()).expect("utf8 payload")
                            }
                            RawData::String(text) => text.to_string(),
                        };
                        observed.push((
                            payload,
                            event
                                .tags
                                .get("access_source")
                                .expect("access_source tag")
                                .to_string(),
                        ));
                    }
                }
                Err(err) if matches!(err.reason(), SourceReason::EOF) => break,
                Err(err) => panic!("unexpected source error: {err}"),
            }
        }

        let a_path = dir.path().join("a.log").display().to_string();
        let b_path = dir.path().join("b.log").display().to_string();
        let mut seen_b = false;
        let mut a_lines = Vec::new();
        let mut b_lines = Vec::new();
        for (payload, path) in observed {
            if path == b_path {
                seen_b = true;
                b_lines.push(payload);
                continue;
            }
            assert_eq!(path, a_path, "unexpected matched path");
            assert!(
                !seen_b,
                "multi-file wildcard source must finish a.log before moving to b.log"
            );
            a_lines.push(payload);
        }
        a_lines.sort();
        b_lines.sort();
        assert_eq!(
            a_lines,
            vec![
                "a1".to_string(),
                "a2".to_string(),
                "a3".to_string(),
                "a4".to_string()
            ]
        );
        assert_eq!(b_lines, vec!["b1".to_string(), "b2".to_string()]);
    }

    fn build_glob_spec(base: &Path, file: &str, instances: Option<i64>) -> ResolvedSourceSpec {
        let mut params = TomlMap::new();
        params.insert(
            "base".into(),
            toml::Value::String(base.display().to_string()),
        );
        params.insert("file".into(), toml::Value::String(file.into()));
        if let Some(value) = instances {
            params.insert("instances".into(), toml::Value::Integer(value));
        }
        ResolvedSourceSpec {
            name: "file_glob".into(),
            kind: "file".into(),
            connector_id: String::new(),
            params: parammap_from_toml_map(params),
            tags: vec![],
        }
    }

    // -- data_format parsing ----------------------------------------------

    fn build_data_format_spec(file: &str, data_format: &str) -> ResolvedSourceSpec {
        let mut params = TomlMap::new();
        params.insert("base".into(), toml::Value::String("/".into()));
        params.insert("file".into(), toml::Value::String(file.into()));
        params.insert(
            "data_format".into(),
            toml::Value::String(data_format.into()),
        );
        ResolvedSourceSpec {
            name: "file_df".into(),
            kind: "file".into(),
            connector_id: String::new(),
            params: parammap_from_toml_map(params),
            tags: vec![],
        }
    }

    #[test]
    fn file_spec_data_format_defaults_to_ndjson() {
        let spec = build_spec("input.log", None);
        let resolved = FileSourceSpec::from_resolved(&spec).expect("parse spec");
        assert_eq!(
            resolved.format,
            crate::sources::batch::arrow::WireFormat::Ndjson
        );
    }

    #[test]
    fn file_spec_data_format_parses_arrow() {
        for (value, expected) in [
            ("ndjson", crate::sources::batch::arrow::WireFormat::Ndjson),
            (
                "arrow_ipc",
                crate::sources::batch::arrow::WireFormat::ArrowStream,
            ),
            (
                "arrow_framed",
                crate::sources::batch::arrow::WireFormat::ArrowFramed,
            ),
        ] {
            let spec = build_data_format_spec("input.log", value);
            let resolved = FileSourceSpec::from_resolved(&spec).expect("parse spec");
            assert_eq!(resolved.format, expected, "mismatch for {value}");
        }
    }

    #[test]
    fn file_spec_data_format_rejects_unknown() {
        let spec = build_data_format_spec("input.log", "arrowipcc");
        let err = FileSourceSpec::from_resolved(&spec).expect_err("unknown data_format");
        assert!(err.to_string().contains("data_format must be one of"));
    }

    #[tokio::test]
    async fn build_arrow_ipc_emits_whole_file_as_bytes() {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::ipc::writer::StreamWriter;
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        // Write an Arrow IPC stream to a temp file.
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, false)]));
        let mut ipc = Vec::new();
        {
            let mut w = StreamWriter::try_new(&mut ipc, &schema).unwrap();
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(StringArray::from(vec!["hello", "world"]))],
            )
            .unwrap();
            w.write(&batch).unwrap();
            w.finish().unwrap();
        }
        let file = NamedTempFile::new().expect("temp file");
        std::fs::write(file.path(), &ipc).expect("write arrow ipc");

        let rel = file
            .path()
            .strip_prefix("/")
            .expect("strip root")
            .display()
            .to_string();
        let spec = build_data_format_spec(&rel, "arrow_ipc");
        let ctx = SourceBuildCtx::new(std::path::PathBuf::from("."));
        let fac = FileSourceFactory;
        let mut svc = fac
            .build(&spec, &ctx)
            .await
            .expect("build arrow file source");

        // Arrow formats produce one whole-file source.
        assert_eq!(svc.sources.len(), 1);
        let mut handle = svc.sources.remove(0);
        let (_tx, rx) = async_broadcast::broadcast::<wp_connector_api::ControlEvent>(1);
        handle.source.start(rx).await.expect("start source");
        let mut batch = handle.source.receive().await.expect("read batch");
        assert_eq!(batch.len(), 1);
        let event = batch.pop().expect("one event");

        // The payload is the whole file as raw bytes (decodable back to a batch).
        let bytes = event.payload.as_bytes();
        assert_eq!(bytes, ipc.as_slice());
        assert!(matches!(event.payload, RawData::Bytes(_)));

        // Second receive is EOF.
        assert!(handle.source.receive().await.is_err());
        handle.source.close().await.expect("close source");
    }

    fn build_arrow_glob_spec(base: &Path, file: &str, data_format: &str) -> ResolvedSourceSpec {
        let mut params = TomlMap::new();
        params.insert(
            "base".into(),
            toml::Value::String(base.display().to_string()),
        );
        params.insert("file".into(), toml::Value::String(file.into()));
        params.insert(
            "data_format".into(),
            toml::Value::String(data_format.into()),
        );
        ResolvedSourceSpec {
            name: "file_arrow_glob".into(),
            kind: "file".into(),
            connector_id: String::new(),
            params: parammap_from_toml_map(params),
            tags: vec![],
        }
    }

    fn write_arrow_ipc_file(rows: &[&str]) -> (NamedTempFile, Vec<u8>) {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::ipc::writer::StreamWriter;
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, false)]));
        let mut ipc = Vec::new();
        {
            let mut w = StreamWriter::try_new(&mut ipc, &schema).unwrap();
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(StringArray::from(rows.to_vec()))],
            )
            .unwrap();
            w.write(&batch).unwrap();
            w.finish().unwrap();
        }
        let tmp = NamedTempFile::new().expect("temp file");
        std::fs::write(tmp.path(), &ipc).expect("write arrow ipc");
        (tmp, ipc)
    }

    async fn build_only(spec: &ResolvedSourceSpec) -> wp_connector_api::SourceSvcIns {
        let ctx = SourceBuildCtx::new(std::path::PathBuf::from("."));
        FileSourceFactory
            .build(spec, &ctx)
            .await
            .expect("build source")
    }

    #[tokio::test]
    async fn build_arrow_framed_emits_whole_file_as_bytes() {
        // Build a wp_arrow frame and ensure the factory emits it whole.
        let (_ipc_tmp, ipc) = write_arrow_ipc_file(&["x", "y"]);
        let mut frame = Vec::new();
        frame.extend_from_slice(&1u32.to_be_bytes()); // tag_len = 1
        frame.extend_from_slice(b"t");
        frame.extend_from_slice(&ipc);

        let file = NamedTempFile::new().expect("temp file");
        std::fs::write(file.path(), &frame).expect("write framed");
        let rel = file
            .path()
            .strip_prefix("/")
            .expect("strip root")
            .display()
            .to_string();

        let spec = build_data_format_spec(&rel, "arrow_framed");
        let mut svc = build_only(&spec).await;
        assert_eq!(svc.sources.len(), 1);
        let mut handle = svc.sources.remove(0);
        let (_tx, rx) = async_broadcast::broadcast::<wp_connector_api::ControlEvent>(1);
        handle.source.start(rx).await.expect("start source");
        let mut batch = handle.source.receive().await.expect("read batch");
        let event = batch.pop().expect("one event");
        assert_eq!(event.payload.as_bytes(), frame.as_slice());
        assert!(handle.source.receive().await.is_err()); // EOF
        handle.source.close().await.expect("close source");
    }

    #[tokio::test]
    async fn build_arrow_with_instances_is_not_byte_sharded() {
        // Arrow is binary; instances must NOT split a single file into byte
        // ranges (that would corrupt the stream). Expect exactly one source.
        let (tmp, _ipc) = write_arrow_ipc_file(&["a", "b", "c"]);
        let mut params = TomlMap::new();
        params.insert("base".into(), toml::Value::String("/".into()));
        params.insert(
            "file".into(),
            toml::Value::String(
                tmp.path()
                    .strip_prefix("/")
                    .expect("strip root")
                    .display()
                    .to_string(),
            ),
        );
        params.insert("instances".into(), toml::Value::Integer(4));
        params.insert(
            "data_format".into(),
            toml::Value::String("arrow_ipc".into()),
        );
        let spec = ResolvedSourceSpec {
            name: "file_arrow_inst".into(),
            kind: "file".into(),
            connector_id: String::new(),
            params: parammap_from_toml_map(params),
            tags: vec![],
        };

        let svc = build_only(&spec).await;
        // One whole-file binary source, not four byte-range shards.
        assert_eq!(svc.sources.len(), 1);
    }

    #[tokio::test]
    async fn build_arrow_glob_produces_one_source_per_file() {
        // Multiple matched files -> one binary source each (inter-file parallelism).
        let dir = TempDir::new().expect("temp dir");
        std::fs::write(dir.path().join("a.arrow"), b"arrow-a-bytes").expect("write a");
        std::fs::write(dir.path().join("b.arrow"), b"arrow-b-bytes").expect("write b");

        let spec = build_arrow_glob_spec(dir.path(), "*.arrow", "arrow_ipc");
        let svc = build_only(&spec).await;
        assert_eq!(svc.sources.len(), 2);
    }
}
