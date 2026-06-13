use std::io::BufWriter;
use std::sync::Arc;

use arrow::ipc::writer::StreamWriter;
use async_trait::async_trait;
use orion_error::conversion::{SourceErr, SourceRawErr, ToStructError};
use std::fs;
use std::io::ErrorKind;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use wp_connector_api::{
    AsyncCtrl, AsyncRawDataSink, AsyncRecordSink, SinkBuildCtx, SinkReason, SinkResult,
    SinkSpec as ResolvedSinkSpec,
};
use wp_data_fmt::{FormatType, RecordFormatter};
use wp_model_core::model::DataRecord;
use wp_model_core::model::fmt_def::TextFmt;

use super::arrow_conv::{
    data_record_to_batch, data_records_to_batch, infer_schema_from_record, sink_err,
};

#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(test)]
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(test)]
static SYNC_ALL_COUNTER: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
fn take_sync_all_count() -> usize {
    SYNC_ALL_COUNTER.swap(0, Ordering::Relaxed)
}

#[cfg(test)]
fn record_sync_all_call() {
    SYNC_ALL_COUNTER.fetch_add(1, Ordering::Relaxed);
}

#[cfg(not(test))]
fn record_sync_all_call() {}

#[derive(Clone, Debug)]
pub struct FileSinkSpec {
    fmt: TextFmt,
    base: String,
    file_name: String,
    sync: bool,
}

impl FileSinkSpec {
    pub fn from_resolved(_kind: &str, spec: &ResolvedSinkSpec) -> SinkResult<Self> {
        if let Some(s) = spec.params.get("fmt").and_then(|v| v.as_str()) {
            let ok = matches!(s, "json" | "csv" | "show" | "kv" | "raw" | "proto-text");
            if !ok {
                return Err(SinkReason::core_conf().to_err().with_detail(format!(
                    "invalid fmt: '{}'; allowed: json,csv,show,kv,raw,proto-text",
                    s
                )));
            }
        }
        let fmt = spec
            .params
            .get("fmt")
            .and_then(|v| v.as_str())
            .map(TextFmt::from)
            .unwrap_or(TextFmt::Json);
        let base = spec
            .params
            .get("base")
            .and_then(|v| v.as_str())
            .unwrap_or("./data/out_dat")
            .to_string();
        let file_name = spec
            .params
            .get("file")
            .and_then(|v| v.as_str())
            .unwrap_or("out.dat")
            .to_string();
        let sync = spec
            .params
            .get("sync")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        Ok(Self {
            fmt,
            base,
            file_name,
            sync,
        })
    }

    pub fn text_fmt(&self) -> TextFmt {
        self.fmt
    }

    pub fn sync(&self) -> bool {
        self.sync
    }

    pub fn resolve_path(&self, _ctx: &SinkBuildCtx) -> String {
        resolve_output_path(&self.base, &self.file_name, _ctx)
            .display()
            .to_string()
    }
}

pub(crate) fn resolve_output_path(
    base: &str,
    file_name: &str,
    ctx: &SinkBuildCtx,
) -> std::path::PathBuf {
    let base_path = std::path::Path::new(base);
    let base_dir = if base_path.is_absolute() {
        base_path.to_path_buf()
    } else {
        ctx.work_root.join(base_path)
    };
    let file_path = if ctx.replica_cnt > 1 {
        shard_relative_file(std::path::Path::new(file_name), ctx.replica_idx)
    } else {
        std::path::PathBuf::from(file_name)
    };
    base_dir.join(file_path)
}

fn shard_relative_file(path: &std::path::Path, replica_idx: usize) -> std::path::PathBuf {
    let parent = path.parent().unwrap_or_else(|| std::path::Path::new(""));
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("out.dat");
    parent.join(shard_file_name(file_name, replica_idx))
}

fn shard_file_name(file_name: &str, replica_idx: usize) -> String {
    if let Some(inner) = file_name.strip_suffix(".lock") {
        return format!("{}.lock", shard_file_name(inner, replica_idx));
    }
    let path = std::path::Path::new(file_name);
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or(file_name);
    match path.extension().and_then(|s| s.to_str()) {
        Some(ext) => format!("{stem}-r{replica_idx}.{ext}"),
        None => format!("{stem}-r{replica_idx}"),
    }
}

pub struct AsyncFileSink {
    path: String,
    out_io: tokio::fs::File,
    sync: bool,
    lock_released: bool,
}

impl Drop for AsyncFileSink {
    fn drop(&mut self) {
        if let Err(e) = self.unlock_lockfile() {
            log::error!("unlock file sink lock failed: {}", e);
        }
    }
}

impl AsyncFileSink {
    pub async fn new(out_path: &str) -> SinkResult<Self> {
        Self::with_sync(out_path, false).await
    }

    pub async fn with_sync(out_path: &str, sync: bool) -> SinkResult<Self> {
        if let Some(parent) = std::path::Path::new(out_path).parent()
            && !parent.exists()
        {
            fs::create_dir_all(parent).source_err(SinkReason::Sink, "create output dir")?;
        }
        let out_io = OpenOptions::new()
            .append(true)
            .create(true)
            .open(out_path)
            .await
            .source_err(SinkReason::Sink, "open output file")?;
        Ok(Self {
            path: out_path.to_string(),
            out_io,
            sync,
            lock_released: !out_path.ends_with(".lock"),
        })
    }

    fn unlock_lockfile(&mut self) -> std::io::Result<()> {
        if self.lock_released || !self.path.ends_with(".lock") {
            self.lock_released = true;
            return Ok(());
        }
        if let Some(new_path) = self.path.strip_suffix(".lock") {
            match fs::rename(&self.path, new_path) {
                Ok(()) => {
                    self.lock_released = true;
                    Ok(())
                }
                Err(err) if err.kind() == ErrorKind::NotFound => {
                    self.lock_released = true;
                    Ok(())
                }
                Err(err) => Err(err),
            }
        } else {
            self.lock_released = true;
            Ok(())
        }
    }
}

#[async_trait]
impl AsyncCtrl for AsyncFileSink {
    async fn stop(&mut self) -> SinkResult<()> {
        self.out_io
            .sync_all()
            .await
            .map_err(|e| sink_err("file sync on stop fail", e))?;
        if let Err(e) = self.unlock_lockfile() {
            log::error!("unlock file sink on stop failed: {}", e);
        }
        Ok(())
    }

    async fn reconnect(&mut self) -> SinkResult<()> {
        Ok(())
    }
}

#[async_trait]
impl AsyncRawDataSink for AsyncFileSink {
    async fn sink_bytes(&mut self, data: &[u8]) -> SinkResult<()> {
        self.out_io
            .write_all(data)
            .await
            .map_err(|e| sink_err("file out fail", e))?;

        if self.sync {
            self.out_io
                .sync_all()
                .await
                .map_err(|e| sink_err("file sync fail", e))?;
            record_sync_all_call();
        }
        Ok(())
    }

    async fn sink_str(&mut self, data: &str) -> SinkResult<()> {
        if data.as_bytes().last() == Some(&b'\n') {
            self.sink_bytes(data.as_bytes()).await
        } else {
            let mut buffer = Vec::with_capacity(data.len() + 1);
            buffer.extend_from_slice(data.as_bytes());
            buffer.push(b'\n');
            self.sink_bytes(&buffer).await
        }
    }

    async fn sink_str_batch(&mut self, data: Vec<&str>) -> SinkResult<()> {
        if data.is_empty() {
            return Ok(());
        }

        let mut total_len = 0;
        for str_data in &data {
            total_len += str_data.len();
            if str_data.as_bytes().last().is_none_or(|&b| b != b'\n') {
                total_len += 1;
            }
        }

        let mut buffer = Vec::with_capacity(total_len);
        for str_data in &data {
            buffer.extend_from_slice(str_data.as_bytes());
            if str_data.as_bytes().last().is_none_or(|&b| b != b'\n') {
                buffer.push(b'\n');
            }
        }

        self.out_io
            .write_all(&buffer)
            .await
            .map_err(|e| sink_err("file out fail", e))?;

        if self.sync {
            self.out_io
                .sync_all()
                .await
                .map_err(|e| sink_err("file sync fail", e))?;
            record_sync_all_call();
        }

        Ok(())
    }

    async fn sink_bytes_batch(&mut self, data: Vec<&[u8]>) -> SinkResult<()> {
        if data.is_empty() {
            return Ok(());
        }

        let mut total_len = 0;
        for bytes_data in &data {
            total_len += bytes_data.len();
            if bytes_data.last().is_none_or(|&b| b != b'\n') {
                total_len += 1;
            }
        }

        let mut buffer = Vec::with_capacity(total_len);
        for bytes_data in &data {
            buffer.extend_from_slice(bytes_data);
            if bytes_data.last().is_none_or(|&b| b != b'\n') {
                buffer.push(b'\n');
            }
        }

        self.out_io
            .write_all(&buffer)
            .await
            .map_err(|e| sink_err("file out fail", e))?;

        if self.sync {
            self.out_io
                .sync_all()
                .await
                .map_err(|e| sink_err("file sync fail", e))?;
            record_sync_all_call();
        }

        Ok(())
    }
}

pub struct FormattedFileSink {
    fmt: TextFmt,
    inner: AsyncFileSink,
}

impl FormattedFileSink {
    pub fn new(fmt: TextFmt, inner: AsyncFileSink) -> Self {
        Self { fmt, inner }
    }

    fn format_record(&self, data: &DataRecord) -> String {
        FormatType::from(&self.fmt).fmt_record(data)
    }
}

#[async_trait]
impl AsyncCtrl for FormattedFileSink {
    async fn stop(&mut self) -> SinkResult<()> {
        self.inner.stop().await
    }

    async fn reconnect(&mut self) -> SinkResult<()> {
        self.inner.reconnect().await
    }
}

#[async_trait]
impl AsyncRecordSink for FormattedFileSink {
    async fn sink_record(&mut self, data: &DataRecord) -> SinkResult<()> {
        let formatted = self.format_record(data);
        self.inner.sink_str(&formatted).await
    }

    async fn sink_records(&mut self, data: Vec<std::sync::Arc<DataRecord>>) -> SinkResult<()> {
        if data.is_empty() {
            return Ok(());
        }
        let batch: Vec<String> = data
            .iter()
            .map(|record| self.format_record(record))
            .collect();
        let refs: Vec<&str> = batch.iter().map(|s| s.as_str()).collect();
        self.inner.sink_str_batch(refs).await
    }
}

#[async_trait]
impl AsyncRawDataSink for FormattedFileSink {
    async fn sink_str(&mut self, data: &str) -> SinkResult<()> {
        self.inner.sink_str(data).await
    }

    async fn sink_bytes(&mut self, data: &[u8]) -> SinkResult<()> {
        self.inner.sink_bytes(data).await
    }

    async fn sink_str_batch(&mut self, data: Vec<&str>) -> SinkResult<()> {
        self.inner.sink_str_batch(data).await
    }

    async fn sink_bytes_batch(&mut self, data: Vec<&[u8]>) -> SinkResult<()> {
        self.inner.sink_bytes_batch(data).await
    }
}

// ---------------------------------------------------------------------------
// ArrowFileSink — Arrow IPC Stream format file sink
// ---------------------------------------------------------------------------

pub struct ArrowFileSink {
    path: String,
    writer: Mutex<Option<StreamWriter<BufWriter<std::fs::File>>>>,
    schema: Mutex<Option<Arc<arrow::datatypes::Schema>>>,
    sync: bool,
    sent_cnt: u64,
}

impl ArrowFileSink {
    pub fn new(path: &str, sync: bool) -> SinkResult<Self> {
        if let Some(parent) = std::path::Path::new(path).parent()
            && !parent.exists()
        {
            fs::create_dir_all(parent).source_err(SinkReason::Sink, "create output dir")?;
        }
        Ok(Self {
            path: path.to_string(),
            writer: Mutex::new(None),
            schema: Mutex::new(None),
            sync,
            sent_cnt: 0,
        })
    }

    /// Lazily create the StreamWriter on first write, inferring schema from data.
    async fn ensure_writer(
        &self,
        record: &DataRecord,
    ) -> SinkResult<(
        tokio::sync::MutexGuard<'_, Option<StreamWriter<BufWriter<std::fs::File>>>>,
        Arc<arrow::datatypes::Schema>,
    )> {
        let mut schema_guard = self.schema.lock().await;
        if schema_guard.is_none() {
            let schema = Arc::new(infer_schema_from_record(record));
            let file = std::fs::OpenOptions::new()
                .append(true)
                .create(true)
                .open(&self.path)
                .source_err(SinkReason::Sink, "open arrow output file")?;
            let writer = StreamWriter::try_new(BufWriter::new(file), &schema)
                .source_raw_err(SinkReason::Sink, "create arrow stream writer")?;
            *schema_guard = Some(Arc::clone(&schema));
            let mut writer_guard = self.writer.lock().await;
            *writer_guard = Some(writer);
        }
        let schema = Arc::clone(schema_guard.as_ref().unwrap());
        drop(schema_guard);
        let writer_guard = self.writer.lock().await;
        Ok((writer_guard, schema))
    }
}

#[async_trait]
impl AsyncRecordSink for ArrowFileSink {
    async fn sink_record(&mut self, data: &DataRecord) -> SinkResult<()> {
        let (mut writer_opt, schema) = self.ensure_writer(data).await?;
        let batch = data_record_to_batch(data, &schema)?;
        let writer = writer_opt.as_mut().unwrap();
        writer
            .write(&batch)
            .map_err(|e| sink_err("arrow_file write batch fail", e))?;
        writer
            .flush()
            .map_err(|e| sink_err("arrow_file flush fail", e))?;
        if self.sync {
            writer
                .get_mut()
                .get_mut()
                .sync_all()
                .map_err(|e| sink_err("arrow_file sync fail", e))?;
        }
        drop(writer_opt);

        self.sent_cnt = self.sent_cnt.saturating_add(1);
        if self.sent_cnt == 1 {
            log::info!("arrow_file sink first-send: rows=1");
        }
        Ok(())
    }

    async fn sink_records(&mut self, data: Vec<Arc<DataRecord>>) -> SinkResult<()> {
        if data.is_empty() {
            return Ok(());
        }
        let row_count = data.len();
        let (mut writer_opt, schema) = self.ensure_writer(&data[0]).await?;
        let batch = data_records_to_batch(&data, &schema)?;
        let writer = writer_opt.as_mut().unwrap();
        writer
            .write(&batch)
            .map_err(|e| sink_err("arrow_file write batch fail", e))?;
        writer
            .flush()
            .map_err(|e| sink_err("arrow_file flush fail", e))?;
        if self.sync {
            writer
                .get_mut()
                .get_mut()
                .sync_all()
                .map_err(|e| sink_err("arrow_file sync fail", e))?;
        }
        drop(writer_opt);

        if self.sent_cnt == 0 {
            let schema_guard = self.schema.lock().await;
            log::info!(
                "arrow_file sink first-send: rows={} cols={}",
                row_count,
                schema_guard.as_ref().map(|s| s.fields().len()).unwrap_or(0),
            );
        }
        self.sent_cnt = self.sent_cnt.saturating_add(1);
        Ok(())
    }
}

#[async_trait]
impl AsyncRawDataSink for ArrowFileSink {
    async fn sink_str(&mut self, _data: &str) -> SinkResult<()> {
        Err(SinkReason::Sink
            .to_err()
            .with_detail("arrow_file sink only accepts records"))
    }

    async fn sink_bytes(&mut self, _data: &[u8]) -> SinkResult<()> {
        Err(SinkReason::Sink
            .to_err()
            .with_detail("arrow_file sink only accepts records"))
    }

    async fn sink_str_batch(&mut self, _data: Vec<&str>) -> SinkResult<()> {
        Err(SinkReason::Sink
            .to_err()
            .with_detail("arrow_file sink only accepts records"))
    }

    async fn sink_bytes_batch(&mut self, _data: Vec<&[u8]>) -> SinkResult<()> {
        Err(SinkReason::Sink
            .to_err()
            .with_detail("arrow_file sink only accepts records"))
    }
}

#[async_trait]
impl AsyncCtrl for ArrowFileSink {
    async fn stop(&mut self) -> SinkResult<()> {
        let mut writer_opt = self.writer.lock().await;
        if let Some(writer) = writer_opt.as_mut() {
            writer
                .finish()
                .map_err(|e| sink_err("arrow_file finish fail", e))?;
            if self.sync {
                writer
                    .get_mut()
                    .get_mut()
                    .sync_all()
                    .map_err(|e| sink_err("arrow_file sync on stop fail", e))?;
            }
        }
        Ok(())
    }

    async fn reconnect(&mut self) -> SinkResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Write as _;
    use std::path::Path;
    use std::path::PathBuf;
    use std::sync::Arc;

    use wp_model_core::model::{Field as ModelField, FieldStorage};

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn formatted_file_sink_writes_json_record() -> anyhow::Result<()> {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("wp_file_sink_{}.json", ts));
        let inner = AsyncFileSink::new(path.to_string_lossy().as_ref())
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let mut sink = FormattedFileSink::new(TextFmt::Json, inner);
        sink.sink_record(&DataRecord::default())
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        sink.stop().await.map_err(|e| anyhow::anyhow!("{e}"))?;
        let body = fs::read_to_string(path)?;
        assert!(body.trim_start().starts_with('{'));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stop_unlocks_only_own_lock() -> anyhow::Result<()> {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let base = std::env::temp_dir().join(format!("wp_rescue_unlock_{}", ts));
        let own_lock = base.join("group1/sinkA-001.dat.lock");
        let other_lock = base.join("group1/sinkB-001.dat.lock");
        if let Some(p) = own_lock.parent() {
            fs::create_dir_all(p)?;
        }
        if let Some(p) = other_lock.parent() {
            fs::create_dir_all(p)?;
        }

        fs::File::create(&other_lock)?.write_all(b"test")?;

        let mut sink = AsyncFileSink::new(own_lock.to_string_lossy().as_ref())
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        AsyncRawDataSink::sink_str(&mut sink, "line1")
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        AsyncCtrl::stop(&mut sink)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        assert!(!Path::new(own_lock.to_string_lossy().as_ref()).exists());
        assert!(Path::new(base.join("group1/sinkA-001.dat").to_string_lossy().as_ref()).exists());
        assert!(Path::new(other_lock.to_string_lossy().as_ref()).exists());

        let _ = fs::remove_dir_all(&base);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sync_parameter_controls_fsync_calls() -> anyhow::Result<()> {
        use wp_connector_api::{AsyncCtrl, AsyncRawDataSink};

        take_sync_all_count();

        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let base = std::env::temp_dir().join(format!("wp_sync_test_{}", ts));
        fs::create_dir_all(&base)?;

        let sync_file = base.join("sync_true.dat.lock");
        let mut sink_sync = AsyncFileSink::with_sync(sync_file.to_string_lossy().as_ref(), true)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        AsyncRawDataSink::sink_str(&mut sink_sync, "line1")
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        AsyncRawDataSink::sink_str(&mut sink_sync, "line2")
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let sync_calls = take_sync_all_count();
        assert_eq!(sync_calls, 2);
        AsyncCtrl::stop(&mut sink_sync)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        let no_sync_file = base.join("sync_false.dat.lock");
        let mut sink_no_sync =
            AsyncFileSink::with_sync(no_sync_file.to_string_lossy().as_ref(), false)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
        AsyncRawDataSink::sink_str(&mut sink_no_sync, "line1")
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        AsyncRawDataSink::sink_str(&mut sink_no_sync, "line2")
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let sync_calls = take_sync_all_count();
        assert_eq!(sync_calls, 0);
        AsyncCtrl::stop(&mut sink_no_sync)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        let _ = fs::remove_dir_all(&base);
        Ok(())
    }

    #[test]
    fn resolve_output_path_uses_work_root_and_replica_suffix() {
        let ctx = SinkBuildCtx::new_with_replica(PathBuf::from("/tmp/demo"), 2, 4);
        let path = resolve_output_path("./data/out_dat", "sink/default.json", &ctx);
        assert_eq!(
            path,
            PathBuf::from("/tmp/demo/data/out_dat/sink/default-r2.json")
        );

        let lock_path = resolve_output_path("./data/out_dat", "sink/default.dat.lock", &ctx);
        assert_eq!(
            lock_path,
            PathBuf::from("/tmp/demo/data/out_dat/sink/default-r2.dat.lock")
        );
    }

    // ---- ArrowFileSink tests ----

    fn tmp_arrow_path(label: &str) -> PathBuf {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("wp_arrow_fs_{ts}_{label}.arrow"))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn arrow_file_sink_write_and_read_back() {
        use arrow::ipc::reader::StreamReader;
        use arrow::record_batch::RecordBatch;
        use wp_model_core::model::{Field as ModelField, FieldStorage};

        let path = tmp_arrow_path("roundtrip");

        // Write
        {
            let mut sink = ArrowFileSink::new(path.to_string_lossy().as_ref(), false).unwrap();

            let recs: Vec<Arc<DataRecord>> = vec![
                Arc::new(DataRecord::from(vec![
                    FieldStorage::from(ModelField::from_chars("name", "alice")),
                    FieldStorage::from(ModelField::from_chars("count", "42")),
                ])),
                Arc::new(DataRecord::from(vec![
                    FieldStorage::from(ModelField::from_chars("name", "bob")),
                    FieldStorage::from(ModelField::from_chars("count", "7")),
                ])),
            ];

            sink.sink_records(recs).await.unwrap();
            sink.stop().await.unwrap();
        }

        // Read back
        let file = std::fs::File::open(&path).unwrap();
        let reader = StreamReader::try_new(file, None).unwrap();
        let batches: Vec<RecordBatch> = reader.map(|r| r.unwrap()).collect();

        assert!(!batches.is_empty(), "should have at least one batch");
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
        assert_eq!(batches[0].num_columns(), 2);

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn arrow_file_sink_sync_writes_through() {
        use arrow::ipc::reader::StreamReader;

        let path = tmp_arrow_path("sync");

        {
            let mut sink = ArrowFileSink::new(
                path.to_string_lossy().as_ref(),
                true, // sync enabled
            )
            .unwrap();

            let rec = Arc::new(DataRecord::from(vec![FieldStorage::from(
                ModelField::from_chars("x", "hello"),
            )]));

            sink.sink_record(&rec).await.unwrap();
            // After sink_record with sync=true, data should already be on disk
            sink.stop().await.unwrap();
        }

        // File should exist and be readable
        let file = std::fs::File::open(&path).unwrap();
        let reader = StreamReader::try_new(file, None).unwrap();
        let batches: Vec<_> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn arrow_file_sink_empty_records_noop() {
        let path = tmp_arrow_path("empty");

        {
            let mut sink = ArrowFileSink::new(path.to_string_lossy().as_ref(), false).unwrap();

            // Empty sink_records should be a no-op
            sink.sink_records(Vec::new()).await.unwrap();
            sink.stop().await.unwrap();
        }

        // File is created lazily on first write; empty records should not create it.
        assert!(!path.exists());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn arrow_file_sink_appends_on_reopen() {
        use arrow::ipc::reader::StreamReader;

        let path = tmp_arrow_path("append");

        // First open: write one record
        {
            let mut sink = ArrowFileSink::new(path.to_string_lossy().as_ref(), false).unwrap();

            let rec = Arc::new(DataRecord::from(vec![FieldStorage::from(
                ModelField::from_chars("v", "first"),
            )]));
            sink.sink_record(&rec).await.unwrap();
            sink.stop().await.unwrap();
        }

        let size_after_first = std::fs::metadata(&path).unwrap().len();

        // Second open: append another record
        {
            let mut sink = ArrowFileSink::new(path.to_string_lossy().as_ref(), false).unwrap();

            let rec = Arc::new(DataRecord::from(vec![FieldStorage::from(
                ModelField::from_chars("v", "second"),
            )]));
            sink.sink_record(&rec).await.unwrap();
            sink.stop().await.unwrap();
        }

        let size_after_second = std::fs::metadata(&path).unwrap().len();
        assert!(
            size_after_second > size_after_first,
            "append mode: file should grow after reopening and writing more records"
        );

        // Each segment is self-contained: read both independently
        let file = std::fs::File::open(&path).unwrap();
        let reader = StreamReader::try_new(file, None).unwrap();
        let first_rows: usize = reader.map(|r| r.unwrap().num_rows()).sum();
        assert_eq!(first_rows, 1, "first segment should have 1 row");

        let _ = std::fs::remove_file(&path);
    }
}
