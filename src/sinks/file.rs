use async_trait::async_trait;
use std::fs;
use std::io::ErrorKind;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use wp_connector_api::{
    AsyncCtrl, AsyncRawDataSink, AsyncRecordSink, SinkBuildCtx, SinkError, SinkReason, SinkResult,
    SinkSpec as ResolvedSinkSpec,
};
use wp_data_fmt::{FormatType, RecordFormatter};
use wp_model_core::model::DataRecord;
use wp_model_core::model::fmt_def::TextFmt;

type AnyResult<T> = anyhow::Result<T>;

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

fn sink_err<E>(msg: &'static str, err: E) -> wp_connector_api::SinkError
where
    E: std::fmt::Display,
{
    SinkError::from(SinkReason::sink(msg)).with_detail(err.to_string())
}

#[derive(Clone, Debug)]
pub struct FileSinkSpec {
    fmt: TextFmt,
    base: String,
    file_name: String,
    sync: bool,
}

impl FileSinkSpec {
    pub fn from_resolved(_kind: &str, spec: &ResolvedSinkSpec) -> AnyResult<Self> {
        if let Some(s) = spec.params.get("fmt").and_then(|v| v.as_str()) {
            let ok = matches!(s, "json" | "csv" | "show" | "kv" | "raw" | "proto-text");
            if !ok {
                anyhow::bail!(
                    "invalid fmt: '{}'; allowed: json,csv,show,kv,raw,proto-text",
                    s
                );
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
    pub async fn new(out_path: &str) -> AnyResult<Self> {
        Self::with_sync(out_path, false).await
    }

    pub async fn with_sync(out_path: &str, sync: bool) -> AnyResult<Self> {
        if let Some(parent) = std::path::Path::new(out_path).parent()
            && !parent.exists()
        {
            fs::create_dir_all(parent)?;
        }
        let out_io = OpenOptions::new()
            .append(true)
            .create(true)
            .open(out_path)
            .await?;
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

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Write as _;
    use std::path::Path;
    use std::path::PathBuf;

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn formatted_file_sink_writes_json_record() -> AnyResult<()> {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("wp_file_sink_{}.json", ts));
        let inner = AsyncFileSink::new(path.to_string_lossy().as_ref()).await?;
        let mut sink = FormattedFileSink::new(TextFmt::Json, inner);
        sink.sink_record(&DataRecord::default()).await?;
        sink.stop().await?;
        let body = fs::read_to_string(path)?;
        assert!(body.trim_start().starts_with('{'));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stop_unlocks_only_own_lock() -> AnyResult<()> {
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

        let mut sink = AsyncFileSink::new(own_lock.to_string_lossy().as_ref()).await?;
        AsyncRawDataSink::sink_str(&mut sink, "line1").await?;
        AsyncCtrl::stop(&mut sink).await?;

        assert!(!Path::new(own_lock.to_string_lossy().as_ref()).exists());
        assert!(Path::new(base.join("group1/sinkA-001.dat").to_string_lossy().as_ref()).exists());
        assert!(Path::new(other_lock.to_string_lossy().as_ref()).exists());

        let _ = fs::remove_dir_all(&base);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sync_parameter_controls_fsync_calls() -> AnyResult<()> {
        use wp_connector_api::{AsyncCtrl, AsyncRawDataSink};

        take_sync_all_count();

        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let base = std::env::temp_dir().join(format!("wp_sync_test_{}", ts));
        fs::create_dir_all(&base)?;

        let sync_file = base.join("sync_true.dat.lock");
        let mut sink_sync =
            AsyncFileSink::with_sync(sync_file.to_string_lossy().as_ref(), true).await?;
        AsyncRawDataSink::sink_str(&mut sink_sync, "line1").await?;
        AsyncRawDataSink::sink_str(&mut sink_sync, "line2").await?;
        let sync_calls = take_sync_all_count();
        assert_eq!(sync_calls, 2);
        AsyncCtrl::stop(&mut sink_sync).await?;

        let no_sync_file = base.join("sync_false.dat.lock");
        let mut sink_no_sync =
            AsyncFileSink::with_sync(no_sync_file.to_string_lossy().as_ref(), false).await?;
        AsyncRawDataSink::sink_str(&mut sink_no_sync, "line1").await?;
        AsyncRawDataSink::sink_str(&mut sink_no_sync, "line2").await?;
        let sync_calls = take_sync_all_count();
        assert_eq!(sync_calls, 0);
        AsyncCtrl::stop(&mut sink_no_sync).await?;

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
}
