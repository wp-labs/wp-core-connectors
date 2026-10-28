use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};
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
use wp_connector_utils::codec::Encoder;
use wp_data_fmt::{FormatType, RecordFormatter};
use wp_model_core::model::DataRecord;
use wp_model_core::model::fmt_def::TextFmt;

use crate::net::CodecConfig;

/// 滚动文件默认保留个数（不含当前正在写入的文件）。
pub const DEFAULT_MAX_ROLLS: u32 = 3;
/// 滚动文件个数上限，防止误配置写出过多历史文件。
pub const MAX_ROLLS_LIMIT: u32 = 1024;

/// 滚动文件的归档压缩方式。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RollCompress {
    /// 不压缩：`out.dat.1`
    #[default]
    None,
    /// gzip：`out.dat.1.gz`
    Gzip,
    /// zip（Deflate）：`out.dat.1.zip`
    Zip,
}

impl RollCompress {
    /// 归档文件扩展名（含前导点；不压缩时为空）。
    pub fn extension(self) -> &'static str {
        match self {
            RollCompress::None => "",
            RollCompress::Gzip => ".gz",
            RollCompress::Zip => ".zip",
        }
    }

    pub(crate) fn parse(value: Option<&serde_json::Value>) -> SinkResult<Self> {
        let Some(value) = value else {
            return Ok(Self::None);
        };
        if value.is_null() {
            return Ok(Self::None);
        }
        match value {
            // 布尔开关：true 等价于 gzip
            serde_json::Value::Bool(true) => Ok(Self::Gzip),
            serde_json::Value::Bool(false) => Ok(Self::None),
            serde_json::Value::String(s) => match s.trim().to_ascii_lowercase().as_str() {
                "" | "none" | "off" | "false" => Ok(Self::None),
                "gzip" | "gz" => Ok(Self::Gzip),
                "zip" => Ok(Self::Zip),
                other => Err(SinkReason::core_conf().to_err().with_detail(format!(
                    "invalid roll_compress: '{other}'; allowed: none,gzip,zip"
                ))),
            },
            other => Err(SinkReason::core_conf().to_err().with_detail(format!(
                "roll_compress must be \"none\"/\"gzip\"/\"zip\" or a bool (got {other})"
            ))),
        }
    }
}

use super::arrow_conv::{
    data_record_to_batch, data_records_to_batch, encode_ipc_frame_multi, infer_schema_from_record,
    sink_err,
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
    codec: CodecConfig,
    /// 单文件最大字节数；`None` 表示不限制、不滚动。
    max_size: Option<u64>,
    /// 保留的滚动文件个数（不含当前文件）；`0` 表示滚动时直接丢弃旧文件。
    max_rolls: u32,
    /// 滚动归档压缩方式；`None` 表示不压缩。
    roll_compress: RollCompress,
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
        let codec = CodecConfig::from_params(&spec.params)
            .map_err(|e| SinkReason::core_conf().to_err().with_detail(e.to_string()))?;
        codec
            .validate()
            .map_err(|e| SinkReason::core_conf().to_err().with_detail(e.to_string()))?;
        let max_size = parse_max_size(spec.params.get("max_size"))?;
        let max_rolls = parse_max_rolls(spec.params.get("max_rolls"), max_size.is_some())?;
        let roll_compress = RollCompress::parse(spec.params.get("roll_compress"))?;
        if roll_compress != RollCompress::None && max_size.is_none() {
            return Err(SinkReason::core_conf().to_err().with_detail(
                "roll_compress requires max_size > 0; set max_size to enable file rolling",
            ));
        }
        Ok(Self {
            fmt,
            base,
            file_name,
            sync,
            codec,
            max_size,
            max_rolls,
            roll_compress,
        })
    }

    pub fn text_fmt(&self) -> TextFmt {
        self.fmt
    }

    pub fn sync(&self) -> bool {
        self.sync
    }

    pub fn codec(&self) -> &CodecConfig {
        &self.codec
    }

    /// 单文件最大字节数；`None` 表示不限容量、不滚动。
    pub fn max_size(&self) -> Option<u64> {
        self.max_size
    }

    /// 保留的滚动文件个数（不含当前文件）。
    pub fn max_rolls(&self) -> u32 {
        self.max_rolls
    }

    /// 滚动归档压缩方式。
    pub fn roll_compress(&self) -> RollCompress {
        self.roll_compress
    }

    pub fn resolve_path(&self, _ctx: &SinkBuildCtx) -> String {
        resolve_output_path(&self.base, &self.file_name, _ctx)
            .display()
            .to_string()
    }
}

/// 解析 `max_size`：支持整数字节数或带单位的字符串（`10MB` / `512k` / `1GiB`）。
/// `0` 视为不限制（返回 `None`）。
pub(crate) fn parse_max_size(value: Option<&serde_json::Value>) -> SinkResult<Option<u64>> {
    let Some(value) = value else { return Ok(None) };
    if value.is_null() {
        return Ok(None);
    }
    let bytes = match value {
        serde_json::Value::Number(n) => n.as_u64().ok_or_else(|| {
            SinkReason::core_conf()
                .to_err()
                .with_detail(format!("max_size must be a non-negative integer (got {n})"))
        })?,
        serde_json::Value::String(s) => parse_size_str(s)?,
        other => {
            return Err(SinkReason::core_conf().to_err().with_detail(format!(
                "max_size must be an integer or a size string like '10MB' (got {other})"
            )));
        }
    };
    Ok(if bytes == 0 { None } else { Some(bytes) })
}

/// 解析 `<数字><单位>` 形式的容量，单位按 1024 进制，大小写不敏感。
fn parse_size_str(raw: &str) -> SinkResult<u64> {
    let s = raw.trim();
    if s.is_empty() {
        return Err(size_conf_err(raw));
    }
    let digits_end = s
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(s.len());
    let (num, unit) = s.split_at(digits_end);
    let base: f64 = num.parse().map_err(|_| size_conf_err(raw))?;
    let mult: f64 = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1.0,
        "k" | "kb" | "kib" => 1024.0,
        "m" | "mb" | "mib" => 1024.0 * 1024.0,
        "g" | "gb" | "gib" => 1024.0 * 1024.0 * 1024.0,
        "t" | "tb" | "tib" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        _ => return Err(size_conf_err(raw)),
    };
    let bytes = base * mult;
    if !bytes.is_finite() || bytes < 0.0 {
        return Err(size_conf_err(raw));
    }
    Ok(bytes as u64)
}

fn size_conf_err(raw: &str) -> wp_connector_api::SinkError {
    SinkReason::core_conf().to_err().with_detail(format!(
        "invalid max_size: '{raw}'; expect bytes or a size string like '10MB'"
    ))
}

/// 解析 `max_rolls`：未配置默认 [`DEFAULT_MAX_ROLLS`]；
/// 显式配置时必须同时启用 `max_size`，否则配置非法。
pub(crate) fn parse_max_rolls(
    value: Option<&serde_json::Value>,
    size_enabled: bool,
) -> SinkResult<u32> {
    let Some(value) = value else {
        return Ok(DEFAULT_MAX_ROLLS);
    };
    if value.is_null() {
        return Ok(DEFAULT_MAX_ROLLS);
    }
    let rolls = match value.as_u64() {
        Some(n) if n <= u64::from(MAX_ROLLS_LIMIT) => n as u32,
        Some(n) => {
            return Err(SinkReason::core_conf()
                .to_err()
                .with_detail(format!("max_rolls must be <= {MAX_ROLLS_LIMIT} (got {n})")));
        }
        None => {
            return Err(SinkReason::core_conf().to_err().with_detail(format!(
                "max_rolls must be a non-negative integer (got {value})"
            )));
        }
    };
    if !size_enabled {
        return Err(SinkReason::core_conf()
            .to_err()
            .with_detail("max_rolls requires max_size > 0; set max_size to enable file rolling"));
    }
    Ok(rolls)
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

async fn open_append(path: &str) -> SinkResult<tokio::fs::File> {
    OpenOptions::new()
        .append(true)
        .create(true)
        .open(path)
        .await
        .source_err(SinkReason::Sink, "open output file")
}

/// 第 `index` 个历史文件的路径（带归档压缩扩展名）：`a.dat` -> `a.dat.1.gz`。
pub(crate) fn rolled_path_with(path: &Path, index: u32, compress: RollCompress) -> PathBuf {
    let base = rolled_base_path(path, index);
    let ext = compress.extension();
    if ext.is_empty() {
        return base;
    }
    let file_name = base
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("out.dat.1");
    match file_name.strip_suffix(".lock") {
        Some(inner) => base.with_file_name(format!("{inner}{ext}.lock")),
        None => base.with_file_name(format!("{file_name}{ext}")),
    }
}

fn rolled_base_path(path: &Path, index: u32) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("out.dat");
    let (stem, lock) = match file_name.strip_suffix(".lock") {
        Some(inner) => (inner, ".lock"),
        None => (file_name, ""),
    };
    path.with_file_name(format!("{stem}.{index}{lock}"))
}

/// 已存在的历史文件可能来自不同压缩方式（用户改过配置），轮转时按变体各自处理。
const ROLL_VARIANTS: [RollCompress; 3] =
    [RollCompress::None, RollCompress::Gzip, RollCompress::Zip];

/// 删除各变体最旧的文件，并把 `.{n}` 依次后移一位（`.{n}` -> `.{n+1}`）。
fn shift_archives(path: &Path, max_rolls: u32) -> std::io::Result<()> {
    for variant in ROLL_VARIANTS {
        remove_if_exists(&rolled_path_with(path, max_rolls, variant))?;
        for idx in (1..max_rolls).rev() {
            rename_if_exists(
                &rolled_path_with(path, idx, variant),
                &rolled_path_with(path, idx + 1, variant),
            )?;
        }
    }
    Ok(())
}

/// 把已关闭的活动文件归档为第 `index` 个历史文件；成功时删除原文件。
/// 失败时由调用方回退为不压缩归档，避免丢数据。
fn archive_file(src: &Path, dst: &Path, compress: RollCompress) -> std::io::Result<()> {
    match compress {
        RollCompress::None => return fs::rename(src, dst),
        RollCompress::Gzip => {
            let mut input = BufReader::new(fs::File::open(src)?);
            let out = fs::File::create(dst)?;
            let mut encoder =
                flate2::write::GzEncoder::new(BufWriter::new(out), flate2::Compression::default());
            std::io::copy(&mut input, &mut encoder)?;
            let writer = encoder.finish()?;
            let file = writer.into_inner().map_err(|err| err.into_error())?;
            file.sync_all()?;
        }
        RollCompress::Zip => {
            let mut input = BufReader::new(fs::File::open(src)?);
            let out = fs::File::create(dst)?;
            let mut writer = zip::ZipWriter::new(BufWriter::new(out));
            let name = src
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("out.dat")
                .to_string();
            let options = zip::write::SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Deflated);
            writer.start_file(name, options).map_err(zip_to_io)?;
            std::io::copy(&mut input, &mut writer)?;
            let inner = writer.finish().map_err(zip_to_io)?;
            let file = inner.into_inner().map_err(|err| err.into_error())?;
            file.sync_all()?;
        }
    }
    remove_if_exists(src)
}

fn zip_to_io(err: zip::result::ZipError) -> std::io::Error {
    std::io::Error::other(err)
}

fn remove_if_exists(path: &Path) -> std::io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

fn rename_if_exists(from: &Path, to: &Path) -> std::io::Result<()> {
    match fs::rename(from, to) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

pub struct AsyncFileSink {
    path: String,
    /// 当前活动文件句柄；滚动时先关闭再重命名（Windows 不允许重命名已打开文件）。
    out_io: Option<tokio::fs::File>,
    sync: bool,
    lock_released: bool,
    codec: Option<Box<dyn Encoder>>,
    /// 保留 codec 配置，滚动后用于重建编码器（压缩/加密流不能跨文件）。
    codec_cfg: Option<CodecConfig>,
    /// 单文件最大字节数；`None` 表示不限制、不滚动。
    max_size: Option<u64>,
    /// 保留的滚动文件个数（不含当前文件）。
    max_rolls: u32,
    /// 滚动归档的压缩方式。
    roll_compress: RollCompress,
    /// 当前文件已写入（编码后）字节数。
    written: u64,
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
        let out_io = open_append(out_path).await?;
        // 续写已有文件时，容量从当前大小起算。
        let written = out_io.metadata().await.map(|m| m.len()).unwrap_or(0);
        Ok(Self {
            path: out_path.to_string(),
            out_io: Some(out_io),
            sync,
            lock_released: !out_path.ends_with(".lock"),
            codec: None,
            codec_cfg: None,
            max_size: None,
            max_rolls: DEFAULT_MAX_ROLLS,
            roll_compress: RollCompress::None,
            written,
        })
    }

    /// 设置滚动归档的压缩方式（`none` / `gzip` / `zip`）。
    pub fn with_roll_compress(mut self, roll_compress: RollCompress) -> Self {
        self.roll_compress = roll_compress;
        self
    }

    /// 配置容量上限与滚动保留个数；`max_size = None` 表示不限容量。
    pub fn with_roll(mut self, max_size: Option<u64>, max_rolls: u32) -> Self {
        self.max_size = max_size.filter(|v| *v > 0);
        self.max_rolls = max_rolls;
        self
    }

    /// 附加压缩/加密编码器；后续写入会先 `encode` 再落盘。
    ///
    /// 注意：仅注入编码器无法在滚动时重建（压缩/加密流不能跨文件），
    /// 需要滚动时请用 [`Self::with_codec_config`]。
    pub fn with_codec(mut self, codec: Option<Box<dyn Encoder>>) -> Self {
        self.codec = codec;
        self.codec_cfg = None;
        self
    }

    /// 按 codec 配置构建编码器并保留配置，使滚动后能重新开一个完整编码流。
    pub fn with_codec_config(mut self, cfg: CodecConfig) -> SinkResult<Self> {
        self.codec = cfg
            .build_encoder()
            .map_err(|e| sink_err("codec build fail", e))?;
        self.codec_cfg = Some(cfg);
        Ok(self)
    }

    /// 单文件最大字节数；`None` 表示不限容量。
    pub fn max_size(&self) -> Option<u64> {
        self.max_size
    }

    /// 保留的滚动文件个数（不含当前文件）。
    pub fn max_rolls(&self) -> u32 {
        self.max_rolls
    }

    /// 滚动归档的压缩方式。
    pub fn roll_compress(&self) -> RollCompress {
        self.roll_compress
    }

    fn file_mut(&mut self) -> SinkResult<&mut tokio::fs::File> {
        self.out_io.as_mut().ok_or_else(|| {
            sink_err(
                "file sink closed",
                std::io::Error::new(ErrorKind::BrokenPipe, "file sink handle closed"),
            )
        })
    }

    /// 写入 `incoming` 字节后是否会超过单文件上限（空文件不滚动，避免小上限下反复空转）。
    fn should_roll(&self, incoming: u64) -> bool {
        match self.max_size {
            Some(max) if self.written > 0 => self.written.saturating_add(incoming) > max,
            _ => false,
        }
    }

    /// 滚动：收尾当前文件 → 按序号挪动历史文件 → 重建活动文件与编码流。
    async fn rotate(&mut self) -> SinkResult<()> {
        // 1. 冲刷 codec 尾部，让本文件成为完整的压缩/加密流
        if let Some(codec) = &mut self.codec {
            if self.codec_cfg.is_none() {
                return Err(SinkReason::core_conf().to_err().with_detail(
                    "file sink rolling requires codec config (use with_codec_config)",
                ));
            }
            let mut tail = Vec::new();
            codec
                .finish(&mut tail)
                .map_err(|e| sink_err("codec finish fail", e))?;
            if !tail.is_empty() {
                self.file_mut()?
                    .write_all(&tail)
                    .await
                    .map_err(|e| sink_err("file out fail", e))?;
            }
        }

        // 2. 关闭句柄后再重命名，兼容 Windows 的文件占用语义
        if let Some(file) = self.out_io.take() {
            file.sync_all()
                .await
                .map_err(|e| sink_err("file sync before roll fail", e))?;
            drop(file);
        }

        // 3. 历史文件轮转：out.dat -> out.dat.1 -> out.dat.2 ...
        //    配置了 roll_compress 时归档为 out.dat.1.gz / out.dat.1.zip
        if let Err(err) = self.rotate_archives().await {
            // 轮转失败不能让 sink 永久失活：尽力重新打开活动文件并校正字节计数
            match open_append(&self.path).await {
                Ok(file) => {
                    self.written = file.metadata().await.map(|m| m.len()).unwrap_or(0);
                    self.out_io = Some(file);
                }
                Err(reopen_err) => {
                    log::error!(
                        "file sink reopen after roll failure failed path='{}': {}",
                        self.path,
                        reopen_err
                    );
                }
            }
            return Err(err);
        }

        // 4. 重新打开空的活动文件，并重建 codec（新文件需要一个新流）
        let file = open_append(&self.path).await?;
        self.out_io = Some(file);
        if let Some(cfg) = &self.codec_cfg {
            self.codec = cfg
                .build_encoder()
                .map_err(|e| sink_err("codec build fail", e))?;
        }
        let rolled_bytes = self.written;
        self.written = 0;
        if self.max_rolls == 0 {
            log::debug!(
                "file sink rolled path='{}' bytes={} (max_rolls=0, old file discarded)",
                self.path,
                rolled_bytes
            );
        } else {
            log::debug!(
                "file sink rolled path='{}' archive='{}' bytes={} compress={:?}",
                self.path,
                rolled_path_with(Path::new(&self.path), 1, self.roll_compress).display(),
                rolled_bytes,
                self.roll_compress
            );
        }
        Ok(())
    }

    /// 历史文件轮转 + （可选）归档压缩。
    async fn rotate_archives(&mut self) -> SinkResult<()> {
        let path = PathBuf::from(&self.path);
        if self.max_rolls == 0 {
            remove_if_exists(&path).map_err(|e| sink_err("file roll fail", e))?;
            return Ok(());
        }
        shift_archives(&path, self.max_rolls).map_err(|e| sink_err("file roll fail", e))?;

        if self.roll_compress == RollCompress::None {
            return rename_if_exists(&path, &rolled_path_with(&path, 1, RollCompress::None))
                .map_err(|e| sink_err("file roll fail", e));
        }

        let compress = self.roll_compress;
        let dst = rolled_path_with(&path, 1, compress);
        let (src_bg, dst_bg) = (path.clone(), dst.clone());
        // 大文件压缩是阻塞 IO，放到 blocking 线程池，不卡异步运行时
        let result =
            tokio::task::spawn_blocking(move || archive_file(&src_bg, &dst_bg, compress)).await;
        let plain_fallback = |cause: &dyn std::fmt::Display| -> SinkResult<()> {
            // 压缩失败不能丢数据：回退为不压缩归档
            log::warn!(
                "file sink compress rolled file '{}' failed: {}; fallback to plain archive",
                dst.display(),
                cause
            );
            let _ = remove_if_exists(&dst);
            rename_if_exists(&path, &rolled_path_with(&path, 1, RollCompress::None))
                .map_err(|e| sink_err("file roll fallback fail", e))
        };
        match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => plain_fallback(&err),
            Err(err) => plain_fallback(&err),
        }
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

    /// 统一写入入口：有 codec 先编码再写文件；超过单文件上限时先滚动。
    async fn write_bytes(&mut self, data: &[u8]) -> SinkResult<()> {
        let mut encoded = Vec::new();
        let payload: &[u8] = match &mut self.codec {
            Some(codec) => {
                codec
                    .encode(data, &mut encoded)
                    .map_err(|e| sink_err("codec encode fail", e))?;
                &encoded
            }
            None => data,
        };
        if !payload.is_empty() {
            if self.should_roll(payload.len() as u64) {
                self.rotate().await?;
            }
            self.file_mut()?
                .write_all(payload)
                .await
                .map_err(|e| sink_err("file out fail", e))?;
            self.written = self.written.saturating_add(payload.len() as u64);
        }
        if self.sync {
            self.file_mut()?
                .sync_all()
                .await
                .map_err(|e| sink_err("file sync fail", e))?;
            record_sync_all_call();
        }
        Ok(())
    }
}

#[async_trait]
impl AsyncCtrl for AsyncFileSink {
    async fn stop(&mut self) -> SinkResult<()> {
        // 冲刷 codec 尾部（压缩层未达块阈值的残留），否则丢末段数据
        if let Some(codec) = &mut self.codec {
            let mut tail = Vec::new();
            codec
                .finish(&mut tail)
                .map_err(|e| sink_err("codec finish fail", e))?;
            if !tail.is_empty() {
                self.file_mut()?
                    .write_all(&tail)
                    .await
                    .map_err(|e| sink_err("file out fail", e))?;
            }
        }
        self.file_mut()?
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
        self.write_bytes(data).await
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

        self.write_bytes(&buffer).await
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

        self.write_bytes(&buffer).await
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

    async fn sink_records_with_meta(
        &mut self,
        meta: wp_connector_api::BatchMeta,
        data: Vec<std::sync::Arc<DataRecord>>,
    ) -> SinkResult<()> {
        let data = wp_connector_utils::batch::inject_oml_name(&meta, data);
        self.sink_records(data).await
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

/// Internal writer state, dispatching on `data_format`.
enum ArrowFileWriter {
    /// Legacy `arrow_ipc` mode: lazily-created `StreamWriter` (opened on first
    /// write with the inferred schema).
    Stream(Box<Option<StreamWriter<BufWriter<std::fs::File>>>>),
    /// `arrow_framed` mode: batches buffered in memory, flushed on `stop()`.
    FramedBuf {
        batches: Vec<arrow::record_batch::RecordBatch>,
        /// Tag to use when encoding the frame; updated by `sink_records_with_meta`.
        tag: String,
    },
}

pub struct ArrowFileSink {
    path: String,
    writer: Mutex<ArrowFileWriter>,
    schema: Mutex<Option<Arc<arrow::datatypes::Schema>>>,
    sync: bool,
    sent_cnt: u64,
    /// Whether to produce wp_arrow frames (`arrow_framed`).
    framed: bool,
}

impl ArrowFileSink {
    pub fn new(path: &str, sync: bool) -> SinkResult<Self> {
        Self::with_format(path, sync, false, "")
    }

    /// Create with explicit `data_format`.
    ///
    /// When `framed` is true, batches are buffered in memory and written as a
    /// single wp_arrow frame (`[tag_len][tag][IPC]`) on `stop()`. Otherwise the
    /// legacy `arrow_ipc` behaviour is used (continuous `StreamWriter` append).
    ///
    /// **Durability note for `framed` mode:** all data stays in memory until
    /// `stop()` is called. If the process crashes before `stop()`, no data will
    /// be persisted. For incremental durability, use `arrow_ipc` mode with
    /// `sync = true`.
    pub fn with_format(path: &str, sync: bool, framed: bool, tag: &str) -> SinkResult<Self> {
        if let Some(parent) = std::path::Path::new(path).parent()
            && !parent.exists()
        {
            fs::create_dir_all(parent).source_err(SinkReason::Sink, "create output dir")?;
        }
        let writer = if framed {
            ArrowFileWriter::FramedBuf {
                batches: Vec::new(),
                tag: tag.to_string(),
            }
        } else {
            // Stream mode: lazily created on first write.
            ArrowFileWriter::Stream(Box::new(None))
        };
        Ok(Self {
            path: path.to_string(),
            writer: Mutex::new(writer),
            schema: Mutex::new(None),
            sync,
            sent_cnt: 0,
            framed,
        })
    }

    /// Ensure the schema is inferred (first-write), returning the schema.
    /// For Stream mode, lazily creates the `StreamWriter` with the real schema.
    async fn ensure_schema(
        &self,
        record: &DataRecord,
    ) -> SinkResult<Arc<arrow::datatypes::Schema>> {
        let mut schema_guard = self.schema.lock().await;
        if schema_guard.is_none() {
            let schema = Arc::new(infer_schema_from_record(record));
            *schema_guard = Some(Arc::clone(&schema));
            // For Stream mode, create the writer lazily with the real schema.
            if !self.framed {
                let mut writer_guard = self.writer.lock().await;
                if let ArrowFileWriter::Stream(opt) = &mut *writer_guard
                    && opt.is_none()
                {
                    let file = std::fs::OpenOptions::new()
                        .append(true)
                        .create(true)
                        .open(&self.path)
                        .source_err(SinkReason::Sink, "open arrow output file")?;
                    let writer = StreamWriter::try_new(BufWriter::new(file), &schema)
                        .source_raw_err(SinkReason::Sink, "create arrow stream writer")?;
                    **opt = Some(writer);
                }
            }
        }
        Ok(Arc::clone(schema_guard.as_ref().unwrap()))
    }

    /// Write a batch to the `Stream`-mode writer, flushing and optionally syncing.
    fn write_stream_batch(
        writer_opt: &mut Option<StreamWriter<BufWriter<std::fs::File>>>,
        batch: &arrow::record_batch::RecordBatch,
        sync: bool,
    ) -> SinkResult<()> {
        let writer = writer_opt.as_mut().unwrap();
        writer
            .write(batch)
            .map_err(|e| sink_err("arrow_file write batch fail", e))?;
        writer
            .flush()
            .map_err(|e| sink_err("arrow_file flush fail", e))?;
        if sync {
            writer
                .get_mut()
                .get_mut()
                .sync_all()
                .map_err(|e| sink_err("arrow_file sync fail", e))?;
        }
        Ok(())
    }
}

#[async_trait]
impl AsyncRecordSink for ArrowFileSink {
    async fn sink_record(&mut self, data: &DataRecord) -> SinkResult<()> {
        let schema = self.ensure_schema(data).await?;
        let batch = data_record_to_batch(data, &schema)?;

        if self.framed {
            let mut writer_guard = self.writer.lock().await;
            if let ArrowFileWriter::FramedBuf { batches, .. } = &mut *writer_guard {
                batches.push(batch);
            }
        } else {
            let mut writer_guard = self.writer.lock().await;
            if let ArrowFileWriter::Stream(writer_opt) = &mut *writer_guard {
                Self::write_stream_batch(writer_opt.as_mut(), &batch, self.sync)?;
            }
        }

        self.sent_cnt = self.sent_cnt.saturating_add(1);
        if self.sent_cnt == 1 {
            log::info!("arrow_file sink first-send: rows=1 framed={}", self.framed);
        }
        Ok(())
    }

    async fn sink_records(&mut self, data: Vec<Arc<DataRecord>>) -> SinkResult<()> {
        if data.is_empty() {
            return Ok(());
        }
        let row_count = data.len();
        let schema = self.ensure_schema(&data[0]).await?;
        let batch = data_records_to_batch(&data, &schema)?;

        if self.framed {
            let mut writer_guard = self.writer.lock().await;
            if let ArrowFileWriter::FramedBuf { batches, .. } = &mut *writer_guard {
                batches.push(batch);
            }
        } else {
            let mut writer_guard = self.writer.lock().await;
            if let ArrowFileWriter::Stream(writer_opt) = &mut *writer_guard {
                Self::write_stream_batch(writer_opt.as_mut(), &batch, self.sync)?;
            }
        }

        if self.sent_cnt == 0 {
            let schema_guard = self.schema.lock().await;
            log::info!(
                "arrow_file sink first-send: rows={} cols={} framed={}",
                row_count,
                schema_guard.as_ref().map(|s| s.fields().len()).unwrap_or(0),
                self.framed,
            );
        }
        self.sent_cnt = self.sent_cnt.saturating_add(1);
        Ok(())
    }

    async fn sink_records_with_meta(
        &mut self,
        meta: wp_connector_api::BatchMeta,
        data: Vec<std::sync::Arc<DataRecord>>,
    ) -> SinkResult<()> {
        if self.framed {
            // Update the FramedBuf tag if meta provides an oml_name
            if let Some(name) = meta.oml_name()
                && !name.is_empty()
            {
                let mut writer_guard = self.writer.lock().await;
                if let ArrowFileWriter::FramedBuf { tag, .. } = &mut *writer_guard {
                    *tag = name.to_string();
                }
            }
        }
        self.sink_records(data).await
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
        let mut writer_guard = self.writer.lock().await;
        match &mut *writer_guard {
            ArrowFileWriter::Stream(writer_opt) => {
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
            }
            ArrowFileWriter::FramedBuf { batches, tag } => {
                let payload = encode_ipc_frame_multi(tag, batches)?;
                std::fs::write(&self.path, &payload)
                    .source_err(SinkReason::Sink, "arrow_file write framed")?;
                if self.sync {
                    let file = std::fs::File::open(&self.path)
                        .source_err(SinkReason::Sink, "arrow_file open for sync")?;
                    file.sync_all()
                        .map_err(|e| sink_err("arrow_file sync on stop fail", e))?;
                }
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

    /// file sink 的 `compression` + `encryption`：写入小于压缩块阈值的多行，`stop()` 冲刷尾部，
    /// 读回字节后经 `build_decoder` 还原明文；密文里不应出现明文。
    #[tokio::test(flavor = "multi_thread")]
    async fn file_sink_encrypt_compress_roundtrip() -> anyhow::Result<()> {
        use wp_connector_api::{AsyncCtrl, AsyncRawDataSink};
        use wp_connector_utils::codec::{
            Cipher, CompressConfig, CompressionAlgo, EncryptConfig, build_decoder, build_encoder,
        };

        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("wp_file_codec_{}.dat", ts));

        let compress = CompressConfig {
            algo: CompressionAlgo::Zstd,
            level: 3,
        };
        let encrypt = EncryptConfig {
            cipher: Cipher::Aes256Gcm,
            key: (0u8..32).collect(),
        };
        let encoder = build_encoder(Some(&compress), Some(&encrypt)).unwrap();
        let mut sink = AsyncFileSink::new(path.to_string_lossy().as_ref())
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .with_codec(Some(encoder));

        AsyncRawDataSink::sink_str(&mut sink, "hello codec")
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        AsyncRawDataSink::sink_str(&mut sink, "second line")
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        AsyncCtrl::stop(&mut sink)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        let wire = fs::read(&path)?;
        assert!(!wire.is_empty(), "encoded file should not be empty");
        assert!(
            !wire
                .windows(b"hello codec".len())
                .any(|w| w == b"hello codec"),
            "ciphertext should not contain plaintext"
        );

        let mut decoder = build_decoder(Some(&compress), Some(&encrypt)).unwrap();
        let mut plain = Vec::new();
        decoder
            .decode(&wire, &mut plain)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        decoder
            .finish(&mut plain)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let text = String::from_utf8_lossy(&plain);
        assert!(text.contains("hello codec\n"));
        assert!(text.contains("second line\n"));

        let _ = fs::remove_file(&path);
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

    fn tmp_dir(label: &str) -> PathBuf {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("wp_file_{label}_{ts}"));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn mk_file_spec(params: &[(&str, serde_json::Value)]) -> ResolvedSinkSpec {
        let mut map = wp_connector_api::ParamMap::new();
        for (k, v) in params {
            map.insert((*k).to_string(), v.clone());
        }
        ResolvedSinkSpec {
            group: "g".into(),
            name: "file_demo".into(),
            kind: "file".into(),
            connector_id: "file_json_sink".into(),
            params: map,
            filter: None,
        }
    }

    /// 无归档压缩时的历史文件路径。
    fn rolled(path: &Path, index: u32) -> PathBuf {
        rolled_path_with(path, index, RollCompress::None)
    }

    #[test]
    fn file_spec_parses_roll_params() {
        let spec = mk_file_spec(&[
            ("max_size", serde_json::json!("10MB")),
            ("max_rolls", serde_json::json!(5)),
        ]);
        let resolved = FileSinkSpec::from_resolved("file", &spec).expect("valid roll params");
        assert_eq!(resolved.max_size(), Some(10 * 1024 * 1024));
        assert_eq!(resolved.max_rolls(), 5);

        // 未配置容量：不限制，滚动个数仍是默认值
        let plain = FileSinkSpec::from_resolved("file", &mk_file_spec(&[])).unwrap();
        assert_eq!(plain.max_size(), None);
        assert_eq!(plain.max_rolls(), DEFAULT_MAX_ROLLS);

        // max_size = 0 等价于不限制
        let unlimited = FileSinkSpec::from_resolved(
            "file",
            &mk_file_spec(&[("max_size", serde_json::json!(0))]),
        )
        .unwrap();
        assert_eq!(unlimited.max_size(), None);
    }

    #[test]
    fn file_spec_rejects_invalid_roll_params() {
        let bad_size = FileSinkSpec::from_resolved(
            "file",
            &mk_file_spec(&[("max_size", serde_json::json!("10XB"))]),
        );
        assert!(bad_size.is_err(), "unknown size unit should be rejected");

        let no_size = FileSinkSpec::from_resolved(
            "file",
            &mk_file_spec(&[("max_rolls", serde_json::json!(3))]),
        );
        assert!(
            no_size.is_err(),
            "max_rolls without max_size should be rejected"
        );

        let too_many = FileSinkSpec::from_resolved(
            "file",
            &mk_file_spec(&[
                ("max_size", serde_json::json!(1024)),
                ("max_rolls", serde_json::json!(MAX_ROLLS_LIMIT + 1)),
            ]),
        );
        assert!(too_many.is_err(), "max_rolls over limit should be rejected");
    }

    #[test]
    fn rolled_path_keeps_lock_suffix_last() {
        assert_eq!(
            rolled(Path::new("/tmp/a/out.dat"), 1),
            PathBuf::from("/tmp/a/out.dat.1")
        );
        assert_eq!(
            rolled(Path::new("/tmp/a/out.dat.lock"), 2),
            PathBuf::from("/tmp/a/out.dat.2.lock")
        );
        assert_eq!(
            rolled_path_with(Path::new("/tmp/a/out.dat.lock"), 3, RollCompress::Gzip),
            PathBuf::from("/tmp/a/out.dat.3.gz.lock")
        );
        assert_eq!(
            rolled_path_with(Path::new("/tmp/a/out.dat"), 1, RollCompress::Zip),
            PathBuf::from("/tmp/a/out.dat.1.zip")
        );
    }

    /// 8 条 11 字节记录、上限 25 字节：每个文件 2 条，滚动 3 次后只保留 `.1`~`.3`。
    #[tokio::test(flavor = "multi_thread")]
    async fn file_sink_rolls_by_size_and_keeps_max_rolls() -> anyhow::Result<()> {
        use wp_connector_api::{AsyncCtrl, AsyncRawDataSink};

        let dir = tmp_dir("roll_size");
        let path = dir.join("roll.dat");
        let mut sink = AsyncFileSink::with_sync(path.to_string_lossy().as_ref(), false)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .with_roll(Some(25), 3);

        for _ in 0..8 {
            AsyncRawDataSink::sink_str(&mut sink, "0123456789")
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
        }
        AsyncCtrl::stop(&mut sink)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        assert_eq!(fs::read_to_string(&path)?.lines().count(), 2);
        let mut total = 2;
        for idx in 1..=3 {
            let archived = rolled(&path, idx);
            let body = fs::read_to_string(&archived)
                .map_err(|e| anyhow::anyhow!("read {archived:?}: {e}"))?;
            assert_eq!(body.lines().count(), 2, "{archived:?} should hold 2 lines");
            total += 2;
        }
        assert!(!rolled(&path, 4).exists(), "keep only max_rolls files");
        assert_eq!(total, 8, "no records lost across rolls");

        let _ = fs::remove_dir_all(&dir);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn file_sink_without_max_size_never_rolls() -> anyhow::Result<()> {
        use wp_connector_api::{AsyncCtrl, AsyncRawDataSink};

        let dir = tmp_dir("no_roll");
        let path = dir.join("flat.dat");
        let mut sink = AsyncFileSink::new(path.to_string_lossy().as_ref())
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        for _ in 0..50 {
            AsyncRawDataSink::sink_str(&mut sink, "0123456789")
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
        }
        AsyncCtrl::stop(&mut sink)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        assert_eq!(fs::read_to_string(&path)?.lines().count(), 50);
        assert!(!rolled(&path, 1).exists());

        let _ = fs::remove_dir_all(&dir);
        Ok(())
    }

    /// 滚动时重建加密流：每个文件都必须能独立解密，且不出现明文。
    #[tokio::test(flavor = "multi_thread")]
    async fn file_sink_roll_restarts_codec_stream() -> anyhow::Result<()> {
        use wp_connector_api::{AsyncCtrl, AsyncRawDataSink};
        use wp_connector_utils::codec::Cipher;

        let dir = tmp_dir("roll_codec");
        let path = dir.join("cipher.dat");
        let mut params = wp_connector_api::ParamMap::new();
        params.insert(
            "encryption".into(),
            serde_json::json!({"enabled": true, "algo": "aes-256-gcm", "key": "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"}),
        );
        let cfg = CodecConfig::from_params(&params)?;
        let mut sink = AsyncFileSink::with_sync(path.to_string_lossy().as_ref(), false)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .with_roll(Some(120), 5)
            .with_codec_config(cfg.clone())
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        for i in 0..12 {
            AsyncRawDataSink::sink_str(&mut sink, &format!("record-{i:02}"))
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
        }
        AsyncCtrl::stop(&mut sink)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        let encrypt = wp_connector_utils::codec::EncryptConfig {
            cipher: Cipher::Aes256Gcm,
            key: (0u8..32).collect(),
        };
        let mut seen = Vec::new();
        for file in [
            path.clone(),
            rolled(&path, 1),
            rolled(&path, 2),
            rolled(&path, 3),
            rolled(&path, 4),
            rolled(&path, 5),
        ] {
            let wire = fs::read(&file)?;
            assert!(!wire.is_empty(), "{file:?} should not be empty");
            assert!(
                !wire.windows(b"record-".len()).any(|w| w == b"record-"),
                "{file:?} should be ciphertext"
            );
            let mut decoder = wp_connector_utils::codec::build_decoder(None, Some(&encrypt))?;
            let mut plain = Vec::new();
            decoder.decode(&wire, &mut plain)?;
            decoder.finish(&mut plain)?;
            let text = String::from_utf8_lossy(&plain);
            seen.extend(text.lines().map(|s| s.to_string()));
        }
        seen.sort();
        let expected: Vec<String> = (0..12).map(|i| format!("record-{i:02}")).collect();
        assert_eq!(seen, expected);

        let _ = fs::remove_dir_all(&dir);
        Ok(())
    }

    #[test]
    fn file_spec_parses_roll_compress() {
        let spec = |value: serde_json::Value| {
            mk_file_spec(&[
                ("max_size", serde_json::json!(1024)),
                ("roll_compress", value),
            ])
        };

        let parse = |value: serde_json::Value| {
            FileSinkSpec::from_resolved("file", &spec(value))
                .map(|s| s.roll_compress())
                .map_err(|_| ())
        };

        assert_eq!(parse(serde_json::json!("gzip")), Ok(RollCompress::Gzip));
        assert_eq!(parse(serde_json::json!("GZ")), Ok(RollCompress::Gzip));
        assert_eq!(parse(serde_json::json!("zip")), Ok(RollCompress::Zip));
        assert_eq!(parse(serde_json::json!("none")), Ok(RollCompress::None));
        assert_eq!(parse(serde_json::json!(true)), Ok(RollCompress::Gzip));
        assert_eq!(parse(serde_json::json!(false)), Ok(RollCompress::None));
        assert_eq!(parse(serde_json::json!("rar")), Err(()));

        // 未开启滚动时配置归档压缩属于配置错误
        let no_size = FileSinkSpec::from_resolved(
            "file",
            &mk_file_spec(&[("roll_compress", serde_json::json!("gzip"))]),
        );
        assert!(no_size.is_err());

        // 默认不压缩
        let plain = FileSinkSpec::from_resolved("file", &mk_file_spec(&[])).unwrap();
        assert_eq!(plain.roll_compress(), RollCompress::None);
    }

    #[test]
    fn shift_archives_moves_each_variant() {
        let dir = tmp_dir("shift_variants");
        let active = dir.join("mix.dat");
        for variant in ROLL_VARIANTS {
            fs::File::create(rolled_path_with(&active, 1, variant)).unwrap();
        }

        shift_archives(&active, 3).unwrap();

        for variant in ROLL_VARIANTS {
            assert!(!rolled_path_with(&active, 1, variant).exists());
            assert!(
                rolled_path_with(&active, 2, variant).exists(),
                "{variant:?} archive should shift to .2"
            );
        }

        let _ = fs::remove_dir_all(&dir);
    }

    /// 归档压缩为 gzip：历史文件是能直接 `gunzip` 的 `.gz`。
    #[tokio::test(flavor = "multi_thread")]
    async fn file_sink_roll_compresses_with_gzip() -> anyhow::Result<()> {
        use std::io::Read as _;
        use wp_connector_api::{AsyncCtrl, AsyncRawDataSink};

        let dir = tmp_dir("roll_gzip");
        let path = dir.join("gz.dat");
        let mut sink = AsyncFileSink::new(path.to_string_lossy().as_ref())
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .with_roll(Some(25), 2)
            .with_roll_compress(RollCompress::Gzip);

        for _ in 0..8 {
            AsyncRawDataSink::sink_str(&mut sink, "0123456789")
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
        }
        AsyncCtrl::stop(&mut sink)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        assert_eq!(fs::read_to_string(&path)?.lines().count(), 2);
        let mut total = 2;
        for idx in 1..=2 {
            let archived = rolled_path_with(&path, idx, RollCompress::Gzip);
            assert!(archived.exists(), "missing {archived:?}");
            assert!(
                !rolled(&path, idx).exists(),
                "should not leave plain archive"
            );
            let mut text = String::new();
            flate2::read::GzDecoder::new(fs::File::open(&archived)?).read_to_string(&mut text)?;
            assert_eq!(text.lines().count(), 2);
            total += 2;
        }
        assert_eq!(total, 6, "8 records with keep=2 keeps the newest 6");

        let _ = fs::remove_dir_all(&dir);
        Ok(())
    }

    /// 归档压缩为 zip：历史文件是标准 zip，内含单个同名条目。
    #[tokio::test(flavor = "multi_thread")]
    async fn file_sink_roll_compresses_with_zip() -> anyhow::Result<()> {
        use std::io::Read as _;
        use wp_connector_api::{AsyncCtrl, AsyncRawDataSink};

        let dir = tmp_dir("roll_zip");
        let path = dir.join("zd.dat");
        let mut sink = AsyncFileSink::new(path.to_string_lossy().as_ref())
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .with_roll(Some(25), 1)
            .with_roll_compress(RollCompress::Zip);

        for _ in 0..4 {
            AsyncRawDataSink::sink_str(&mut sink, "0123456789")
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
        }
        AsyncCtrl::stop(&mut sink)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        let archived = rolled_path_with(&path, 1, RollCompress::Zip);
        assert!(archived.exists(), "missing {archived:?}");
        let mut zip = zip::ZipArchive::new(fs::File::open(&archived)?)?;
        assert_eq!(zip.len(), 1);
        let mut text = String::new();
        zip.by_index(0)?.read_to_string(&mut text)?;
        assert_eq!(text.lines().count(), 2);
        assert!(zip.by_name("zd.dat").is_ok(), "entry keeps the file name");

        let _ = fs::remove_dir_all(&dir);
        Ok(())
    }

    /// `max_rolls = 0`：滚动时直接丢弃旧文件，不留任何历史。
    #[tokio::test(flavor = "multi_thread")]
    async fn file_sink_max_rolls_zero_discards_old_file() -> anyhow::Result<()> {
        use wp_connector_api::{AsyncCtrl, AsyncRawDataSink};

        let dir = tmp_dir("roll_zero");
        let path = dir.join("zero.dat");
        let mut sink = AsyncFileSink::new(path.to_string_lossy().as_ref())
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .with_roll(Some(25), 0);

        for _ in 0..4 {
            AsyncRawDataSink::sink_str(&mut sink, "0123456789")
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
        }
        AsyncCtrl::stop(&mut sink)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        assert_eq!(fs::read_to_string(&path)?.lines().count(), 2);
        for variant in ROLL_VARIANTS {
            assert!(
                !rolled_path_with(&path, 1, variant).exists(),
                "{variant:?} archive should not exist when max_rolls = 0"
            );
        }

        let _ = fs::remove_dir_all(&dir);
        Ok(())
    }

    /// 续写已有文件时，容量从文件现有大小起算，超限先归档旧内容再写。
    #[tokio::test(flavor = "multi_thread")]
    async fn file_sink_resumes_existing_size_for_rolling() -> anyhow::Result<()> {
        use wp_connector_api::{AsyncCtrl, AsyncRawDataSink};

        let dir = tmp_dir("roll_resume");
        let path = dir.join("resume.dat");
        fs::write(&path, "0123456789\n0123456789\n")?;

        let mut sink = AsyncFileSink::new(path.to_string_lossy().as_ref())
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .with_roll(Some(25), 3);
        AsyncRawDataSink::sink_str(&mut sink, "0123456789")
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        AsyncCtrl::stop(&mut sink)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        assert_eq!(fs::read_to_string(&path)?.lines().count(), 1);
        assert_eq!(fs::read_to_string(rolled(&path, 1))?.lines().count(), 2);

        let _ = fs::remove_dir_all(&dir);
        Ok(())
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

    // ---- ArrowFileSink arrow_framed tests ----

    #[tokio::test(flavor = "multi_thread")]
    async fn arrow_file_sink_framed_write_and_read_back() {
        use crate::sources::batch::arrow::decode_arrow_framed_batches;
        use wp_connector_api::SourceEvent;
        use wp_model_core::raw::RawData;

        let path = tmp_arrow_path("framed_rt");

        // Write in framed mode
        {
            let mut sink = ArrowFileSink::with_format(
                path.to_string_lossy().as_ref(),
                false,
                true,
                "test_tag",
            )
            .unwrap();

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

        // Read back using the source-side decode
        let file_bytes = std::fs::read(&path).unwrap();
        let event = SourceEvent::new(
            1,
            "key",
            RawData::Bytes(bytes::Bytes::from(file_bytes)),
            Default::default(),
        );
        let batches = decode_arrow_framed_batches(&vec![event]).unwrap();
        assert_eq!(batches.len(), 1, "framed file should yield one batch");
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[0].num_columns(), 2);

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn arrow_file_sink_framed_single_record() {
        use crate::sources::batch::arrow::decode_arrow_framed_batches;
        use wp_connector_api::SourceEvent;
        use wp_model_core::raw::RawData;

        let path = tmp_arrow_path("framed_single");

        {
            let mut sink =
                ArrowFileSink::with_format(path.to_string_lossy().as_ref(), false, true, "tag")
                    .unwrap();

            let rec = Arc::new(DataRecord::from(vec![FieldStorage::from(
                ModelField::from_chars("x", "hello"),
            )]));
            sink.sink_record(&rec).await.unwrap();
            sink.stop().await.unwrap();
        }

        let file_bytes = std::fs::read(&path).unwrap();
        let event = SourceEvent::new(
            1,
            "key",
            RawData::Bytes(bytes::Bytes::from(file_bytes)),
            Default::default(),
        );
        let batches = decode_arrow_framed_batches(&vec![event]).unwrap();
        assert_eq!(batches[0].num_rows(), 1);

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn arrow_file_sink_framed_empty_stop_writes_header() {
        let path = tmp_arrow_path("framed_empty");

        {
            let mut sink =
                ArrowFileSink::with_format(path.to_string_lossy().as_ref(), false, true, "header")
                    .unwrap();
            // No records written — stop should still write the frame header.
            sink.stop().await.unwrap();
        }

        let file_bytes = std::fs::read(&path).unwrap();
        // Frame header: [4B tag_len=6]["header"]
        assert_eq!(&file_bytes[0..4], &6u32.to_be_bytes());
        assert_eq!(&file_bytes[4..10], b"header");
        assert_eq!(file_bytes.len(), 10, "only header, no IPC body");

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn arrow_file_sink_framed_with_sync() {
        use crate::sources::batch::arrow::decode_arrow_framed_batches;
        use wp_connector_api::SourceEvent;
        use wp_model_core::raw::RawData;

        let path = tmp_arrow_path("framed_sync");

        {
            let mut sink = ArrowFileSink::with_format(
                path.to_string_lossy().as_ref(),
                true, // sync enabled
                true,
                "sync_tag",
            )
            .unwrap();

            let rec = Arc::new(DataRecord::from(vec![FieldStorage::from(
                ModelField::from_chars("x", "synced"),
            )]));
            sink.sink_record(&rec).await.unwrap();
            // Framed mode buffers; data is not on disk until stop()
            sink.stop().await.unwrap();
        }

        let file_bytes = std::fs::read(&path).unwrap();
        let event = SourceEvent::new(
            1,
            "key",
            RawData::Bytes(bytes::Bytes::from(file_bytes)),
            Default::default(),
        );
        let batches = decode_arrow_framed_batches(&vec![event]).unwrap();
        assert_eq!(batches[0].num_rows(), 1);

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn arrow_file_sink_framed_multiple_sink_calls() {
        use crate::sources::batch::arrow::decode_arrow_framed_batches;
        use wp_connector_api::SourceEvent;
        use wp_model_core::raw::RawData;

        let path = tmp_arrow_path("framed_multi");

        {
            let mut sink =
                ArrowFileSink::with_format(path.to_string_lossy().as_ref(), false, true, "multi")
                    .unwrap();

            // Multiple sink_record calls should accumulate batches
            for val in ["a", "b", "c"] {
                let rec = Arc::new(DataRecord::from(vec![FieldStorage::from(
                    ModelField::from_chars("v", val),
                )]));
                sink.sink_record(&rec).await.unwrap();
            }
            sink.stop().await.unwrap();
        }

        let file_bytes = std::fs::read(&path).unwrap();
        let event = SourceEvent::new(
            1,
            "key",
            RawData::Bytes(bytes::Bytes::from(file_bytes)),
            Default::default(),
        );
        let batches = decode_arrow_framed_batches(&vec![event]).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3, "all three records across all batches");

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn arrow_file_sink_framed_empty_tag() {
        use crate::sources::batch::arrow::decode_arrow_framed_batches;
        use wp_connector_api::SourceEvent;
        use wp_model_core::raw::RawData;

        let path = tmp_arrow_path("framed_empty_tag");

        {
            let mut sink =
                ArrowFileSink::with_format(path.to_string_lossy().as_ref(), false, true, "")
                    .unwrap();

            let rec = Arc::new(DataRecord::from(vec![FieldStorage::from(
                ModelField::from_chars("x", "data"),
            )]));
            sink.sink_record(&rec).await.unwrap();
            sink.stop().await.unwrap();
        }

        let file_bytes = std::fs::read(&path).unwrap();
        // tag_len = 0, zero bytes for tag, then IPC stream
        assert_eq!(&file_bytes[0..4], &0u32.to_be_bytes());
        let event = SourceEvent::new(
            1,
            "key",
            RawData::Bytes(bytes::Bytes::from(file_bytes)),
            Default::default(),
        );
        let batches = decode_arrow_framed_batches(&vec![event]).unwrap();
        assert_eq!(batches[0].num_rows(), 1);

        let _ = std::fs::remove_file(&path);
    }
}
