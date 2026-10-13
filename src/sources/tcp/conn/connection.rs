use crate::net::CodecConfig;
use crate::sources::event_id::next_event_id;
use crate::sources::tcp::codec::StreamDecoder;
use crate::sources::tcp::framing::{
    FramingExtractor, FramingMode, MAX_FRAME_BYTES, MAX_LEN_DIGITS,
};
use bytes::buf::UninitSlice;
use bytes::{BufMut, Bytes, BytesMut};
use std::collections::VecDeque;
use std::io::ErrorKind;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio_rustls::server::TlsStream as ServerTlsStream;
use wp_connector_api::{SourceBatch, SourceEvent, SourceReason, SourceResult, Tags};
use wp_model_core::raw::RawData;

/// TCP 连接流：明文或 TLS（服务端）。
pub enum ConnStream {
    Plain(TcpStream),
    Tls(Box<ServerTlsStream<TcpStream>>),
}

const DEFAULT_BATCH_CAPACITY: usize = 128;
const MAX_BATCH_BYTES: usize = 64 * 1024; // soft cap; single payload may exceed but only single event allowed
const MAX_PENDING_BYTES: usize = 256 * 1024;
// Per-read capacity budget. Reading unbounded into the BytesMut (which grows
// to the socket backlog size) lets the buffer balloon to GBs under backpressure,
// after which macOS read() fails with EINVAL (os error 22). Every read is routed
// through `BoundedBufMut`, which both guarantees this much spare capacity and
// clamps a single syscall to at most this many bytes, so the buffer can't balloon.
const MAX_READ_BYTES: usize = 256 * 1024;
// When idle and buffer is large, shrink capacity to reduce RSS footprint.
// Balanced shrink thresholds：空闲时将过大的缓冲收缩到较小基线
const SHRINK_HIGH_WATER_BYTES: usize = 1024 * 1024; // 若 capacity 超过 1MiB 且 len==0 则收缩
const SHRINK_TARGET_BYTES: usize = 256 * 1024; // 收缩到 256KiB（降低扩容↔收缩抖动）

pub enum ReadOutcome {
    NoData,
    Produced(SourceBatch),
    Closed,
}

pub struct TcpConnection {
    stream: ConnStream,
    client_addr: SocketAddr,
    framing: FramingMode,
    batcher: BatchBuilder,
}

impl TcpConnection {
    fn raw_fd(&self) -> i32 {
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            match &self.stream {
                ConnStream::Plain(s) => s.as_raw_fd(),
                ConnStream::Tls(s) => s.get_ref().0.as_raw_fd(),
            }
        }
        #[cfg(not(unix))]
        {
            -1
        }
    }
}

impl Drop for TcpConnection {
    fn drop(&mut self) {
        debug_data!(
            "Dropping TCP connection {} fd={} (pending_events={} pending_bytes={})",
            self.client_addr,
            self.raw_fd(),
            self.batcher.pending_len(),
            self.batcher.pending_bytes()
        );
    }
}

struct BatchBuilder {
    buffer: BytesMut,
    base_tags: Tags,
    batch_capacity: usize,
    source_key: String,
    pending_events: VecDeque<SourceEvent>,
    pending_bytes: usize,
    max_batch_bytes: usize,
    max_pending_bytes: usize,
    codec: Option<StreamDecoder>,
}

impl TcpConnection {
    pub fn new(
        stream: ConnStream,
        client_addr: SocketAddr,
        framing: FramingMode,
        base_tags: Tags,
        tcp_recv_bytes: usize,
        source_key: String,
        codec: CodecConfig,
    ) -> Self {
        let capacity = tcp_recv_bytes.max(1024);
        let stream_decoder = match StreamDecoder::new(&codec) {
            Ok(sd) => sd,
            Err(e) => {
                // 配置已在 from_params 校验过，此处失败属异常；降级为不启用 codec 并告警。
                log::error!("tcp source codec init failed (codec disabled): {e}");
                None
            }
        };
        let conn = Self {
            stream,
            client_addr,
            framing,
            batcher: BatchBuilder::new(
                BytesMut::with_capacity(capacity),
                base_tags,
                source_key,
                DEFAULT_BATCH_CAPACITY,
                MAX_BATCH_BYTES,
                MAX_PENDING_BYTES,
                stream_decoder,
            ),
        };
        debug_data!(
            "Created TCP connection {} fd={}",
            conn.client_addr,
            conn.raw_fd()
        );
        conn
    }

    pub fn try_read_batch(&mut self) -> SourceResult<ReadOutcome> {
        let mut produced = SourceBatch::with_capacity(self.batcher.batch_capacity);
        let mut produced_bytes = 0usize;
        self.batcher
            .fill_batch_from_pending(&mut produced, &mut produced_bytes);
        if !produced.is_empty() {
            return Ok(ReadOutcome::Produced(produced));
        }
        // F1：先把上次因 batch cap 未取完、仍留在 buffer 中的完整帧取出，
        // 否则对端静默时这些帧会一直滞留到下次成功读或 EOF。
        self.batcher.drain_messages(
            self.framing,
            self.client_addr.ip(),
            &mut produced,
            &mut produced_bytes,
        )?;
        if !produced.is_empty() {
            return Ok(ReadOutcome::Produced(produced));
        }
        loop {
            match self.batcher.bounded_try_read(&self.stream) {
                Ok(0) => {
                    // EOF — drain any frames still buffered or pending before
                    // closing. `drain_messages` breaks on a batch byte/capacity
                    // cap, leaving the tail of the stream in the buffer; if we
                    // close now, those events are silently dropped (a single
                    // oversized frame like a big conn batch can leave the
                    // smaller auth/dns frames behind).
                    // F3：EOF 尾处理（Line 一律；Auto 在非长度前缀起点时也按行交付）。
                    self.flush_eof_line();
                    self.batcher.drain_messages(
                        self.framing,
                        self.client_addr.ip(),
                        &mut produced,
                        &mut produced_bytes,
                    )?;
                    self.batcher
                        .fill_batch_from_pending(&mut produced, &mut produced_bytes);
                    if !produced.is_empty() {
                        return Ok(ReadOutcome::Produced(produced));
                    }
                    info_data!(
                        "TCP conn {} try_read returned EOF (pending_events={} pending_bytes={})",
                        self.client_addr,
                        self.batcher.pending_len(),
                        self.batcher.pending_bytes()
                    );
                    return Ok(ReadOutcome::Closed);
                }
                Ok(n) => {
                    trace_data!(
                        "TCP conn {} try_read read {}B (pending_before={} bytes_before={})",
                        self.client_addr,
                        n,
                        self.batcher.pending_len(),
                        self.batcher.pending_bytes()
                    );
                    self.batcher.drain_messages(
                        self.framing,
                        self.client_addr.ip(),
                        &mut produced,
                        &mut produced_bytes,
                    )?;
                    if !produced.is_empty() {
                        return Ok(ReadOutcome::Produced(produced));
                    }
                    continue;
                }
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                    if produced.is_empty() {
                        // No immediate data; opportunistically shrink buffer if idle
                        self.batcher.maybe_shrink();
                        return Ok(ReadOutcome::NoData);
                    } else {
                        return Ok(ReadOutcome::Produced(produced));
                    }
                }
                Err(e) => {
                    // 读错即注销连接。无需在此 drain 或交付已解码帧：任何可交付帧要么在
                    // 进入读循环前（顶部 fill/drain），要么在循环内 `Ok(n)` 臂被 drain 并
                    // 立即返回；能走到这里时 `pending` 已空、`buffer` 只剩半帧。
                    return Err(SourceReason::disconnect(format!(
                        "tcp read error ({}): {}",
                        self.client_addr, e
                    )));
                }
            }
        }
    }

    /// 异步读取一块数据：Plain 走 readable + 零拷贝；Tls 走 read_buf。
    async fn read_chunk(&mut self) -> std::io::Result<usize> {
        match &mut self.stream {
            ConnStream::Plain(s) => {
                s.readable().await?;
                self.batcher.try_read_plain(s)
            }
            ConnStream::Tls(s) => self.batcher.tls_read(s).await,
        }
    }

    pub async fn read_batch(&mut self) -> SourceResult<ReadOutcome> {
        let mut produced = SourceBatch::with_capacity(self.batcher.batch_capacity);
        let mut produced_bytes = 0usize;
        self.batcher
            .fill_batch_from_pending(&mut produced, &mut produced_bytes);
        if !produced.is_empty() {
            return Ok(ReadOutcome::Produced(produced));
        }
        // F1：先取出 buffer 中上次未取完的完整帧（见 try_read_batch）。
        self.batcher.drain_messages(
            self.framing,
            self.client_addr.ip(),
            &mut produced,
            &mut produced_bytes,
        )?;
        if !produced.is_empty() {
            return Ok(ReadOutcome::Produced(produced));
        }
        loop {
            match self.read_chunk().await {
                Ok(0) => {
                    // EOF — drain any frames still buffered or pending before
                    // closing (see try_read_batch: a batch-cap break leaves the
                    // tail of the stream unprocessed).
                    // F3：EOF 尾处理（同 try_read_batch）。
                    self.flush_eof_line();
                    self.batcher.drain_messages(
                        self.framing,
                        self.client_addr.ip(),
                        &mut produced,
                        &mut produced_bytes,
                    )?;
                    self.batcher
                        .fill_batch_from_pending(&mut produced, &mut produced_bytes);
                    if !produced.is_empty() {
                        return Ok(ReadOutcome::Produced(produced));
                    }
                    info_data!(
                        "TCP conn {} blocking read returned EOF (pending_events={} pending_bytes={})",
                        self.client_addr,
                        self.batcher.pending_len(),
                        self.batcher.pending_bytes()
                    );
                    return Ok(ReadOutcome::Closed);
                }
                Ok(n) => {
                    trace_data!(
                        "TCP conn {} blocking read read {}B (pending_before={} bytes_before={})",
                        self.client_addr,
                        n,
                        self.batcher.pending_len(),
                        self.batcher.pending_bytes()
                    );
                    self.batcher.drain_messages(
                        self.framing,
                        self.client_addr.ip(),
                        &mut produced,
                        &mut produced_bytes,
                    )?;
                    if !produced.is_empty() {
                        return Ok(ReadOutcome::Produced(produced));
                    }
                }
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                    if produced.is_empty() {
                        self.batcher.maybe_shrink();
                    }
                    continue;
                }
                Err(e) => {
                    // 读错即注销连接。原因同 `try_read_batch`：可交付帧早在顶部或
                    // `Ok(n)` 臂被 drain 并返回，走到此处 `pending` 为空、`buffer` 只剩半帧。
                    return Err(SourceReason::disconnect(format!(
                        "tcp read error ({}): {}",
                        self.client_addr, e
                    )));
                }
            }
        }
    }

    /// F3：EOF 尾处理。`Line` 一律；`Auto` 在残余**不是**长度前缀起点时也按行交付；
    /// `Len` 的残余是无法补全的不完整帧，不处理。
    fn flush_eof_line(&mut self) {
        let line_like = match self.framing {
            FramingMode::Line => true,
            FramingMode::Auto => !self.batcher.starts_with_length_prefix(),
            FramingMode::Len => false,
        };
        if line_like {
            self.batcher.append_eof_line_delimiter();
        }
    }

    pub fn client_ip(&self) -> IpAddr {
        self.client_addr.ip()
    }

    pub fn pending_len(&self) -> usize {
        self.batcher.pending_len()
    }

    pub fn pending_bytes(&self) -> usize {
        self.batcher.pending_bytes()
    }

    pub fn has_pending(&self) -> bool {
        self.batcher.pending_len() > 0
    }
}

/// 把一次 `BufMut` 写入限制在至多 `limit` 字节的适配器。
///
/// `try_read_buf`（明文）与 `read_buf`（TLS）都会把 `chunk_mut()` 给出的**全部**空闲
/// 容量作为单次 syscall 的目标，而 `BytesMut::remaining_mut()` 恒为 `isize::MAX - len`
/// （永远超大），无法用来判断容量。故这里：① 保证至少有 `limit` 空闲容量；② 只向读取
/// 方暴露 ≤ `limit` 的空闲容量，把单次 syscall 夹到 `MAX_READ_BYTES`，避免缓冲在背压下膨胀。
struct BoundedBufMut<'a> {
    inner: &'a mut BytesMut,
    remaining: usize,
}

impl<'a> BoundedBufMut<'a> {
    fn new(inner: &'a mut BytesMut, limit: usize) -> Self {
        if inner.spare_capacity_mut().len() < limit {
            inner.reserve(limit);
        }
        let remaining = inner.spare_capacity_mut().len().min(limit);
        Self { inner, remaining }
    }
}

unsafe impl BufMut for BoundedBufMut<'_> {
    fn remaining_mut(&self) -> usize {
        self.remaining
    }

    fn chunk_mut(&mut self) -> &mut UninitSlice {
        let spare = self.inner.spare_capacity_mut();
        let cap = spare.len().min(self.remaining);
        UninitSlice::uninit(&mut spare[..cap])
    }

    unsafe fn advance_mut(&mut self, cnt: usize) {
        // SAFETY: 读取方保证 `cnt <= chunk_mut().len() <= remaining`，且这 `cnt` 个字节已写入。
        unsafe { self.inner.advance_mut(cnt) };
        self.remaining = self.remaining.saturating_sub(cnt);
    }
}

impl BatchBuilder {
    fn new(
        buffer: BytesMut,
        base_tags: Tags,
        source_key: String,
        batch_capacity: usize,
        max_batch_bytes: usize,
        max_pending_bytes: usize,
        codec: Option<StreamDecoder>,
    ) -> Self {
        Self {
            buffer,
            base_tags,
            batch_capacity,
            source_key,
            pending_events: VecDeque::new(),
            pending_bytes: 0,
            max_batch_bytes,
            max_pending_bytes,
            codec,
        }
    }

    /// Bounded direct read into the internal `buffer` (no staging copy).
    ///
    /// v0.8.2 read straight into the BytesMut via `try_read_buf`, which was
    /// fast; v0.8.3 moved to a fixed staging buffer + copy to keep the buffer
    /// bounded (macOS read() EINVAL), which regressed TCP throughput ~2.6x
    /// (50.6万/s → 19.4万/s in the parse_to_blackhole benchmark). This keeps
    /// the direct zero-copy read path but reserves at least `MAX_READ_BYTES` of
    /// spare capacity before each read, so a single read can't balloon the
    /// buffer to GBs under a large socket backlog.
    fn bounded_try_read(&mut self, stream: &ConnStream) -> std::io::Result<usize> {
        match stream {
            ConnStream::Plain(s) => self.try_read_plain(s),
            ConnStream::Tls(_) => Err(std::io::Error::from(ErrorKind::WouldBlock)),
        }
    }

    fn try_read_plain(&mut self, stream: &TcpStream) -> std::io::Result<usize> {
        let mut bounded = BoundedBufMut::new(&mut self.buffer, MAX_READ_BYTES);
        stream.try_read_buf(&mut bounded)
    }

    async fn tls_read(
        &mut self,
        stream: &mut ServerTlsStream<TcpStream>,
    ) -> std::io::Result<usize> {
        let mut bounded = BoundedBufMut::new(&mut self.buffer, MAX_READ_BYTES);
        stream.read_buf(&mut bounded).await
    }

    /// Opportunistically shrink the internal buffer when idle to reclaim memory.
    fn maybe_shrink(&mut self) {
        if self.buffer.is_empty() && self.buffer.capacity() > SHRINK_HIGH_WATER_BYTES {
            // Recreate with a smaller baseline capacity to actually release memory.
            self.buffer = BytesMut::with_capacity(SHRINK_TARGET_BYTES);
        }
    }

    fn fill_batch_from_pending(&mut self, batch: &mut SourceBatch, produced_bytes: &mut usize) {
        while let Some(event) = self.pending_events.pop_front() {
            let event_size = event_payload_len(&event);
            self.pending_bytes = self.pending_bytes.saturating_sub(event_size);
            let would_exceed = *produced_bytes + event_size > self.max_batch_bytes;
            if batch.len() >= self.batch_capacity {
                self.push_pending_front(event, event_size);
                break;
            }
            if would_exceed && !batch.is_empty() {
                debug_data!(
                    "TCP source '{}' batch hit byte cap: current_bytes={} event_size={} limit={} pending_requeue={}",
                    self.source_key,
                    produced_bytes,
                    event_size,
                    self.max_batch_bytes,
                    self.pending_events.len() + 1
                );
                self.push_pending_front(event, event_size);
                break;
            }
            *produced_bytes = produced_bytes.saturating_add(event_size);
            batch.push(event);
            if would_exceed {
                break;
            }
        }
    }

    fn drain_messages(
        &mut self,
        framing: FramingMode,
        peer_ip: IpAddr,
        batch: &mut SourceBatch,
        produced_bytes: &mut usize,
    ) -> SourceResult<()> {
        if self.pending_bytes >= self.max_pending_bytes {
            debug_data!(
                "TCP source '{}' stop draining buffer on pending byte cap: pending_events={} pending_bytes={} cap={}",
                self.source_key,
                self.pending_events.len(),
                self.pending_bytes,
                self.max_pending_bytes
            );
            return Ok(());
        }
        // 若启用 codec：先把 buffer 里的完整帧解码成明文（不完整帧留在 buffer）
        if let Some(codec) = &mut self.codec {
            codec.decode(&mut self.buffer)?;
        }
        loop {
            // 从明文（codec）或原始（无 codec）缓冲取一条消息；借用只在本块内有效，
            // 避免与后续 `self.build_event`/`push_pending_back` 的借用冲突。
            let payload = {
                let buf = if let Some(codec) = self.codec.as_mut() {
                    codec.plain_mut()
                } else {
                    &mut self.buffer
                };
                extract_message(framing, buf)
            };
            let Some(payload) = payload else { break };
            let event = self.build_event(payload, peer_ip);
            let event_size = event_payload_len(&event);
            let would_exceed = *produced_bytes + event_size > self.max_batch_bytes;
            if batch.len() >= self.batch_capacity {
                self.push_pending_back(event, event_size);
                if self.pending_bytes >= self.max_pending_bytes {
                    debug_data!(
                        "TCP source '{}' pending byte cap reached after batch spill: pending_events={} pending_bytes={} cap={}",
                        self.source_key,
                        self.pending_events.len(),
                        self.pending_bytes,
                        self.max_pending_bytes
                    );
                }
                break;
            }
            if would_exceed && !batch.is_empty() {
                debug_data!(
                    "TCP source '{}' batch hit byte cap while draining buffer: current_bytes={} event_size={} limit={} pending_after={}",
                    self.source_key,
                    produced_bytes,
                    event_size,
                    self.max_batch_bytes,
                    self.pending_events.len()
                );
                self.push_pending_back(event, event_size);
                if self.pending_bytes >= self.max_pending_bytes {
                    debug_data!(
                        "TCP source '{}' pending byte cap reached after byte-budget spill: pending_events={} pending_bytes={} cap={}",
                        self.source_key,
                        self.pending_events.len(),
                        self.pending_bytes,
                        self.max_pending_bytes
                    );
                }
                break;
            }
            *produced_bytes = produced_bytes.saturating_add(event_size);
            batch.push(event);
            if would_exceed {
                debug_data!(
                    "TCP source '{}' batch reached byte cap after push: total_bytes={} events={} limit={} pending_after={}",
                    self.source_key,
                    produced_bytes,
                    batch.len(),
                    self.max_batch_bytes,
                    self.pending_events.len()
                );
                break;
            }
            if self.pending_bytes >= self.max_pending_bytes {
                break;
            }
        }
        // O：Len 框架下，畸形/超大的长度前缀立即断开（不必等缓冲涨到 ~10MB）。
        // 兜底：待分帧数据超过「单帧上限 + 前缀余量」仍分不出帧（覆盖 Line/Auto/codec）→ 断开。
        if batch.is_empty() {
            let invalid_prefix = framing == FramingMode::Len
                && FramingExtractor::leading_length_prefix_invalid(self.unframed_buf());
            let oversized = self.unframed_bytes() > MAX_FRAME_BYTES + MAX_LEN_DIGITS + 1;
            if invalid_prefix || oversized {
                return Err(SourceReason::disconnect(format!(
                    "tcp source '{}' unframed buffer rejected (invalid_prefix={invalid_prefix}, unframed={} > {}), dropping connection",
                    self.source_key,
                    self.unframed_bytes(),
                    MAX_FRAME_BYTES + MAX_LEN_DIGITS + 1
                )));
            }
        }
        Ok(())
    }

    fn pending_len(&self) -> usize {
        self.pending_events.len()
    }

    fn pending_bytes(&self) -> usize {
        self.pending_bytes
    }

    /// 尚未被 framing 消费的待分帧字节数（codec 时含解码后明文与未解码的原始缓冲）。
    fn unframed_bytes(&self) -> usize {
        match &self.codec {
            Some(codec) => codec.plain_len() + self.buffer.len(),
            None => self.buffer.len(),
        }
    }

    /// 待分帧数据的起始缓冲（有 codec 时为解码后的明文，否则为原始缓冲），用于前缀判定。
    fn unframed_buf(&self) -> &BytesMut {
        match &self.codec {
            Some(codec) => codec.plain(),
            None => &self.buffer,
        }
    }

    /// 残余是否以「长度前缀」开头（`<digits><SP>`）；用于 Auto 框架的 EOF 判定。
    fn starts_with_length_prefix(&self) -> bool {
        FramingExtractor::has_length_prefix(self.unframed_buf())
    }

    /// F3：Line 框架 EOF 尾处理——若残余（明文/原始）里有非空白内容，补一个 `'\n'`，
    /// 使末尾无换行的最后一条记录也能被抽出。Len/Auto 的残余是无法补全的不完整帧，不处理。
    fn append_eof_line_delimiter(&mut self) {
        let buf = match &mut self.codec {
            Some(codec) => codec.plain_mut(),
            None => &mut self.buffer,
        };
        if buf.iter().any(|b| !matches!(b, b'\r' | b' ' | b'\t')) {
            buf.extend_from_slice(b"\n");
        }
    }

    fn push_pending_back(&mut self, event: SourceEvent, event_size: usize) {
        self.pending_bytes = self.pending_bytes.saturating_add(event_size);
        self.pending_events.push_back(event);
    }

    fn push_pending_front(&mut self, event: SourceEvent, event_size: usize) {
        self.pending_bytes = self.pending_bytes.saturating_add(event_size);
        self.pending_events.push_front(event);
    }

    fn build_event(&self, payload: Bytes, peer_ip: IpAddr) -> SourceEvent {
        let mut event = SourceEvent::new(
            next_event_id(),
            &self.source_key,
            RawData::Bytes(payload),
            Arc::new(self.base_tags.clone()),
        );
        event.ups_ip = Some(peer_ip);
        event
    }
}

fn extract_message(framing: FramingMode, buffer: &mut BytesMut) -> Option<Bytes> {
    match framing {
        FramingMode::Line => FramingExtractor::extract_line_message(buffer),
        FramingMode::Len => FramingExtractor::extract_length_prefixed_message(buffer),
        FramingMode::Auto => FramingExtractor::extract_length_prefixed_message(buffer)
            .or_else(|| FramingExtractor::extract_line_message(buffer)),
    }
}

pub fn batch_bytes(batch: &SourceBatch) -> usize {
    batch.iter().map(event_payload_len).sum()
}

fn event_payload_len(ev: &SourceEvent) -> usize {
    match &ev.payload {
        RawData::String(s) => s.len(),
        RawData::Bytes(b) => b.len(),
        RawData::ArcBytes(b) => b.len(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BufMut;
    use tokio::io::AsyncWriteExt;

    /// 竞态安全地做一次非阻塞读取：先等 socket 可读再调用 `try_read_batch`。
    /// `try_read_batch` 不阻塞，单次调用可能在字节到达前拿到 `NoData`；这里
    /// 在 `NoData` 时继续等待下一次可读，直到切出完整帧或被关闭。
    /// 仅用于明文连接：TLS 的非阻塞读恒为 `NoData`，会在此空转，故直接返回一次。
    async fn try_read_ready(conn: &mut TcpConnection) -> ReadOutcome {
        loop {
            if !matches!(&conn.stream, ConnStream::Plain(_)) {
                return conn.try_read_batch().expect("try_read_batch");
            }
            {
                if let ConnStream::Plain(stream) = &conn.stream {
                    stream.readable().await.expect("socket readable");
                }
            }
            match conn.try_read_batch().expect("try_read_batch") {
                ReadOutcome::NoData => continue,
                other => return other,
            }
        }
    }

    /// 累积非阻塞读到 `expected` 个事件（或对端关闭 / 超时）。避免多写分片时
    /// 单次 `try_read_batch` 只拿到部分帧而误判；每次等待都用超时兜底，避免
    /// 对端保持打开且无数据时无限挂起。
    async fn read_events_try(conn: &mut TcpConnection, expected: usize) -> SourceBatch {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let mut events: SourceBatch = Vec::new();
        while events.len() < expected {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, try_read_ready(conn)).await {
                Ok(ReadOutcome::Produced(batch)) => events.extend(batch),
                Ok(ReadOutcome::Closed) => break,
                Ok(ReadOutcome::NoData) => {}
                Err(_) => break,
            }
        }
        events
    }

    #[tokio::test]
    async fn try_read_batch_respects_payload_budget() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().unwrap();
        // 发送量必须超过单批字节上限，确保 byte cap 边界被触发。
        let frame_payload = 4096usize;
        let frames = MAX_BATCH_BYTES / frame_payload + 2;
        let writer = tokio::spawn(async move {
            let mut client = tokio::net::TcpStream::connect(addr)
                .await
                .expect("connect client");
            let line = vec![b'a'; frame_payload];
            for _ in 0..frames {
                client.write_all(&line).await.unwrap();
                client.write_all(b"\n").await.unwrap();
            }
        });

        let (stream, peer) = listener.accept().await.expect("accept connection");
        let mut conn = TcpConnection::new(
            ConnStream::Plain(stream),
            peer,
            FramingMode::Line,
            Tags::new(),
            MAX_READ_BYTES, // 单次读能跨过 byte cap，才能真正验证上限
            "test".into(),
            crate::net::CodecConfig::default(),
        );

        // 并发读：不要先 await writer——发送量已超过 socket 缓冲，先等写完成会与读互相阻塞。
        // 逐批读到收满 frames，断言每一批都不超字节上限，且所有帧恰好产出一次；
        // 同时确认总字节确实超过上限，证明 byte cap 边界被真正触发。
        let mut produced_bytes = 0usize;
        let mut produced_events = 0usize;
        while produced_events < frames {
            match try_read_ready(&mut conn).await {
                ReadOutcome::Produced(batch) => {
                    let batch_bytes: usize = batch.iter().map(event_payload_len).sum();
                    assert!(
                        batch_bytes <= MAX_BATCH_BYTES,
                        "batch exceeded byte limit: {batch_bytes}"
                    );
                    produced_bytes += batch_bytes;
                    produced_events += batch.len();
                }
                ReadOutcome::Closed => break,
                ReadOutcome::NoData => {}
            }
        }
        writer.await.unwrap();
        assert_eq!(
            produced_events, frames,
            "every frame should be produced exactly once"
        );
        assert!(
            produced_bytes > MAX_BATCH_BYTES,
            "workload should exceed the batch byte cap"
        );
    }

    #[tokio::test]
    async fn bounded_read_keeps_buffer_bounded_under_backlog_burst() {
        // Regression for macOS EINVAL (os error 22): an *unbounded* read into
        // the BytesMut under a large socket backlog balloons the buffer to GBs,
        // after which macOS read() fails with EINVAL and the connection dies.
        // The bounded read must clamp a single read to MAX_READ_BYTES regardless
        // of backlog size, and must not lose events.
        const FRAME_PAYLOAD: usize = 4096;
        const TOTAL_BYTES: usize = 4 * 1024 * 1024; // 4 MiB backlog, >> MAX_READ_BYTES

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().unwrap();

        let writer = tokio::spawn(async move {
            let mut client = tokio::net::TcpStream::connect(addr)
                .await
                .expect("connect client");
            let header = format!("{} ", FRAME_PAYLOAD);
            let payload = vec![b'x'; FRAME_PAYLOAD];
            let mut written = 0usize;
            while written + header.len() + FRAME_PAYLOAD <= TOTAL_BYTES {
                client.write_all(header.as_bytes()).await.unwrap();
                client.write_all(&payload).await.unwrap();
                written += header.len() + FRAME_PAYLOAD;
            }
            client.shutdown().await.unwrap();
        });

        let (stream, peer) = listener.accept().await.expect("accept connection");
        // Large receive buffer so the whole backlog sits in the socket — with the
        // pre-fix unbounded read, a single try_read_buf would pull it all into the
        // BytesMut at once.
        let std_stream = stream.into_std().expect("into_std");
        socket2::SockRef::from(&std_stream)
            .set_recv_buffer_size(TOTAL_BYTES * 2)
            .expect("set recv buffer");
        let stream = tokio::net::TcpStream::from_std(std_stream).expect("from_std");
        writer.await.unwrap();

        // 大 `tcp_recv_bytes`：让缓冲容量远大于 MAX_READ_BYTES。这正是「单次读必须被
        // 夹住」才能防御的配置（旧代码 `remaining_mut()` 守卫恒假，单次读会拉进整个 backlog）。
        let mut conn = TcpConnection::new(
            ConnStream::Plain(stream),
            peer,
            FramingMode::Len,
            Tags::new(),
            TOTAL_BYTES,
            "test".into(),
            crate::net::CodecConfig::default(),
        );

        // 核心回归：单次 syscall 读必须被夹到 MAX_READ_BYTES。旧代码里
        // `remaining_mut()` 恒为 `isize::MAX - len`、守卫永不生效，一次 try_read_buf
        // 会把 ~4MiB backlog 全部拉进 buffer（→ MBs → macOS read() EINVAL）。
        // 直接在 drain 之前断言原始读入字节数（不经过 drain，无法被“抽帧”掩盖）。
        let n = conn
            .batcher
            .bounded_try_read(&conn.stream)
            .expect("first bounded read should succeed");
        assert!(n > 0, "expected the socket backlog to yield bytes");
        assert!(
            n <= MAX_READ_BYTES,
            "a single read must be clamped to MAX_READ_BYTES, got {n}"
        );

        let mut total_payload = 0usize;
        let mut peak_buf = conn.batcher.buffer.len();
        loop {
            match conn.try_read_batch().expect("read should not error") {
                ReadOutcome::Produced(batch) => {
                    for ev in &batch {
                        total_payload += event_payload_len(ev);
                    }
                    peak_buf = peak_buf.max(conn.batcher.buffer.len());
                }
                ReadOutcome::NoData => {
                    // All data is already in the socket buffer, so this is
                    // transient; yield and continue until EOF.
                    tokio::task::yield_now().await;
                }
                ReadOutcome::Closed => break,
            }
        }

        // No loss: every frame's payload must be extracted exactly once.
        let header_len = format!("{} ", FRAME_PAYLOAD).len();
        let frames = TOTAL_BYTES / (header_len + FRAME_PAYLOAD);
        assert_eq!(
            total_payload,
            frames * FRAME_PAYLOAD,
            "all frames must be extracted without loss"
        );
        // The buffer must never hold the ENTIRE backlog at once: pre-fix the
        // first read pulled the whole socket buffer into it (→ MBs → EINVAL).
        // Under continuous reading of a pre-fed backlog the buffer can still
        // accumulate a tail (drain is batch-capped); the fix's guarantee is that
        // it never jumps to the full backlog in a single read. Production
        // backpressure paces reads so the tail stays small in practice.
        assert!(
            peak_buf < TOTAL_BYTES,
            "buffer peak must stay below the full backlog, got {} bytes",
            peak_buf
        );
    }

    #[tokio::test]
    async fn direct_bounded_read_no_loss_order_and_buffer_ceiling() {
        // Exercises the bounded-direct-read path (bounded_try_read) end to end:
        // - every line must arrive exactly once and in order across many batches
        //   (crosses the 128-event / 64KiB batch caps and the 256KiB pending cap);
        // - the internal buffer must never balloon to the full backlog (the
        //   macOS EINVAL regression this read bounds against).
        const LINES: usize = 4000;
        // ~256B per line → total backlog ~1MiB, far above a single read budget.
        let body = "x".repeat(240);
        let mut frame = String::with_capacity(250);
        let mut frames: Vec<String> = Vec::with_capacity(LINES);
        for i in 0..LINES {
            frame.clear();
            frame.push_str(&format!("{i:05}"));
            frame.push_str(&body);
            frame.push('\n');
            frames.push(frame.clone());
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().unwrap();
        let send_frames = frames.clone();
        let writer = tokio::spawn(async move {
            let mut client = tokio::net::TcpStream::connect(addr)
                .await
                .expect("connect client");
            for chunk in send_frames.chunks(100) {
                let mut buf = String::new();
                for f in chunk {
                    buf.push_str(f);
                }
                client.write_all(buf.as_bytes()).await.expect("write chunk");
            }
            client.shutdown().await.expect("shutdown");
        });

        let (stream, peer) = listener.accept().await.expect("accept");
        let mut conn = TcpConnection::new(
            ConnStream::Plain(stream),
            peer,
            FramingMode::Line,
            Tags::new(),
            4096,
            "test".into(),
            crate::net::CodecConfig::default(),
        );
        // Drain concurrently with the writer: awaiting the writer first would
        // deadlock once the backlog exceeds the socket buffers (1MiB > default).

        let backlog_bytes = frames.iter().map(String::len).sum::<usize>();
        let mut got: Vec<u32> = Vec::with_capacity(LINES);
        let mut peak_capacity = conn.batcher.buffer.capacity();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out after {}/{} lines",
                got.len(),
                LINES
            );
            let outcome =
                tokio::time::timeout(std::time::Duration::from_secs(2), conn.read_batch())
                    .await
                    .expect("read_batch timeout")
                    .expect("read_batch error");
            match outcome {
                ReadOutcome::Produced(batch) => {
                    for ev in &batch {
                        let RawData::Bytes(bytes) = &ev.payload else {
                            panic!("expected bytes payload")
                        };
                        let text = std::str::from_utf8(bytes).expect("utf8");
                        got.push(text[..5].parse().expect("seq"));
                    }
                    peak_capacity = peak_capacity.max(conn.batcher.buffer.capacity());
                }
                ReadOutcome::NoData => tokio::task::yield_now().await,
                ReadOutcome::Closed => break,
            }
        }

        assert_eq!(got.len(), LINES, "no loss");
        writer.await.expect("writer task");
        assert!(
            got.windows(2).all(|w| w[0] + 1 == w[1]),
            "order preserved without duplicates"
        );
        // Buffer capacity (not just length) must stay far below the backlog:
        // the pre-fix unbounded read would swallow the whole backlog at once.
        assert!(
            peak_capacity < backlog_bytes,
            "buffer capacity {} must stay below backlog {backlog_bytes}",
            peak_capacity
        );
    }

    #[tokio::test]
    async fn test_length_prefixed_framing() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().unwrap();

        let writer = tokio::spawn(async move {
            let mut client = tokio::net::TcpStream::connect(addr)
                .await
                .expect("connect client");

            // Send length-prefixed messages
            let messages = vec!["hello", "world", "length", "prefixed"];
            for msg in messages {
                client
                    .write_all(format!("{} {}", msg.len(), msg).as_bytes())
                    .await
                    .unwrap();
            }
        });

        let (stream, peer) = listener.accept().await.expect("accept connection");
        let mut conn = TcpConnection::new(
            ConnStream::Plain(stream),
            peer,
            FramingMode::Len,
            Tags::new(),
            8192,
            "test_len".into(),
            crate::net::CodecConfig::default(),
        );

        writer.await.unwrap();

        let batch = read_events_try(&mut conn, 4).await;
        assert_eq!(batch.len(), 4, "expected produced outcome");

        let payloads: Vec<String> = batch
            .iter()
            .map(|ev| match &ev.payload {
                RawData::Bytes(b) => String::from_utf8_lossy(b).to_string(),
                _ => panic!("expected bytes payload"),
            })
            .collect();

        assert_eq!(payloads, vec!["hello", "world", "length", "prefixed"]);
    }

    #[tokio::test]
    async fn test_auto_framing_handles_mixed_modes() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().unwrap();

        let writer = tokio::spawn(async move {
            let mut client = tokio::net::TcpStream::connect(addr)
                .await
                .expect("connect client");

            // Mix of newline and length-prefixed messages
            client.write_all(b"line1\n").await.unwrap();
            client.write_all(b"5 hello").await.unwrap();
            client.write_all(b"\n").await.unwrap();
            client.write_all(b"7 message").await.unwrap();
        });

        let (stream, peer) = listener.accept().await.expect("accept connection");
        let mut conn = TcpConnection::new(
            ConnStream::Plain(stream),
            peer,
            FramingMode::Auto,
            Tags::new(),
            8192,
            "test_auto".into(),
            crate::net::CodecConfig::default(),
        );

        writer.await.unwrap();

        let batch = read_events_try(&mut conn, 4).await;
        assert_eq!(batch.len(), 4, "expected produced outcome");

        let payloads: Vec<String> = batch
            .iter()
            .map(|ev| match &ev.payload {
                RawData::Bytes(b) => String::from_utf8_lossy(b).to_string(),
                _ => panic!("expected bytes payload"),
            })
            .collect();

        assert_eq!(payloads, vec!["line1", "hello", "", "message"]);
    }

    #[test]
    fn test_batch_builder_maybe_shrink() {
        let mut batcher = BatchBuilder::new(
            BytesMut::with_capacity(2 * 1024 * 1024), // 2MiB
            Tags::new(),
            "test".into(),
            10,
            64 * 1024,
            MAX_PENDING_BYTES,
            None,
        );

        // Fill buffer with data
        batcher.buffer.put(&[0u8; 1000][..]);
        assert_eq!(batcher.buffer.capacity(), 2 * 1024 * 1024);

        // Clear and try to shrink (should shrink because capacity > SHRINK_HIGH_WATER_BYTES)
        batcher.buffer.clear();
        batcher.maybe_shrink();
        assert_eq!(batcher.buffer.capacity(), SHRINK_TARGET_BYTES);

        // Fill with small buffer
        let mut batcher2 = BatchBuilder::new(
            BytesMut::with_capacity(100 * 1024), // 100KiB
            Tags::new(),
            "test".into(),
            10,
            64 * 1024,
            MAX_PENDING_BYTES,
            None,
        );

        batcher2.buffer.clear();
        batcher2.maybe_shrink();
        // Should not shrink because capacity is less than SHRINK_HIGH_WATER_BYTES
        assert_eq!(batcher2.buffer.capacity(), 100 * 1024);
    }

    #[test]
    fn test_fill_batch_from_pending_with_byte_limit() {
        let mut batcher = BatchBuilder::new(
            BytesMut::new(),
            Tags::new(),
            "test".into(),
            10,
            100, // Small byte limit for testing
            MAX_PENDING_BYTES,
            None,
        );

        // Create pending events that exceed byte limit
        let peer_ip = "127.0.0.1".parse().unwrap();
        let event1 = batcher.build_event(Bytes::from(vec![0u8; 60]), peer_ip);
        let event2 = batcher.build_event(Bytes::from(vec![0u8; 60]), peer_ip);
        let event3 = batcher.build_event(Bytes::from(vec![0u8; 20]), peer_ip);

        batcher.push_pending_back(event1, 60);
        batcher.push_pending_back(event2, 60);
        batcher.push_pending_back(event3, 20);

        let mut batch = SourceBatch::new();
        let mut produced_bytes = 0;

        batcher.fill_batch_from_pending(&mut batch, &mut produced_bytes);

        // Should only include the first event (60 bytes) as second would exceed limit
        assert_eq!(batch.len(), 1);
        assert_eq!(produced_bytes, 60);
        assert_eq!(batcher.pending_events.len(), 2); // Two events remain
        assert_eq!(batcher.pending_bytes(), 80);
    }

    #[test]
    fn test_drain_messages_stops_when_pending_bytes_hit_cap() {
        let mut batcher = BatchBuilder::new(
            BytesMut::from(&b"line1\nline2\nline3\nline4\n"[..]),
            Tags::new(),
            "test".into(),
            1,
            MAX_BATCH_BYTES,
            10,
            None,
        );
        let mut batch = SourceBatch::new();
        let mut produced_bytes = 0;
        let peer_ip = "127.0.0.1".parse().unwrap();

        batcher
            .drain_messages(FramingMode::Line, peer_ip, &mut batch, &mut produced_bytes)
            .unwrap();

        assert_eq!(batch.len(), 1, "首条消息应先进入当前 batch");
        assert_eq!(batcher.pending_len(), 1, "溢出的下一条消息应进入 pending");
        assert_eq!(batcher.pending_bytes(), 5);
        assert!(
            !batcher.buffer.is_empty(),
            "达到 pending byte cap 后应停止继续抽取消息，剩余数据保留在 buffer"
        );
    }

    #[test]
    fn test_event_payload_len() {
        let id = next_event_id();
        let source_key = "test";
        let tags = Arc::new(Tags::new());

        // Test String payload
        let event_str = SourceEvent::new(
            id,
            source_key,
            RawData::String("hello world".to_string()),
            tags.clone(),
        );
        assert_eq!(event_payload_len(&event_str), 11);

        // Test Bytes payload
        let event_bytes = SourceEvent::new(
            id,
            source_key,
            RawData::Bytes(vec![0u8; 42].into()),
            tags.clone(),
        );
        assert_eq!(event_payload_len(&event_bytes), 42);

        // Test ArcBytes payload
        let event_arc = SourceEvent::new(
            id,
            source_key,
            RawData::ArcBytes(Arc::new(vec![0u8; 100])),
            tags,
        );
        assert_eq!(event_payload_len(&event_arc), 100);
    }

    /// 源侧 TLS 读取路径：`ConnStream::Tls` 经 `TcpConnection::read_batch` 解出分帧消息。
    #[tokio::test]
    async fn tls_stream_reads_framed_message() {
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let cert_path = dir.path().join("cert.pem");
        let key_path = dir.path().join("key.pem");
        std::fs::write(&cert_path, certified.cert.pem().as_bytes()).unwrap();
        std::fs::write(&key_path, certified.key_pair.serialize_pem().as_bytes()).unwrap();

        let server_tls = crate::net::TlsConfig {
            cert: Some(cert_path.to_string_lossy().into_owned()),
            key: Some(key_path.to_string_lossy().into_owned()),
            ..Default::default()
        };
        let acceptor = tokio_rustls::TlsAcceptor::from(server_tls.build_server_config().unwrap());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client_tls = crate::net::TlsConfig {
            insecure: true,
            ..Default::default()
        };
        let connector = tokio_rustls::TlsConnector::from(client_tls.build_client_config().unwrap());
        let server_name = rustls::pki_types::ServerName::try_from("localhost".to_string()).unwrap();
        let client = tokio::spawn(async move {
            let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
            let mut tls = connector.connect(server_name, tcp).await.unwrap();
            tls.write_all(b"hello tls\n").await.unwrap();
        });

        let (stream, peer) = listener.accept().await.unwrap();
        let tls_stream = acceptor.accept(stream).await.unwrap();
        let mut conn = TcpConnection::new(
            ConnStream::Tls(Box::new(tls_stream)),
            peer,
            FramingMode::Line,
            Tags::new(),
            8192,
            "tls_test".to_string(),
            crate::net::CodecConfig::default(),
        );

        let outcome = tokio::time::timeout(std::time::Duration::from_secs(2), conn.read_batch())
            .await
            .expect("read timeout")
            .expect("read error");
        let batch = match outcome {
            ReadOutcome::Produced(b) => b,
            ReadOutcome::NoData => panic!("expected a produced batch"),
            ReadOutcome::Closed => panic!("connection closed before data"),
        };

        assert_eq!(batch.len(), 1);
        let ev = batch.into_iter().next().unwrap();
        match ev.payload {
            RawData::Bytes(b) => assert_eq!(&b[..], b"hello tls"),
            RawData::String(s) => assert_eq!(s, "hello tls"),
            other => panic!("unexpected payload variant: {:?}", other),
        }
        client.await.unwrap();
    }

    /// F1：一次突发把数据全部读进内部 buffer、但被 batch cap 截断后，剩余完整帧
    /// 不应依赖「新的 socket 数据」或 EOF 才交付——下一次读应直接 drain buffer 交出。
    /// （依赖回环 socket 缓冲能一次性接住突发；与 backlog 回归测试同前提。）
    #[tokio::test]
    async fn leftover_buffer_frames_drain_without_new_data() {
        const FRAME: usize = 4096;
        const FRAMES: usize = MAX_BATCH_BYTES / FRAME + 2; // 超出单批字节上限
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let writer = tokio::spawn(async move {
            let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
            let line = vec![b'q'; FRAME];
            for _ in 0..FRAMES {
                client.write_all(&line).await.unwrap();
                client.write_all(b"\n").await.unwrap();
            }
            // 保持连接打开、且不再发送新数据（不触发 EOF）。
            std::future::pending::<()>().await;
        });
        let (stream, peer) = listener.accept().await.unwrap();
        let mut conn = TcpConnection::new(
            ConnStream::Plain(stream),
            peer,
            FramingMode::Line,
            Tags::new(),
            MAX_READ_BYTES, // 让首次读一次吸收整个突发；否则读被 buffer 容量限制、触不到 byte cap
            "test_leftover".into(),
            crate::net::CodecConfig::default(),
        );
        // 等突发全部落到 socket（此后既无新数据、也无 EOF）。
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let events = read_events_try(&mut conn, FRAMES).await;
        writer.abort();
        assert_eq!(
            events.len(),
            FRAMES,
            "leftover buffer frames must be delivered without new data or EOF"
        );
        // 只比 len 可能掩盖「丢一帧 + 重复一帧」，故同时校验内容。
        for ev in &events {
            match &ev.payload {
                RawData::Bytes(b) => {
                    assert_eq!(b.len(), FRAME, "frame truncated");
                    assert!(b.iter().all(|&x| x == b'q'), "frame corrupted");
                }
                _ => panic!("expected bytes payload"),
            }
        }
    }

    /// F1 负例：不完整的尾帧不得被提前交付；补全后才交付一次。
    #[tokio::test]
    async fn partial_trailing_frame_withheld_until_complete() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (stream, peer) = listener.accept().await.unwrap();
        let mut conn = TcpConnection::new(
            ConnStream::Plain(stream),
            peer,
            FramingMode::Line,
            Tags::new(),
            8192,
            "test_partial".into(),
            crate::net::CodecConfig::default(),
        );

        let to_text = |ev: &SourceEvent| match &ev.payload {
            RawData::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
            RawData::String(s) => s.clone(),
            RawData::ArcBytes(b) => String::from_utf8_lossy(b).into_owned(),
        };

        client.write_all(b"a\nb\npart").await.unwrap(); // 末尾无换行
        let events = read_events_try(&mut conn, 2).await;
        let seen: Vec<String> = events.iter().map(to_text).collect();
        assert_eq!(
            seen,
            vec!["a", "b"],
            "the incomplete trailing frame must be withheld"
        );

        client.write_all(b"\n").await.unwrap(); // 补全尾帧
        let more = read_events_try(&mut conn, 1).await;
        let seen2: Vec<String> = more.iter().map(to_text).collect();
        assert_eq!(seen2, vec!["part"]);
    }

    /// F1（异步慢路径 `read_batch`）：突发被一次读吸进内部 buffer、又被 batch cap
    /// 截断后，剩余完整帧必须在**没有新 socket 数据、也没有 EOF** 时被直接 drain 交出。
    /// 否则 `read_batch` 会阻塞在 `read_chunk().await`（对端静默），使响应发生延迟。
    /// 与 `leftover_buffer_frames_drain_without_new_data` 同前提，但走阻塞读路径。
    #[tokio::test]
    async fn read_batch_drains_leftover_buffer_without_new_data() {
        const FRAME: usize = 4096;
        const FRAMES: usize = MAX_BATCH_BYTES / FRAME + 2; // 超出单批字节上限
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let writer = tokio::spawn(async move {
            let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
            let line = vec![b'r'; FRAME];
            for _ in 0..FRAMES {
                client.write_all(&line).await.unwrap();
                client.write_all(b"\n").await.unwrap();
            }
            // 保持连接打开、且此后不再发送：既无新数据、也不触发 EOF。
            std::future::pending::<()>().await;
        });
        let (stream, peer) = listener.accept().await.unwrap();
        let mut conn = TcpConnection::new(
            ConnStream::Plain(stream),
            peer,
            FramingMode::Line,
            Tags::new(),
            MAX_READ_BYTES, // 首次读一次吸收整个突发；否则读被 buffer 容量限制、触不到 byte cap
            "test_rb_leftover".into(),
            crate::net::CodecConfig::default(),
        );
        // 等突发全部落到 socket（此后既无新数据、也无 EOF）。
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let mut events: SourceBatch = Vec::new();
        while events.len() < FRAMES {
            // `read_batch` 只在有空闲字节时才会立刻返回；此处等待必须有上界——
            // 若 F1 缺失，第 3 次调用会永久阻塞在 socket 上而被 timeout 捕获。
            let outcome =
                tokio::time::timeout(std::time::Duration::from_secs(2), conn.read_batch())
                    .await
                    .expect("read_batch blocked: leftover buffer frames not drained (F1 missing?)")
                    .expect("read_batch should not error");
            match outcome {
                ReadOutcome::Produced(batch) => events.extend(batch),
                ReadOutcome::Closed => break,
                ReadOutcome::NoData => unreachable!("read_batch never yields NoData"),
            }
        }
        writer.abort();
        assert_eq!(
            events.len(),
            FRAMES,
            "leftover buffer frames must be delivered by read_batch without new data or EOF"
        );
        for ev in &events {
            match &ev.payload {
                RawData::Bytes(b) => {
                    assert_eq!(b.len(), FRAME, "frame truncated");
                    assert!(b.iter().all(|&x| x == b'r'), "frame corrupted");
                }
                _ => panic!("expected bytes payload"),
            }
        }
    }

    /// R3：`BoundedBufMut` 只向读取方暴露 ≤ limit 的空闲容量，把单次读夹住。
    /// 即便底层 `BytesMut` 空闲容量远大于 limit，暴露的 chunk 也必须等于 limit。
    #[test]
    fn bounded_buf_mut_exposes_at_most_limit() {
        let mut buf = BytesMut::with_capacity(MAX_READ_BYTES * 4);
        let mut b = BoundedBufMut::new(&mut buf, MAX_READ_BYTES);
        assert_eq!(b.remaining_mut(), MAX_READ_BYTES);
        assert_eq!(b.chunk_mut().len(), MAX_READ_BYTES);
    }

    /// R3：底层空闲容量不足 limit 时，`BoundedBufMut` 会先 reserve 到至少 limit；
    /// 反之（空闲已足够）则不浪费地 reserve。
    #[test]
    fn bounded_buf_mut_reserves_when_spare_below_limit() {
        let mut buf = BytesMut::with_capacity(16);
        {
            let b = BoundedBufMut::new(&mut buf, MAX_READ_BYTES);
            assert_eq!(b.remaining_mut(), MAX_READ_BYTES);
        }
        assert!(buf.spare_capacity_mut().len() >= MAX_READ_BYTES);
    }

    /// R3：经适配器写入应正确落到 inner，且 `remaining` 随写入递减（回归 unsafe 路径）。
    #[test]
    fn bounded_buf_mut_writes_through_to_inner() {
        let mut buf = BytesMut::new();
        {
            let mut b = BoundedBufMut::new(&mut buf, MAX_READ_BYTES);
            b.put_slice(b"hello world");
            assert_eq!(b.remaining_mut(), MAX_READ_BYTES - 11);
            // chunk_mut 应给出剩余可用（≥ limit-11，因 limit 远大于 11）
            assert_eq!(b.chunk_mut().len(), MAX_READ_BYTES - 11);
        }
        assert_eq!(&buf[..], b"hello world");
    }

    /// R3：TLS 路径经 `BoundedBufMut` 读入，验证其每次读不超过 MAX_READ_BYTES，且超过
    /// 单次上限的大载荷分多次读完后无丢失（明文与 TLS 夹读一致）。
    /// 注：rustls 每次至多返回一个 TLS record，故本测试不能「回归触发」无界读
    /// （那由 `bounded_read_keeps_buffer_bounded_under_backlog_burst` 覆盖），
    /// 它保证的是 TLS 侧的契约与不丢失。
    #[tokio::test]
    async fn tls_read_is_clamped_to_max_read_bytes() {
        const PAYLOAD: usize = MAX_READ_BYTES * 2; // > 单次读上限
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let cert_path = dir.path().join("cert.pem");
        let key_path = dir.path().join("key.pem");
        std::fs::write(&cert_path, certified.cert.pem().as_bytes()).unwrap();
        std::fs::write(&key_path, certified.key_pair.serialize_pem().as_bytes()).unwrap();
        let server_tls = crate::net::TlsConfig {
            cert: Some(cert_path.to_string_lossy().into_owned()),
            key: Some(key_path.to_string_lossy().into_owned()),
            ..Default::default()
        };
        let acceptor = tokio_rustls::TlsAcceptor::from(server_tls.build_server_config().unwrap());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client_tls = crate::net::TlsConfig {
            insecure: true,
            ..Default::default()
        };
        let connector = tokio_rustls::TlsConnector::from(client_tls.build_client_config().unwrap());
        let server_name = rustls::pki_types::ServerName::try_from("localhost".to_string()).unwrap();
        let payload = vec![b't'; PAYLOAD];
        let client = tokio::spawn(async move {
            let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
            let mut tls = connector.connect(server_name, tcp).await.unwrap();
            tls.write_all(&payload).await.unwrap();
            tls.flush().await.unwrap();
            // 保持连接，避免 EOF 干扰读取。
            std::future::pending::<()>().await;
        });

        let (stream, peer) = listener.accept().await.unwrap();
        let tls_stream = acceptor.accept(stream).await.unwrap();
        let mut conn = TcpConnection::new(
            ConnStream::Tls(Box::new(tls_stream)),
            peer,
            FramingMode::Line,
            Tags::new(),
            8192,
            "tls_clamp".to_string(),
            crate::net::CodecConfig::default(),
        );

        let mut total = 0usize;
        while total < PAYLOAD {
            let ConnStream::Tls(tls) = &mut conn.stream else {
                unreachable!("expected TLS stream")
            };
            let n = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                conn.batcher.tls_read(tls.as_mut()),
            )
            .await
            .expect("tls_read timed out")
            .expect("tls_read error");
            assert!(n > 0, "unexpected EOF before all payload read");
            assert!(
                n <= MAX_READ_BYTES,
                "tls_read must be clamped to MAX_READ_BYTES, got {n}"
            );
            total += n;
        }
        assert_eq!(total, PAYLOAD);
        client.abort();
    }

    /// O：长度前缀声明超过单帧上限、且缓冲已积累超过上限时，`drain_messages` 必须
    /// 返回 Err（交给上层注销该连接），避免超大/畸形前缀导致缓冲无界增长。
    #[test]
    fn drain_messages_errors_when_unframed_buffer_exceeds_max_frame() {
        let mut buf = BytesMut::new();
        // 声明恰好等于上限的长度（会被提取器拒绝）→ 后续字节全部滞留于缓冲。
        buf.extend_from_slice(format!("{MAX_FRAME_BYTES} ").as_bytes());
        buf.extend_from_slice(&vec![b'x'; MAX_FRAME_BYTES + 16]); // 累计 > 上限 + 前缀余量
        let mut batcher = BatchBuilder::new(
            buf,
            Tags::new(),
            "test_over_frame".into(),
            DEFAULT_BATCH_CAPACITY,
            MAX_BATCH_BYTES,
            MAX_PENDING_BYTES,
            None,
        );
        let mut batch: SourceBatch = Vec::new();
        let mut produced_bytes = 0usize;
        let r = batcher.drain_messages(
            FramingMode::Len,
            IpAddr::from([127, 0, 0, 1]),
            &mut batch,
            &mut produced_bytes,
        );
        assert!(
            r.is_err(),
            "oversized unframed buffer must error (not grow forever)"
        );
        assert!(batch.is_empty());
    }

    /// O 反例：合法的长度前缀、但 payload 尚未收齐 → 继续等待，不报错。
    #[test]
    fn drain_messages_ok_for_in_progress_len_frame() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"100 "); // 声明 100 字节，仅给 3 字节
        buf.extend_from_slice(b"abc");
        let mut batcher = BatchBuilder::new(
            buf,
            Tags::new(),
            "test_in_progress".into(),
            DEFAULT_BATCH_CAPACITY,
            MAX_BATCH_BYTES,
            MAX_PENDING_BYTES,
            None,
        );
        let mut batch: SourceBatch = Vec::new();
        let mut produced_bytes = 0usize;
        assert!(
            batcher
                .drain_messages(
                    FramingMode::Len,
                    IpAddr::from([127, 0, 0, 1]),
                    &mut batch,
                    &mut produced_bytes,
                )
                .is_ok(),
            "a legitimate in-progress frame must not error"
        );
        assert!(batch.is_empty());
    }

    /// O：Len 框架下，畸形/超大长度前缀应立即断开（无需等缓冲涨到 ~10MB）。
    #[test]
    fn drain_messages_errors_immediately_on_invalid_len_prefix() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(format!("{MAX_FRAME_BYTES} ").as_bytes()); // 恰好上限 → 非法
        buf.extend_from_slice(b"xyz");
        let mut batcher = BatchBuilder::new(
            buf,
            Tags::new(),
            "test_invalid_prefix".into(),
            DEFAULT_BATCH_CAPACITY,
            MAX_BATCH_BYTES,
            MAX_PENDING_BYTES,
            None,
        );
        let mut batch: SourceBatch = Vec::new();
        let mut produced_bytes = 0usize;
        assert!(
            batcher
                .drain_messages(
                    FramingMode::Len,
                    IpAddr::from([127, 0, 0, 1]),
                    &mut batch,
                    &mut produced_bytes,
                )
                .is_err(),
            "an invalid length prefix must be rejected immediately"
        );
    }

    /// O 反例：合法帧长度接近上限、尚在收尾（不完整）时，不得被误判为超限报错。
    #[test]
    fn drain_messages_ok_for_legit_near_max_frame_in_progress() {
        let n = MAX_FRAME_BYTES - 1; // 合法帧长度（< 上限）
        let mut buf = BytesMut::new();
        buf.extend_from_slice(format!("{n} ").as_bytes());
        buf.extend_from_slice(&vec![b'z'; n - 1]); // 差一字节才收齐
        let mut batcher = BatchBuilder::new(
            buf,
            Tags::new(),
            "test_legit_frame".into(),
            DEFAULT_BATCH_CAPACITY,
            MAX_BATCH_BYTES,
            MAX_PENDING_BYTES,
            None,
        );
        let mut batch: SourceBatch = Vec::new();
        let mut produced_bytes = 0usize;
        assert!(
            batcher
                .drain_messages(
                    FramingMode::Len,
                    IpAddr::from([127, 0, 0, 1]),
                    &mut batch,
                    &mut produced_bytes,
                )
                .is_ok(),
            "a legitimate in-progress frame must not be flagged as oversized"
        );
        assert!(batch.is_empty());
    }

    /// F3：Line 框架下，EOF 时末尾无换行的最后一条记录也应交付（此前被静默丢弃）。
    #[tokio::test]
    async fn eof_flushes_trailing_line_without_newline() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (stream, peer) = listener.accept().await.unwrap();
        let mut conn = TcpConnection::new(
            ConnStream::Plain(stream),
            peer,
            FramingMode::Line,
            Tags::new(),
            8192,
            "test_eof_tail".into(),
            crate::net::CodecConfig::default(),
        );

        client.write_all(b"a\nb\nlast-no-newline").await.unwrap();
        client.shutdown().await.unwrap(); // 触发 EOF

        let mut got: Vec<String> = Vec::new();
        loop {
            match conn.try_read_batch().expect("read should not error") {
                ReadOutcome::Produced(batch) => {
                    for ev in &batch {
                        got.push(match &ev.payload {
                            RawData::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
                            RawData::String(s) => s.clone(),
                            RawData::ArcBytes(b) => String::from_utf8_lossy(b).into_owned(),
                        });
                    }
                }
                ReadOutcome::NoData => tokio::task::yield_now().await,
                ReadOutcome::Closed => break,
            }
        }
        assert_eq!(got, vec!["a", "b", "last-no-newline"]);
    }

    /// F3 反例：Len 框架下 EOF 的残余是不完整帧，不得被交付。
    #[tokio::test]
    async fn eof_does_not_flush_incomplete_len_frame() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (stream, peer) = listener.accept().await.unwrap();
        let mut conn = TcpConnection::new(
            ConnStream::Plain(stream),
            peer,
            FramingMode::Len,
            Tags::new(),
            8192,
            "test_eof_len".into(),
            crate::net::CodecConfig::default(),
        );

        // 声明长度 10、只给 3 字节 → EOF 时仍未收齐
        client.write_all(b"10 hel").await.unwrap();
        client.shutdown().await.unwrap();

        let mut got = 0usize;
        loop {
            match conn.try_read_batch().expect("read should not error") {
                ReadOutcome::Produced(batch) => got += batch.len(),
                ReadOutcome::NoData => tokio::task::yield_now().await,
                ReadOutcome::Closed => break,
            }
        }
        assert_eq!(
            got, 0,
            "an incomplete length frame must not be delivered at EOF"
        );
    }

    /// F3（Auto）：`Auto` 框架下末行无换行（且非长度前缀起点）时，EOF 也应交付。
    #[tokio::test]
    async fn eof_flushes_auto_trailing_line() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (stream, peer) = listener.accept().await.unwrap();
        let mut conn = TcpConnection::new(
            ConnStream::Plain(stream),
            peer,
            FramingMode::Auto,
            Tags::new(),
            8192,
            "test_eof_auto".into(),
            crate::net::CodecConfig::default(),
        );

        client.write_all(b"a\nb\nlast-no-newline").await.unwrap();
        client.shutdown().await.unwrap();

        let mut got: Vec<String> = Vec::new();
        loop {
            match conn.try_read_batch().expect("read should not error") {
                ReadOutcome::Produced(batch) => {
                    for ev in &batch {
                        got.push(match &ev.payload {
                            RawData::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
                            RawData::String(s) => s.clone(),
                            RawData::ArcBytes(b) => String::from_utf8_lossy(b).into_owned(),
                        });
                    }
                }
                ReadOutcome::NoData => tokio::task::yield_now().await,
                ReadOutcome::Closed => break,
            }
        }
        assert_eq!(got, vec!["a", "b", "last-no-newline"]);
    }

    /// F3 反例（Auto）：EOF 残余以长度前缀开头（不完整 len 帧）→ 不得按行交付。
    #[tokio::test]
    async fn eof_does_not_flush_auto_incomplete_len_frame() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (stream, peer) = listener.accept().await.unwrap();
        let mut conn = TcpConnection::new(
            ConnStream::Plain(stream),
            peer,
            FramingMode::Auto,
            Tags::new(),
            8192,
            "test_eof_auto_len".into(),
            crate::net::CodecConfig::default(),
        );

        // 起始为 "100 "（长度前缀起点）但 payload 未收齐
        client.write_all(b"100 abc").await.unwrap();
        client.shutdown().await.unwrap();

        let mut got = 0usize;
        loop {
            match conn.try_read_batch().expect("read should not error") {
                ReadOutcome::Produced(batch) => got += batch.len(),
                ReadOutcome::NoData => tokio::task::yield_now().await,
                ReadOutcome::Closed => break,
            }
        }
        assert_eq!(
            got, 0,
            "an incomplete len frame under Auto must not be flushed as a line"
        );
    }
}
