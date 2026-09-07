use crate::sources::event_id::next_event_id;
use crate::sources::tcp::framing::{FramingExtractor, FramingMode};
use bytes::{BufMut, Bytes, BytesMut};
use std::collections::VecDeque;
use std::io::ErrorKind;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tokio::net::TcpStream;
use wp_connector_api::{SourceBatch, SourceEvent, SourceReason, SourceResult, Tags};
use wp_model_core::raw::RawData;

const DEFAULT_BATCH_CAPACITY: usize = 128;
const MAX_BATCH_BYTES: usize = 64 * 1024; // soft cap; single payload may exceed but only single event allowed
const MAX_PENDING_BYTES: usize = 256 * 1024;
// Per-read capacity budget. Reading unbounded into the BytesMut (which grows
// to the socket backlog size) lets the buffer balloon to GBs under backpressure,
// after which macOS read() fails with EINVAL (os error 22). Before each read we
// ensure at least this much spare capacity, so a single read stays in the
// low-MiB range and the buffer can't balloon.
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
    stream: TcpStream,
    client_addr: SocketAddr,
    framing: FramingMode,
    batcher: BatchBuilder,
}

impl TcpConnection {
    fn raw_fd(&self) -> i32 {
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            self.stream.as_raw_fd()
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
}

impl TcpConnection {
    pub fn new(
        stream: TcpStream,
        client_addr: SocketAddr,
        framing: FramingMode,
        base_tags: Tags,
        tcp_recv_bytes: usize,
        source_key: String,
    ) -> Self {
        let capacity = tcp_recv_bytes.max(1024);
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
        loop {
            match self.batcher.bounded_try_read(&self.stream) {
                Ok(0) => {
                    // EOF — drain any frames still buffered or pending before
                    // closing. `drain_messages` breaks on a batch byte/capacity
                    // cap, leaving the tail of the stream in the buffer; if we
                    // close now, those events are silently dropped (a single
                    // oversized frame like a big conn batch can leave the
                    // smaller auth/dns frames behind).
                    self.batcher.drain_messages(
                        self.framing,
                        self.client_addr.ip(),
                        &mut produced,
                        &mut produced_bytes,
                    );
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
                    );
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
                    return Err(SourceReason::disconnect(format!(
                        "tcp read error ({}): {}",
                        self.client_addr, e
                    )));
                }
            }
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
        loop {
            if let Err(e) = self.stream.readable().await {
                return Err(SourceReason::disconnect(format!(
                    "tcp readable error ({}): {}",
                    self.client_addr, e
                )));
            }
            match self.batcher.bounded_try_read(&self.stream) {
                Ok(0) => {
                    // EOF — drain any frames still buffered or pending before
                    // closing (see try_read_batch: a batch-cap break leaves the
                    // tail of the stream unprocessed).
                    self.batcher.drain_messages(
                        self.framing,
                        self.client_addr.ip(),
                        &mut produced,
                        &mut produced_bytes,
                    );
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
                    );
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
                    return Err(SourceReason::disconnect(format!(
                        "tcp read error ({}): {}",
                        self.client_addr, e
                    )));
                }
            }
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

impl BatchBuilder {
    fn new(
        buffer: BytesMut,
        base_tags: Tags,
        source_key: String,
        batch_capacity: usize,
        max_batch_bytes: usize,
        max_pending_bytes: usize,
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
    fn bounded_try_read(&mut self, stream: &TcpStream) -> std::io::Result<usize> {
        if self.buffer.remaining_mut() < MAX_READ_BYTES {
            self.buffer.reserve(MAX_READ_BYTES);
        }
        stream.try_read_buf(&mut self.buffer)
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
    ) {
        if self.pending_bytes >= self.max_pending_bytes {
            debug_data!(
                "TCP source '{}' stop draining buffer on pending byte cap: pending_events={} pending_bytes={} cap={}",
                self.source_key,
                self.pending_events.len(),
                self.pending_bytes,
                self.max_pending_bytes
            );
            return;
        }
        while let Some(payload) = extract_message(framing, &mut self.buffer) {
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
    }

    fn pending_len(&self) -> usize {
        self.pending_events.len()
    }

    fn pending_bytes(&self) -> usize {
        self.pending_bytes
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

    #[tokio::test]
    async fn try_read_batch_respects_payload_budget() {
        if std::env::var("WP_NET_TESTS").unwrap_or_default() != "1" {
            return;
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().unwrap();
        let writer = tokio::spawn(async move {
            let mut client = tokio::net::TcpStream::connect(addr)
                .await
                .expect("connect client");
            let line = vec![b'a'; 4096];
            for _ in 0..10 {
                client.write_all(&line).await.unwrap();
                client.write_all(b"\n").await.unwrap();
            }
        });

        let (stream, peer) = listener.accept().await.expect("accept connection");
        let mut conn = TcpConnection::new(
            stream,
            peer,
            FramingMode::Line,
            Tags::new(),
            8192,
            "test".into(),
        );
        writer.await.unwrap();

        let first = conn.try_read_batch().expect("first batch should succeed");
        let mut total_bytes = 0usize;
        if let ReadOutcome::Produced(batch) = first {
            for ev in &batch {
                total_bytes += event_payload_len(ev);
            }
            assert!(
                total_bytes <= MAX_BATCH_BYTES,
                "first batch should not exceed byte limit"
            );
        } else {
            panic!("expected produced outcome");
        }

        let second = conn.try_read_batch().expect("second batch should succeed");
        if let ReadOutcome::Produced(batch) = second {
            assert!(!batch.is_empty());
        } else {
            panic!("expected remaining data");
        }
    }

    #[tokio::test]
    async fn bounded_read_keeps_buffer_bounded_under_backlog_burst() {
        // Regression for macOS EINVAL (os error 22): an *unbounded* read into
        // the BytesMut under a large socket backlog balloons the buffer to GBs,
        // after which macOS read() fails with EINVAL and the connection dies.
        // The bounded read (MAX_READ_BYTES/call) must keep the internal buffer
        // small regardless of backlog size, and must not lose events.
        if std::env::var("WP_NET_TESTS").unwrap_or_default() != "1" {
            return;
        }
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

        let mut conn = TcpConnection::new(
            stream,
            peer,
            FramingMode::Len,
            Tags::new(),
            128,
            "test".into(),
        );

        // Core regression: the FIRST read must be bounded. Pre-fix a single
        // try_read_buf pulled the whole 4 MiB socket backlog into the buffer at
        // once (→ MBs → macOS read() EINVAL); post-fix it's capped at
        // MAX_READ_BYTES + one partial frame.
        let mut total_payload = 0usize;
        let first = conn.try_read_batch().expect("first read should succeed");
        match first {
            ReadOutcome::Produced(batch) => {
                assert!(!batch.is_empty(), "first batch should have data");
                for ev in &batch {
                    total_payload += event_payload_len(ev);
                }
                assert!(
                    conn.batcher.buffer.len() <= MAX_READ_BYTES + FRAME_PAYLOAD,
                    "first read must be bounded, got {} bytes in buffer",
                    conn.batcher.buffer.len()
                );
            }
            _ => panic!("expected produced outcome on first read"),
        }
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
        if std::env::var("WP_NET_TESTS").unwrap_or_default() != "1" {
            return;
        }
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
            stream,
            peer,
            FramingMode::Line,
            Tags::new(),
            4096,
            "test".into(),
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
        if std::env::var("WP_NET_TESTS").unwrap_or_default() != "1" {
            return;
        }

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
            stream,
            peer,
            FramingMode::Len,
            Tags::new(),
            8192,
            "test_len".into(),
        );

        writer.await.unwrap();

        let result = conn.try_read_batch().expect("read should succeed");
        if let ReadOutcome::Produced(batch) = result {
            assert_eq!(batch.len(), 4);

            let payloads: Vec<String> = batch
                .iter()
                .map(|ev| match &ev.payload {
                    RawData::Bytes(b) => String::from_utf8_lossy(b).to_string(),
                    _ => panic!("expected bytes payload"),
                })
                .collect();

            assert_eq!(payloads, vec!["hello", "world", "length", "prefixed"]);
        } else {
            panic!("expected produced outcome");
        }
    }

    #[tokio::test]
    async fn test_auto_framing_handles_mixed_modes() {
        if std::env::var("WP_NET_TESTS").unwrap_or_default() != "1" {
            return;
        }

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
            stream,
            peer,
            FramingMode::Auto,
            Tags::new(),
            8192,
            "test_auto".into(),
        );

        writer.await.unwrap();

        let result = conn.try_read_batch().expect("read should succeed");
        if let ReadOutcome::Produced(batch) = result {
            assert_eq!(batch.len(), 4);

            let payloads: Vec<String> = batch
                .iter()
                .map(|ev| match &ev.payload {
                    RawData::Bytes(b) => String::from_utf8_lossy(b).to_string(),
                    _ => panic!("expected bytes payload"),
                })
                .collect();

            assert_eq!(payloads, vec!["line1", "hello", "", "message"]);
        } else {
            panic!("expected produced outcome");
        }
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
        );
        let mut batch = SourceBatch::new();
        let mut produced_bytes = 0;
        let peer_ip = "127.0.0.1".parse().unwrap();

        batcher.drain_messages(FramingMode::Line, peer_ip, &mut batch, &mut produced_bytes);

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
}
