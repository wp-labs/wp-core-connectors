//! TCP DataSource implementation: consume accepted `TcpStream` directly and batch events.

use std::collections::{HashMap, VecDeque};

use async_trait::async_trait;
use tokio::sync::mpsc;
use wp_connector_api::{CtrlRx, DataSource, SourceBatch, SourceReason, SourceResult, Tags};

use crate::net::CodecConfig;

use super::ConnectionRegistry;
use super::conn::connection::{ReadOutcome, TcpConnection, batch_bytes};
use super::framing::FramingMode;
use super::worker::ConnectionRegistration;

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// 轻量 `select_all`：对一组已装箱的 future 依次轮询，返回最先完成的一个及其下标。
struct FirstReady<F> {
    futures: Vec<F>,
}

impl<F: Future + Unpin> Future for FirstReady<F> {
    type Output = (F::Output, usize);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        for (idx, fut) in this.futures.iter_mut().enumerate() {
            if let Poll::Ready(out) = Pin::new(fut).poll(cx) {
                return Poll::Ready((out, idx));
            }
        }
        Poll::Pending
    }
}

/// `wait_any_ready` 中每条连接阻塞读的装箱 future：产出 `(conn_id, 读取结果)`。
type BatchReadFuture<'a> =
    Pin<Box<dyn Future<Output = (u64, SourceResult<ReadOutcome>)> + Send + 'a>>;

struct ConnectionGuard<'a> {
    source: &'a mut TcpSource,
    conn_id: u64,
    conn: Option<TcpConnection>,
}

impl<'a> ConnectionGuard<'a> {
    fn new(source: &'a mut TcpSource, conn_id: u64) -> Option<Self> {
        source.connections.remove(&conn_id).map(|conn| Self {
            source,
            conn_id,
            conn: Some(conn),
        })
    }

    fn connection(&mut self) -> &mut TcpConnection {
        self.conn
            .as_mut()
            .expect("connection guard missing TcpConnection")
    }

    fn requeue(&mut self) {
        if let Some(conn) = self.conn.take() {
            self.source.push_connection_back(self.conn_id, conn);
        }
    }

    fn deregister(&mut self, reason: &str) {
        self.source.deregister_connection(self.conn_id, reason);
        self.conn.take();
    }

    /// 非阻塞读取：只做一次 `try_read_batch`。切出完整帧则归还连接并返回；
    /// 无数据（半帧）则把连接放回队尾，交由外层继续轮询其它连接。
    fn try_read(mut self) -> SourceResult<Option<SourceBatch>> {
        let key = self.source.key.clone();
        let conn_id = self.conn_id;
        let (peer, try_result) = {
            let conn = self.connection();
            let peer = conn.client_ip();
            let pending = conn.pending_len();
            let pending_bytes = conn.pending_bytes();
            trace_data!(
                "TCP source '{}' begin try_read (conn={}, pending={}, bytes={})",
                key,
                conn_id,
                pending,
                pending_bytes
            );
            (peer, conn.try_read_batch())
        };

        match try_result {
            Err(e) => {
                error_ctrl!(
                    "TCP source '{}' conn {} try_read error (peer={}): {}",
                    self.source.key,
                    self.conn_id,
                    peer,
                    e
                );
                let reason = format!("try_read error from {}: {}", peer, e);
                self.deregister(&reason);
                Err(e)
            }
            Ok(ReadOutcome::Produced(batch)) => {
                let pending_after = self
                    .conn
                    .as_ref()
                    .map(|conn| conn.pending_len())
                    .unwrap_or_default();
                trace_ctrl!(
                    "TCP source '{}' conn {} produced {} events via try_read (peer={}, batch_bytes={}, pending_after={})",
                    self.source.key,
                    self.conn_id,
                    batch.len(),
                    peer,
                    batch_bytes(&batch),
                    pending_after
                );
                self.requeue();
                Ok(Some(batch))
            }
            Ok(ReadOutcome::Closed) => {
                let (pending_events, pending_bytes) = self
                    .conn
                    .as_ref()
                    .map(|conn| (conn.pending_len(), conn.pending_bytes()))
                    .unwrap_or((0, 0));
                info_ctrl!(
                    "TCP source '{}' conn {} closed during try_read (peer={}, pending_events={}, pending_bytes={})",
                    self.source.key,
                    self.conn_id,
                    peer,
                    pending_events,
                    pending_bytes
                );
                let reason = format!(
                    "peer {} closed during try_read (pending_events={} pending_bytes={})",
                    peer, pending_events, pending_bytes
                );
                self.deregister(&reason);
                Ok(None)
            }
            Ok(ReadOutcome::NoData) => {
                self.requeue();
                Ok(None)
            }
        }
    }
}

impl<'a> Drop for ConnectionGuard<'a> {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            self.source.push_connection_back(self.conn_id, conn);
        }
    }
}

/// TCP 数据源：消费 acceptor 交付的已接受连接，按帧切分为事件批次。
///
/// 不变量：本实例的**全部**连接来自**同一个** acceptor（同一个 `addr:port`，即同一个
/// `tcp_src` connector），因此 **transport 单一**——要么全为明文，要么全为 TLS
/// （TLS 由 listener 决定、逐连接不变，见 `worker/listener_loop.rs`）。故不存在
/// 「同一实例内 Plain 与 TLS 混流」；快/慢路径无需处理混合 transport。
pub struct TcpSource {
    key: String,
    base_tags: Tags,
    framing: FramingMode,
    tcp_recv_bytes: usize,
    codec: CodecConfig,
    registry: ConnectionRegistry,
    connection_rx: mpsc::Receiver<ConnectionRegistration>,
    connections: HashMap<u64, TcpConnection>,
    connection_order: VecDeque<u64>,
    started: bool,
    awaiting_logged: bool,
    conn_rx_closed: bool,
}

impl TcpSource {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        key: String,
        tags: Tags,
        _address: String,
        tcp_recv_bytes: usize,
        framing: FramingMode,
        registry: ConnectionRegistry,
        connection_rx: mpsc::Receiver<ConnectionRegistration>,
        codec: CodecConfig,
    ) -> SourceResult<Self> {
        let mut base_tags = Tags::new();
        for (k, v) in tags.iter() {
            base_tags.set(k, v);
        }
        Ok(Self {
            key,
            base_tags,
            framing,
            tcp_recv_bytes,
            codec,
            registry,
            connection_rx,
            connections: HashMap::new(),
            connection_order: VecDeque::new(),
            started: false,
            awaiting_logged: false,
            conn_rx_closed: false,
        })
    }

    pub fn active_connections(&self) -> usize {
        self.connections.len()
    }

    fn register_connection(&mut self, reg: ConnectionRegistration) {
        let connection = TcpConnection::new(
            reg.stream,
            reg.peer_addr,
            self.framing,
            self.base_tags.clone(),
            self.tcp_recv_bytes,
            self.key.clone(),
            self.codec.clone(),
        );
        self.registry
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(reg.connection_id);
        self.connections.insert(reg.connection_id, connection);
        self.connection_order.push_back(reg.connection_id);
        self.awaiting_logged = false;
        info_ctrl!(
            "TCP source '{}' registered connection {} (peer={}, active={})",
            self.key,
            reg.connection_id,
            reg.peer_addr,
            self.connections.len()
        );
    }

    fn deregister_connection(&mut self, conn_id: u64, reason: &str) {
        self.registry
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&conn_id);
        self.connections.remove(&conn_id);
        if let Some(pos) = self.connection_order.iter().position(|&id| id == conn_id) {
            self.connection_order.remove(pos);
        }
        info_ctrl!(
            "TCP source '{}' deregistered connection {} (reason={}, remaining={})",
            self.key,
            conn_id,
            reason,
            self.connections.len()
        );
    }

    fn drain_new_connections(&mut self) {
        while let Ok(reg) = self.connection_rx.try_recv() {
            self.register_connection(reg);
        }
    }

    async fn wait_for_connection(&mut self) -> SourceResult<bool> {
        match self.connection_rx.recv().await {
            Some(reg) => {
                info_ctrl!(
                    "TCP source '{}' received pending connection {} from {}",
                    self.key,
                    reg.connection_id,
                    reg.peer_addr
                );
                self.register_connection(reg);
                Ok(true)
            }
            None => {
                warn_ctrl!(
                    "TCP source '{}' connection channel closed; no further connectors will arrive",
                    self.key
                );
                Ok(false)
            }
        }
    }

    fn push_connection_back(&mut self, conn_id: u64, conn: TcpConnection) {
        self.connections.insert(conn_id, conn);
        self.connection_order.push_back(conn_id);
    }

    /// 把某条连接移到 `connection_order` 队尾（慢路径服务后轮转，保证公平）。
    fn rotate_connection(&mut self, conn_id: u64) {
        if let Some(pos) = self.connection_order.iter().position(|&id| id == conn_id)
            && pos + 1 < self.connection_order.len()
        {
            self.connection_order.remove(pos);
            self.connection_order.push_back(conn_id);
        }
    }

    fn acquire_connection_guard(&mut self, conn_id: u64) -> Option<ConnectionGuard<'_>> {
        ConnectionGuard::new(self, conn_id)
    }

    /// 非阻塞轮询：对 `connection_order` 中的每条连接各做一次 `try_read`（轮转）。
    /// 任意一条切出完整帧立即返回；全部无数据时返回 `None`（连接已放回队尾）。
    fn drain_ready(&mut self) -> SourceResult<Option<SourceBatch>> {
        let remaining = self.connection_order.len();
        for _ in 0..remaining {
            let conn_id = match self.connection_order.pop_front() {
                Some(id) => id,
                None => break,
            };
            let Some(guard) = self.acquire_connection_guard(conn_id) else {
                warn_ctrl!(
                    "TCP source '{}' lost connection {} before processing; skipping",
                    self.key,
                    conn_id
                );
                continue;
            };
            if let Some(batch) = guard.try_read()? {
                return Ok(Some(batch));
            }
        }
        Ok(None)
    }

    /// 等待任意一条连接可读（或新连接到达）。对每条连接并发地发起阻塞读
    /// （`read_batch`），由 `select_all` 选出最先就绪的一条：长记录的半帧只会
    /// 让该条连接自己的 future 挂起，不会饿死其它连接。
    async fn wait_any_ready(&mut self) -> SourceResult<Option<SourceBatch>> {
        enum Wake {
            NewConn(Option<ConnectionRegistration>),
            Batch(u64, SourceResult<ReadOutcome>),
        }

        if self.connections.is_empty() {
            return Ok(None);
        }

        let channel_closed = self.conn_rx_closed;
        let mut became_closed = false;
        let wake = {
            let Self {
                connections,
                connection_order,
                connection_rx,
                ..
            } = self;
            // 先按 HashMap 序建 future（拿到不相交的 `&mut`），再按 `connection_order`
            // 排序，使「最靠前」的连接排在 `FirstReady` 首位——与快路径 `drain_ready`
            // 共用同一套轮转，避免固定顺序偏好导致饿死。
            let mut pending: Vec<(u64, BatchReadFuture<'_>)> =
                Vec::with_capacity(connections.len());
            for (&conn_id, conn) in connections.iter_mut() {
                pending.push((
                    conn_id,
                    Box::pin(async move {
                        let outcome = conn.read_batch().await;
                        (conn_id, outcome)
                    }),
                ));
            }
            let rank: HashMap<u64, usize> = connection_order
                .iter()
                .enumerate()
                .map(|(i, &id)| (id, i))
                .collect();
            pending.sort_by_key(|(id, _)| rank.get(id).copied().unwrap_or(usize::MAX));
            let futures: Vec<BatchReadFuture<'_>> =
                pending.into_iter().map(|(_, fut)| fut).collect();
            let wait_any = FirstReady { futures };
            tokio::pin!(wait_any);
            if channel_closed {
                let ((conn_id, outcome), _idx) = (&mut wait_any).await;
                Wake::Batch(conn_id, outcome)
            } else {
                tokio::select! {
                    reg = connection_rx.recv() => {
                        if reg.is_none() {
                            became_closed = true;
                        }
                        Wake::NewConn(reg)
                    }
                    ((conn_id, outcome), _idx) = &mut wait_any => Wake::Batch(conn_id, outcome),
                }
            }
        };

        if became_closed {
            self.conn_rx_closed = true;
        }

        match wake {
            Wake::NewConn(Some(reg)) => {
                self.register_connection(reg);
                Ok(None)
            }
            Wake::NewConn(None) => {
                warn_ctrl!(
                    "TCP source '{}' connection channel closed; no further connectors will arrive",
                    self.key
                );
                Ok(None)
            }
            Wake::Batch(conn_id, Ok(ReadOutcome::Produced(batch))) => {
                trace_data!(
                    "TCP source '{}' conn {} produced {} events via wait_any_ready (batch_bytes={})",
                    self.key,
                    conn_id,
                    batch.len(),
                    batch_bytes(&batch)
                );
                // 轮转：服务过的连接退到队尾，与快路径一致。
                self.rotate_connection(conn_id);
                Ok(Some(batch))
            }
            Wake::Batch(_conn_id, Ok(ReadOutcome::NoData)) => Ok(None),
            Wake::Batch(conn_id, Ok(ReadOutcome::Closed)) => {
                self.deregister_connection(conn_id, "peer closed during wait");
                Ok(None)
            }
            Wake::Batch(conn_id, Err(e)) => {
                warn_data!(
                    "TCP source '{}' conn {} read error while waiting: {}",
                    self.key,
                    conn_id,
                    e
                );
                self.deregister_connection(conn_id, "blocking read error during wait");
                Err(e)
            }
        }
    }
}

#[async_trait]
impl DataSource for TcpSource {
    async fn receive(&mut self) -> SourceResult<SourceBatch> {
        if !self.started {
            return Err(SourceReason::supplier_error("TCP source not started"));
        }
        loop {
            self.drain_new_connections();

            if self.connections.is_empty() {
                if !self.awaiting_logged {
                    debug_data!("TCP source '{}' awaiting connectors (current=0)", self.key);
                    self.awaiting_logged = true;
                }
                if !self.wait_for_connection().await? {
                    return Err(SourceReason::disconnect(format!(
                        "TCP source '{}' no active connections",
                        self.key
                    )));
                }
                continue;
            }
            self.awaiting_logged = false;

            // 快路径：非阻塞轮询所有连接，任意一条切出完整帧即返回。
            // 这样某条连接上的半帧不会再阻塞其它连接的读取。
            if let Some(batch) = self.drain_ready()? {
                if !batch.is_empty() {
                    return Ok(batch);
                }
                continue;
            }

            // 慢路径：当前所有连接都没有完整帧。等待“任意一条连接可读”
            // 或“新连接到达”，而不是只阻塞在某一条连接上。
            if self.connections.is_empty() {
                continue;
            }
            if let Some(batch) = self.wait_any_ready().await?
                && !batch.is_empty()
            {
                return Ok(batch);
            }
        }
    }

    fn try_receive(&mut self) -> Option<SourceBatch> {
        None
    }

    fn can_try_receive(&mut self) -> bool {
        false
        //true
    }

    fn identifier(&self) -> String {
        self.key.clone()
    }

    async fn start(&mut self, _ctrl_rx: CtrlRx) -> SourceResult<()> {
        if self.started {
            return Err(SourceReason::supplier_error("TCP source already started"));
        }
        self.started = true;
        self.awaiting_logged = false;
        info_data!("TCP source '{}' started", self.key);
        Ok(())
    }

    async fn close(&mut self) -> SourceResult<()> {
        self.connections.clear();
        self.connection_order.clear();
        self.started = false;
        self.awaiting_logged = false;
        self.conn_rx_closed = false;
        self.registry
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
        info_data!("TCP source '{}' closed", self.key);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::conn::ConnStream;
    use super::*;
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;

    /// 构造一个已 start 的 TcpSource，返回它和注册通道。
    async fn new_source() -> (TcpSource, mpsc::Sender<ConnectionRegistration>) {
        let registry = Arc::new(Mutex::new(HashSet::new()));
        let (reg_tx, reg_rx) = mpsc::channel(16);
        let mut source = TcpSource::new(
            "test_tcp".to_string(),
            Tags::default(),
            "127.0.0.1:0".to_string(),
            8192,
            FramingMode::Line,
            registry,
            reg_rx,
            crate::net::CodecConfig::default(),
        )
        .unwrap();
        let (_ctrl_tx, ctrl_rx) = async_broadcast::broadcast(1);
        source.start(ctrl_rx).await.unwrap();
        (source, reg_tx)
    }

    /// 建一对已连通的 client / 服务端 stream。
    async fn connected() -> (tokio::net::TcpStream, ConnStream, std::net::SocketAddr) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (stream, peer) = listener.accept().await.unwrap();
        (client, ConnStream::Plain(stream), peer)
    }

    async fn register(
        reg_tx: &mpsc::Sender<ConnectionRegistration>,
        id: u64,
        stream: ConnStream,
        peer: std::net::SocketAddr,
    ) {
        reg_tx
            .send(ConnectionRegistration {
                connection_id: id,
                stream,
                peer_addr: peer,
            })
            .await
            .unwrap();
    }

    /// 在超时内取一批事件；超时即视为失败（测试期望有数据）。
    async fn recv(source: &mut TcpSource, timeout_ms: u64) -> SourceBatch {
        tokio::time::timeout(
            std::time::Duration::from_millis(timeout_ms),
            source.receive(),
        )
        .await
        .expect("receive timed out")
        .expect("receive failed")
    }

    /// 在超时内取一批事件；超时返回 None（容忍间隙，不会 panic）。
    async fn maybe_recv(source: &mut TcpSource, timeout_ms: u64) -> Option<SourceBatch> {
        match tokio::time::timeout(
            std::time::Duration::from_millis(timeout_ms),
            source.receive(),
        )
        .await
        {
            Ok(Ok(batch)) => Some(batch),
            Ok(Err(e)) => panic!("receive failed: {e}"),
            Err(_) => None,
        }
    }

    fn texts(batch: &SourceBatch) -> Vec<String> {
        batch
            .iter()
            .map(|ev| match &ev.payload {
                wp_model_core::raw::RawData::String(s) => s.clone(),
                wp_model_core::raw::RawData::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
                wp_model_core::raw::RawData::ArcBytes(b) => String::from_utf8_lossy(b).into_owned(),
            })
            .collect()
    }

    #[tokio::test]
    async fn test_tcp_source_creation() {
        let registry = Arc::new(Mutex::new(HashSet::new()));
        let (_tx, rx) = mpsc::channel(8);
        let source = TcpSource::new(
            "test_tcp".to_string(),
            Tags::default(),
            "127.0.0.1:0".to_string(),
            8192,
            FramingMode::Line,
            registry,
            rx,
            crate::net::CodecConfig::default(),
        );
        assert!(source.is_ok());
    }

    #[tokio::test]
    async fn receive_reads_from_registered_connection() {
        let registry = Arc::new(Mutex::new(HashSet::new()));
        let (reg_tx, reg_rx) = mpsc::channel(8);
        let mut source = TcpSource::new(
            "test_tcp".to_string(),
            Tags::default(),
            "127.0.0.1:0".to_string(),
            8192,
            FramingMode::Line,
            registry.clone(),
            reg_rx,
            crate::net::CodecConfig::default(),
        )
        .unwrap();
        let (_ctrl_tx, ctrl_rx) = async_broadcast::broadcast(1);
        source.start(ctrl_rx).await.unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client =
            tokio::spawn(async move { tokio::net::TcpStream::connect(addr).await.unwrap() });
        let (stream, peer_addr) = listener.accept().await.unwrap();
        let mut client_stream = client.await.unwrap();

        reg_tx
            .send(ConnectionRegistration {
                connection_id: 1,
                stream: ConnStream::Plain(stream),
                peer_addr,
            })
            .await
            .unwrap();
        registry.lock().unwrap_or_else(|e| e.into_inner()).insert(1);

        use tokio::io::AsyncWriteExt;
        client_stream.write_all(b"hello\n").await.unwrap();

        let batch = tokio::time::timeout(std::time::Duration::from_secs(1), source.receive())
            .await
            .expect("receive result")
            .unwrap();
        assert_eq!(batch.len(), 1);
    }

    #[tokio::test]
    async fn connection_requeued_for_multiple_batches() {
        let registry = Arc::new(Mutex::new(HashSet::new()));
        let (reg_tx, reg_rx) = mpsc::channel(8);
        let mut source = TcpSource::new(
            "test_tcp".to_string(),
            Tags::default(),
            "127.0.0.1:0".to_string(),
            8192,
            FramingMode::Line,
            registry.clone(),
            reg_rx,
            crate::net::CodecConfig::default(),
        )
        .unwrap();
        let (_ctrl_tx, ctrl_rx) = async_broadcast::broadcast(1);
        source.start(ctrl_rx).await.unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client =
            tokio::spawn(async move { tokio::net::TcpStream::connect(addr).await.unwrap() });
        let (stream, peer_addr) = listener.accept().await.unwrap();
        let mut client_stream = client.await.unwrap();

        reg_tx
            .send(ConnectionRegistration {
                connection_id: 2,
                stream: ConnStream::Plain(stream),
                peer_addr,
            })
            .await
            .unwrap();
        registry.lock().unwrap_or_else(|e| e.into_inner()).insert(2);

        use tokio::io::AsyncWriteExt;
        client_stream.write_all(b"first\n").await.unwrap();

        let first = tokio::time::timeout(std::time::Duration::from_secs(1), source.receive())
            .await
            .expect("first receive result")
            .unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(source.active_connections(), 1);

        client_stream.write_all(b"second\n").await.unwrap();

        let second = tokio::time::timeout(std::time::Duration::from_secs(1), source.receive())
            .await
            .expect("second receive result")
            .unwrap();
        assert_eq!(second.len(), 1);
    }

    /// 回归：#367 —— 一条连接发半帧（无换行）时，不得饿死其它已就绪的连接。
    /// 旧实现在 A 的 `try_read` 拿不到完整帧后落入阻塞读，只服务 A；
    /// 新实现应先把 A 放回队尾，继续读到 B / C 的完整行。
    #[tokio::test]
    async fn half_frame_connection_does_not_block_others() {
        use std::time::Duration;
        use tokio::io::AsyncWriteExt;

        let registry = Arc::new(Mutex::new(HashSet::new()));
        let (reg_tx, reg_rx) = mpsc::channel(8);
        let mut source = TcpSource::new(
            "test_tcp".to_string(),
            Tags::default(),
            "127.0.0.1:0".to_string(),
            8192,
            FramingMode::Line,
            registry.clone(),
            reg_rx,
            crate::net::CodecConfig::default(),
        )
        .unwrap();
        let (_ctrl_tx, ctrl_rx) = async_broadcast::broadcast(1);
        source.start(ctrl_rx).await.unwrap();

        // 连接 A：发 8192B 半行（无换行）后保持不动。
        let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client_a = tokio::net::TcpStream::connect(listener_a.local_addr().unwrap())
            .await
            .unwrap();
        let (stream_a, peer_a) = listener_a.accept().await.unwrap();
        client_a.write_all(&vec![b'A'; 8192]).await.unwrap();

        // 连接 B / C：发完整短行。
        let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client_b = tokio::net::TcpStream::connect(listener_b.local_addr().unwrap())
            .await
            .unwrap();
        let (stream_b, peer_b) = listener_b.accept().await.unwrap();
        client_b.write_all(b"B-line\n").await.unwrap();

        let listener_c = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client_c = tokio::net::TcpStream::connect(listener_c.local_addr().unwrap())
            .await
            .unwrap();
        let (stream_c, peer_c) = listener_c.accept().await.unwrap();
        client_c.write_all(b"C-line\n").await.unwrap();

        // 按 A,B,C 顺序注册，让 A 落在 connection_order 队首（复现原 bug 的前置条件）。
        for (id, stream, peer) in [
            (1u64, ConnStream::Plain(stream_a), peer_a),
            (2u64, ConnStream::Plain(stream_b), peer_b),
            (3u64, ConnStream::Plain(stream_c), peer_c),
        ] {
            registry
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(id);
            reg_tx
                .send(ConnectionRegistration {
                    connection_id: id,
                    stream,
                    peer_addr: peer,
                })
                .await
                .unwrap();
        }

        // 让 A 的半帧字节先到达 socket。
        tokio::time::sleep(Duration::from_millis(50)).await;

        // 关键断言：A 未补换行时，B / C 的完整行仍应在超时内被 receive 取出。
        let mut seen: HashSet<String> = HashSet::new();
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while !(seen.contains("B-line") && seen.contains("C-line"))
            && std::time::Instant::now() < deadline
        {
            let batch = tokio::time::timeout(Duration::from_millis(500), source.receive())
                .await
                .expect("receive blocked while another connection held a half frame")
                .unwrap();
            for ev in batch {
                match ev.payload {
                    wp_model_core::raw::RawData::Bytes(b) => {
                        seen.insert(String::from_utf8_lossy(&b).into_owned());
                    }
                    wp_model_core::raw::RawData::String(s) => {
                        seen.insert(s);
                    }
                    wp_model_core::raw::RawData::ArcBytes(b) => {
                        seen.insert(String::from_utf8_lossy(&b).into_owned());
                    }
                }
            }
        }
        assert!(
            seen.contains("B-line"),
            "connection B starved by A's half frame"
        );
        assert!(
            seen.contains("C-line"),
            "connection C starved by A's half frame"
        );

        // 保持三条 client 连接存活到断言结束。
        drop((client_a, client_b, client_c));
    }

    /// 多条同时就绪的连接都要被服务到（不饿死任何一条）；跨连接顺序不保证。
    #[tokio::test]
    async fn all_ready_connections_are_served() {
        use tokio::io::AsyncWriteExt;
        let (mut source, reg_tx) = new_source().await;

        let mut clients = Vec::new();
        for id in 1..=3u64 {
            let (mut client, stream, peer) = connected().await;
            client
                .write_all(format!("c{id}\n").as_bytes())
                .await
                .unwrap();
            register(&reg_tx, id, stream, peer).await;
            clients.push(client);
        }

        let mut seen: HashSet<String> = HashSet::new();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while seen.len() < 3 && std::time::Instant::now() < deadline {
            if let Some(batch) = maybe_recv(&mut source, 500).await {
                for t in texts(&batch) {
                    seen.insert(t);
                }
            }
        }
        assert_eq!(
            seen.len(),
            3,
            "all three ready connections should be served"
        );
        for id in 1..=3u64 {
            assert!(seen.contains(&format!("c{id}")), "missing c{id}");
        }

        // 再给 1 号发一行，仍应被送达。
        clients[0].write_all(b"c1-again\n").await.unwrap();
        let mut got: HashSet<String> = HashSet::new();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while !got.contains("c1-again") && std::time::Instant::now() < deadline {
            if let Some(batch) = maybe_recv(&mut source, 500).await {
                for t in texts(&batch) {
                    got.insert(t);
                }
            }
        }
        assert!(got.contains("c1-again"));
    }

    /// 新连接到达应唤醒阻塞中的 wait_any_ready（select 的新连接分支）。
    #[tokio::test]
    async fn new_connection_wakes_blocked_wait_any_ready() {
        use tokio::io::AsyncWriteExt;
        let (mut source, reg_tx) = new_source().await;

        // A 已注册但无数据，使 receive 进入 wait_any_ready 阻塞。
        let (_client_a, stream_a, peer_a) = connected().await;
        register(&reg_tx, 1, stream_a, peer_a).await;

        // 稍后注册 B，且 B 已有数据。
        let (mut client_b, stream_b, peer_b) = connected().await;
        client_b.write_all(b"new\n").await.unwrap();
        let tx = reg_tx.clone();
        let helper = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            register(&tx, 2, stream_b, peer_b).await;
        });

        let batch = recv(&mut source, 2000).await;
        helper.await.unwrap();
        assert_eq!(texts(&batch), vec!["new"]);
        assert_eq!(source.active_connections(), 2);
        drop(client_b);
    }

    /// 等待期间对端关闭：该连接应被注销，其它连接仍能被服务。
    #[tokio::test]
    async fn peer_close_during_wait_deregisters_and_keeps_others() {
        use tokio::io::AsyncWriteExt;
        let (mut source, reg_tx) = new_source().await;

        let (mut client_a, stream_a, peer_a) = connected().await;
        register(&reg_tx, 1, stream_a, peer_a).await;
        let (client_b, stream_b, peer_b) = connected().await;
        register(&reg_tx, 2, stream_b, peer_b).await;

        let closer = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            drop(client_b); // 关闭 B -> 触发 Closed
        });
        // A 的写入单独放在一个任务里，并把 client_a 返还，保持 A 打开到断言之后
        // （每连接任务会一直读到 EOF，若在此处 drop 就相当于把 A 也关了）。
        let a_writer = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            client_a.write_all(b"A\n").await.unwrap();
            client_a
        });

        let batch = recv(&mut source, 3000).await;
        closer.await.unwrap();
        let client_a = a_writer.await.unwrap();
        assert_eq!(texts(&batch), vec!["A"]);
        assert_eq!(
            source.active_connections(),
            1,
            "closed connection should be deregistered"
        );
        drop(client_a);
    }

    /// 半帧连接之后补全换行：先服务其它连接，随后仍能交付被缓存的半帧。
    #[tokio::test]
    async fn half_frame_completes_after_others_served() {
        use tokio::io::AsyncWriteExt;
        let (mut source, reg_tx) = new_source().await;

        let (mut client_a, stream_a, peer_a) = connected().await;
        client_a.write_all(b"partial").await.unwrap(); // 无换行
        register(&reg_tx, 1, stream_a, peer_a).await;

        let (mut client_b, stream_b, peer_b) = connected().await;
        client_b.write_all(b"b\n").await.unwrap();
        register(&reg_tx, 2, stream_b, peer_b).await;

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // 第一轮应拿到 B，而不是被 A 的半帧堵住。
        assert_eq!(texts(&recv(&mut source, 1000).await), vec!["b"]);

        // A 补上换行后，之前缓存的半帧应被补全并交付（requeue 不丢缓冲）。
        client_a.write_all(b"\n").await.unwrap();
        assert_eq!(texts(&recv(&mut source, 1000).await), vec!["partial"]);
    }

    /// 多连接交错发送：不能丢事件，也不能重复。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn interleaved_writers_no_event_loss() {
        use tokio::io::AsyncWriteExt;
        const CONNS: u64 = 3;
        const PER: usize = 50;

        let (mut source, reg_tx) = new_source().await;

        let mut writers = Vec::new();
        for id in 1..=CONNS {
            let (mut client, stream, peer) = connected().await;
            register(&reg_tx, id, stream, peer).await;
            writers.push(tokio::spawn(async move {
                for seq in 0..PER {
                    client
                        .write_all(format!("c{id}-{seq}\n").as_bytes())
                        .await
                        .unwrap();
                    if seq % 10 == 0 {
                        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    }
                }
            }));
        }

        let total = CONNS as usize * PER;
        // 用计数而非 HashSet：重复投递也必须能被发现。
        let mut counts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while counts.len() < total && std::time::Instant::now() < deadline {
            if let Some(batch) = maybe_recv(&mut source, 500).await {
                for t in texts(&batch) {
                    *counts.entry(t).or_insert(0) += 1;
                }
            }
        }
        for w in writers {
            w.await.unwrap();
        }
        assert_eq!(counts.len(), total, "unexpected or duplicate event keys");
        assert!(
            counts.values().all(|&c| c == 1),
            "some events were delivered more than once"
        );
        for id in 1..=CONNS {
            for seq in 0..PER {
                assert!(
                    counts.contains_key(&format!("c{id}-{seq}")),
                    "missing c{id}-{seq}"
                );
            }
        }
    }

    /// 注册通道关闭后：conn_rx_closed 置位，已有连接仍能被服务，不忙轮询。
    #[tokio::test]
    async fn channel_closed_still_serves_existing_connections() {
        use tokio::io::AsyncWriteExt;
        let (mut source, reg_tx) = new_source().await;

        let (mut client_a, stream_a, peer_a) = connected().await;
        register(&reg_tx, 1, stream_a, peer_a).await;
        // 关闭注册通道：receive 应把 conn_rx_closed 置位，而不是忙轮询。
        drop(reg_tx);

        let helper = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            client_a.write_all(b"hello\n").await.unwrap();
        });

        let batch = recv(&mut source, 2000).await;
        helper.await.unwrap();
        assert_eq!(texts(&batch), vec!["hello"]);
    }

    /// 持续就绪的连接不能饿死其它连接（轮转公平性）。
    #[tokio::test]
    async fn continuously_ready_connection_does_not_starve_others() {
        use tokio::io::AsyncWriteExt;
        let (mut source, reg_tx) = new_source().await;

        // A：持续写（始终有数据），先注册以占据 connection_order 队首。
        let (mut client_a, stream_a, peer_a) = connected().await;
        register(&reg_tx, 1, stream_a, peer_a).await;
        let a_writer = tokio::spawn(async move {
            for i in 0..1000u32 {
                if client_a
                    .write_all(format!("a-{i}\n").as_bytes())
                    .await
                    .is_err()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        });

        // B：只发一行。
        let (mut client_b, stream_b, peer_b) = connected().await;
        register(&reg_tx, 2, stream_b, peer_b).await;
        client_b.write_all(b"b\n").await.unwrap();

        // 头几批里就应该出现 B；若轮转失效，B 会被一直挡在 A 后面。
        let mut seen: HashSet<String> = HashSet::new();
        for _ in 0..5 {
            for t in texts(&recv(&mut source, 1000).await) {
                seen.insert(t);
            }
            if seen.contains("b") {
                break;
            }
        }
        a_writer.abort();
        assert!(
            seen.contains("b"),
            "a continuously-ready connection starved a ready one"
        );
    }

    /// 无连接 + 注册通道关闭 -> receive 返回 Err（不挂起、不空转）。
    #[tokio::test]
    async fn receive_errors_when_channel_closed_and_no_connections() {
        let (mut source, reg_tx) = new_source().await;
        drop(reg_tx);
        let res = tokio::time::timeout(std::time::Duration::from_secs(1), source.receive()).await;
        assert!(matches!(res, Ok(Err(_))), "expected a disconnect error");
    }

    /// 未 start 就 receive -> Err（不 panic、不挂起）。
    #[tokio::test]
    async fn receive_before_start_errors() {
        let registry = Arc::new(Mutex::new(HashSet::new()));
        let (_tx, rx) = mpsc::channel(1);
        let mut source = TcpSource::new(
            "t".to_string(),
            Tags::default(),
            "127.0.0.1:0".to_string(),
            8192,
            FramingMode::Line,
            registry,
            rx,
            crate::net::CodecConfig::default(),
        )
        .unwrap();
        let res = tokio::time::timeout(std::time::Duration::from_secs(1), source.receive()).await;
        assert!(
            matches!(res, Ok(Err(_))),
            "receive before start should error"
        );
    }

    /// 对端在 receive 之前就已关闭：该连接应被注销，最终返回 Err。
    #[tokio::test]
    async fn peer_closed_before_receive_deregisters() {
        let (mut source, reg_tx) = new_source().await;
        let (client, stream, peer) = connected().await;
        register(&reg_tx, 1, stream, peer).await;
        drop(client); // FIN，且无数据
        drop(reg_tx); // 通道关闭：注销后不再有连接 -> Err
        let res = tokio::time::timeout(std::time::Duration::from_secs(2), source.receive()).await;
        assert!(
            matches!(res, Ok(Err(_))),
            "expected a disconnect error after the peer closed"
        );
        assert_eq!(
            source.active_connections(),
            0,
            "the closed connection should be deregistered"
        );
    }

    /// 一次突发超过单批上限后，剩余帧（pending/缓冲）仍应被投递，不在无新数据时滞留。
    #[tokio::test]
    async fn burst_tail_is_delivered_via_pending() {
        use tokio::io::AsyncWriteExt;
        const FRAME: usize = 4096;
        const FRAMES: usize = 20; // 20 * 4096 > 单批字节上限（64KiB）-> 触发溢出
        let (mut source, reg_tx) = new_source().await;
        let (mut client, stream, peer) = connected().await;
        register(&reg_tx, 1, stream, peer).await;
        let writer = tokio::spawn(async move {
            let line = vec![b'z'; FRAME];
            for _ in 0..FRAMES {
                client.write_all(&line).await.unwrap();
                client.write_all(b"\n").await.unwrap();
            }
        }); // 结束即关闭连接，EOF 会 flush 剩余缓冲

        let mut total = 0usize;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while total < FRAMES && std::time::Instant::now() < deadline {
            if let Some(batch) = maybe_recv(&mut source, 500).await {
                total += batch.len();
            }
        }
        writer.await.unwrap();
        assert_eq!(
            total, FRAMES,
            "the burst tail should be delivered, not stranded"
        );
    }

    /// 大记录半帧：先服务其它连接；补齐后该帧应完整交付一次（requeue 不丢缓冲）。
    #[tokio::test]
    async fn large_half_frame_completes_intact() {
        use tokio::io::AsyncWriteExt;
        const BIG: usize = 8192;
        let (mut source, reg_tx) = new_source().await;

        let (mut client_a, stream_a, peer_a) = connected().await;
        client_a.write_all(&vec![b'A'; BIG]).await.unwrap(); // 无换行
        register(&reg_tx, 1, stream_a, peer_a).await;

        let (mut client_b, stream_b, peer_b) = connected().await;
        client_b.write_all(b"b\n").await.unwrap();
        register(&reg_tx, 2, stream_b, peer_b).await;

        // 先拿到 B，不被 A 的大半帧堵住。
        assert_eq!(texts(&recv(&mut source, 1000).await), vec!["b"]);

        // A 补齐换行后，交付恰好一帧、长度/内容完整。
        client_a.write_all(b"\n").await.unwrap();
        let batch = recv(&mut source, 1000).await;
        assert_eq!(batch.len(), 1);
        match &batch[0].payload {
            wp_model_core::raw::RawData::Bytes(b) => {
                assert_eq!(b.len(), BIG, "large frame truncated across requeue");
                assert!(b.iter().all(|&x| x == b'A'), "large frame corrupted");
            }
            _ => panic!("expected bytes payload"),
        }
    }

    /// 两个空闲连接：之后其中一个发送时，其数据仍应被交付。
    #[tokio::test]
    async fn idle_connections_then_one_sends() {
        use tokio::io::AsyncWriteExt;
        let (mut source, reg_tx) = new_source().await;
        let (mut client_a, stream_a, peer_a) = connected().await;
        register(&reg_tx, 1, stream_a, peer_a).await;
        let (_client_b, stream_b, peer_b) = connected().await;
        register(&reg_tx, 2, stream_b, peer_b).await;

        let helper = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            client_a.write_all(b"A\n").await.unwrap();
        });
        let batch = recv(&mut source, 2000).await;
        helper.await.unwrap();
        assert_eq!(texts(&batch), vec!["A"]);
        assert_eq!(source.active_connections(), 2);
    }

    /// 慢路径公平性：`wait_any_ready` 应服务 `connection_order` 队首，并把该连接
    /// 轮转到队尾（避免固定顺序偏好饿死其它连接）。
    #[tokio::test]
    async fn wait_any_ready_rotates_served_connection() {
        use tokio::io::AsyncWriteExt;
        let (mut source, reg_tx) = new_source().await;

        // 两条都已写好完整行（调用慢路径时都就绪）。
        let (mut ca, sa, pa) = connected().await;
        ca.write_all(b"a\n").await.unwrap();
        register(&reg_tx, 1, sa, pa).await;
        let (mut cb, sb, pb) = connected().await;
        cb.write_all(b"b\n").await.unwrap();
        register(&reg_tx, 2, sb, pb).await;

        // 等两条写都到达 socket（否则可能只有一条就绪，断言会不适当地失败）。
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        // 排入 connections/connection_order，但**不**走 receive（保持两者都就绪）。
        source.drain_new_connections();
        assert_eq!(source.active_connections(), 2);
        let front = source.connection_order.front().copied().expect("front");

        // 直接驱动慢路径：应服务队首连接（id=1 -> "a"）。
        let batch = source
            .wait_any_ready()
            .await
            .unwrap()
            .expect("should produce");
        assert_eq!(texts(&batch), vec!["a"]);

        // 队首已被轮转到队尾:现在的队首应是另一条。
        assert_eq!(
            source.connection_order.back().copied(),
            Some(front),
            "served connection should rotate to the back"
        );
    }

    /// P：registry 锁中毒后不应 panic（poison-tolerant）。
    #[tokio::test]
    async fn poisoned_registry_lock_does_not_panic() {
        let registry: ConnectionRegistry = Arc::new(Mutex::new(HashSet::new()));
        // 让锁中毒：持锁线程 panic。
        let r2 = registry.clone();
        let h = std::thread::spawn(move || {
            let _g = r2.lock().unwrap();
            panic!("poison the registry mutex");
        });
        assert!(h.join().is_err());
        assert!(registry.is_poisoned(), "registry mutex should be poisoned");

        let (reg_tx, reg_rx) = mpsc::channel(8);
        let mut source = TcpSource::new(
            "poison".to_string(),
            Tags::default(),
            "127.0.0.1:0".to_string(),
            8192,
            FramingMode::Line,
            registry.clone(),
            reg_rx,
            crate::net::CodecConfig::default(),
        )
        .unwrap();
        let (_tx, ctrl_rx) = async_broadcast::broadcast(1);
        source.start(ctrl_rx).await.unwrap();

        // register_connection（插入 registry）在中毒锁下不能 panic。
        let (client, stream, peer) = connected().await;
        register(&reg_tx, 1, stream, peer).await;
        source.drain_new_connections();
        // close（clear registry）同样不能 panic。
        source.close().await.unwrap();
        drop(client);
        assert!(
            registry
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .is_empty()
        );
    }
}
