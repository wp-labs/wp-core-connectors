use crate::sources::tcp::ConnectionRegistry;
use crate::sources::tcp::conn::ConnStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc};
use tokio::time;
use wp_connector_api::{SourceReason, SourceResult};

use std::net::SocketAddr;

/// TLS 握手超时：避免慢/恶意客户端在握手阶段阻塞 accept 循环。
const TLS_HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// 进程内单调递增的 connection id（避免用时间戳纳秒导致碰撞）。
static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

pub struct ConnectionRegistration {
    pub connection_id: u64,
    pub stream: ConnStream,
    pub peer_addr: SocketAddr,
}

/// TCP listener loop: bind/accept and dispatch sockets to TcpSource instances.
pub struct TcpListenerLoop {
    pub(crate) key: String,
    pub(crate) address: String,
    pub(crate) max_connections: usize,
    pub(crate) registry: ConnectionRegistry,
    pub(crate) stop_tx: broadcast::Sender<()>,
    pub(crate) instance_reg_txs: Vec<mpsc::Sender<ConnectionRegistration>>,
    pub(crate) next_reader_idx: usize,
    pub(crate) tls: Option<Arc<rustls::ServerConfig>>,
}

impl TcpListenerLoop {
    pub fn new(
        key: String,
        address: String,
        max_connections: usize,
        registry: ConnectionRegistry,
        stop_tx: broadcast::Sender<()>,
        instance_reg_txs: Vec<mpsc::Sender<ConnectionRegistration>>,
        tls: Option<Arc<rustls::ServerConfig>>,
    ) -> Self {
        Self {
            key,
            address,
            max_connections,
            registry,
            stop_tx,
            instance_reg_txs,
            next_reader_idx: 0,
            tls,
        }
    }

    pub async fn run(&mut self) -> SourceResult<()> {
        let listener = TcpListener::bind(&self.address).await.map_err(|e| {
            SourceReason::disconnect(format!(
                "failed to bind TCP socket to {}: {}",
                self.address, e
            ))
        })?;

        let local = listener
            .local_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| self.address.clone());

        info_ctrl!(
            "TCP listen '{}' addr={} local={}",
            self.key,
            self.address,
            local
        );

        let mut stop_rx = self.stop_tx.subscribe();
        let mut accept_interval = time::interval(time::Duration::from_millis(10));
        accept_interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = stop_rx.recv() => {
                    info_ctrl!("TCP listener loop '{}' stopped", self.key);
                    break;
                }
                _ = accept_interval.tick() => {
                    if let Err(e) = self.accept_connection(&listener).await {
                        error_ctrl!("TCP listener loop '{}' accept failed: {}", self.key, e);
                    }
                }
            }
        }

        Ok(())
    }

    async fn accept_connection(&mut self, listener: &TcpListener) -> SourceResult<()> {
        let active = self
            .registry
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len();
        if active >= self.max_connections {
            warn_ctrl!(
                "TCP listener loop '{}' reached max connections ({} >= {}), skip incoming",
                self.key,
                active,
                self.max_connections
            );
            return Ok(());
        }

        match time::timeout(time::Duration::from_millis(1), listener.accept()).await {
            Ok(Ok((stream, addr))) => {
                // 开启安全传输时，先完成 TLS 握手再分发连接
                let stream = match &self.tls {
                    Some(config) => {
                        let acceptor = tokio_rustls::TlsAcceptor::from(config.clone());
                        match time::timeout(TLS_HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await {
                            Ok(Ok(tls_stream)) => ConnStream::Tls(Box::new(tls_stream)),
                            Ok(Err(e)) => {
                                error_ctrl!(
                                    "TCP listener loop '{}' TLS handshake with {} failed: {}",
                                    self.key,
                                    addr,
                                    e
                                );
                                return Ok(());
                            }
                            Err(_) => {
                                error_ctrl!(
                                    "TCP listener loop '{}' TLS handshake with {} timed out",
                                    self.key,
                                    addr
                                );
                                return Ok(());
                            }
                        }
                    }
                    None => ConnStream::Plain(stream),
                };
                // 单调递增 id（进程内唯一），避免时间戳纳秒碰撞（F6）。
                let connection_id = NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
                self.registry
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .insert(connection_id);

                info_ctrl!(
                    "TCP listener loop '{}' accepted peer {} as connection {} (active={})",
                    self.key,
                    addr,
                    connection_id,
                    active + 1
                );
                if let Some(tx) = self.next_reader_sender() {
                    if let Err(e) = tx
                        .send(ConnectionRegistration {
                            connection_id,
                            stream,
                            peer_addr: addr,
                        })
                        .await
                    {
                        warn_ctrl!(
                            "TCP listener loop '{}' failed to dispatch connection {}: {}",
                            self.key,
                            connection_id,
                            e
                        );
                        self.registry
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .remove(&connection_id);
                    }
                } else {
                    error_ctrl!(
                        "TCP listener loop '{}' has no reader instances to receive connection {}",
                        self.key,
                        connection_id
                    );
                    self.registry
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .remove(&connection_id);
                }
            }
            Ok(Err(e)) => {
                error_ctrl!("TCP listener loop '{}' accept error: {}", self.key, e);
            }
            Err(_) => {}
        }
        Ok(())
    }

    fn next_reader_sender(&mut self) -> Option<mpsc::Sender<ConnectionRegistration>> {
        if self.instance_reg_txs.is_empty() {
            return None;
        }
        let idx = self.next_reader_idx % self.instance_reg_txs.len();
        self.next_reader_idx = (self.next_reader_idx + 1) % self.instance_reg_txs.len();
        Some(self.instance_reg_txs[idx].clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::tcp::ConnectionRegistry;
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};

    /// F6：两次 accept 得到不同且单调递增的 connection id。
    #[tokio::test]
    async fn accepted_connections_get_distinct_monotonic_ids() {
        let registry: ConnectionRegistry = Arc::new(Mutex::new(HashSet::new()));
        let (stop_tx, _stop_rx) = broadcast::channel::<()>(8);
        let (reg_tx, mut reg_rx) = mpsc::channel(8);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut listener_loop = TcpListenerLoop::new(
            "id_test".to_string(),
            addr.to_string(),
            1000,
            registry.clone(),
            stop_tx,
            vec![reg_tx],
            None,
        );

        // 连接先建立（进入 accept backlog），再重试 accept 直到拿到注册。
        let _c1 = tokio::net::TcpStream::connect(addr).await.unwrap();
        let r1 = accept_until_registered(&mut listener_loop, &listener, &mut reg_rx).await;

        let _c2 = tokio::net::TcpStream::connect(addr).await.unwrap();
        let r2 = accept_until_registered(&mut listener_loop, &listener, &mut reg_rx).await;

        assert_ne!(r1.connection_id, r2.connection_id, "ids must be distinct");
        assert!(r2.connection_id > r1.connection_id, "ids must be monotonic");
        let reg = registry.lock().unwrap_or_else(|e| e.into_inner());
        assert!(reg.contains(&r1.connection_id));
        assert!(reg.contains(&r2.connection_id));
    }

    /// F6 强版：连续 accept 多条连接，id 必须两两不同。时间戳纳秒方案在快速连续
    /// accept 下可能碰撞（id 相同 -> registry/HashMap 互相覆盖、丢连接），
    /// 单调计数器则不会。
    #[tokio::test]
    async fn many_rapid_accepts_get_unique_ids() {
        const CONNS: usize = 16;
        let registry: ConnectionRegistry = Arc::new(Mutex::new(HashSet::new()));
        let (stop_tx, _stop_rx) = broadcast::channel::<()>(8);
        let (reg_tx, mut reg_rx) = mpsc::channel(8);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut listener_loop = TcpListenerLoop::new(
            "many_ids".to_string(),
            addr.to_string(),
            100000,
            registry.clone(),
            stop_tx,
            vec![reg_tx],
            None,
        );

        let mut ids = HashSet::new();
        let mut clients = Vec::with_capacity(CONNS);
        for _ in 0..CONNS {
            clients.push(tokio::net::TcpStream::connect(addr).await.unwrap());
            let reg = accept_until_registered(&mut listener_loop, &listener, &mut reg_rx).await;
            assert!(
                ids.insert(reg.connection_id),
                "duplicate connection id {}",
                reg.connection_id
            );
        }
        assert_eq!(ids.len(), CONNS, "every accept must yield a unique id");
        assert_eq!(
            registry.lock().unwrap_or_else(|e| e.into_inner()).len(),
            CONNS
        );
    }

    /// P：registry 锁中毒后 accept 路径不应 panic（poison-tolerant）。
    #[tokio::test]
    async fn poisoned_registry_lock_does_not_panic_on_accept() {
        let registry: ConnectionRegistry = Arc::new(Mutex::new(HashSet::new()));
        // 让锁中毒：持锁线程 panic。
        let r2 = registry.clone();
        let h = std::thread::spawn(move || {
            let _g = r2.lock().unwrap();
            panic!("poison the registry mutex");
        });
        assert!(h.join().is_err());
        assert!(registry.is_poisoned(), "registry mutex should be poisoned");

        let (stop_tx, _stop_rx) = broadcast::channel::<()>(8);
        let (reg_tx, mut reg_rx) = mpsc::channel(8);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut listener_loop = TcpListenerLoop::new(
            "poison_accept".to_string(),
            addr.to_string(),
            1000,
            registry.clone(),
            stop_tx,
            vec![reg_tx],
            None,
        );

        // accept_connection 会读 registry（max_connections 计数）并写入新 id，
        // 中毒锁下都必须容错返回数据而不是 panic。
        let _c = tokio::net::TcpStream::connect(addr).await.unwrap();
        let reg = accept_until_registered(&mut listener_loop, &listener, &mut reg_rx).await;
        assert!(
            registry
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .contains(&reg.connection_id)
        );
    }

    async fn accept_until_registered(
        listener_loop: &mut TcpListenerLoop,
        listener: &TcpListener,
        reg_rx: &mut mpsc::Receiver<ConnectionRegistration>,
    ) -> ConnectionRegistration {
        for _ in 0..200 {
            listener_loop.accept_connection(listener).await.unwrap();
            if let Ok(reg) = reg_rx.try_recv() {
                return reg;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        panic!("connection was not accepted within the retry budget");
    }
}
