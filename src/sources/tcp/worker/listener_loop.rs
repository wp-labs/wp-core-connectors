use crate::sources::tcp::ConnectionRegistry;
use crate::sources::tcp::conn::ConnStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, broadcast, mpsc};
use tokio::time;
use wp_connector_api::{SourceReason, SourceResult};

use std::net::SocketAddr;

/// TLS 握手超时：避免慢/恶意客户端在握手阶段阻塞 accept 循环。
const TLS_HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// 同时进行的（TLS）握手上限：握手在独立 task 中执行，此上限防止握手风暴耗尽资源。
const MAX_CONCURRENT_HANDSHAKES: usize = 1024;

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
    /// 限制同时进行的握手数（握手在独立 task 中执行，不阻塞 accept 循环）。
    handshake_limit: Arc<Semaphore>,
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
            handshake_limit: Arc::new(Semaphore::new(MAX_CONCURRENT_HANDSHAKES)),
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

        // accept 与「握手 + 分发」解耦：accept 立即返回，握手/分发在独立 task 中执行，
        // 故慢/恶意客户端的握手不会阻塞其它连接的 accept，也不会延迟 stop 信号。
        // 直接 select 在 listener.accept() 上（而非固定 tick），新连接可即时接入。
        loop {
            tokio::select! {
                _ = stop_rx.recv() => {
                    info_ctrl!("TCP listener loop '{}' stopped", self.key);
                    break;
                }
                accepted = listener.accept() => {
                    match accepted {
                        Ok((stream, addr)) => self.handle_accepted(stream, addr),
                        Err(e) => {
                            error_ctrl!("TCP listener loop '{}' accept error: {}", self.key, e);
                            // accept 资源类错误（如 EMFILE/ENFILE）会持续返回，若立即重试会忙循环；
                            // 短退避（同时仍可被 stop 打断的前一项 select 轮询覆盖）。
                            time::sleep(time::Duration::from_millis(10)).await;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// 仅用于测试：以 1ms 超时接受一个连接并处理（生产路径走 `run` 的 `select!` 分支）。
    #[cfg(test)]
    async fn accept_connection(&mut self, listener: &TcpListener) -> SourceResult<()> {
        match time::timeout(time::Duration::from_millis(1), listener.accept()).await {
            Ok(Ok((stream, addr))) => {
                self.handle_accepted(stream, addr);
                Ok(())
            }
            Ok(Err(e)) => {
                error_ctrl!("TCP listener loop '{}' accept error: {}", self.key, e);
                Ok(())
            }
            Err(_) => Ok(()),
        }
    }

    /// 处理一个已 accept 的连接：限流 → 取握手 permit → 分配 id、登记 → 在独立 task 中
    /// 完成（可选）TLS 握手并分发。**同步返回**，握手不阻塞 accept 循环。
    fn handle_accepted(&mut self, stream: TcpStream, addr: SocketAddr) {
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
            return;
        }

        // 握手并发上限：非阻塞取 permit；拿不到说明握手风暴 → 丢弃该连接。
        let permit = match self.handshake_limit.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                warn_ctrl!(
                    "TCP listener loop '{}' handshake concurrency limit ({}) reached, dropping {}",
                    self.key,
                    MAX_CONCURRENT_HANDSHAKES,
                    addr
                );
                return;
            }
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

        let reg_tx = self.next_reader_sender();
        let tls = self.tls.clone();
        let registry = self.registry.clone();
        let key = self.key.clone();

        tokio::spawn(async move {
            let _permit = permit;
            // （可选）TLS 握手——在独立 task 内完成，不阻塞 accept 循环。
            let stream = match tls {
                Some(config) => {
                    let acceptor = tokio_rustls::TlsAcceptor::from(config);
                    match time::timeout(TLS_HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await {
                        Ok(Ok(tls_stream)) => ConnStream::Tls(Box::new(tls_stream)),
                        Ok(Err(e)) => {
                            error_ctrl!(
                                "TCP listener loop '{}' TLS handshake with {} failed: {}",
                                key,
                                addr,
                                e
                            );
                            registry
                                .lock()
                                .unwrap_or_else(|e| e.into_inner())
                                .remove(&connection_id);
                            return;
                        }
                        Err(_) => {
                            error_ctrl!(
                                "TCP listener loop '{}' TLS handshake with {} timed out",
                                key,
                                addr
                            );
                            registry
                                .lock()
                                .unwrap_or_else(|e| e.into_inner())
                                .remove(&connection_id);
                            return;
                        }
                    }
                }
                None => ConnStream::Plain(stream),
            };

            match reg_tx {
                Some(tx) => {
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
                            key,
                            connection_id,
                            e
                        );
                        registry
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .remove(&connection_id);
                    }
                }
                None => {
                    error_ctrl!(
                        "TCP listener loop '{}' has no reader instances to receive connection {}",
                        key,
                        connection_id
                    );
                    registry
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .remove(&connection_id);
                }
            }
        });
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

    /// #1：一个客户端只建 TCP 连接、不完成 TLS 握手（握手悬挂）时，不得阻塞其它连接
    /// 的接入与分发——accept 与握手已解耦（握手在独立 task 中执行）。
    #[tokio::test]
    async fn stalled_tls_handshake_does_not_block_other_connections() {
        // TLS 服务端配置
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
        let tls_config = Some(server_tls.build_server_config().unwrap());

        let registry: ConnectionRegistry = Arc::new(Mutex::new(HashSet::new()));
        let (stop_tx, _stop_rx) = broadcast::channel::<()>(8);
        let (reg_tx, mut reg_rx) = mpsc::channel(8);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut listener_loop = TcpListenerLoop::new(
            "tls_stall".to_string(),
            addr.to_string(),
            1000,
            registry.clone(),
            stop_tx,
            vec![reg_tx],
            tls_config,
        );

        // 客户端 1：只建 TCP，不发 ClientHello（握手悬挂）。
        let _stalled = tokio::net::TcpStream::connect(addr).await.unwrap();
        // 客户端 2：完成 TLS 握手。
        let client_tls = crate::net::TlsConfig {
            insecure: true,
            ..Default::default()
        };
        let connector = tokio_rustls::TlsConnector::from(client_tls.build_client_config().unwrap());
        let server_name = rustls::pki_types::ServerName::try_from("localhost".to_string()).unwrap();
        let handshaken = tokio::spawn(async move {
            let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
            let _tls = connector.connect(server_name, tcp).await.unwrap();
            std::future::pending::<()>().await;
        });

        // 驱动 accept 直到客户端 2 被登记并分发。若握挂在 accept 循环里串行执行，
        // 第一个 accept 会被客户端 1 卡住（~10s），此 2s 上限就会触发。
        let reg = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let _ = listener_loop.accept_connection(&listener).await;
                if let Ok(reg) = reg_rx.try_recv() {
                    return reg;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("a stalled TLS handshake must not block accepting other connections");

        assert!(
            matches!(reg.stream, ConnStream::Tls(_)),
            "the dispatched connection should be the TLS stream"
        );
        handshaken.abort();
    }

    /// #1：`run()` 真实 accept 循环——接入并分发一条连接，随后响应停止信号退出。
    #[tokio::test]
    async fn run_accepts_and_dispatches_then_stops() {
        let registry: ConnectionRegistry = Arc::new(Mutex::new(HashSet::new()));
        let (stop_tx, _stop_rx) = broadcast::channel::<()>(8);
        let (reg_tx, mut reg_rx) = mpsc::channel(8);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener); // run() 会重新 bind 该地址

        let mut listener_loop = TcpListenerLoop::new(
            "run_test".to_string(),
            addr.to_string(),
            1000,
            registry.clone(),
            stop_tx.clone(),
            vec![reg_tx],
            None,
        );
        let run_task = tokio::spawn(async move { listener_loop.run().await });

        // 等 bind 完成并接入一条连接
        let client = {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            loop {
                match tokio::net::TcpStream::connect(addr).await {
                    Ok(s) => break s,
                    Err(_) if std::time::Instant::now() < deadline => {
                        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    }
                    Err(e) => panic!("connect failed: {e}"),
                }
            }
        };
        let reg = tokio::time::timeout(std::time::Duration::from_secs(2), reg_rx.recv())
            .await
            .expect("dispatch timed out")
            .expect("registration");
        assert!(matches!(reg.stream, ConnStream::Plain(_)));
        assert!(
            registry
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .contains(&reg.connection_id)
        );

        // 停止：run() 应退出
        let _ = stop_tx.send(());
        tokio::time::timeout(std::time::Duration::from_secs(2), run_task)
            .await
            .expect("run() should stop after stop signal")
            .expect("run task panicked")
            .expect("run() returned Err");
        drop(client);
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
