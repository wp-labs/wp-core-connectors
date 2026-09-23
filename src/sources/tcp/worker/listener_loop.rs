use crate::sources::tcp::ConnectionRegistry;
use crate::sources::tcp::conn::ConnStream;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc};
use tokio::time;
use wp_connector_api::{SourceReason, SourceResult};

use std::net::SocketAddr;

/// TLS 握手超时：避免慢/恶意客户端在握手阶段阻塞 accept 循环。
const TLS_HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

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
        let active = self.registry.lock().unwrap().len();
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
                let connection_id = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as u64;
                self.registry.lock().unwrap().insert(connection_id);

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
                        self.registry.lock().unwrap().remove(&connection_id);
                    }
                } else {
                    error_ctrl!(
                        "TCP listener loop '{}' has no reader instances to receive connection {}",
                        self.key,
                        connection_id
                    );
                    self.registry.lock().unwrap().remove(&connection_id);
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
