//! TCP 源吞吐微基准（读 + 分帧路径）。
//!
//! 直接驱动真实的 `TcpSource`：建 `conns` 条连接、每条连续发 `lines` 条定长行，
//! 并发跑 `receive()` 收集事件，测 EPS / MB/s。用于对同一 crate 的不同
//! `source.rs` 实现做 A/B（只换 `source.rs`，本文件不变）。
//!
//! 运行（默认被 `WP_TCP_BENCH` 关闭，避免拖慢常规测试）：
//! ```sh
//! WP_TCP_BENCH=1 WP_TCP_BENCH_LABEL=task WP_TCP_BENCH_CONNS=1 \
//!   cargo test --release --lib bench_tcp_source_throughput -- --nocapture
//! ```

use super::conn::ConnStream;
use super::framing::FramingMode;
use super::source::TcpSource;
use super::worker::ConnectionRegistration;
use crate::net::CodecConfig;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use wp_connector_api::{DataSource, Tags};

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bench_tcp_source_throughput() {
    if std::env::var("WP_TCP_BENCH").unwrap_or_default() != "1" {
        return;
    }
    let label = std::env::var("WP_TCP_BENCH_LABEL").unwrap_or_else(|_| "n/a".into());
    let conns = env_usize("WP_TCP_BENCH_CONNS", 1);
    let lines = env_usize("WP_TCP_BENCH_LINES", 200_000);
    let payload = env_usize("WP_TCP_BENCH_LINELEN", 200);
    let total_expected = conns * lines;

    let registry = Arc::new(Mutex::new(HashSet::new()));
    let (reg_tx, reg_rx) = mpsc::channel(64);
    let mut source = TcpSource::new(
        "bench".to_string(),
        Tags::default(),
        "127.0.0.1:0".to_string(),
        64 * 1024,
        FramingMode::Line,
        registry.clone(),
        reg_rx,
        CodecConfig::default(),
    )
    .unwrap();
    let (_ctrl_tx, ctrl_rx) = async_broadcast::broadcast(1);
    source.start(ctrl_rx).await.unwrap();

    // 建连接 + 注册 + 起 writer。
    let mut writers = Vec::with_capacity(conns);
    for id in 0..conns {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut client = TcpStream::connect(addr).await.unwrap();
        let (stream, peer) = listener.accept().await.unwrap();
        registry.lock().unwrap().insert(id as u64);
        reg_tx
            .send(ConnectionRegistration {
                connection_id: id as u64,
                stream: ConnStream::Plain(stream),
                peer_addr: peer,
            })
            .await
            .unwrap();

        let line: Vec<u8> = {
            let mut v = format!("c{id}-").into_bytes();
            v.resize(payload, b'x');
            v.push(b'\n');
            v
        };
        writers.push(tokio::spawn(async move {
            for _ in 0..lines {
                client.write_all(&line).await.unwrap();
            }
        }));
    }

    let start = Instant::now();
    let mut total = 0usize;
    let deadline = start + Duration::from_secs(120);
    while total < total_expected && Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(10), source.receive()).await {
            Ok(Ok(batch)) => total += batch.len(),
            Ok(Err(e)) => panic!("[{label}] receive error: {e}"),
            Err(_) => break,
        }
    }
    let elapsed = start.elapsed();
    for w in writers {
        let _ = w.await;
    }

    let secs = elapsed.as_secs_f64().max(1e-9);
    let eps = total as f64 / secs;
    let mbps = (total * (payload + 1)) as f64 / secs / 1_048_576.0;
    println!(
        "BENCH label={label} conns={conns} lines={lines} total={total}/{total_expected} \
         elapsed_ms={:.1} eps={:.0} MBps={:.1}",
        elapsed.as_secs_f64() * 1000.0,
        eps,
        mbps
    );
    assert_eq!(
        total, total_expected,
        "[{label}] not all events were received"
    );
}
