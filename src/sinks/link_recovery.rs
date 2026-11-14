//! TCP 类 sink 的连接失效恢复状态（`TcpSink` / `SyslogSink` 共用）。
//!
//! 对端重启后旧 socket 会持续报错，这里提供退避重连的状态与 gate，
//! 让 connector 自身就能恢复，而不依赖 wp-motor 的坏 sink 维护线程。

use std::time::Duration;

use orion_error::conversion::ToStructError;
use wp_connector_api::{SinkError, SinkReason};

/// 重连退避的起点与上限。
pub(super) const BACKOFF_INITIAL: Duration = Duration::from_secs(1);
pub(super) const BACKOFF_MAX: Duration = Duration::from_secs(30);
/// 单次重连建连（TCP connect + 可选 TLS 握手）的超时。
///
/// 没有超时时，对端主机黑洞（不是拒绝而是不响应）会把 sink 的发送路径挂住，
/// 退避重连反而变成长时间卡顿。
pub(super) const RECONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// 下一次重连的退避时长：1s 起步、翻倍、30s 封顶。
pub(super) fn next_retry_backoff(current: Option<Duration>) -> Duration {
    match current {
        Some(backoff) => (backoff * 2).min(BACKOFF_MAX),
        None => BACKOFF_INITIAL,
    }
}

/// 链路失效后的退避重连状态；`state == None` 表示连接可用。
#[derive(Default)]
pub(super) struct LinkRecovery {
    state: Option<LinkState>,
}

struct LinkState {
    next_attempt: tokio::time::Instant,
    backoff: Duration,
}

impl LinkRecovery {
    pub(super) fn is_down(&self) -> bool {
        self.state.is_some()
    }

    /// 退避未到期时返回可直接上抛的错误；已到期或连接健康时返回 `None`。
    pub(super) fn gate_error(&self) -> Option<SinkError> {
        let state = self.state.as_ref()?;
        if self.is_due() {
            return None;
        }
        let now = tokio::time::Instant::now();
        Some(SinkReason::Sink.to_err().with_detail(format!(
            "sink connection lost, reconnect in {}ms",
            (state.next_attempt - now).as_millis()
        )))
    }

    /// 当前是否允许发起重连（健康或退避到期）。
    pub(super) fn is_due(&self) -> bool {
        match &self.state {
            Some(state) => tokio::time::Instant::now() >= state.next_attempt,
            None => true,
        }
    }

    /// 记录一次失败并按指数退避安排下一次重连。
    pub(super) fn on_failure(&mut self) {
        let backoff = next_retry_backoff(self.state.as_ref().map(|s| s.backoff));
        self.state = Some(LinkState {
            next_attempt: tokio::time::Instant::now() + backoff,
            backoff,
        });
    }

    /// 连接已重建；后续写入成功即视为恢复。
    pub(super) fn on_connected(&mut self) {
        self.state = None;
    }

    pub(super) fn backoff(&self) -> Option<Duration> {
        self.state.as_ref().map(|s| s.backoff)
    }

    #[cfg(test)]
    pub(super) fn force_due(&mut self) {
        if let Some(state) = &mut self.state {
            state.next_attempt = tokio::time::Instant::now();
        }
    }

    #[cfg(test)]
    pub(super) fn force_delay(&mut self, delay: Duration) {
        if let Some(state) = &mut self.state {
            state.next_attempt = tokio::time::Instant::now() + delay;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_until_cap() {
        assert_eq!(next_retry_backoff(None), BACKOFF_INITIAL);
        assert_eq!(
            next_retry_backoff(Some(BACKOFF_INITIAL)),
            Duration::from_secs(2)
        );
        assert_eq!(
            next_retry_backoff(Some(Duration::from_secs(2))),
            Duration::from_secs(4)
        );
        assert_eq!(next_retry_backoff(Some(BACKOFF_MAX)), BACKOFF_MAX);
        assert_eq!(
            next_retry_backoff(Some(BACKOFF_MAX * 2)),
            BACKOFF_MAX,
            "backoff must never exceed the cap"
        );
    }

    #[test]
    fn gate_blocks_until_backoff_elapses() {
        let mut recovery = LinkRecovery::default();
        assert!(!recovery.is_down());
        assert!(recovery.is_due());
        assert!(recovery.gate_error().is_none());

        recovery.on_failure();
        assert!(recovery.is_down());
        assert_eq!(recovery.backoff(), Some(BACKOFF_INITIAL));
        let err = recovery.gate_error().expect("must be gated");
        assert!(err.to_string().contains("reconnect in"));

        recovery.force_due();
        assert!(recovery.gate_error().is_none());

        recovery.on_connected();
        assert!(!recovery.is_down());
    }
}
