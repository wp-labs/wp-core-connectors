use super::NetWriter;
use super::Transport;
#[cfg(unix)]
use std::os::unix::io::AsRawFd;
#[cfg(unix)]
use std::time::Duration;
use tokio::net::TcpStream;

/// TCP keepalive 参数：空闲 30s 开始探测，探测间隔 10s，连续 3 次无响应判定断链。
///
/// 接收端异常消失（半开连接）时，内核探测失败会让后续读写返回错误，而不是让 socket
/// 一直“看起来可用”；sink 侧因此能及时触发自动重连。
#[cfg(unix)]
#[allow(dead_code)] // 个别 Unix 平台不支持自定义探测间隔
const KEEPALIVE_IDLE: Duration = Duration::from_secs(30);
#[cfg(unix)]
#[allow(dead_code)]
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);

/// 为 TCP/TLS 的底层连接开启 keepalive；不支持的平台直接返回 `Ok(())`。
pub(super) fn enable_tcp_keepalive(stream: &TcpStream) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let sock = socket2::SockRef::from(stream);
        #[allow(unused_mut)]
        let mut keepalive = socket2::TcpKeepalive::new().with_time(KEEPALIVE_IDLE);
        #[cfg(not(any(
            target_os = "openbsd",
            target_os = "redox",
            target_os = "solaris",
            target_os = "nto",
            target_os = "espidf",
            target_os = "vita",
            target_os = "haiku",
            target_os = "horizon"
        )))]
        {
            keepalive = keepalive.with_interval(KEEPALIVE_INTERVAL);
        }
        // TCP_KEEPCNT：探测 3 次无响应即断链（约 30s + 3×10s 内发现半开连接）。
        #[cfg(any(
            target_os = "android",
            target_os = "dragonfly",
            target_os = "emscripten",
            target_os = "freebsd",
            target_os = "fuchsia",
            target_os = "illumos",
            target_os = "ios",
            target_os = "visionos",
            target_os = "linux",
            target_os = "macos",
            target_os = "netbsd",
            target_os = "tvos",
            target_os = "watchos",
            target_os = "cygwin",
        ))]
        {
            keepalive = keepalive.with_retries(3);
        }
        sock.set_tcp_keepalive(&keepalive)
    }
    #[cfg(not(unix))]
    {
        let _ = stream;
        Ok(())
    }
}

impl NetWriter {
    /// Return Some(pending_bytes) if supported, None if unsupported, Err on syscall error.
    pub(super) fn os_sendq_len(&self) -> Result<Option<usize>, std::io::Error> {
        #[cfg(unix)]
        {
            if let Transport::Tcp(stream) = &self.transport {
                let fd = stream.as_raw_fd();
                // Linux: ioctl TIOCOUTQ (get number of unsent bytes in socket send queue)
                #[cfg(target_os = "linux")]
                unsafe {
                    let mut outq: libc::c_int = 0;
                    let rc = libc::ioctl(fd, libc::TIOCOUTQ, &mut outq);
                    if rc == -1 {
                        return Err(std::io::Error::last_os_error());
                    }
                    return Ok(Some(outq as usize));
                }
                // Darwin/BSD: getsockopt(SOL_SOCKET, SO_NWRITE)
                #[cfg(any(
                    target_os = "macos",
                    target_os = "ios",
                    target_os = "freebsd",
                    target_os = "openbsd",
                    target_os = "netbsd"
                ))]
                unsafe {
                    let mut n: libc::c_int = 0;
                    let mut len: libc::socklen_t = std::mem::size_of::<libc::c_int>() as _;
                    #[allow(non_upper_case_globals)]
                    let opt = libc::SO_NWRITE;
                    let rc = libc::getsockopt(
                        fd,
                        libc::SOL_SOCKET,
                        opt,
                        &mut n as *mut _ as *mut _,
                        &mut len,
                    );
                    if rc == -1 {
                        return Err(std::io::Error::last_os_error());
                    }
                    return Ok(Some(n as usize));
                }
            }
        }
        Ok(None)
    }

    /// 返回 SO_SNDBUF（内核发送缓冲区大小，字节）。不支持的平台返回 None。
    pub(super) fn os_sndbuf_size(&self) -> Result<Option<usize>, std::io::Error> {
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            if let Transport::Tcp(stream) = &self.transport {
                let fd = stream.as_raw_fd();
                unsafe {
                    let mut n: libc::c_int = 0;
                    let mut len: libc::socklen_t = std::mem::size_of::<libc::c_int>() as _;
                    let rc = libc::getsockopt(
                        fd,
                        libc::SOL_SOCKET,
                        libc::SO_SNDBUF,
                        &mut n as *mut _ as *mut _,
                        &mut len,
                    );
                    if rc == -1 {
                        return Err(std::io::Error::last_os_error());
                    }
                    return Ok(Some(n as usize));
                }
            }
        }
        Ok(None)
    }

    /// 获取（并缓存）SO_SNDBUF；测试可覆写。
    pub(super) fn get_cached_sndbuf(&mut self) -> Option<usize> {
        #[cfg(test)]
        {
            if let Some(v) = self.sndbuf_override {
                return Some(v);
            }
        }
        if let Some(v) = self.cached_sndbuf {
            return Some(v);
        }
        if let Ok(Some(v)) = self.os_sndbuf_size() {
            self.cached_sndbuf = Some(v);
            return Some(v);
        }
        None
    }

    /// 读取 send-queue 与 sndbuf（测试下支持覆盖）。
    pub(super) fn peek_kernel_queues(&mut self) -> (Option<usize>, Option<usize>) {
        let pending = {
            #[cfg(test)]
            {
                if let Some(p) = self.pending_override {
                    Some(p)
                } else {
                    self.os_sendq_len().unwrap_or(None)
                }
            }
            #[cfg(not(test))]
            {
                self.os_sendq_len().unwrap_or(None)
            }
        };
        #[allow(unused_mut)]
        let mut sndbuf = self.get_cached_sndbuf();
        #[cfg(test)]
        {
            if self.sndbuf_override.is_some() {
                sndbuf = self.sndbuf_override;
            }
        }
        (pending, sndbuf)
    }
}
