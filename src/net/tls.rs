//! TLS（安全传输）配置解析与 rustls 配置构建。
//!
//! `tls = { … }` 子对象同时服务两端：
//! - TCP source（server）：`cert` + `key` 必填；`ca` 可选（开启 mTLS，校验客户端证书）。
//! - TCP sink（client）：`ca` 用于校验服务端证书，**缺省时回落系统原生信任库**；
//!   `insecure = true` 跳过校验；`cert`+`key` 可选（客户端 mTLS 证书）；
//!   `server_name` 可选（SNI，缺省取目标主机名）。

use anyhow::{anyhow, ensure};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use serde_json::Value;
use std::sync::Arc;

#[derive(Debug, Clone, Default)]
pub struct TlsConfig {
    /// 证书链（PEM 路径）。source 必填；sink 仅在 mTLS 时填。
    pub cert: Option<String>,
    /// 私钥（PEM 路径），与 `cert` 成对出现。
    pub key: Option<String>,
    /// CA 证书（PEM 路径）。source：客户端 CA（开启 mTLS）；sink：校验服务端证书，
    /// 缺省时使用系统原生信任库（rustls-native-certs）。
    pub ca: Option<String>,
    /// SNI / 服务端名（sink 用，缺省取目标主机名）。
    pub server_name: Option<String>,
    /// 跳过证书校验（仅 sink 使用；危险）。
    pub insecure: bool,
}

impl TlsConfig {
    /// 从参数里的 `tls` 子对象解析；未配置或 `enabled != true` 返回 `None`。
    /// 字段类型错误（`tls` 非对象、`enabled`/`insecure` 非布尔、路径非字符串）直接报错。
    pub fn from_param(v: Option<&Value>) -> anyhow::Result<Option<Self>> {
        let Some(obj) = v else { return Ok(None) };
        let Some(obj) = obj.as_object() else {
            return Err(anyhow!("tls 必须是对象（形如 tls.enabled = true）"));
        };
        let enabled = match obj.get("enabled") {
            Some(v) => v
                .as_bool()
                .ok_or_else(|| anyhow!("tls.enabled must be a boolean"))?,
            None => false,
        };
        if !enabled {
            return Ok(None);
        }
        const ALLOWED: &[&str] = &["enabled", "cert", "key", "ca", "server_name", "insecure"];
        if let Some(unknown) = obj.keys().find(|k| !ALLOWED.contains(&k.as_str())) {
            return Err(anyhow!(
                "tls 未知字段 '{unknown}'（允许：{}）",
                ALLOWED.join(", ")
            ));
        }
        let s = |k: &str| -> anyhow::Result<Option<String>> {
            match obj.get(k) {
                Some(v) => Ok(Some(
                    v.as_str()
                        .ok_or_else(|| anyhow!("tls.{k} must be a string"))?
                        .to_string(),
                )),
                None => Ok(None),
            }
        };
        let insecure = match obj.get("insecure") {
            Some(v) => v
                .as_bool()
                .ok_or_else(|| anyhow!("tls.insecure must be a boolean"))?,
            None => false,
        };
        let cfg = Self {
            cert: s("cert")?,
            key: s("key")?,
            ca: s("ca")?,
            server_name: s("server_name")?,
            insecure,
        };
        ensure!(
            cfg.cert.is_some() == cfg.key.is_some(),
            "tls.cert 与 tls.key 必须成对出现"
        );
        Ok(Some(cfg))
    }

    fn provider() -> Arc<rustls::crypto::CryptoProvider> {
        Arc::new(rustls::crypto::ring::default_provider())
    }

    /// 构建服务端（source）配置。
    pub fn build_server_config(&self) -> anyhow::Result<Arc<rustls::ServerConfig>> {
        let certs = load_certs(
            self.cert
                .as_deref()
                .ok_or_else(|| anyhow!("tls.cert 必填"))?,
        )?;
        let key = load_key(self.key.as_deref().ok_or_else(|| anyhow!("tls.key 必填"))?)?;
        let config = match &self.ca {
            Some(ca) => {
                let roots = load_root_store(ca)?;
                let verifier =
                    rustls::server::WebPkiClientVerifier::builder(Arc::new(roots)).build()?;
                rustls::ServerConfig::builder_with_provider(Self::provider())
                    .with_safe_default_protocol_versions()?
                    .with_client_cert_verifier(verifier)
                    .with_single_cert(certs, key)?
            }
            None => rustls::ServerConfig::builder_with_provider(Self::provider())
                .with_safe_default_protocol_versions()?
                .with_no_client_auth()
                .with_single_cert(certs, key)?,
        };
        Ok(Arc::new(config))
    }

    /// 构建客户端（sink）配置。
    pub fn build_client_config(&self) -> anyhow::Result<Arc<rustls::ClientConfig>> {
        // 客户端 mTLS 证书：成对加载（`from_param` 已保证成对；此处对直接构造也兜底）。
        let client_auth = match (self.cert.as_deref(), self.key.as_deref()) {
            (Some(cert), Some(key)) => Some((load_certs(cert)?, load_key(key)?)),
            (None, None) => None,
            _ => return Err(anyhow!("tls.cert 与 tls.key 必须成对出现（客户端 mTLS）")),
        };

        let builder = if self.insecure {
            rustls::ClientConfig::builder_with_provider(Self::provider())
                .with_safe_default_protocol_versions()?
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(NoVerifier))
        } else {
            let roots = match self.ca.as_deref() {
                Some(ca) => load_root_store(ca)?,
                None => load_native_root_store()?,
            };
            rustls::ClientConfig::builder_with_provider(Self::provider())
                .with_safe_default_protocol_versions()?
                .with_root_certificates(Arc::new(roots))
        };

        let config = match client_auth {
            Some((certs, key)) => builder.with_client_auth_cert(certs, key)?,
            None => builder.with_no_client_auth(),
        };
        Ok(Arc::new(config))
    }

    /// sink 端 SNI 服务端名；缺省取目标主机名。
    pub fn server_name(&self, default_host: &str) -> anyhow::Result<ServerName<'static>> {
        let name = self.server_name.as_deref().unwrap_or(default_host);
        ServerName::try_from(name.to_string())
            .map_err(|e| anyhow!("invalid tls.server_name '{name}': {e}"))
    }
}

fn load_certs(path: &str) -> anyhow::Result<Vec<CertificateDer<'static>>> {
    let certs = CertificateDer::pem_file_iter(path)?.collect::<Result<Vec<_>, _>>()?;
    ensure!(!certs.is_empty(), "no certificates found in {path}");
    Ok(certs)
}

fn load_key(path: &str) -> anyhow::Result<PrivateKeyDer<'static>> {
    Ok(PrivateKeyDer::from_pem_file(path)?)
}

fn load_root_store(path: &str) -> anyhow::Result<rustls::RootCertStore> {
    let mut roots = rustls::RootCertStore::empty();
    for cert in CertificateDer::pem_file_iter(path)? {
        roots.add(cert?)?;
    }
    Ok(roots)
}

/// 加载系统原生信任库（Linux 读系统证书目录，Windows 读系统存储，macOS 读钥匙串），
/// 受 `SSL_CERT_FILE` / `SSL_CERT_DIR` 环境变量影响。
fn load_native_root_store() -> anyhow::Result<rustls::RootCertStore> {
    let result = rustls_native_certs::load_native_certs();
    let mut roots = rustls::RootCertStore::empty();
    for cert in result.certs {
        // 单个无效证书不阻断整体加载
        let _ = roots.add(cert);
    }
    ensure!(
        !roots.is_empty(),
        "no native root certificates found in the system trust store (errors: {:?})",
        result.errors
    );
    Ok(roots)
}

/// 跳过证书校验（仅 `insecure = true` 使用）。
#[derive(Debug)]
struct NoVerifier;

impl rustls::client::danger::ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::transport::NetWriter;
    use serde_json::json;

    #[test]
    fn from_param_none_when_absent() {
        assert!(TlsConfig::from_param(None).unwrap().is_none());
    }

    #[test]
    fn from_param_none_when_disabled() {
        let v = json!({ "enabled": false });
        assert!(TlsConfig::from_param(Some(&v)).unwrap().is_none());
    }

    #[test]
    fn from_param_requires_cert_key_pair() {
        let v = json!({ "enabled": true, "cert": "/c.pem" });
        assert!(TlsConfig::from_param(Some(&v)).is_err());
    }

    #[test]
    fn from_param_parses_fields() {
        let v = json!({
            "enabled": true,
            "cert": "/c.pem",
            "key": "/k.pem",
            "ca": "/ca.pem",
            "server_name": "localhost",
            "insecure": true,
        });
        let cfg = TlsConfig::from_param(Some(&v)).unwrap().unwrap();
        assert_eq!(cfg.cert.as_deref(), Some("/c.pem"));
        assert_eq!(cfg.key.as_deref(), Some("/k.pem"));
        assert_eq!(cfg.ca.as_deref(), Some("/ca.pem"));
        assert_eq!(cfg.server_name.as_deref(), Some("localhost"));
        assert!(cfg.insecure);
    }

    #[test]
    fn server_name_defaults_to_host() {
        let cfg = TlsConfig::default();
        let sn = cfg.server_name("example.com").unwrap();
        assert_eq!(sn.to_str(), "example.com");
    }

    /// 端到端：自签证书下，NetWriter 客户端 ↔ TLS 服务端 握手并收发数据。
    #[tokio::test]
    async fn tls_roundtrip_writer_to_server() {
        // 生成自签证书（SAN=localhost）
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let cert_path = dir.path().join("cert.pem");
        let key_path = dir.path().join("key.pem");
        std::fs::write(&cert_path, certified.cert.pem().as_bytes()).unwrap();
        std::fs::write(&key_path, certified.key_pair.serialize_pem().as_bytes()).unwrap();

        // 服务端配置
        let server_tls = TlsConfig {
            cert: Some(cert_path.to_string_lossy().into_owned()),
            key: Some(key_path.to_string_lossy().into_owned()),
            ..Default::default()
        };
        let acceptor = tokio_rustls::TlsAcceptor::from(server_tls.build_server_config().unwrap());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut tls = acceptor.accept(stream).await.unwrap();
            let mut buf = [0u8; 64];
            let n = tokio::io::AsyncReadExt::read(&mut tls, &mut buf)
                .await
                .unwrap();
            buf[..n].to_vec()
        });

        // 客户端：自签证书跳过校验
        let client_tls = TlsConfig {
            insecure: true,
            ..Default::default()
        };
        let mut writer = NetWriter::connect_tcp_tls(&addr.to_string(), "localhost", &client_tls)
            .await
            .unwrap();
        writer.write(b"hello tls").await.unwrap();
        writer.shutdown().await.unwrap();

        let received = server.await.unwrap();
        assert_eq!(received, b"hello tls");
    }

    fn write_self_signed(dir: &tempfile::TempDir, san: &str) -> (String, String) {
        let certified = rcgen::generate_simple_self_signed(vec![san.to_string()]).unwrap();
        let cert_path = dir.path().join(format!("{san}.pem"));
        let key_path = dir.path().join(format!("{san}.key"));
        std::fs::write(&cert_path, certified.cert.pem().as_bytes()).unwrap();
        std::fs::write(&key_path, certified.key_pair.serialize_pem().as_bytes()).unwrap();
        (
            cert_path.to_string_lossy().into_owned(),
            key_path.to_string_lossy().into_owned(),
        )
    }

    #[test]
    fn from_param_rejects_non_object() {
        let v = json!("not-an-object");
        assert!(TlsConfig::from_param(Some(&v)).is_err());
    }

    #[test]
    fn from_param_rejects_non_bool_enabled() {
        let v = json!({ "enabled": "true" });
        assert!(TlsConfig::from_param(Some(&v)).is_err());
    }

    #[test]
    fn from_param_rejects_non_bool_insecure() {
        let v = json!({ "enabled": true, "insecure": "true" });
        assert!(TlsConfig::from_param(Some(&v)).is_err());
    }

    #[test]
    fn from_param_rejects_non_string_field() {
        let v = json!({ "enabled": true, "cert": 123, "key": "/k.pem" });
        assert!(TlsConfig::from_param(Some(&v)).is_err());
    }

    #[test]
    fn server_config_requires_cert_and_key() {
        assert!(TlsConfig::default().build_server_config().is_err());
    }

    #[test]
    fn client_config_uses_native_roots_when_ca_absent() {
        // 无 ca、非 insecure：回落系统原生信任库；无系统证书的环境下应报「无原生根证书」而非旧「ca 必填」。
        match TlsConfig::default().build_client_config() {
            Ok(_) => {}
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("native root") || msg.contains("trust store"),
                    "unexpected error: {msg}"
                );
            }
        }
    }

    #[test]
    fn server_config_with_ca_builds() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = write_self_signed(&dir, "localhost");
        let (ca, _) = write_self_signed(&dir, "ca");
        let cfg = TlsConfig {
            cert: Some(cert),
            key: Some(key),
            ca: Some(ca),
            ..Default::default()
        };
        assert!(cfg.build_server_config().is_ok());
    }

    #[tokio::test]
    async fn client_verifies_server_with_ca() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = write_self_signed(&dir, "localhost");

        let server_tls = TlsConfig {
            cert: Some(cert.clone()),
            key: Some(key),
            ..Default::default()
        };
        let acceptor = tokio_rustls::TlsAcceptor::from(server_tls.build_server_config().unwrap());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let _ = acceptor.accept(stream).await.expect("server handshake");
        });

        let client_tls = TlsConfig {
            ca: Some(cert),
            server_name: Some("localhost".into()),
            ..Default::default()
        };
        let _writer = NetWriter::connect_tcp_tls(&addr.to_string(), "localhost", &client_tls)
            .await
            .expect("client should trust the server cert");

        server.await.unwrap();
    }

    #[tokio::test]
    async fn client_rejects_untrusted_server() {
        let dir = tempfile::tempdir().unwrap();
        let (server_cert, server_key) = write_self_signed(&dir, "localhost");
        let (other_ca, _) = write_self_signed(&dir, "other-ca");

        let server_tls = TlsConfig {
            cert: Some(server_cert),
            key: Some(server_key),
            ..Default::default()
        };
        let acceptor = tokio_rustls::TlsAcceptor::from(server_tls.build_server_config().unwrap());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let _ = acceptor.accept(stream).await; // 客户端会拒绝，握手失败
        });

        let client_tls = TlsConfig {
            ca: Some(other_ca),
            server_name: Some("localhost".into()),
            ..Default::default()
        };
        let result = NetWriter::connect_tcp_tls(&addr.to_string(), "localhost", &client_tls).await;
        assert!(
            result.is_err(),
            "client should reject an untrusted server cert"
        );

        let _ = server.await;
    }

    #[test]
    fn from_param_rejects_unknown_field() {
        let v = json!({ "enabled": true, "cert_": "/c.pem" });
        let err = TlsConfig::from_param(Some(&v)).unwrap_err().to_string();
        assert!(err.contains("未知字段"), "unexpected error: {err}");
        assert!(err.contains("cert_"), "unexpected error: {err}");
    }

    #[test]
    fn from_param_ignores_unknown_when_disabled() {
        // disabled 时整块忽略，未知字段不报错（向后兼容的宽松语义）
        let v = json!({ "enabled": false, "typo_field": "x" });
        assert!(TlsConfig::from_param(Some(&v)).unwrap().is_none());
    }

    #[test]
    fn server_name_uses_explicit() {
        let cfg = TlsConfig {
            server_name: Some("wparse.example.com".into()),
            ..Default::default()
        };
        let sn = cfg.server_name("default-host").unwrap();
        assert_eq!(sn.to_str(), "wparse.example.com");
    }

    #[test]
    fn client_config_rejects_partial_cert_key() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, _) = write_self_signed(&dir, "localhost");
        // 直接构造只给 cert 不给 key（绕过 from_param 的成对校验）也应报错
        let cfg = TlsConfig {
            cert: Some(cert),
            key: None,
            ..Default::default()
        };
        let err = cfg.build_client_config().unwrap_err().to_string();
        assert!(err.contains("成对"), "unexpected error: {err}");
    }

    /// 客户端 `insecure = true`（跳过服务端校验）+ 客户端证书（mTLS）时，客户端证书仍应被
    /// 呈现——服务端用 CA 开启 mTLS，只有客户端确实提交了证书握手才成功。
    #[tokio::test]
    async fn client_insecure_with_mtls_presents_client_cert() {
        let dir = tempfile::tempdir().unwrap();

        // 自建 CA
        let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();
        let ca_path = dir.path().join("ca.pem");
        std::fs::write(&ca_path, ca_cert.pem().as_bytes()).unwrap();

        // 服务端证书（由 CA 签发）
        let server_key = rcgen::KeyPair::generate().unwrap();
        let server_params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        let server_cert = server_params.signed_by(&server_key, &ca_cert, &ca_key).unwrap();
        let server_cert_path = dir.path().join("server.pem");
        let server_key_path = dir.path().join("server.key");
        std::fs::write(&server_cert_path, server_cert.pem().as_bytes()).unwrap();
        std::fs::write(&server_key_path, server_key.serialize_pem().as_bytes()).unwrap();

        // 客户端证书（由同一 CA 签发）
        let client_key = rcgen::KeyPair::generate().unwrap();
        let client_params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        let client_cert = client_params.signed_by(&client_key, &ca_cert, &ca_key).unwrap();
        let client_cert_path = dir.path().join("client.pem");
        let client_key_path = dir.path().join("client.key");
        std::fs::write(&client_cert_path, client_cert.pem().as_bytes()).unwrap();
        std::fs::write(&client_key_path, client_key.serialize_pem().as_bytes()).unwrap();

        // 服务端：mTLS（要求并校验客户端证书）
        let server_tls = TlsConfig {
            cert: Some(server_cert_path.to_string_lossy().into_owned()),
            key: Some(server_key_path.to_string_lossy().into_owned()),
            ca: Some(ca_path.to_string_lossy().into_owned()),
            ..Default::default()
        };
        let acceptor = tokio_rustls::TlsAcceptor::from(server_tls.build_server_config().unwrap());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let _ = acceptor.accept(stream).await.expect("server mTLS handshake");
        });

        // 客户端：insecure（跳过服务端校验）+ 客户端证书
        let client_tls = TlsConfig {
            insecure: true,
            cert: Some(client_cert_path.to_string_lossy().into_owned()),
            key: Some(client_key_path.to_string_lossy().into_owned()),
            ..Default::default()
        };
        let _writer = NetWriter::connect_tcp_tls(&addr.to_string(), "localhost", &client_tls)
            .await
            .expect("client should present its cert even with insecure=true");

        server.await.unwrap();
    }
}
