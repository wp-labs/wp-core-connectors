//! 压缩 / 加密配置解析：把 connector 的 `compression` / `encryption` 参数映射到
//! `wp-connector-utils` 的 codec（`build_encoder`）。
//!
//! 配置形态（与 `tls = { … }` 同构）：
//! ```toml
//! [params.compression]
//! enabled = true
//! algo = "zstd"      # gzip | zstd
//! level = 3
//!
//! [params.encryption]
//! enabled = true
//! algo = "sm4-gcm"   # aes-256-gcm | sm4-gcm
//! key = "…hex…"
//! ```

use anyhow::anyhow;
use serde_json::Value;
use wp_connector_api::ParamMap;
use wp_connector_utils::codec::{
    Cipher, CompressConfig, CompressionAlgo, Encoder, EncryptConfig, build_encoder,
};

#[derive(Debug, Clone, Default)]
pub struct CodecConfig {
    compression: Option<CompressConfig>,
    encryption: Option<EncryptConfig>,
}

impl CodecConfig {
    /// 从 params 里的 `compression` / `encryption` 子对象解析；未启用时返回全 `None`。
    /// 字段类型 / 算法名错误直接报错。
    pub fn from_params(params: &ParamMap) -> anyhow::Result<Self> {
        Ok(Self {
            compression: parse_compression(params.get("compression"))?,
            encryption: parse_encryption(params.get("encryption"))?,
        })
    }

    /// 是否启用了任一 codec 层。
    pub fn is_enabled(&self) -> bool {
        self.compression.is_some() || self.encryption.is_some()
    }

    /// 构建编码器（sink 侧）。未启用返回 `None`。
    pub fn build_encoder(&self) -> anyhow::Result<Option<Box<dyn Encoder>>> {
        if !self.is_enabled() {
            return Ok(None);
        }
        Ok(Some(build_encoder(
            self.compression.as_ref(),
            self.encryption.as_ref(),
        )?))
    }

    /// 校验配置（含密钥长度 / 压缩级别），失败即配置错误。
    pub fn validate(&self) -> anyhow::Result<()> {
        self.build_encoder().map(|_| ())
    }
}

fn parse_compression(v: Option<&Value>) -> anyhow::Result<Option<CompressConfig>> {
    let Some(v) = v else { return Ok(None) };
    if v.is_null() {
        return Ok(None);
    }
    let obj = v
        .as_object()
        .ok_or_else(|| anyhow!("compression 必须是对象（形如 compression.enabled = true）"))?;
    if !obj
        .get("enabled")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        return Ok(None);
    }
    let algo = match obj.get("algo").and_then(|v| v.as_str()).unwrap_or("zstd") {
        "gzip" => CompressionAlgo::Gzip,
        "zstd" => CompressionAlgo::Zstd,
        other => {
            return Err(anyhow!(
                "invalid compression.algo: '{other}'（允许 gzip | zstd）"
            ));
        }
    };
    let level = obj.get("level").and_then(|v| v.as_i64()).unwrap_or(3) as i32;
    Ok(Some(CompressConfig { algo, level }))
}

fn parse_encryption(v: Option<&Value>) -> anyhow::Result<Option<EncryptConfig>> {
    let Some(v) = v else { return Ok(None) };
    if v.is_null() {
        return Ok(None);
    }
    let obj = v
        .as_object()
        .ok_or_else(|| anyhow!("encryption 必须是对象（形如 encryption.enabled = true）"))?;
    if !obj
        .get("enabled")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        return Ok(None);
    }
    let cipher = match obj
        .get("algo")
        .and_then(|v| v.as_str())
        .unwrap_or("aes-256-gcm")
    {
        "aes-256-gcm" => Cipher::Aes256Gcm,
        "sm4-gcm" => Cipher::Sm4Gcm,
        other => {
            return Err(anyhow!(
                "invalid encryption.algo: '{other}'（允许 aes-256-gcm | sm4-gcm）"
            ));
        }
    };
    let key_hex = obj
        .get("key")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("encryption.key 必填（hex 编码）"))?;
    let key = decode_hex(key_hex).map_err(|e| anyhow!("invalid encryption.key: {e}"))?;
    Ok(Some(EncryptConfig { cipher, key }))
}

fn decode_hex(s: &str) -> Result<Vec<u8>, String> {
    let s = s.trim();
    if !s.len().is_multiple_of(2) {
        return Err("hex string must have even length".into());
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|e| e.to_string()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn params(compression: Value, encryption: Value) -> ParamMap {
        let mut p = ParamMap::new();
        p.insert("compression".into(), compression);
        p.insert("encryption".into(), encryption);
        p
    }

    #[test]
    fn disabled_by_default() {
        let p = ParamMap::new();
        assert!(!CodecConfig::from_params(&p).unwrap().is_enabled());
    }

    #[test]
    fn parses_compression_and_encryption() {
        let p = params(
            json!({ "enabled": true, "algo": "gzip", "level": 6 }),
            json!({ "enabled": true, "algo": "sm4-gcm", "key": "000102030405060708090a0b0c0d0e0f" }),
        );
        let cfg = CodecConfig::from_params(&p).unwrap();
        assert!(cfg.is_enabled());
        assert!(cfg.build_encoder().unwrap().is_some());
    }

    #[test]
    fn rejects_bad_algo() {
        let p = params(json!({ "enabled": true, "algo": "lz4" }), json!(null));
        assert!(CodecConfig::from_params(&p).is_err());
        let p2 = params(json!(null), json!({ "enabled": true, "algo": "des" }));
        assert!(CodecConfig::from_params(&p2).is_err());
    }

    #[test]
    fn rejects_bad_key_length() {
        // sm4-gcm 需 16 字节，给 32 字节（64 hex）应报错
        let p = params(
            json!(null),
            json!({ "enabled": true, "algo": "sm4-gcm", "key": "00".repeat(32) }),
        );
        let cfg = CodecConfig::from_params(&p).unwrap();
        assert!(cfg.validate().is_err());
    }
}
