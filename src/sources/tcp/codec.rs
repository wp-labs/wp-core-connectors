//! source 侧 codec：从字节流中切出完整 codec 帧并「解密 + 解压」成明文。
//!
//! 帧格式与 `wp_connector_utils::codec` 的编码器一致：
//! `magic(2 "WP") | version(1) | kind(1) | length(4 BE) | payload(length)`。
//!
//! 关键点：`wp_connector_utils::codec::Decoder::decode` 只接受**完整帧**（半帧会报
//! `truncated ...`），而 TCP 是字节流、一次 `read` 可能只拿到半帧，因此这里负责按
//! `magic + length` 边界切帧，只把完整帧交给 decoder，不完整帧留在缓冲区等下次读。

use bytes::BytesMut;
use wp_connector_api::{SourceReason, SourceResult};
use wp_connector_utils::codec::Decoder;

use crate::net::CodecConfig;

/// codec 帧头：magic(2) + version(1) + kind(1) + length(4 BE) = 8 字节。
/// 与 `wp_connector_utils::codec` 的 `FRAME_MAGIC`/`FRAME_VERSION` 保持一致。
const FRAME_MAGIC: [u8; 2] = [0x57, 0x50]; // "WP"
const FRAME_VERSION: u8 = 1;
const FRAME_HEADER_LEN: usize = 8;

/// 单帧 payload 上限：sink 实际产出的是压缩块（64 KiB）或单条消息，远小于该值。
/// 用于拒绝恶意 / 异常对端声明的超大帧（`u32` 长度可到 4 GiB），避免缓冲无界增长。
const MAX_CODEC_FRAME_BYTES: usize = 64 * 1024 * 1024; // 64 MiB

/// 流式解码器：持有 `Decoder` 与明文缓冲，负责从原始字节流切帧并解码。
pub struct StreamDecoder {
    decoder: Box<dyn Decoder>,
    /// 已解码、尚未被 framing 消费的明文（按行 / 长度前缀）。
    plain: BytesMut,
}

impl StreamDecoder {
    /// 构建解码器；未启用 codec 时返回 `Ok(None)`。
    pub fn new(codec: &CodecConfig) -> anyhow::Result<Option<Self>> {
        let Some(decoder) = codec.build_decoder()? else {
            return Ok(None);
        };
        Ok(Some(Self {
            decoder,
            plain: BytesMut::new(),
        }))
    }

    /// 明文缓冲，供 framing 层消费。
    pub fn plain_mut(&mut self) -> &mut BytesMut {
        &mut self.plain
    }

    /// 从 `raw` 中尽可能多地切出完整帧并解码追加到 `plain`；不完整帧保留在 `raw`。
    ///
    /// 帧头非法（magic/version 不对）或解码失败时返回 `supplier_error`，上层据此断开连接。
    pub fn decode(&mut self, raw: &mut BytesMut) -> SourceResult<()> {
        loop {
            let total = match frame_boundary(raw) {
                Ok(Some(n)) => n,
                Ok(None) => break,
                Err(e) => return Err(SourceReason::supplier_error(e)),
            };
            let frame = raw.split_to(total);
            let mut out = Vec::new();
            self.decoder
                .decode(&frame, &mut out)
                .map_err(|e| SourceReason::supplier_error(format!("codec decode error: {e}")))?;
            self.plain.extend_from_slice(&out);
        }
        Ok(())
    }
}

/// 返回 `buf` 开头首个完整帧的总长度（含 8 字节帧头）。
/// - `Ok(Some(n))`：存在完整帧，长度为 `n`；
/// - `Ok(None)`：帧头 / payload 尚未收齐，需等待更多字节；
/// - `Err(e)`：帧头非法（magic/version 不对）或 payload 超上限，协议错误。
fn frame_boundary(buf: &[u8]) -> Result<Option<usize>, String> {
    if buf.len() < FRAME_HEADER_LEN {
        return Ok(None);
    }
    if buf[0] != FRAME_MAGIC[0] || buf[1] != FRAME_MAGIC[1] {
        return Err("bad codec frame magic (peer not sending codec frames?)".into());
    }
    if buf[2] != FRAME_VERSION {
        return Err(format!("unsupported codec frame version {}", buf[2]));
    }
    let len = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]) as usize;
    if len > MAX_CODEC_FRAME_BYTES {
        return Err(format!("codec frame too large: {len} bytes"));
    }
    let total = FRAME_HEADER_LEN + len;
    if buf.len() < total {
        return Ok(None);
    }
    Ok(Some(total))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use wp_connector_api::ParamMap;
    use wp_connector_utils::codec::{
        Cipher, CompressConfig, CompressionAlgo, EncryptConfig, build_encoder,
    };

    /// 用给定压缩/加密配置编码一条明文，返回完整 wire 字节。
    fn encode(
        compression: Option<&CompressConfig>,
        encryption: Option<&EncryptConfig>,
        data: &[u8],
    ) -> Vec<u8> {
        let mut encoder = build_encoder(compression, encryption).unwrap();
        let mut wire = Vec::new();
        encoder.encode(data, &mut wire).unwrap();
        let mut tail = Vec::new();
        encoder.finish(&mut tail).unwrap();
        wire.extend_from_slice(&tail);
        wire
    }

    /// 从 params 构造 CodecConfig（供 `StreamDecoder::new` 使用）。
    fn codec_config(compression: serde_json::Value, encryption: serde_json::Value) -> CodecConfig {
        let mut params = ParamMap::new();
        params.insert("compression".into(), compression);
        params.insert("encryption".into(), encryption);
        CodecConfig::from_params(&params).unwrap()
    }

    /// 用 `chunk_size` 分块喂入 `StreamDecoder`，收集解码出的全部明文。
    fn decode_all(dec: &mut StreamDecoder, wire: &[u8], chunk_size: usize) -> Vec<u8> {
        let mut raw = BytesMut::new();
        let mut out = Vec::new();
        for chunk in wire.chunks(chunk_size) {
            raw.extend_from_slice(chunk);
            dec.decode(&mut raw).unwrap();
            out.extend_from_slice(&dec.plain);
            dec.plain.clear();
        }
        out
    }

    #[test]
    fn frame_boundary_complete_partial_and_bad_magic() {
        // 完整帧：magic "WP" + version 1 + kind 0x01 + len 3 + payload "abc"
        let mut buf = vec![0x57, 0x50, 0x01, 0x01, 0, 0, 0, 3, b'a', b'b', b'c'];
        assert_eq!(frame_boundary(&buf).unwrap(), Some(11));

        // 半帧（payload 未收齐）
        buf.truncate(9);
        assert_eq!(frame_boundary(&buf).unwrap(), None);

        // 帧头不足 8 字节
        buf.truncate(4);
        assert_eq!(frame_boundary(&buf).unwrap(), None);

        // 坏 magic
        let bad = vec![0x00, 0x00, 0x01, 0x01, 0, 0, 0, 0];
        assert!(frame_boundary(&bad).is_err());
    }

    #[test]
    fn frame_boundary_zero_length_and_oversize() {
        // 零长帧：合法（解码出空 payload），边界 = 8 字节帧头
        let zero = vec![0x57, 0x50, 0x01, 0x01, 0, 0, 0, 0];
        assert_eq!(frame_boundary(&zero).unwrap(), Some(8));

        // 声明 4 GiB 的帧：超上限，拒绝
        let huge = vec![0x57, 0x50, 0x01, 0x01, 0xff, 0xff, 0xff, 0xff];
        assert!(frame_boundary(&huge).is_err());

        // 坏 version
        let bad_ver = vec![0x57, 0x50, 0x02, 0x01, 0, 0, 0, 0];
        assert!(frame_boundary(&bad_ver).is_err());
    }

    #[test]
    fn disabled_codec_returns_none() {
        let codec = CodecConfig::default();
        assert!(StreamDecoder::new(&codec).unwrap().is_none());
    }

    /// 端到端：sink 侧编码（zstd+sm4）→ 分块喂入 `StreamDecoder` → 还原明文。
    /// 分块模拟 TCP 半帧/跨帧到达，验证流式解码不丢数据。
    #[test]
    fn stream_decoder_roundtrip_partial_frames() {
        let compress = CompressConfig {
            algo: CompressionAlgo::Zstd,
            level: 3,
        };
        let encrypt = EncryptConfig {
            cipher: Cipher::Sm4Gcm,
            key: (0u8..16).collect(),
        };
        let codec = codec_config(
            json!({ "enabled": true, "algo": "zstd", "level": 3 }),
            json!({ "enabled": true, "algo": "sm4-gcm", "key": "000102030405060708090a0b0c0d0e0f" }),
        );
        let wire = encode(Some(&compress), Some(&encrypt), b"line1\nline2\n");

        let mut dec = StreamDecoder::new(&codec).unwrap().expect("codec enabled");
        let out = decode_all(&mut dec, &wire, 7);
        assert_eq!(String::from_utf8_lossy(&out), "line1\nline2\n");
    }

    /// AES-256-GCM（硬件加速的常见路径）+ zstd 往返。
    #[test]
    fn stream_decoder_roundtrip_aes() {
        let compress = CompressConfig {
            algo: CompressionAlgo::Zstd,
            level: 3,
        };
        let encrypt = EncryptConfig {
            cipher: Cipher::Aes256Gcm,
            key: (0u8..32).collect(),
        };
        let codec = codec_config(
            json!({ "enabled": true, "algo": "zstd", "level": 3 }),
            json!({ "enabled": true, "algo": "aes-256-gcm", "key": "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f" }),
        );
        let wire = encode(Some(&compress), Some(&encrypt), b"hello aes\n");

        let mut dec = StreamDecoder::new(&codec).unwrap().expect("codec enabled");
        let out = decode_all(&mut dec, &wire, 3);
        assert_eq!(String::from_utf8_lossy(&out), "hello aes\n");
    }

    /// 仅压缩（无加密）：zstd 往返。
    #[test]
    fn stream_decoder_roundtrip_compression_only() {
        let compress = CompressConfig {
            algo: CompressionAlgo::Gzip,
            level: 6,
        };
        let codec = codec_config(
            json!({ "enabled": true, "algo": "gzip", "level": 6 }),
            json!(null),
        );
        let wire = encode(Some(&compress), None, b"gzip only\n");

        let mut dec = StreamDecoder::new(&codec).unwrap().expect("codec enabled");
        let out = decode_all(&mut dec, &wire, 5);
        assert_eq!(String::from_utf8_lossy(&out), "gzip only\n");
    }

    /// 仅加密（无压缩）：sm4-gcm 往返，且小消息会立即产出（无压缩缓冲）。
    #[test]
    fn stream_decoder_roundtrip_encryption_only() {
        let encrypt = EncryptConfig {
            cipher: Cipher::Sm4Gcm,
            key: (0u8..16).collect(),
        };
        let codec = codec_config(
            json!(null),
            json!({ "enabled": true, "algo": "sm4-gcm", "key": "000102030405060708090a0b0c0d0e0f" }),
        );
        let wire = encode(None, Some(&encrypt), b"enc only\n");

        let mut dec = StreamDecoder::new(&codec).unwrap().expect("codec enabled");
        let out = decode_all(&mut dec, &wire, 1);
        assert_eq!(String::from_utf8_lossy(&out), "enc only\n");
    }

    /// 多条消息（多帧）在单次 decode 中全部还原。
    #[test]
    fn stream_decoder_multiple_frames() {
        let encrypt = EncryptConfig {
            cipher: Cipher::Sm4Gcm,
            key: (0u8..16).collect(),
        };
        let codec = codec_config(
            json!(null),
            json!({ "enabled": true, "algo": "sm4-gcm", "key": "000102030405060708090a0b0c0d0e0f" }),
        );
        // 逐条编码，模拟 sink 逐消息 encode（每条消息一个独立加密帧）
        let mut wire = encode(None, Some(&encrypt), b"one\n");
        wire.extend_from_slice(&encode(None, Some(&encrypt), b"two\n"));
        wire.extend_from_slice(&encode(None, Some(&encrypt), b"three\n"));

        let mut dec = StreamDecoder::new(&codec).unwrap().expect("codec enabled");
        let out = decode_all(&mut dec, &wire, 16);
        assert_eq!(String::from_utf8_lossy(&out), "one\ntwo\nthree\n");
    }

    /// 坏 magic 时 `decode` 返回错误（而非静默放行）。
    #[test]
    fn stream_decoder_bad_magic_errors() {
        let codec = codec_config(
            json!(null),
            json!({ "enabled": true, "algo": "sm4-gcm", "key": "000102030405060708090a0b0c0d0e0f" }),
        );
        let mut dec = StreamDecoder::new(&codec).unwrap().expect("codec enabled");
        let mut raw = BytesMut::from(&b"not-a-codec-frame"[..]);
        assert!(dec.decode(&mut raw).is_err());
    }
}
