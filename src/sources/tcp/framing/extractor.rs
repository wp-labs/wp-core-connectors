use bytes::{Buf, Bytes, BytesMut};
use memchr::memchr;

use super::{MAX_FRAME_BYTES, MAX_LEN_DIGITS};

/// Message framing extractor (line, length-prefixed RFC6587)
pub struct FramingExtractor;

impl FramingExtractor {
    /// Extract line-separated message from buffer (drops trailing \n and any \r)
    pub fn extract_line_message(buf: &mut BytesMut) -> Option<Bytes> {
        let newline_pos = memchr(b'\n', buf.as_ref())?;
        let mut chunk = buf.split_to(newline_pos + 1);
        chunk.truncate(newline_pos);
        if let Some(first_cr) = memchr(b'\r', chunk.as_ref()) {
            let mut write_idx = first_cr;
            for read_idx in (first_cr + 1)..chunk.len() {
                let byte = chunk[read_idx];
                if byte != b'\r' {
                    chunk[write_idx] = byte;
                    write_idx += 1;
                }
            }
            chunk.truncate(write_idx);
        }
        Some(chunk.freeze())
    }

    /// Extract length-prefixed message from buffer: "<len><SP><payload>"
    pub fn extract_length_prefixed_message(buf: &mut BytesMut) -> Option<Bytes> {
        let mut pos = 0;
        let mut len: usize = 0;
        while pos < buf.len() && pos < MAX_LEN_DIGITS && buf[pos].is_ascii_digit() {
            // 用 checked 运算：避免 32 位平台上 10 位数长度前缀的 `len*10` 溢出（debug panic /
            // release 回绕）。溢出即视为非法前缀，返回 None。
            len = len
                .checked_mul(10)?
                .checked_add((buf[pos] - b'0') as usize)?;
            pos += 1;
        }
        if pos == 0 || pos >= buf.len() || buf[pos] != b' ' {
            return None;
        }
        // 长度上限：拒绝超大前缀（合法帧不会超过 MAX_FRAME_BYTES），交给上层
        // 的缓冲上限守卫注销该连接，避免缓冲无界增长。
        if len >= MAX_FRAME_BYTES {
            return None;
        }
        pos += 1;
        if pos.checked_add(len)? > buf.len() {
            return None;
        }
        buf.advance(pos);
        let payload = buf.split_to(len);
        Some(payload.freeze())
    }

    pub fn has_length_prefix(buf: &BytesMut) -> bool {
        let mut pos = 0;
        let mut found_digit = false;
        while pos < buf.len() && pos < 10 {
            if buf[pos].is_ascii_digit() {
                found_digit = true;
                pos += 1;
            } else if buf[pos] == b' ' && found_digit {
                return true;
            } else {
                break;
            }
        }
        false
    }

    pub fn has_newline(buf: &BytesMut) -> bool {
        buf.contains(&b'\n')
    }

    /// 缓冲起始处是否是一个「完整但非法」的长度前缀（溢出，或 ≥ `MAX_FRAME_BYTES`）。
    /// 「完整」= 已出现 `<digits><SP>`。尚未出现空格（未收齐）或非数字开头 → `false`。
    ///
    /// 用于 `Len` 框架下立即断开畸形/超大前缀的连接，而不必等缓冲涨到上限。
    pub fn leading_length_prefix_invalid(buf: &BytesMut) -> bool {
        let mut pos = 0usize;
        let mut val: usize = 0;
        let mut overflow = false;
        while pos < buf.len() && pos < MAX_LEN_DIGITS && buf[pos].is_ascii_digit() {
            match val
                .checked_mul(10)
                .and_then(|v| v.checked_add((buf[pos] - b'0') as usize))
            {
                Some(v) => val = v,
                None => overflow = true,
            }
            pos += 1;
        }
        if pos == 0 || pos >= buf.len() || buf[pos] != b' ' {
            // 位数已到上限、下一个仍是数字 → 前缀过长（畸形）。
            return pos == MAX_LEN_DIGITS && pos < buf.len() && buf[pos].is_ascii_digit();
        }
        overflow || val >= MAX_FRAME_BYTES
    }
}

#[cfg(test)]
mod tests {
    use super::FramingExtractor;
    use bytes::{Buf, Bytes, BytesMut};

    // Line message extraction tests
    #[test]
    fn test_extract_line_message() {
        let mut buf = BytesMut::from("hello world\n");
        let result = FramingExtractor::extract_line_message(&mut buf);

        assert!(result.is_some());
        assert_eq!(result.unwrap(), Bytes::from("hello world"));
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn test_extract_line_message_with_crlf() {
        let mut buf = BytesMut::from("hello world\r\n");
        let result = FramingExtractor::extract_line_message(&mut buf);

        assert!(result.is_some());
        assert_eq!(result.unwrap(), Bytes::from("hello world"));
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn test_extract_line_message_no_newline() {
        let mut buf = BytesMut::from("hello world");
        let result = FramingExtractor::extract_line_message(&mut buf);

        assert!(result.is_none());
        assert_eq!(buf, BytesMut::from("hello world"));
    }

    #[test]
    fn test_extract_line_message_multiple_lines() {
        let mut buf = BytesMut::from("first\nsecond\n");

        let result1 = FramingExtractor::extract_line_message(&mut buf);
        assert_eq!(result1.unwrap(), Bytes::from("first"));

        let result2 = FramingExtractor::extract_line_message(&mut buf);
        assert_eq!(result2.unwrap(), Bytes::from("second"));

        assert_eq!(buf.len(), 0);
    }

    // Length-prefixed message extraction tests
    #[test]
    fn test_extract_length_prefixed_message() {
        let mut buf = BytesMut::from("5 hello");
        let result = FramingExtractor::extract_length_prefixed_message(&mut buf);

        assert!(result.is_some());
        assert_eq!(result.unwrap(), Bytes::from("hello"));
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn test_extract_length_prefixed_message_multi_digit() {
        let payload = "multi digit payload";
        let mut buf = BytesMut::from(&format!("{} {}", payload.len(), payload)[..]);
        let result = FramingExtractor::extract_length_prefixed_message(&mut buf);

        assert!(result.is_some());
        assert_eq!(result.unwrap(), Bytes::from(payload));
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn test_extract_length_prefixed_message_zero_length() {
        let mut buf = BytesMut::from("0 ");
        let result = FramingExtractor::extract_length_prefixed_message(&mut buf);

        assert!(result.is_some());
        assert_eq!(result.unwrap(), Bytes::from(""));
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn test_extract_length_prefixed_message_invalid() {
        let mut buf1 = BytesMut::from("5hello"); // missing space
        let mut buf2 = BytesMut::from("a hello"); // non-digit
        let mut buf3 = BytesMut::from("10 hello"); // incomplete payload

        assert!(FramingExtractor::extract_length_prefixed_message(&mut buf1).is_none());
        assert!(FramingExtractor::extract_length_prefixed_message(&mut buf2).is_none());
        assert!(FramingExtractor::extract_length_prefixed_message(&mut buf3).is_none());
    }

    // Helper function tests
    #[test]
    fn test_has_length_prefix() {
        assert!(FramingExtractor::has_length_prefix(&BytesMut::from(
            "5 hello"
        )));
        assert!(FramingExtractor::has_length_prefix(&BytesMut::from("0 ")));

        assert!(!FramingExtractor::has_length_prefix(&BytesMut::from("")));
        assert!(!FramingExtractor::has_length_prefix(&BytesMut::from(
            "hello"
        )));
        assert!(!FramingExtractor::has_length_prefix(&BytesMut::from(
            "5hello"
        )));
    }

    #[test]
    fn test_has_newline() {
        assert!(FramingExtractor::has_newline(&BytesMut::from("hello\n")));
        assert!(FramingExtractor::has_newline(&BytesMut::from("hello\r\n")));

        assert!(!FramingExtractor::has_newline(&BytesMut::from("")));
        assert!(!FramingExtractor::has_newline(&BytesMut::from("hello")));
        assert!(!FramingExtractor::has_newline(&BytesMut::from("hello\r")));
    }

    // Integration test
    #[test]
    fn test_mixed_scenarios() {
        // Test binary data
        let mut data = Vec::new();
        data.extend_from_slice(b"5 ");
        data.extend_from_slice(&[0x00, 0x01, 0x02, 0xFF, 0xFE]);
        let mut buf = BytesMut::from(&data[..]);
        let result = FramingExtractor::extract_length_prefixed_message(&mut buf);
        assert_eq!(
            result.unwrap(),
            Bytes::from(&[0x00, 0x01, 0x02, 0xFF, 0xFE][..])
        );

        // Test partial buffer with newline
        let mut buf = BytesMut::from("5 hello\nworld\n");
        let msg1 = FramingExtractor::extract_length_prefixed_message(&mut buf).unwrap();
        assert_eq!(msg1, Bytes::from("hello"));
        // After length-prefixed extraction, buffer should be "\nworld\n"
        assert!(buf.starts_with(b"\n"));
        buf.advance(1); // Skip the newline
        let msg2 = FramingExtractor::extract_line_message(&mut buf).unwrap();
        assert_eq!(msg2, Bytes::from("world"));
        assert_eq!(buf.len(), 0);
    }

    /// O：超大 / 溢出的长度前缀必须被拒绝（返回 None）且不消费任何字节，
    /// 避免「帧永不完成」导致缓冲无限增长，也避免 32 位平台上 `len*10` 溢出。
    #[test]
    fn rejects_oversized_and_overflow_prefix() {
        use super::super::MAX_FRAME_BYTES;
        // 一个「完整」但恰好达到上限的帧：旧实现会切出这 10MB；新实现按上限拒绝，
        // 且不消费任何字节。
        let header = format!("{MAX_FRAME_BYTES} ");
        let mut buf = BytesMut::with_capacity(header.len() + MAX_FRAME_BYTES);
        buf.extend_from_slice(header.as_bytes());
        buf.extend_from_slice(&vec![b'x'; MAX_FRAME_BYTES]);
        let total = buf.len();
        assert!(FramingExtractor::extract_length_prefixed_message(&mut buf).is_none());
        assert_eq!(buf.len(), total, "oversized frame must not be consumed");

        // 10 位超大前缀（32 位平台上 checked 运算会溢出）→ 不 panic、返回 None
        let mut buf2 = BytesMut::from("9999999999 ");
        assert!(FramingExtractor::extract_length_prefixed_message(&mut buf2).is_none());
        assert_eq!(buf2.len(), 11, "overflowing prefix must not be consumed");
    }

    /// O：#4 依赖的 `leading_length_prefix_invalid` 能识别「超标」与「位数超长」前缀。
    #[test]
    fn leading_length_prefix_invalid_detects_overlong_and_oversized() {
        use super::super::MAX_FRAME_BYTES;
        // 合法短前缀 → false
        assert!(!FramingExtractor::leading_length_prefix_invalid(
            &BytesMut::from("100 ")
        ));
        // 未收齐（无空格）→ false
        assert!(!FramingExtractor::leading_length_prefix_invalid(
            &BytesMut::from("100")
        ));
        // 恰好等/超上限 → true
        assert!(FramingExtractor::leading_length_prefix_invalid(
            &BytesMut::from(format!("{MAX_FRAME_BYTES} ").as_bytes())
        ));
        // 数字位数超限（> MAX_LEN_DIGITS）→ true
        assert!(FramingExtractor::leading_length_prefix_invalid(
            &BytesMut::from("12345678901 ")
        ));
    }
}
