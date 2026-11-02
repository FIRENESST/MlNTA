//! BEEP frame codec (RFC 3080 §2.2, RFC 3081 §3.1).

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::Error;

/// Longest header line we accept, including the trailing CRLF.
pub const MAX_HEADER_LINE: usize = 128;
const TRAILER: &[u8] = b"END\r\n";
const MAX_I31: u64 = 2_147_483_647;
const MAX_U32: u64 = 4_294_967_295;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FrameType {
    Msg,
    Rpy,
    Err,
    Ans,
    Nul,
}

impl FrameType {
    pub fn keyword(self) -> &'static str {
        match self {
            FrameType::Msg => "MSG",
            FrameType::Rpy => "RPY",
            FrameType::Err => "ERR",
            FrameType::Ans => "ANS",
            FrameType::Nul => "NUL",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataFrame {
    pub ftype: FrameType,
    pub channel: u32,
    pub msgno: u32,
    pub more: bool,
    pub seqno: u32,
    /// Only meaningful for ANS frames.
    pub ansno: u32,
    pub payload: Bytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SeqFrame {
    pub channel: u32,
    pub ackno: u32,
    pub window: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    Data(DataFrame),
    Seq(SeqFrame),
}

/// Incremental decoder. Feed bytes into a `BytesMut` and call [`FrameDecoder::decode`]
/// until it returns `Ok(None)`.
#[derive(Debug, Clone)]
pub struct FrameDecoder {
    max_payload: usize,
}

impl FrameDecoder {
    /// `max_payload` bounds memory: no legitimate frame can exceed the largest window we advertise.
    pub fn new(max_payload: usize) -> Self {
        FrameDecoder { max_payload }
    }

    pub fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Frame>, Error> {
        let scan = buf.len().min(MAX_HEADER_LINE);
        let line_end = match buf[..scan].windows(2).position(|w| w == b"\r\n") {
            Some(p) => p,
            None if buf.len() >= MAX_HEADER_LINE => {
                return Err(Error::Protocol("frame header line too long".into()))
            }
            None => return Ok(None),
        };
        let line = std::str::from_utf8(&buf[..line_end])
            .map_err(|_| Error::Protocol("frame header is not ASCII".into()))?;
        let parts: Vec<&str> = line.split(' ').collect();
        let head_len = line_end + 2;

        if parts[0] == "SEQ" {
            if parts.len() != 4 {
                return Err(Error::Protocol(format!("malformed SEQ header: {line:?}")));
            }
            let seq = SeqFrame {
                channel: num(parts[1], MAX_I31)?,
                ackno: num(parts[2], MAX_U32)?,
                window: num(parts[3], MAX_I31)?,
            };
            buf.advance(head_len);
            return Ok(Some(Frame::Seq(seq)));
        }

        let ftype = match parts[0] {
            "MSG" => FrameType::Msg,
            "RPY" => FrameType::Rpy,
            "ERR" => FrameType::Err,
            "ANS" => FrameType::Ans,
            "NUL" => FrameType::Nul,
            other => return Err(Error::Protocol(format!("unknown frame keyword {other:?}"))),
        };
        let expected = if ftype == FrameType::Ans { 7 } else { 6 };
        if parts.len() != expected {
            return Err(Error::Protocol(format!("malformed frame header: {line:?}")));
        }
        let channel = num(parts[1], MAX_I31)?;
        let msgno = num(parts[2], MAX_I31)?;
        let more = match parts[3] {
            "." => false,
            "*" => true,
            _ => return Err(Error::Protocol(format!("bad continuation indicator in {line:?}"))),
        };
        let seqno = num(parts[4], MAX_U32)?;
        let size = num(parts[5], MAX_I31)? as usize;
        let ansno = if ftype == FrameType::Ans { num(parts[6], MAX_I31)? } else { 0 };
        if ftype == FrameType::Nul && (size != 0 || more) {
            return Err(Error::Protocol("NUL frame must have size 0 and no continuation".into()));
        }
        if size > self.max_payload {
            return Err(Error::Protocol(format!("frame payload {size} exceeds limit {}", self.max_payload)));
        }
        let total = head_len + size + TRAILER.len();
        if buf.len() < total {
            buf.reserve(total - buf.len());
            return Ok(None);
        }
        if &buf[head_len + size..total] != TRAILER {
            return Err(Error::Protocol("frame trailer is not END CRLF".into()));
        }
        buf.advance(head_len);
        let payload = buf.split_to(size).freeze();
        buf.advance(TRAILER.len());
        Ok(Some(Frame::Data(DataFrame { ftype, channel, msgno, more, seqno, ansno, payload })))
    }
}

fn num(s: &str, max: u64) -> Result<u32, Error> {
    if s.is_empty() || s.len() > 10 || !s.bytes().all(|b| b.is_ascii_digit()) {
        return Err(Error::Protocol(format!("bad number {s:?} in frame header")));
    }
    let v: u64 = s.parse().map_err(|_| Error::Protocol(format!("bad number {s:?}")))?;
    if v > max {
        return Err(Error::Protocol(format!("number {v} out of range")));
    }
    Ok(v as u32)
}

pub fn encode_data(f: &DataFrame) -> Bytes {
    let mut out = BytesMut::with_capacity(f.payload.len() + 64);
    let more = if f.more { '*' } else { '.' };
    let head = if f.ftype == FrameType::Ans {
        format!(
            "ANS {} {} {} {} {} {}\r\n",
            f.channel,
            f.msgno,
            more,
            f.seqno,
            f.payload.len(),
            f.ansno
        )
    } else {
        format!(
            "{} {} {} {} {} {}\r\n",
            f.ftype.keyword(),
            f.channel,
            f.msgno,
            more,
            f.seqno,
            f.payload.len()
        )
    };
    out.put_slice(head.as_bytes());
    out.put_slice(&f.payload);
    out.put_slice(TRAILER);
    out.freeze()
}

pub fn encode_seq(s: &SeqFrame) -> Bytes {
    Bytes::from(format!("SEQ {} {} {}\r\n", s.channel, s.ackno, s.window))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_all(input: &[u8]) -> Result<Vec<Frame>, Error> {
        let mut dec = FrameDecoder::new(1 << 20);
        let mut buf = BytesMut::from(input);
        let mut out = vec![];
        while let Some(f) = dec.decode(&mut buf)? {
            out.push(f);
        }
        Ok(out)
    }

    #[test]
    fn roundtrip_all_types() {
        for ftype in [FrameType::Msg, FrameType::Rpy, FrameType::Err, FrameType::Ans] {
            let f = DataFrame {
                ftype,
                channel: 3,
                msgno: 7,
                more: true,
                seqno: 4_294_967_000,
                ansno: if ftype == FrameType::Ans { 9 } else { 0 },
                payload: Bytes::from_static(b"\r\nhello"),
            };
            let enc = encode_data(&f);
            assert_eq!(decode_all(&enc).unwrap(), vec![Frame::Data(f)]);
        }
        let s = SeqFrame { channel: 1, ackno: 123, window: 4096 };
        assert_eq!(decode_all(&encode_seq(&s)).unwrap(), vec![Frame::Seq(s)]);
    }

    #[test]
    fn rfc_greeting_example() {
        let raw = b"RPY 0 0 . 0 52\r\nContent-Type: application/beep+xml\r\n\r\n<greeting />\r\nEND\r\n";
        let frames = decode_all(raw).unwrap();
        match &frames[0] {
            Frame::Data(d) => {
                assert_eq!(d.ftype, FrameType::Rpy);
                assert_eq!(d.payload.len(), 52);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn incremental_and_errors() {
        let enc = encode_data(&DataFrame {
            ftype: FrameType::Msg,
            channel: 1,
            msgno: 0,
            more: false,
            seqno: 0,
            ansno: 0,
            payload: Bytes::from_static(b"abc"),
        });
        let mut dec = FrameDecoder::new(1024);
        let mut buf = BytesMut::new();
        for (i, b) in enc.iter().enumerate() {
            buf.put_u8(*b);
            let r = dec.decode(&mut buf).unwrap();
            assert_eq!(r.is_some(), i == enc.len() - 1);
        }
        assert!(decode_all(b"MSG 1 0 . 0 3\r\nabcENDX\r\n").is_err());
        assert!(decode_all(b"MSG 1 0 . 0 3\r\nabcXND\r\n").is_err());
        assert!(decode_all(b"MSG 1 0 ? 0 0\r\nEND\r\n").is_err());
        assert!(decode_all(b"MSG 2147483648 0 . 0 0\r\nEND\r\n").is_err());
        assert!(decode_all(b"NUL 1 0 . 0 1\r\nxEND\r\n").is_err());
        assert!(decode_all(b"FOO 1 0 . 0 0\r\nEND\r\n").is_err());
        assert!(decode_all(b"MSG  1 0 . 0 0\r\nEND\r\n").is_err());
        assert!(FrameDecoder::new(2).decode(&mut BytesMut::from(&b"MSG 1 0 . 0 3\r\n"[..])).is_err());
    }
}
