//! HTTP/1.1 chunked coding (RFC 9112 §7.1). On `urn:minta:beep:rpc:2` channels a request
//! body sent with `Rpc-Body: chunked` uses it, which gives a streamed body an explicit end,
//! room for trailer fields, and a way to tell completion from an abort (docs/PROTOCOL.md §2.6.2).

use bytes::{BufMut, Bytes, BytesMut};

use crate::mime::{find, split_head, Entity, Headers, MAX_HEAD};

/// One data chunk. `data` must not be empty: a zero-size chunk ends the body.
pub fn chunk(data: &[u8]) -> Bytes {
    let mut out = BytesMut::with_capacity(data.len() + 12);
    out.put_slice(format!("{:x}\r\n", data.len()).as_bytes());
    out.put_slice(data);
    out.put_slice(b"\r\n");
    out.freeze()
}

/// The last chunk plus trailer section.
pub fn last_chunk(trailers: &[(String, String)]) -> Bytes {
    let mut out = BytesMut::from(&b"0\r\n"[..]);
    out.put_slice(&Entity::head_bytes(trailers));
    out.freeze()
}

#[derive(Debug, PartialEq, Eq)]
pub enum Part {
    Data(Bytes),
    Trailers(Headers),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Size,
    Data(usize),
    DataEnd,
    Trailers,
    Done,
}

/// Incremental decoder; input may be split anywhere.
#[derive(Debug)]
pub struct Decoder {
    buf: BytesMut,
    state: State,
}

impl Default for Decoder {
    fn default() -> Self {
        Decoder { buf: BytesMut::new(), state: State::Size }
    }
}

impl Decoder {
    /// True once the last chunk and the trailer section were seen.
    pub fn is_done(&self) -> bool {
        self.state == State::Done
    }

    pub fn push(&mut self, input: &[u8], out: &mut Vec<Part>) -> Result<(), String> {
        self.buf.put_slice(input);
        loop {
            match self.state {
                State::Done => {
                    return if self.buf.is_empty() { Ok(()) } else { Err("data after the last chunk".into()) };
                }
                State::Size => {
                    let Some(p) = find(&self.buf, b"\r\n") else {
                        return if self.buf.len() > 1024 { Err("chunk size line too long".into()) } else { Ok(()) };
                    };
                    let line = self.buf.split_to(p + 2);
                    let text = std::str::from_utf8(&line[..p]).map_err(|_| "bad chunk size line")?;
                    let hex = text.split(';').next().unwrap_or("").trim();
                    let size = usize::from_str_radix(hex, 16).map_err(|_| format!("bad chunk size {hex:?}"))?;
                    self.state = if size == 0 { State::Trailers } else { State::Data(size) };
                }
                State::Data(n) => {
                    if self.buf.is_empty() {
                        return Ok(());
                    }
                    let take = n.min(self.buf.len());
                    out.push(Part::Data(self.buf.split_to(take).freeze()));
                    self.state = if take == n { State::DataEnd } else { State::Data(n - take) };
                }
                State::DataEnd => {
                    if self.buf.len() < 2 {
                        return Ok(());
                    }
                    if &self.buf[..2] != b"\r\n" {
                        return Err("chunk data not followed by CRLF".into());
                    }
                    let _ = self.buf.split_to(2);
                    self.state = State::Size;
                }
                State::Trailers => match split_head(&self.buf, false) {
                    Some((headers, off)) => {
                        let _ = self.buf.split_to(off);
                        if !headers.is_empty() {
                            out.push(Part::Trailers(headers));
                        }
                        self.state = State::Done;
                    }
                    None if self.buf.len() > MAX_HEAD => return Err("trailer section too large".into()),
                    None => return Ok(()),
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_split(wire: &[u8], step: usize) -> (Vec<u8>, Headers, bool) {
        let mut d = Decoder::default();
        let mut parts = vec![];
        for piece in wire.chunks(step) {
            d.push(piece, &mut parts).unwrap();
        }
        let mut data = vec![];
        let mut trailers = vec![];
        for p in parts {
            match p {
                Part::Data(b) => data.extend_from_slice(&b),
                Part::Trailers(t) => trailers = t,
            }
        }
        (data, trailers, d.is_done())
    }

    #[test]
    fn roundtrip_any_split() {
        let mut wire = BytesMut::new();
        wire.put_slice(&chunk(b"hello "));
        wire.put_slice(&chunk(&[0u8, 1, 2, 255]));
        wire.put_slice(&last_chunk(&[("Grpc-Status".into(), "0".into()), ("X-A".into(), "b".into())]));
        for step in [1, 2, 3, 7, 1000] {
            let (data, trailers, done) = decode_split(&wire, step);
            assert_eq!(data, b"hello \x00\x01\x02\xff");
            assert_eq!(trailers, vec![("Grpc-Status".to_string(), "0".to_string()), ("X-A".to_string(), "b".to_string())]);
            assert!(done);
        }
    }

    #[test]
    fn no_trailers_extensions_and_errors() {
        let (data, trailers, done) = decode_split(b"3;ext=1\r\nabc\r\n0\r\n\r\n", 2);
        assert_eq!((data.as_slice(), trailers.is_empty(), done), (&b"abc"[..], true, true));
        let (_, _, done) = decode_split(b"3\r\nabc\r\n", 4);
        assert!(!done, "missing last chunk means an incomplete (aborted) body");
        let mut d = Decoder::default();
        assert!(d.push(b"zz\r\n", &mut vec![]).is_err());
        let mut d = Decoder::default();
        assert!(d.push(b"2\r\nabXY", &mut vec![]).is_err());
    }
}
