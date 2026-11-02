//! MIME entity handling for BEEP payloads (RFC 3080 §2.2.2).

use bytes::{BufMut, Bytes, BytesMut};

pub const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";
pub const BEEP_XML: &str = "application/beep+xml";
/// Largest entity-header block we accept.
pub const MAX_HEAD: usize = 64 * 1024;

pub type Headers = Vec<(String, String)>;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Entity {
    pub headers: Headers,
    pub body: Bytes,
}

impl Entity {
    pub fn new(headers: Headers, body: impl Into<Bytes>) -> Self {
        Entity { headers, body: body.into() }
    }

    pub fn xml(body: impl Into<String>) -> Self {
        Entity::new(vec![("Content-Type".into(), BEEP_XML.into())], body.into())
    }

    pub fn parse(payload: Bytes) -> Entity {
        match split_head(&payload, true) {
            Some((headers, off)) => Entity { headers, body: payload.slice(off..) },
            None => Entity { headers: vec![], body: payload },
        }
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        header(&self.headers, name)
    }

    pub fn content_type(&self) -> &str {
        self.header("Content-Type").unwrap_or(DEFAULT_CONTENT_TYPE)
    }

    pub fn head_bytes(headers: &[(String, String)]) -> BytesMut {
        let mut out = BytesMut::new();
        for (k, v) in headers {
            out.put_slice(sanitize(k).as_bytes());
            out.put_slice(b": ");
            out.put_slice(sanitize(v).as_bytes());
            out.put_slice(b"\r\n");
        }
        out.put_slice(b"\r\n");
        out
    }

    pub fn to_bytes(&self) -> Bytes {
        let mut out = Self::head_bytes(&self.headers);
        out.put_slice(&self.body);
        out.freeze()
    }
}

pub fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers.iter().find(|(k, _)| k.eq_ignore_ascii_case(name)).map(|(_, v)| v.as_str())
}

/// Strip CR/LF so a header value can never inject extra header lines.
pub fn sanitize(s: &str) -> std::borrow::Cow<'_, str> {
    if s.contains(['\r', '\n']) {
        std::borrow::Cow::Owned(s.replace(['\r', '\n'], ""))
    } else {
        std::borrow::Cow::Borrowed(s)
    }
}

/// Try to split an entity head from `buf`.
///
/// Returns `(headers, body_offset)` once the head is complete. With `complete == false`
/// (more bytes may follow) it returns `None` when it cannot decide yet. With
/// `complete == true` a payload that has no recognisable head is treated as all body.
pub fn split_head(buf: &[u8], complete: bool) -> Option<(Headers, usize)> {
    if buf.starts_with(b"\r\n") {
        return Some((vec![], 2));
    }
    match find(buf, b"\r\n") {
        None if complete => Some((vec![], 0)),
        None => None,
        // A first line without a colon means "no head at all" (lenient mode).
        Some(p) if !buf[..p].contains(&b':') => Some((vec![], 0)),
        Some(_) => match find(buf, b"\r\n\r\n") {
            Some(q) => Some((parse_header_lines(&buf[..q + 2]), q + 4)),
            None if complete => Some((vec![], 0)),
            None => None,
        },
    }
}

fn parse_header_lines(block: &[u8]) -> Headers {
    let text = String::from_utf8_lossy(block);
    let mut out: Headers = vec![];
    for line in text.split("\r\n") {
        if line.is_empty() {
            continue;
        }
        if line.starts_with([' ', '\t']) {
            if let Some(last) = out.last_mut() {
                last.1.push(' ');
                last.1.push_str(line.trim());
            }
            continue;
        }
        if let Some((k, v)) = line.split_once(':') {
            out.push((k.trim().to_string(), v.trim().to_string()));
        }
    }
    out
}

pub(crate) fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

/// Incrementally separates an entity head from a body arriving in chunks.
#[derive(Debug, Default)]
pub struct HeadParser {
    buf: BytesMut,
    done: bool,
}

pub enum HeadProgress {
    /// Need more data.
    Pending,
    /// Head complete; `rest` is the first piece of body.
    Done { headers: Headers, rest: Bytes },
    /// Head is unreasonably large.
    TooLarge,
}

impl HeadParser {
    pub fn push(&mut self, chunk: &[u8], last: bool) -> HeadProgress {
        debug_assert!(!self.done);
        self.buf.put_slice(chunk);
        match split_head(&self.buf, last) {
            Some((headers, off)) => {
                self.done = true;
                let mut all = std::mem::take(&mut self.buf);
                let rest = all.split_off(off).freeze();
                HeadProgress::Done { headers, rest }
            }
            None if self.buf.len() > MAX_HEAD => HeadProgress::TooLarge,
            None => HeadProgress::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_variants() {
        let e = Entity::parse(Bytes::from_static(b"Content-Type: text/plain\r\nX-A: 1\r\n  continued\r\n\r\nbody"));
        assert_eq!(e.content_type(), "text/plain");
        assert_eq!(e.header("x-a"), Some("1 continued"));
        assert_eq!(&e.body[..], b"body");

        let e = Entity::parse(Bytes::from_static(b"\r\nonly body"));
        assert!(e.headers.is_empty());
        assert_eq!(&e.body[..], b"only body");
        assert_eq!(e.content_type(), DEFAULT_CONTENT_TYPE);

        let e = Entity::parse(Bytes::from_static(b"<raw xml/>"));
        assert_eq!(&e.body[..], b"<raw xml/>");
        let e = Entity::parse(Bytes::from_static(b""));
        assert!(e.body.is_empty());
    }

    #[test]
    fn roundtrip_and_injection() {
        let e = Entity::new(vec![("A".into(), "x\r\nEvil: 1".into())], Bytes::from_static(b"z"));
        let back = Entity::parse(e.to_bytes());
        assert_eq!(back.headers, vec![("A".to_string(), "xEvil: 1".to_string())]);
        assert_eq!(&back.body[..], b"z");
    }

    #[test]
    fn head_parser_incremental() {
        let src = b"Rpc-Status: 200\r\nContent-Type: a/b\r\n\r\nhello";
        let mut p = HeadParser::default();
        let mut res = None;
        for chunk in src.chunks(3) {
            if let HeadProgress::Done { headers, rest } = p.push(chunk, false) {
                res = Some((headers, rest));
                break;
            }
        }
        let (h, rest) = res.unwrap();
        assert_eq!(header(&h, "rpc-status"), Some("200"));
        assert!(b"hello".starts_with(&rest));
    }
}
