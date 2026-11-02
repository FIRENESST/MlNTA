use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use http::HeaderMap;
use http_body::{Body, Frame};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use tokio::sync::mpsc;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
pub type GwBody = BoxBody<Bytes, BoxError>;

pub fn full(b: impl Into<Bytes>) -> GwBody {
    Full::new(b.into()).map_err(|e| match e {}).boxed()
}

/// Body fed from a channel; an `Err` item aborts the HTTP response.
pub struct ChannelBody {
    pub rx: mpsc::Receiver<Result<Bytes, BoxError>>,
}

impl Body for ChannelBody {
    type Data = Bytes;
    type Error = BoxError;
    fn poll_frame(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(Ok(b))) => Poll::Ready(Some(Ok(Frame::data(b)))),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

const HOP_BY_HOP: [&str; 10] = [
    "connection",
    "keep-alive",
    "proxy-connection",
    "transfer-encoding",
    "te",
    "trailer",
    "upgrade",
    "proxy-authorization",
    "proxy-authenticate",
    "content-length",
];

/// Headers that must not cross the bridge (docs/PROTOCOL.md §2.5): hop-by-hop headers,
/// anything named in `Connection`, BEEP-reserved and gateway-control headers.
pub fn strip_list(headers: &HeaderMap) -> Vec<String> {
    let mut extra: Vec<String> = headers
        .get_all(http::header::CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(|t| t.trim().to_ascii_lowercase())
        .filter(|t| !t.is_empty())
        .collect();
    extra.extend(HOP_BY_HOP.iter().map(|s| s.to_string()));
    extra.push("content-transfer-encoding".into());
    extra
}

pub fn header_pairs(headers: &HeaderMap, strip: &[String]) -> Vec<(String, String)> {
    headers
        .iter()
        .filter(|(k, _)| !strip.iter().any(|s| s == k.as_str()))
        .map(|(k, v)| (canonical(k.as_str()), String::from_utf8_lossy(v.as_bytes()).into_owned()))
        .collect()
}

/// `x-request-id` → `X-Request-Id` (HTTP/2 lowercases names; MIME convention is Title-Case).
pub fn canonical(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut up = true;
    for c in name.chars() {
        if up {
            out.extend(c.to_uppercase());
        } else {
            out.push(c);
        }
        up = c == '-';
    }
    out
}

pub fn is_hop_by_hop(name: &str) -> bool {
    HOP_BY_HOP.iter().any(|h| h.eq_ignore_ascii_case(name)) || name.eq_ignore_ascii_case("content-transfer-encoding")
}

pub fn json_error(code: u32, message: &str) -> Bytes {
    Bytes::from(serde_json::json!({"error": {"code": code, "message": message}}).to_string())
}
