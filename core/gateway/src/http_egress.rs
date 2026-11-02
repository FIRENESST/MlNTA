//! Egress: plain HTTP (HTTP/1.1 and h2c) from any language → BEEP RPC calls.

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use http::{HeaderValue, Request, Response, StatusCode};
use http_body_util::{BodyExt, Limited};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use minta_beep::rpc::{RpcClient, RpcError, RpcRequest};
use minta_beep::session::Reply;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

use crate::config::HttpListener;
use crate::metrics::{Counters, Metrics};
use crate::util::*;

/// Responses up to this size are buffered so the client gets a Content-Length.
const BUFFER_LIMIT: usize = 256 * 1024;

pub struct HttpState {
    pub cfg: HttpListener,
    pub upstreams: Arc<HashMap<String, RpcClient>>,
    pub metrics: Arc<Metrics>,
    pub counters: &'static Counters,
}

pub async fn serve(state: Arc<HttpState>) -> std::io::Result<()> {
    let listener = TcpListener::bind(&state.cfg.listen).await?;
    tracing::info!("http egress listening on {} → upstream {:?}", listener.local_addr()?, state.cfg.upstream);
    loop {
        let (tcp, peer) = match listener.accept().await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!("accept: {e}");
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        let _ = tcp.set_nodelay(true);
        let st = state.clone();
        tokio::spawn(async move {
            let svc = service_fn(move |req| handle(req, st.clone(), peer));
            if let Err(e) = auto::Builder::new(TokioExecutor::new()).serve_connection(TokioIo::new(tcp), svc).await {
                tracing::debug!(%peer, "http connection error: {e}");
            }
        });
    }
}

fn error_response(status: StatusCode, code: u32, message: &str) -> Response<GwBody> {
    let mut r = Response::new(full(json_error(code, message)));
    *r.status_mut() = status;
    r.headers_mut().insert("content-type", HeaderValue::from_static("application/json"));
    if let Ok(v) = HeaderValue::from_str(&code.to_string()) {
        r.headers_mut().insert("x-beep-error-code", v);
    }
    r
}

fn rpc_error_response(e: &RpcError) -> Response<GwBody> {
    let status = StatusCode::from_u16(e.http_status()).unwrap_or(StatusCode::BAD_GATEWAY);
    let message = match e {
        RpcError::Remote { message, .. } => message.clone(),
        other => other.to_string(),
    };
    error_response(status, e.beep_code(), &message)
}

async fn handle(req: Request<Incoming>, st: Arc<HttpState>, peer: SocketAddr) -> Result<Response<GwBody>, Infallible> {
    let _inflight = Metrics::start(st.counters);
    let resp = if req.uri().path().starts_with("/__beep/") {
        admin(req, &st).await
    } else {
        proxy(req, &st, peer).await
    };
    if resp.status().is_server_error() || resp.headers().contains_key("x-beep-error-code") {
        st.counters.errors.fetch_add(1, Ordering::Relaxed);
    }
    Ok(resp)
}

fn pick_upstream<'a>(st: &'a HttpState, req: &Request<Incoming>, target: &mut String) -> Result<(&'a str, &'a RpcClient), Response<GwBody>> {
    let explicit = req.headers().get("x-beep-upstream").and_then(|v| v.to_str().ok()).map(str::to_string);
    let name: String = match explicit {
        Some(n) => n,
        None => match st.cfg.routes.iter().filter(|r| target.starts_with(&r.prefix)).max_by_key(|r| r.prefix.len()) {
            Some(r) => {
                if r.strip_prefix {
                    let rest = &target[r.prefix.len()..];
                    *target = if rest.starts_with('/') { rest.to_string() } else { format!("/{rest}") };
                }
                r.upstream.clone()
            }
            None => st.cfg.upstream.clone(),
        },
    };
    match st.upstreams.get_key_value(name.as_str()) {
        Some((k, c)) => Ok((k.as_str(), c)),
        None => Err(error_response(StatusCode::BAD_REQUEST, 553, &format!("unknown upstream {name:?}"))),
    }
}

async fn proxy(req: Request<Incoming>, st: &HttpState, peer: SocketAddr) -> Response<GwBody> {
    let mut target = req.uri().path_and_query().map(|p| p.as_str().to_string()).unwrap_or_else(|| "/".into());
    let (_, client) = match pick_upstream(st, &req, &mut target) {
        Ok(c) => c,
        Err(r) => return r,
    };
    let timeout = req
        .headers()
        .get("x-beep-timeout-ms")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(Duration::from_millis);
    let method = req.method().as_str().to_string();
    let is_head = req.method() == http::Method::HEAD;

    let strip = strip_list(req.headers());
    let mut headers: Vec<(String, String)> = Vec::with_capacity(req.headers().len() + 3);
    let authority = req
        .headers()
        .get(http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
        .or_else(|| req.uri().authority().map(|a| a.to_string()));
    for (k, v) in header_pairs(req.headers(), &strip) {
        let lk = k.to_ascii_lowercase();
        if lk == "host" || lk.starts_with("x-beep-") || matches!(lk.as_str(), "rpc-method" | "rpc-target" | "rpc-status" | "rpc-reason" | "rpc-authority" | "rpc-abort") {
            continue;
        }
        if lk == "x-forwarded-for" && st.cfg.forwarded_headers {
            continue; // re-added below with the peer appended
        }
        headers.push((k, v));
    }
    if let Some(a) = authority {
        headers.push(("Rpc-Authority".into(), a));
    }
    if let Some(t) = timeout {
        headers.retain(|(k, _)| !k.eq_ignore_ascii_case("rpc-timeout"));
        headers.push(("Rpc-Timeout".into(), t.as_millis().to_string()));
    }
    if st.cfg.forwarded_headers {
        let prior: Vec<&str> = req.headers().get_all("x-forwarded-for").iter().filter_map(|v| v.to_str().ok()).collect();
        let mut xff = prior.join(", ");
        if !xff.is_empty() {
            xff.push_str(", ");
        }
        xff.push_str(&peer.ip().to_string());
        headers.push(("X-Forwarded-For".into(), xff));
        if !req.headers().contains_key("x-forwarded-proto") {
            headers.push(("X-Forwarded-Proto".into(), "http".into()));
        }
    }

    let body = match Limited::new(req.into_body(), st.cfg.max_body_bytes).collect().await {
        Ok(b) => b.to_bytes(),
        Err(e) => {
            if e.downcast_ref::<http_body_util::LengthLimitError>().is_some() {
                return error_response(StatusCode::PAYLOAD_TOO_LARGE, 554, "request body too large");
            }
            return error_response(StatusCode::BAD_REQUEST, 500, &format!("reading request body: {e}"));
        }
    };
    st.counters.bytes_in.fetch_add(body.len() as u64, Ordering::Relaxed);

    let rpc = RpcRequest { method, target, headers, body };
    let call = match timeout {
        Some(t) => client.call_streaming_timeout(rpc, Some(t)).await,
        None => client.call_streaming(rpc).await,
    };
    let (head, mut body) = match call {
        Ok(r) => r,
        Err(e) => return rpc_error_response(&e),
    };

    let Ok(status) = StatusCode::from_u16(head.status) else {
        return error_response(StatusCode::BAD_GATEWAY, 554, &format!("upstream sent invalid status {}", head.status));
    };
    let mut builder = Response::builder().status(status);
    for (k, v) in &head.headers {
        if is_hop_by_hop(k) {
            continue;
        }
        if let (Ok(name), Ok(val)) = (http::HeaderName::from_bytes(k.as_bytes()), HeaderValue::from_str(v)) {
            builder = builder.header(name, val);
        }
    }
    let no_body = is_head || status == StatusCode::NO_CONTENT || status == StatusCode::NOT_MODIFIED || status.is_informational();

    // Buffer small bodies; stream the rest.
    let mut buf = BytesMut::new();
    loop {
        match body.chunk().await {
            None => {
                st.counters.bytes_out.fetch_add(buf.len() as u64, Ordering::Relaxed);
                let b = if no_body { Bytes::new() } else { buf.freeze() };
                return builder.body(full(b)).unwrap_or_else(|_| error_response(StatusCode::BAD_GATEWAY, 554, "bad response"));
            }
            Some(Ok(c)) => {
                buf.extend_from_slice(&c);
                if buf.len() > BUFFER_LIMIT {
                    break;
                }
            }
            Some(Err(e)) => return rpc_error_response(&e),
        }
    }
    let (tx, rx) = mpsc::channel::<Result<Bytes, BoxError>>(8);
    let counters = st.counters;
    let first = buf.freeze();
    tokio::spawn(async move {
        counters.bytes_out.fetch_add(first.len() as u64, Ordering::Relaxed);
        if tx.send(Ok(first)).await.is_err() {
            return;
        }
        while let Some(c) = body.chunk().await {
            let item = match c {
                Ok(b) => {
                    counters.bytes_out.fetch_add(b.len() as u64, Ordering::Relaxed);
                    Ok(b)
                }
                Err(e) => Err(Box::new(e) as BoxError),
            };
            let stop = item.is_err();
            if tx.send(item).await.is_err() || stop {
                return;
            }
        }
    });
    builder
        .body(http_body_util::BodyExt::boxed(ChannelBody { rx }))
        .unwrap_or_else(|_| error_response(StatusCode::BAD_GATEWAY, 554, "bad response"))
}

async fn admin(req: Request<Incoming>, st: &HttpState) -> Response<GwBody> {
    let json = |status: StatusCode, v: serde_json::Value| {
        let mut r = Response::new(full(v.to_string()));
        *r.status_mut() = status;
        r.headers_mut().insert("content-type", HeaderValue::from_static("application/json"));
        r
    };
    match (req.method().as_str(), req.uri().path()) {
        ("GET", "/__beep/health") => {
            let mut ups = serde_json::Map::new();
            let mut healthy = true;
            for (name, c) in st.upstreams.iter() {
                let ok = tokio::time::timeout(Duration::from_secs(3), c.connect()).await.map(|r| r.is_ok()).unwrap_or(false);
                healthy &= ok;
                ups.insert(name.clone(), serde_json::json!({"address": c.addr(), "connected": ok, "sessions": c.status()}));
            }
            let status = if healthy { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE };
            json(status, serde_json::json!({"status": if healthy {"ok"} else {"degraded"}, "upstreams": ups}))
        }
        ("GET", "/__beep/stats") => json(StatusCode::OK, st.metrics.json()),
        ("GET", "/__beep/metrics") => {
            let mut r = Response::new(full(st.metrics.prometheus()));
            r.headers_mut().insert("content-type", HeaderValue::from_static("text/plain; version=0.0.4"));
            r
        }
        ("POST", "/__beep/raw") => raw(req, st).await,
        _ => error_response(StatusCode::NOT_FOUND, 550, "unknown gateway endpoint"),
    }
}

/// docs/PROTOCOL.md §3.3: send a verbatim payload on a channel of any profile.
async fn raw(req: Request<Incoming>, st: &HttpState) -> Response<GwBody> {
    let Some(profile) = req.headers().get("x-beep-profile").and_then(|v| v.to_str().ok()).map(str::to_string) else {
        return error_response(StatusCode::BAD_REQUEST, 501, "X-Beep-Profile header is required");
    };
    let mut dummy = String::new();
    let client = match pick_upstream(st, &req, &mut dummy) {
        Ok((_, c)) => c.clone(),
        Err(r) => return r,
    };
    let payload = match Limited::new(req.into_body(), st.cfg.max_body_bytes).collect().await {
        Ok(b) => b.to_bytes(),
        Err(_) => return error_response(StatusCode::PAYLOAD_TOO_LARGE, 554, "request body too large"),
    };
    let reply = match client.raw(&profile, payload).await {
        Ok(r) => r,
        Err(e) => return rpc_error_response(&e),
    };
    let (kind, ctype, body) = match reply {
        Reply::Rpy(b) => ("RPY", "application/octet-stream".to_string(), b),
        Reply::Err(b) => ("ERR", "application/octet-stream".to_string(), b),
        Reply::Ans(parts) => {
            let boundary = format!("beep-{:016x}", rand_u64());
            let mut out = BytesMut::new();
            for (_, p) in parts {
                out.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
                out.extend_from_slice(&p);
                out.extend_from_slice(b"\r\n");
            }
            out.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
            ("ANS", format!("multipart/mixed; boundary={boundary}"), out.freeze())
        }
    };
    Response::builder()
        .status(StatusCode::OK)
        .header("x-beep-reply", kind)
        .header("content-type", ctype)
        .body(full(body))
        .unwrap()
}

fn rand_u64() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u128(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos());
    h.finish()
}
