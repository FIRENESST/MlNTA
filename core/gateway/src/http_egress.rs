//! Egress: plain HTTP (HTTP/1.1 and h2c) from any language → BEEP RPC calls.
//!
//! Small requests of known length are buffered into one MSG. Everything else — gRPC,
//! uploads of unknown length, WebSocket upgrades — goes through a duplex call: on an rpc:2
//! upstream the body streams as it arrives and the response can start before it ends.

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use http::{HeaderValue, Request, Response, StatusCode};
use http_body::{Body as _, Frame};
use http_body_util::{BodyExt, Limited};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use minta_beep::rpc::{RequestWriter, ResponseBody, ResponseHead, RpcClient, RpcError, RpcRequest, H_UPGRADE};
use minta_beep::session::Reply;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::config::HttpListener;
use crate::metrics::{Counters, Metrics};
use crate::util::*;

/// Responses up to this size are buffered so the client gets a Content-Length; requests of
/// known length up to this size are sent as one MSG.
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
            let builder = auto::Builder::new(TokioExecutor::new());
            if let Err(e) = builder.serve_connection_with_upgrades(TokioIo::new(tcp), svc).await {
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
    if let RpcError::Transport(minta_beep::Error::TooLarge(n)) = e {
        return error_response(StatusCode::PAYLOAD_TOO_LARGE, e.beep_code(), &format!("request body exceeds {n} bytes for this upstream"));
    }
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

/// Envelope names a client must not set itself (the gateway writes them).
fn is_envelope(lk: &str) -> bool {
    matches!(lk, "rpc-method" | "rpc-target" | "rpc-status" | "rpc-reason" | "rpc-authority" | "rpc-abort" | "rpc-body" | "rpc-trailers" | "rpc-upgrade")
}

async fn proxy(mut req: Request<Incoming>, st: &HttpState, peer: SocketAddr) -> Response<GwBody> {
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
    let upgrade = upgrade_protocol(req.headers());

    let strip = strip_list(req.headers());
    let mut headers: Vec<(String, String)> = Vec::with_capacity(req.headers().len() + 4);
    let authority = req
        .headers()
        .get(http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
        .or_else(|| req.uri().authority().map(|a| a.to_string()));
    for (k, v) in header_pairs(req.headers(), &strip) {
        let lk = k.to_ascii_lowercase();
        if lk == "host" || lk.starts_with("x-beep-") || is_envelope(&lk) {
            continue;
        }
        if lk == "x-forwarded-for" && st.cfg.forwarded_headers {
            continue; // re-added below with the peer appended
        }
        headers.push((k, v));
    }
    if wants_trailers(req.headers()) {
        headers.push(("Te".into(), "trailers".into()));
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

    let grpc = req.headers().get(http::header::CONTENT_TYPE).and_then(|v| v.to_str().ok()).is_some_and(|v| v.starts_with("application/grpc"));
    let small = req.body().is_end_stream()
        || req.headers().get(http::header::CONTENT_LENGTH).and_then(|v| v.to_str().ok()).and_then(|v| v.parse::<usize>().ok()).is_some_and(|n| n <= BUFFER_LIMIT);
    let counters = st.counters;

    if upgrade.is_none() && small && !grpc {
        let body = match Limited::new(req.into_body(), st.cfg.max_body_bytes).collect().await {
            Ok(b) => b.to_bytes(),
            Err(e) => {
                if e.downcast_ref::<http_body_util::LengthLimitError>().is_some() {
                    return error_response(StatusCode::PAYLOAD_TOO_LARGE, 554, "request body too large");
                }
                return error_response(StatusCode::BAD_REQUEST, 500, &format!("reading request body: {e}"));
            }
        };
        counters.bytes_in.fetch_add(body.len() as u64, Ordering::Relaxed);
        let rpc = RpcRequest { method, target, headers, body };
        return match client.call_streaming_timeout(rpc, timeout.or(client.request_timeout())).await {
            Ok((head, body)) => respond(head, body, is_head, counters).await,
            Err(e) => rpc_error_response(&e),
        };
    }

    // Duplex: stream the body while reading the response.
    let on_upgrade = upgrade.is_some().then(|| hyper::upgrade::on(&mut req));
    if let Some(p) = &upgrade {
        headers.push((H_UPGRADE.into(), p.clone()));
    }
    let rpc = RpcRequest { method, target, headers, body: Bytes::new() };
    let (writer, pending) = match client.call_duplex_limited(rpc, timeout.or(client.request_timeout()), st.cfg.max_body_bytes).await {
        Ok(r) => r,
        Err(e) => return rpc_error_response(&e),
    };
    let mut writer = Some(writer);
    if on_upgrade.is_none() {
        tokio::spawn(pump_request(req.into_body(), writer.take().unwrap(), counters));
    }
    let (head, body) = match pending.response().await {
        Ok(r) => r,
        Err(e) => return rpc_error_response(&e),
    };
    if let (Some(on_upgrade), Some(proto), Some(writer)) = (on_upgrade, upgrade, writer) {
        if head.status == 101 {
            return switch_protocols(head, body, writer, on_upgrade, proto, counters);
        }
        // Upgrade refused upstream: end the (empty) request and answer normally.
        tokio::spawn(writer.finish(vec![]));
    }
    respond(head, body, is_head, counters).await
}

/// Feed the HTTP request body (and trailers) into the BEEP request. Returning early drops
/// the writer, which aborts the request upstream.
async fn pump_request(mut body: Incoming, mut writer: RequestWriter, counters: &'static Counters) {
    let mut trailers = Vec::new();
    while let Some(frame) = body.frame().await {
        let Ok(frame) = frame else { return };
        match frame.into_data() {
            Ok(data) => {
                counters.bytes_in.fetch_add(data.len() as u64, Ordering::Relaxed);
                if writer.send(data).await.is_err() {
                    return;
                }
            }
            Err(frame) => {
                if let Ok(t) = frame.into_trailers() {
                    trailers = header_pairs(&t, &[]);
                }
            }
        }
    }
    let _ = writer.finish(trailers).await;
}

fn response_builder(head: &ResponseHead) -> Result<http::response::Builder, Response<GwBody>> {
    let Ok(status) = StatusCode::from_u16(head.status) else {
        return Err(error_response(StatusCode::BAD_GATEWAY, 554, &format!("upstream sent invalid status {}", head.status)));
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
    Ok(builder)
}

/// Turn an RPC response into an HTTP response: a single RPY of modest size is buffered
/// (so it gets a Content-Length); streams are forwarded as they arrive, trailers included.
async fn respond(head: ResponseHead, mut body: ResponseBody, is_head: bool, counters: &'static Counters) -> Response<GwBody> {
    let builder = match response_builder(&head) {
        Ok(b) => b,
        Err(r) => return r,
    };
    let status = StatusCode::from_u16(head.status).unwrap_or(StatusCode::OK);
    let no_body = is_head || status == StatusCode::NO_CONTENT || status == StatusCode::NOT_MODIFIED || status.is_informational();

    let mut buf = BytesMut::new();
    if !body.is_streamed() {
        loop {
            match body.chunk().await {
                None => {
                    counters.bytes_out.fetch_add(buf.len() as u64, Ordering::Relaxed);
                    let data = if no_body { Bytes::new() } else { buf.freeze() };
                    let b = match body.trailers().filter(|t| !t.is_empty()) {
                        Some(t) => with_trailers(data, header_map(t)),
                        None => full(data),
                    };
                    return builder.body(b).unwrap_or_else(|_| error_response(StatusCode::BAD_GATEWAY, 554, "bad response"));
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
    }
    let (tx, gw_body) = ChannelBody::channel(8);
    let first = buf.freeze();
    tokio::spawn(async move {
        if !first.is_empty() {
            counters.bytes_out.fetch_add(first.len() as u64, Ordering::Relaxed);
            if tx.send(Ok(Frame::data(first))).await.is_err() {
                return;
            }
        }
        while let Some(c) = body.chunk().await {
            let item = match c {
                Ok(b) => {
                    counters.bytes_out.fetch_add(b.len() as u64, Ordering::Relaxed);
                    Ok(Frame::data(b))
                }
                Err(e) => Err(Box::new(e) as BoxError),
            };
            let stop = item.is_err();
            if tx.send(item).await.is_err() || stop {
                return;
            }
        }
        if let Some(t) = body.trailers().filter(|t| !t.is_empty()) {
            let _ = tx.send(Ok(Frame::trailers(header_map(t)))).await;
        }
    });
    builder.body(gw_body).unwrap_or_else(|_| error_response(StatusCode::BAD_GATEWAY, 554, "bad response"))
}

/// Answer 101 and tunnel the upgraded connection over the BEEP request/response streams.
fn switch_protocols(
    head: ResponseHead,
    mut body: ResponseBody,
    mut writer: RequestWriter,
    on_upgrade: hyper::upgrade::OnUpgrade,
    proto: String,
    counters: &'static Counters,
) -> Response<GwBody> {
    let mut builder = match response_builder(&head) {
        Ok(b) => b,
        Err(r) => return r,
    };
    builder = builder.header(http::header::CONNECTION, "upgrade");
    if let Ok(v) = HeaderValue::from_str(&proto) {
        builder = builder.header(http::header::UPGRADE, v);
    }
    tokio::spawn(async move {
        let upgraded = match on_upgrade.await {
            Ok(u) => u,
            Err(e) => {
                tracing::debug!("client upgrade failed: {e}");
                return; // dropping the writer aborts the tunnel upstream
            }
        };
        let (mut rd, mut wr) = tokio::io::split(TokioIo::new(upgraded));
        let up = async move {
            let mut buf = vec![0u8; 16 * 1024];
            loop {
                match rd.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        counters.bytes_in.fetch_add(n as u64, Ordering::Relaxed);
                        if writer.send(Bytes::copy_from_slice(&buf[..n])).await.is_err() {
                            return;
                        }
                    }
                    Err(_) => return,
                }
            }
            let _ = writer.finish(vec![]).await;
        };
        let down = async move {
            while let Some(Ok(c)) = body.chunk().await {
                counters.bytes_out.fetch_add(c.len() as u64, Ordering::Relaxed);
                if wr.write_all(&c).await.is_err() || wr.flush().await.is_err() {
                    break;
                }
            }
            let _ = wr.shutdown().await;
        };
        tokio::join!(up, down);
    });
    builder.body(full(Bytes::new())).unwrap_or_else(|_| error_response(StatusCode::BAD_GATEWAY, 554, "bad response"))
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
