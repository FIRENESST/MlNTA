//! Ingress: BEEP RPC requests → an HTTP backend written in any language.
//!
//! Backends: `http://` (HTTP/1.1), `h2c://` (HTTP/2 over cleartext, e.g. gRPC servers) and
//! `https://` (TLS, HTTP/2 or HTTP/1.1 by ALPN). Streamed (rpc:2 chunked) request bodies are
//! forwarded as they arrive, trailers cross in both directions, and `Rpc-Upgrade` requests
//! become HTTP/1.1 upgrades whose connection is tunnelled over the BEEP request/response.

use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use http::{HeaderValue, Request, Uri};
use http_body::{Body as _, Frame};
use http_body_util::BodyExt;
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioIo};
use minta_beep::code;
use minta_beep::rpc::{BodyItem, RequestBody, RequestPart, RpcFailure, RpcHandler, RpcServerRequest, RpcServerResponse};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::metrics::{Counters, Metrics};
use crate::util::{full, header_map, is_hop_by_hop, BoxError, ChannelBody, GwBody};

/// Complete (non-streamed) request bodies are buffered up to this size.
const MAX_BUFFERED: usize = 64 << 20;

pub struct Ingress {
    scheme: http::uri::Scheme,
    authority: http::uri::Authority,
    base_path: String,
    /// `h2c://`: HTTP/2 with prior knowledge; upgrades are impossible.
    http2_only: bool,
    client: Client<HttpsConnector<HttpConnector>, GwBody>,
    rpy_max: usize,
    timeout: Duration,
    counters: &'static Counters,
}

impl Ingress {
    pub fn new(backend: &str, ca_file: Option<PathBuf>, rpy_max: usize, timeout: Duration, counters: &'static Counters) -> Result<Ingress, String> {
        let uri: Uri = backend.parse().map_err(|e| format!("backend {backend:?}: {e}"))?;
        let (scheme, http2_only) = match uri.scheme_str() {
            Some("http") => (http::uri::Scheme::HTTP, false),
            Some("h2c") => (http::uri::Scheme::HTTP, true),
            Some("https") => (http::uri::Scheme::HTTPS, false),
            Some(other) => return Err(format!("backend {backend:?}: unsupported scheme {other:?} (use http://, h2c:// or https://)")),
            None => return Err(format!("backend {backend:?} lacks a scheme")),
        };
        let authority = uri.authority().cloned().ok_or_else(|| format!("backend {backend:?} lacks a host"))?;
        let mut http = HttpConnector::new();
        http.enforce_http(false);
        http.set_nodelay(true);
        http.set_connect_timeout(Some(Duration::from_secs(5)));
        let tls = minta_beep::tls::client_config(&minta_beep::tls::TlsClientOptions { server_name: String::new(), ca_file })
            .map_err(|e| format!("backend TLS: {e}"))?;
        let connector = hyper_rustls::HttpsConnectorBuilder::new()
            .with_tls_config((*tls).clone())
            .https_or_http()
            .enable_http1()
            .enable_http2()
            .wrap_connector(http);
        let client = Client::builder(TokioExecutor::new()).http2_only(http2_only).build(connector);
        Ok(Ingress { scheme, authority, base_path: uri.path().trim_end_matches('/').to_string(), http2_only, client, rpy_max, timeout, counters })
    }

    fn uri_for(&self, target: &str) -> Result<Uri, RpcFailure> {
        let target = if target.starts_with('/') { target.to_string() } else { format!("/{target}") };
        Uri::builder()
            .scheme(self.scheme.clone())
            .authority(self.authority.clone())
            .path_and_query(format!("{}{}", self.base_path, target))
            .build()
            .map_err(|e| RpcFailure::new(code::PARAM_INVALID, format!("bad Rpc-Target: {e}")))
    }
}

#[async_trait]
impl RpcHandler for Ingress {
    fn streaming(&self) -> bool {
        true
    }

    async fn handle(&self, req: RpcServerRequest) -> Result<RpcServerResponse, RpcFailure> {
        let _inflight = Metrics::start(self.counters);
        let r = self.forward(req).await;
        if r.is_err() {
            self.counters.errors.fetch_add(1, Ordering::Relaxed);
        }
        r
    }
}

fn failure(e: impl std::fmt::Display) -> RpcFailure {
    RpcFailure::new(code::TRANSACTION_FAILED, e.to_string())
}

/// Forward a streamed request body (data, then trailers) to the backend request.
fn streamed_body(mut stream: RequestBody, counters: &'static Counters) -> GwBody {
    let (tx, body) = ChannelBody::channel(8);
    tokio::spawn(async move {
        loop {
            let item = match stream.next().await {
                Ok(Some(RequestPart::Data(b))) => {
                    counters.bytes_in.fetch_add(b.len() as u64, Ordering::Relaxed);
                    Ok(Frame::data(b))
                }
                Ok(Some(RequestPart::Trailers(t))) => Ok(Frame::trailers(header_map(&t))),
                Ok(None) => return,
                // An aborted request must reach the backend as an aborted body, not a short one.
                Err(e) => Err(Box::new(e) as BoxError),
            };
            let stop = item.is_err();
            if tx.send(item).await.is_err() || stop {
                return;
            }
        }
    });
    body
}

impl Ingress {
    async fn forward(&self, mut req: RpcServerRequest) -> Result<RpcServerResponse, RpcFailure> {
        let method = http::Method::from_bytes(req.method.as_bytes()).map_err(|_| RpcFailure::new(code::PARAM_INVALID, format!("bad method {:?}", req.method)))?;
        let mut builder = Request::builder().method(method).uri(self.uri_for(&req.target)?);
        let mut host = None;
        let mut has_request_id = false;
        for (k, v) in &req.headers {
            let lk = k.to_ascii_lowercase();
            match lk.as_str() {
                "rpc-authority" => host = Some(v.clone()),
                // X-Beep-User is ours: only the session's SASL identity may set it.
                "rpc-id" | "rpc-timeout" | "rpc-upgrade" | "host" | "x-beep-user" => {}
                "te" if v.trim().eq_ignore_ascii_case("trailers") => builder = builder.header(http::header::TE, "trailers"),
                _ if is_hop_by_hop(&lk) => {}
                _ => {
                    has_request_id |= lk == "x-request-id";
                    if let (Ok(n), Ok(val)) = (http::HeaderName::from_bytes(k.as_bytes()), HeaderValue::from_str(v)) {
                        builder = builder.header(n, val);
                    }
                }
            }
        }
        if let (Some(id), false) = (&req.id, has_request_id) {
            if let Ok(v) = HeaderValue::from_str(id) {
                builder = builder.header("x-request-id", v);
            }
        }
        if let Some(user) = req.identity.as_deref().and_then(|u| HeaderValue::from_str(u).ok()) {
            builder = builder.header("x-beep-user", user);
        }
        if !self.http2_only {
            // HTTP/2 carries the authority in the URI (:authority) instead.
            let host = host.unwrap_or_else(|| self.authority.to_string());
            if let Ok(v) = HeaderValue::from_str(&host) {
                builder = builder.header(http::header::HOST, v);
            }
        }
        if let Some(proto) = &req.upgrade {
            if self.http2_only {
                return Err(RpcFailure::new(code::PARAM_NOT_IMPLEMENTED, "upgrades need an http:// or https:// (HTTP/1.1) backend"));
            }
            builder = builder.header(http::header::CONNECTION, "upgrade");
            builder = builder.header(http::header::UPGRADE, HeaderValue::from_str(proto).map_err(failure)?);
        }

        let mut stream = req.stream.take();
        let body = match (&req.upgrade, stream.take()) {
            (Some(_), s) => {
                stream = s; // carries the tunnelled bytes after the upgrade
                full(Bytes::new())
            }
            (None, Some(s)) if s.is_chunked() => streamed_body(s, self.counters),
            (None, Some(s)) => {
                let (bytes, _) = s.collect(MAX_BUFFERED).await.map_err(|e| RpcFailure::new(e.beep_code(), e.to_string()))?;
                self.counters.bytes_in.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                full(bytes)
            }
            (None, None) => {
                self.counters.bytes_in.fetch_add(req.body.len() as u64, Ordering::Relaxed);
                full(req.body)
            }
        };
        let request = builder.body(body).map_err(|e| RpcFailure::new(code::PARAM_INVALID, e.to_string()))?;

        let timeout = req.timeout.unwrap_or(self.timeout);
        let mut resp = match tokio::time::timeout(timeout, self.client.request(request)).await {
            Err(_) => return Err(RpcFailure::new(code::ABORTED, "backend timed out")),
            Ok(Err(e)) if e.is_connect() => return Err(RpcFailure::new(code::SERVICE_NOT_AVAILABLE, format!("backend unavailable: {}", chain(&e)))),
            Ok(Err(e)) => return Err(RpcFailure::new(code::TRANSACTION_FAILED, format!("backend error: {}", chain(&e)))),
            Ok(Ok(r)) => r,
        };
        let status = resp.status().as_u16();
        let headers: Vec<(String, String)> = resp
            .headers()
            .iter()
            .filter(|(k, _)| !is_hop_by_hop(k.as_str()))
            .map(|(k, v)| (crate::util::canonical(k.as_str()), String::from_utf8_lossy(v.as_bytes()).into_owned()))
            .collect();

        if let (Some(_), Some(stream), 101) = (&req.upgrade, stream, status) {
            let on_upgrade = hyper::upgrade::on(&mut resp);
            return Ok(self.tunnel(headers, on_upgrade, stream));
        }

        let content_length = resp.headers().get(http::header::CONTENT_LENGTH).and_then(|v| v.to_str().ok()).and_then(|v| v.parse::<usize>().ok());
        let mut body = resp.into_body();
        if body.is_end_stream() {
            // No body at all (e.g. a gRPC trailers-only response): one RPY keeps it a single HEADERS frame.
            return Ok(RpcServerResponse::full(status, headers, Bytes::new()));
        }
        match content_length {
            Some(n) if n <= self.rpy_max => {
                let collected = match tokio::time::timeout(timeout, body.collect()).await {
                    Ok(Ok(c)) => c,
                    Ok(Err(e)) => return Err(RpcFailure::new(code::TRANSACTION_FAILED, format!("backend body: {e}"))),
                    Err(_) => return Err(RpcFailure::new(code::ABORTED, "backend timed out")),
                };
                let trailers = collected.trailers().cloned();
                let bytes = collected.to_bytes();
                self.counters.bytes_out.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                match trailers {
                    None => Ok(RpcServerResponse::full(status, headers, bytes)),
                    Some(t) => {
                        let (resp, tx) = RpcServerResponse::frames(status, headers);
                        tokio::spawn(async move {
                            if tx.send(BodyItem::Data(bytes)).await.is_ok() {
                                let _ = tx.send(BodyItem::Trailers(pairs(&t))).await;
                            }
                        });
                        Ok(resp)
                    }
                }
            }
            _ => {
                let (resp, tx) = RpcServerResponse::frames(status, headers);
                let counters = self.counters;
                tokio::spawn(async move {
                    loop {
                        let item = match body.frame().await {
                            None => return,
                            Some(Ok(f)) => match f.into_data() {
                                Ok(data) => {
                                    counters.bytes_out.fetch_add(data.len() as u64, Ordering::Relaxed);
                                    BodyItem::Data(data)
                                }
                                Err(f) => match f.into_trailers() {
                                    Ok(t) => BodyItem::Trailers(pairs(&t)),
                                    Err(_) => continue,
                                },
                            },
                            Some(Err(e)) => BodyItem::Abort(format!("backend body: {e}")),
                        };
                        let last = !matches!(item, BodyItem::Data(_));
                        if tx.send(item).await.is_err() || last {
                            return;
                        }
                    }
                });
                Ok(resp)
            }
        }
    }

    /// 101 from the backend: pipe the upgraded connection through the BEEP request (client →
    /// backend) and the ANS stream (backend → client).
    fn tunnel(&self, headers: Vec<(String, String)>, on_upgrade: hyper::upgrade::OnUpgrade, mut stream: RequestBody) -> RpcServerResponse {
        let (resp, tx) = RpcServerResponse::frames(101, headers);
        let counters = self.counters;
        tokio::spawn(async move {
            let upgraded = match on_upgrade.await {
                Ok(u) => u,
                Err(e) => {
                    let _ = tx.send(BodyItem::Abort(format!("backend upgrade failed: {e}"))).await;
                    return;
                }
            };
            let (mut rd, mut wr) = tokio::io::split(TokioIo::new(upgraded));
            let up = async move {
                while let Ok(Some(part)) = stream.next().await {
                    if let RequestPart::Data(b) = part {
                        counters.bytes_in.fetch_add(b.len() as u64, Ordering::Relaxed);
                        if wr.write_all(&b).await.is_err() || wr.flush().await.is_err() {
                            break;
                        }
                    }
                }
                let _ = wr.shutdown().await;
            };
            let down = async move {
                let mut buf = vec![0u8; 16 * 1024];
                loop {
                    match rd.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            counters.bytes_out.fetch_add(n as u64, Ordering::Relaxed);
                            if tx.send(BodyItem::Data(Bytes::copy_from_slice(&buf[..n]))).await.is_err() {
                                break;
                            }
                        }
                    }
                }
                // Dropping `tx` ends the ANS stream (NUL).
            };
            tokio::join!(up, down);
        });
        resp
    }
}

/// "a: b: c" from an error and its sources (hyper's own message hides the cause).
fn chain(e: &dyn std::error::Error) -> String {
    let mut s = e.to_string();
    let mut cur = e.source();
    while let Some(c) = cur {
        s.push_str(": ");
        s.push_str(&c.to_string());
        cur = c.source();
    }
    s
}

fn pairs(h: &http::HeaderMap) -> Vec<(String, String)> {
    h.iter().map(|(k, v)| (crate::util::canonical(k.as_str()), String::from_utf8_lossy(v.as_bytes()).into_owned())).collect()
}
