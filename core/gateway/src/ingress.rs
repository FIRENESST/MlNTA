//! Ingress: BEEP RPC requests → an HTTP backend written in any language.

use std::sync::atomic::Ordering;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use http::{HeaderValue, Request, Uri};
use http_body_util::{BodyExt, Full};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use minta_beep::code;
use minta_beep::rpc::{RpcFailure, RpcHandler, RpcServerRequest, RpcServerResponse};

use crate::metrics::{Counters, Metrics};
use crate::util::{is_hop_by_hop, BoxError};

pub struct Ingress {
    scheme: http::uri::Scheme,
    authority: http::uri::Authority,
    base_path: String,
    client: Client<HttpConnector, Full<Bytes>>,
    rpy_max: usize,
    timeout: Duration,
    counters: &'static Counters,
}

impl Ingress {
    pub fn new(backend: &str, rpy_max: usize, timeout: Duration, counters: &'static Counters) -> Result<Ingress, String> {
        let uri: Uri = backend.parse().map_err(|e| format!("backend {backend:?}: {e}"))?;
        let scheme = uri.scheme().cloned().ok_or_else(|| format!("backend {backend:?} lacks a scheme"))?;
        if scheme != http::uri::Scheme::HTTP {
            return Err(format!("backend {backend:?}: only http:// backends are supported (terminate TLS in front of the backend)"));
        }
        let authority = uri.authority().cloned().ok_or_else(|| format!("backend {backend:?} lacks a host"))?;
        let mut connector = HttpConnector::new();
        connector.set_nodelay(true);
        connector.set_connect_timeout(Some(Duration::from_secs(5)));
        let client = Client::builder(TokioExecutor::new()).build(connector);
        Ok(Ingress { scheme, authority, base_path: uri.path().trim_end_matches('/').to_string(), client, rpy_max, timeout, counters })
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
    async fn handle(&self, req: RpcServerRequest) -> Result<RpcServerResponse, RpcFailure> {
        let _inflight = Metrics::start(self.counters);
        let r = self.forward(req).await;
        if r.is_err() {
            self.counters.errors.fetch_add(1, Ordering::Relaxed);
        }
        r
    }
}

impl Ingress {
    async fn forward(&self, req: RpcServerRequest) -> Result<RpcServerResponse, RpcFailure> {
        let method = http::Method::from_bytes(req.method.as_bytes()).map_err(|_| RpcFailure::new(code::PARAM_INVALID, format!("bad method {:?}", req.method)))?;
        let mut builder = Request::builder().method(method).uri(self.uri_for(&req.target)?);
        let mut host = None;
        let mut has_request_id = false;
        for (k, v) in &req.headers {
            let lk = k.to_ascii_lowercase();
            match lk.as_str() {
                "rpc-authority" => host = Some(v.clone()),
                "rpc-id" | "rpc-timeout" | "host" => {}
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
        let host = host.unwrap_or_else(|| self.authority.to_string());
        if let Ok(v) = HeaderValue::from_str(&host) {
            builder = builder.header(http::header::HOST, v);
        }
        self.counters.bytes_in.fetch_add(req.body.len() as u64, Ordering::Relaxed);
        let request = builder.body(Full::new(req.body)).map_err(|e| RpcFailure::new(code::PARAM_INVALID, e.to_string()))?;

        let timeout = req.timeout.unwrap_or(self.timeout);
        let resp = match tokio::time::timeout(timeout, self.client.request(request)).await {
            Err(_) => return Err(RpcFailure::new(code::ABORTED, "backend timed out")),
            Ok(Err(e)) if e.is_connect() => return Err(RpcFailure::new(code::SERVICE_NOT_AVAILABLE, format!("backend unavailable: {e}"))),
            Ok(Err(e)) => return Err(RpcFailure::new(code::TRANSACTION_FAILED, format!("backend error: {e}"))),
            Ok(Ok(r)) => r,
        };
        let status = resp.status().as_u16();
        let content_length = resp.headers().get(http::header::CONTENT_LENGTH).and_then(|v| v.to_str().ok()).and_then(|v| v.parse::<usize>().ok());
        let headers: Vec<(String, String)> = resp
            .headers()
            .iter()
            .filter(|(k, _)| !is_hop_by_hop(k.as_str()))
            .map(|(k, v)| (crate::util::canonical(k.as_str()), String::from_utf8_lossy(v.as_bytes()).into_owned()))
            .collect();
        let mut body = resp.into_body();

        match content_length {
            Some(n) if n <= self.rpy_max => {
                let bytes = match tokio::time::timeout(timeout, body.collect()).await {
                    Ok(Ok(b)) => b.to_bytes(),
                    Ok(Err(e)) => return Err(RpcFailure::new(code::TRANSACTION_FAILED, format!("backend body: {e}"))),
                    Err(_) => return Err(RpcFailure::new(code::ABORTED, "backend timed out")),
                };
                self.counters.bytes_out.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                Ok(RpcServerResponse::full(status, headers, bytes))
            }
            _ => {
                let (resp, tx) = RpcServerResponse::stream(status, headers);
                let counters = self.counters;
                tokio::spawn(async move {
                    loop {
                        match body.frame().await {
                            None => return,
                            Some(Ok(f)) => {
                                if let Ok(data) = f.into_data() {
                                    counters.bytes_out.fetch_add(data.len() as u64, Ordering::Relaxed);
                                    if tx.send(Ok(data)).await.is_err() {
                                        return;
                                    }
                                }
                            }
                            Some(Err(e)) => {
                                let e: BoxError = Box::new(e);
                                let _ = tx.send(Err(format!("backend body: {e}"))).await;
                                return;
                            }
                        }
                    }
                });
                Ok(resp)
            }
        }
    }
}
