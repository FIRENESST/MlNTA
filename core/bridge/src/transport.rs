use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue, Request, Response, Uri};
use http_body::{Body as _, Frame};
use http_body_util::BodyExt;
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::{connect::HttpConnector, Client as HttpClient};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, watch, OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;
use tokio_stream::{wrappers::ReceiverStream, Stream};
use tokio_util::sync::CancellationToken;
use tonic::transport::{
    Certificate, Channel, ClientTlsConfig, Endpoint, Identity, Server, ServerTlsConfig,
};
use tonic::{Code, Status, Streaming};

use crate::body::{Body, FrameTx};
use crate::config::{self, GrpcListener, Upstream};
use crate::wire::{
    self,
    bridge_client::BridgeClient,
    bridge_server::{Bridge, BridgeServer},
    packet::Payload,
    Empty, Packet,
};

pub const CHUNK: usize = 64 * 1024;
const MAX_PACKET: usize = 128 * 1024;
const MAX_HEADERS: usize = 64 * 1024;

fn packet(payload: Payload) -> Packet {
    Packet {
        payload: Some(payload),
    }
}

fn rpc_status(status: Status) -> Status {
    // Tonic's transport timeout currently reports CANCELLED with this message.
    // Expose a deadline consistently, whichever of the local/remote timers wins.
    if status.code() == Code::Cancelled && status.message() == "Timeout expired" {
        Status::deadline_exceeded("request deadline exceeded")
    } else {
        status
    }
}

pub async fn bounded<T>(
    cancel: &CancellationToken,
    deadline: Instant,
    future: impl Future<Output = T>,
) -> Result<T, Status> {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(Status::cancelled("call cancelled")),
        r = tokio::time::timeout_at(deadline, future) => r.map_err(|_| Status::deadline_exceeded("request deadline exceeded")),
    }
}

pub fn upgrade(headers: &HeaderMap) -> Option<String> {
    let requested = headers
        .get_all("connection")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .any(|s| s.trim().eq_ignore_ascii_case("upgrade"));
    requested
        .then(|| headers.get("upgrade")?.to_str().ok().map(str::to_owned))
        .flatten()
}

/// Strip hop-by-hop fields, including fields nominated by Connection.
pub fn clean_headers(headers: &HeaderMap, keep_length: bool) -> HeaderMap {
    let extra: Vec<_> = headers
        .get_all("connection")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(|s| s.trim().to_ascii_lowercase())
        .collect();
    let mut out = HeaderMap::new();
    for (k, v) in headers {
        let name = k.as_str();
        if matches!(
            name,
            "connection"
                | "keep-alive"
                | "proxy-connection"
                | "transfer-encoding"
                | "trailer"
                | "upgrade"
                | "proxy-authorization"
                | "proxy-authenticate"
        ) || (!keep_length && name == "content-length")
            || extra.iter().any(|s| s == name)
        {
            continue;
        }
        if name == "te" && !v.as_bytes().eq_ignore_ascii_case(b"trailers") {
            continue;
        }
        out.append(k, v.clone());
    }
    out
}

fn encode_headers(headers: &HeaderMap) -> Vec<wire::Header> {
    headers
        .iter()
        .map(|(k, v)| wire::Header {
            name: k.to_string(),
            value: Bytes::copy_from_slice(v.as_bytes()),
        })
        .collect()
}

fn decode_headers(headers: Vec<wire::Header>) -> Result<HeaderMap, Status> {
    if headers.len() > 256
        || headers
            .iter()
            .map(|h| h.name.len().saturating_add(h.value.len()))
            .sum::<usize>()
            > MAX_HEADERS
    {
        return Err(Status::resource_exhausted("headers too large"));
    }
    let mut out = HeaderMap::new();
    for h in headers {
        let name = HeaderName::from_bytes(h.name.as_bytes())
            .map_err(|_| Status::invalid_argument("invalid header name"))?;
        let value = HeaderValue::from_bytes(&h.value)
            .map_err(|_| Status::invalid_argument("invalid header value"))?;
        out.append(name, value);
    }
    Ok(out)
}

async fn send(
    tx: &mpsc::Sender<Packet>,
    payload: Payload,
    cancel: &CancellationToken,
    deadline: Instant,
) -> Result<(), Status> {
    bounded(cancel, deadline, tx.send(packet(payload)))
        .await?
        .map_err(|_| Status::cancelled("request receiver closed"))
}

async fn encode_body(
    mut body: Body,
    tx: mpsc::Sender<Packet>,
    cancel: CancellationToken,
    deadline: Instant,
    max: usize,
) -> Result<(), Status> {
    let mut total = 0usize;
    let mut trailers_seen = false;
    while let Some(frame) = bounded(&cancel, deadline, body.frame()).await? {
        let frame = frame.map_err(|e| Status::cancelled(format!("reading HTTP body: {e}")))?;
        match frame.into_data() {
            Ok(data) => {
                if trailers_seen {
                    return Err(Status::invalid_argument("data after trailers"));
                }
                total = total
                    .checked_add(data.len())
                    .filter(|n| *n <= max)
                    .ok_or_else(|| {
                        Status::resource_exhausted(format!("request body exceeds {max} bytes"))
                    })?;
                for chunk in data.chunks(CHUNK) {
                    send(
                        &tx,
                        Payload::Data(Bytes::copy_from_slice(chunk)),
                        &cancel,
                        deadline,
                    )
                    .await?;
                }
            }
            Err(frame) => {
                if let Ok(headers) = frame.into_trailers() {
                    if trailers_seen {
                        return Err(Status::invalid_argument("duplicate trailers"));
                    }
                    trailers_seen = true;
                    send(
                        &tx,
                        Payload::Trailers(wire::Headers {
                            fields: encode_headers(&clean_headers(&headers, false)),
                        }),
                        &cancel,
                        deadline,
                    )
                    .await?;
                }
            }
        }
    }
    send(&tx, Payload::End(Empty {}), &cancel, deadline).await
}

async fn decode_body(
    mut stream: Streaming<Packet>,
    tx: FrameTx,
    cancel: CancellationToken,
    deadline: Instant,
    max: usize,
) -> Result<(), Status> {
    let mut total = 0usize;
    let mut trailers_seen = false;
    loop {
        let msg = bounded(&cancel, deadline, stream.message())
            .await?
            .map_err(rpc_status)?
            .ok_or_else(|| Status::cancelled("body aborted before End"))?;
        let frame = match msg.payload {
            Some(Payload::Data(data)) if !trailers_seen => {
                if data.len() > CHUNK {
                    return Err(Status::resource_exhausted("data packet exceeds 64 KiB"));
                }
                total = total
                    .checked_add(data.len())
                    .filter(|n| *n <= max)
                    .ok_or_else(|| {
                        Status::resource_exhausted(format!("body exceeds {max} bytes"))
                    })?;
                Frame::data(data)
            }
            Some(Payload::Trailers(h)) if !trailers_seen => {
                trailers_seen = true;
                Frame::trailers(clean_headers(&decode_headers(h.fields)?, false))
            }
            Some(Payload::End(_)) => {
                if bounded(&cancel, deadline, stream.message())
                    .await??
                    .is_some()
                {
                    return Err(Status::invalid_argument("packets after End"));
                }
                return Ok(());
            }
            _ => return Err(Status::invalid_argument("invalid body packet order")),
        };
        bounded(&cancel, deadline, tx.send(Ok(frame)))
            .await?
            .map_err(|_| Status::cancelled("HTTP body receiver closed"))?;
    }
}

fn spawn_decode(
    stream: Streaming<Packet>,
    tx: FrameTx,
    cancel: CancellationToken,
    deadline: Instant,
    max: usize,
) -> watch::Receiver<Option<Status>> {
    let (failure, rx) = watch::channel(None);
    tokio::spawn(async move {
        if let Err(e) = decode_body(stream, tx.clone(), cancel.clone(), deadline, max).await {
            let _ = failure.send(Some(e.clone()));
            // Closing/cancelling a peer must wake a blocked downstream HTTP reader.
            let _ = bounded(&cancel, deadline, tx.send(Err(Box::new(e)))).await;
        }
    });
    rx
}

#[derive(Clone)]
pub struct Client {
    client: BridgeClient<Channel>,
    token: Option<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>,
}

impl Client {
    pub fn new(cfg: &Upstream) -> Result<Self, String> {
        let mut endpoint = Endpoint::from_shared(cfg.address.clone())
            .map_err(|e| e.to_string())?
            .connect_timeout(Duration::from_secs(5))
            .tcp_nodelay(true);
        if cfg.address.starts_with("https://") {
            let mut tls = ClientTlsConfig::new().with_webpki_roots();
            if let Some(name) = &cfg.server_name {
                tls = tls.domain_name(name);
            }
            if let Some(path) = &cfg.ca_file {
                tls = tls.ca_certificate(Certificate::from_pem(
                    std::fs::read(path).map_err(|e| e.to_string())?,
                ));
            }
            endpoint = endpoint.tls_config(tls).map_err(|e| e.to_string())?;
        }
        let token = config::token(&cfg.token_env)?
            .map(|s| {
                format!("Bearer {s}")
                    .parse()
                    .map_err(|_| "token contains invalid metadata bytes".to_string())
            })
            .transpose()?;
        Ok(Self {
            client: BridgeClient::new(endpoint.connect_lazy())
                .max_decoding_message_size(MAX_PACKET)
                .max_encoding_message_size(MAX_PACKET),
            token,
        })
    }

    pub async fn check(&self) -> Result<(), Status> {
        let mut req = tonic::Request::new(Empty {});
        if let Some(token) = &self.token {
            req.metadata_mut().insert("authorization", token.clone());
        }
        tokio::time::timeout(Duration::from_secs(3), self.client.clone().check(req))
            .await
            .map_err(|_| Status::deadline_exceeded("health check timed out"))??;
        Ok(())
    }

    pub async fn exchange(
        &self,
        req: Request<Body>,
        timeout: Duration,
        max: usize,
        permit: OwnedSemaphorePermit,
    ) -> Result<Response<Body>, Status> {
        let deadline = Instant::now() + timeout;
        let cancel = CancellationToken::new();
        // This body also cancels on an error or a dropped handler before response headers arrive.
        let (response_tx, response_body) = Body::channel(cancel.clone(), Some(permit));
        let (parts, request_body) = req.into_parts();
        let authority = parts
            .headers
            .get("host")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
            .or_else(|| parts.uri.authority().map(ToString::to_string))
            .unwrap_or_default();
        let head = wire::RequestHead {
            method: parts.method.to_string(),
            target: parts
                .uri
                .path_and_query()
                .map(|p| p.as_str())
                .unwrap_or("/")
                .to_string(),
            authority,
            headers: encode_headers(&clean_headers(&parts.headers, false)),
            upgrade: upgrade(&parts.headers).unwrap_or_default(),
            timeout_ms: timeout.as_millis().min(u64::MAX as u128) as u64,
        };
        let (tx, rx) = mpsc::channel(4);
        send(&tx, Payload::Request(head), &cancel, deadline).await?;
        let (failure_tx, mut failure) = watch::channel(None::<Status>);
        let upload_cancel = cancel.clone();
        tokio::spawn(async move {
            if let Err(e) =
                encode_body(request_body, tx, upload_cancel.clone(), deadline, max).await
            {
                let _ = failure_tx.send(Some(e));
                upload_cancel.cancel();
            }
        });
        let mut rpc = tonic::Request::new(ReceiverStream::new(rx));
        rpc.set_timeout(timeout);
        if let Some(token) = &self.token {
            rpc.metadata_mut().insert("authorization", token.clone());
        }
        let receive_head = async {
            let mut client = self.client.clone();
            let mut stream = client.exchange(rpc).await.map_err(rpc_status)?.into_inner();
            let head = match stream
                .message()
                .await
                .map_err(rpc_status)?
                .and_then(|p| p.payload)
            {
                Some(Payload::Response(h)) => h,
                _ => return Err(Status::internal("missing response head")),
            };
            Ok::<_, Status>((head, stream))
        };
        tokio::pin!(receive_head);
        let result = tokio::select! {
            biased;
            r = failure.changed() => {
                if r.is_ok() { return Err(failure.borrow().clone().unwrap_or_else(|| Status::cancelled("upload failed"))); }
                bounded(&cancel, deadline, &mut receive_head).await?
            }
            r = bounded(&cancel, deadline, &mut receive_head) => r?,
        };
        let (head, stream) = match result {
            Ok(r) => r,
            Err(e) => return Err(failure.borrow().clone().unwrap_or(e)),
        };
        let status = http::StatusCode::from_u16(
            head.status
                .try_into()
                .map_err(|_| Status::internal("invalid response status"))?,
        )
        .map_err(|_| Status::internal("invalid response status"))?;
        let mut response = Response::builder().status(status);
        *response.headers_mut().unwrap() = decode_headers(head.headers)?;
        if !head.upgrade.is_empty() && status == http::StatusCode::SWITCHING_PROTOCOLS {
            response = response
                .header("connection", "upgrade")
                .header("upgrade", head.upgrade);
        }
        if head.no_body {
            let mut stream = stream;
            if !matches!(
                bounded(&cancel, deadline, stream.message())
                    .await??
                    .and_then(|p| p.payload),
                Some(Payload::End(_))
            ) || bounded(&cancel, deadline, stream.message())
                .await??
                .is_some()
            {
                return Err(Status::internal("invalid empty response"));
            }
            drop(response_body);
            return response
                .body(Body::full(Bytes::new()))
                .map_err(|e| Status::internal(e.to_string()));
        }
        let receive_cancel = cancel.clone();
        tokio::spawn(async move {
            if let Err(e) = decode_body(
                stream,
                response_tx.clone(),
                receive_cancel.clone(),
                deadline,
                usize::MAX,
            )
            .await
            {
                let e = failure.borrow().clone().unwrap_or(e);
                let _ = response_tx.send(Err(Box::new(e))).await;
            }
        });
        response
            .body(response_body)
            .map_err(|e| Status::internal(e.to_string()))
    }
}

type Connector = HttpsConnector<HttpConnector>;

struct Backend {
    uri: Uri,
    client: HttpClient<Connector, Body>,
    http1: HttpClient<Connector, Body>,
    h2c: bool,
}

impl Backend {
    fn new(cfg: &GrpcListener) -> Result<Self, String> {
        let h2c = cfg.backend.starts_with("h2c://");
        let text = if h2c {
            cfg.backend.replacen("h2c://", "http://", 1)
        } else {
            cfg.backend.clone()
        };
        let uri: Uri = text.parse().map_err(|e| format!("backend: {e}"))?;
        if !matches!(uri.scheme_str(), Some("http" | "https"))
            || uri.authority().is_none()
            || uri.query().is_some()
        {
            return Err("backend must be http://, https:// or h2c:// without a query".into());
        }
        let mut roots = tokio_rustls::rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        if let Some(path) = &cfg.backend_ca_file {
            for cert in rustls_pemfile::certs(&mut std::io::BufReader::new(
                std::fs::File::open(path).map_err(|e| e.to_string())?,
            )) {
                roots
                    .add(cert.map_err(|e| e.to_string())?)
                    .map_err(|e| e.to_string())?;
            }
        }
        let tls = tokio_rustls::rustls::ClientConfig::builder_with_provider(Arc::new(
            tokio_rustls::rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|e| e.to_string())?
        .with_root_certificates(roots)
        .with_no_client_auth();
        let connector = |h2: bool| {
            let mut http = HttpConnector::new();
            http.enforce_http(false);
            http.set_connect_timeout(Some(Duration::from_secs(5)));
            http.set_nodelay(true);
            let builder = hyper_rustls::HttpsConnectorBuilder::new()
                .with_tls_config(tls.clone())
                .https_or_http()
                .enable_http1();
            if h2 {
                builder.enable_http2().wrap_connector(http)
            } else {
                builder.wrap_connector(http)
            }
        };
        Ok(Self {
            uri,
            client: HttpClient::builder(TokioExecutor::new())
                .http2_only(h2c)
                .build(connector(true)),
            http1: HttpClient::builder(TokioExecutor::new()).build(connector(false)),
            h2c,
        })
    }

    fn request(&self, head: &wire::RequestHead, body: Body) -> Result<Request<Body>, Status> {
        if !head.target.starts_with('/') || head.target.starts_with("//") {
            return Err(Status::invalid_argument(
                "target must be an origin-form path starting with /",
            ));
        }
        let uri = Uri::builder()
            .scheme(self.uri.scheme().unwrap().clone())
            .authority(self.uri.authority().unwrap().clone())
            .path_and_query(format!(
                "{}{}",
                self.uri.path().trim_end_matches('/'),
                head.target
            ))
            .build()
            .map_err(|_| Status::invalid_argument("invalid target"))?;
        let method = http::Method::from_bytes(head.method.as_bytes())
            .map_err(|_| Status::invalid_argument("invalid method"))?;
        let mut req = Request::builder()
            .method(method)
            .uri(uri)
            .body(body)
            .map_err(|_| Status::invalid_argument("invalid request"))?;
        *req.headers_mut() = clean_headers(&decode_headers(head.headers.clone())?, false);
        req.headers_mut().remove("host");
        if !self.h2c && !head.authority.is_empty() {
            req.headers_mut().insert(
                "host",
                HeaderValue::from_str(&head.authority)
                    .map_err(|_| Status::invalid_argument("invalid authority"))?,
            );
        }
        if !head.upgrade.is_empty() {
            if self.h2c {
                return Err(Status::unimplemented(
                    "upgrades require an HTTP/1.1 backend",
                ));
            }
            req.headers_mut()
                .insert("connection", HeaderValue::from_static("upgrade"));
            req.headers_mut().insert(
                "upgrade",
                HeaderValue::from_str(&head.upgrade)
                    .map_err(|_| Status::invalid_argument("invalid upgrade protocol"))?,
            );
        }
        Ok(req)
    }
}

struct PacketStream {
    inner: ReceiverStream<Result<Packet, Status>>,
    cancel: CancellationToken,
    _permit: OwnedSemaphorePermit,
}
impl Stream for PacketStream {
    type Item = Result<Packet, Status>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}
impl Drop for PacketStream {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

struct Service {
    backend: Backend,
    cfg: GrpcListener,
    token: Option<String>,
    permits: Arc<Semaphore>,
}

impl Service {
    fn authorize<T>(&self, request: &tonic::Request<T>) -> Result<(), Status> {
        if let Some(token) = &self.token {
            let expected = format!("Bearer {token}");
            let got = request
                .metadata()
                .get("authorization")
                .map(|v| v.as_encoded_bytes())
                .unwrap_or_default();
            let ok = got.len() == expected.len()
                && got
                    .iter()
                    .zip(expected.as_bytes())
                    .fold(0u8, |a, (x, y)| a | (x ^ y))
                    == 0;
            if !ok {
                return Err(Status::unauthenticated("invalid gateway token"));
            }
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl Bridge for Service {
    type ExchangeStream = Pin<Box<dyn Stream<Item = Result<Packet, Status>> + Send>>;
    async fn check(&self, req: tonic::Request<Empty>) -> Result<tonic::Response<Empty>, Status> {
        self.authorize(&req)?;
        Ok(tonic::Response::new(Empty {}))
    }

    async fn exchange(
        &self,
        req: tonic::Request<Streaming<Packet>>,
    ) -> Result<tonic::Response<Self::ExchangeStream>, Status> {
        self.authorize(&req)?;
        let cancel = CancellationToken::new();
        let mut deadline = Instant::now() + Duration::from_millis(self.cfg.timeout_ms);
        let permit = bounded(&cancel, deadline, self.permits.clone().acquire_owned())
            .await?
            .map_err(|_| Status::unavailable("gateway stopped"))?;
        let (out, rx) = mpsc::channel(4);
        let response = PacketStream {
            inner: ReceiverStream::new(rx),
            cancel: cancel.clone(),
            _permit: permit,
        };
        let mut stream = req.into_inner();
        let head = match bounded(&cancel, deadline, stream.message())
            .await??
            .and_then(|p| p.payload)
        {
            Some(Payload::Request(h)) => h,
            _ => {
                return Err(Status::invalid_argument(
                    "first packet must be a request head",
                ))
            }
        };
        if head.timeout_ms != 0 {
            deadline = deadline.min(Instant::now() + Duration::from_millis(head.timeout_ms));
        }
        let upgrading = !head.upgrade.is_empty();
        let (upload_tx, upload_body) = Body::channel(cancel.clone(), None);
        // For upgrades, the gRPC request body carries tunnel bytes after HTTP 101.
        let mut incoming = Some(stream);
        let body = if upgrading {
            drop(upload_body.without_cancellation());
            Body::full(Bytes::new())
        } else {
            upload_body
        };
        // The request body must not own the call's cancellation token: Hyper may drop it
        // immediately after consuming it, while the response still needs to stream.
        let body = if upgrading {
            body
        } else {
            body.without_cancellation()
        };
        let mut failure = if upgrading {
            None
        } else {
            Some(spawn_decode(
                incoming.take().unwrap(),
                upload_tx,
                cancel.clone(),
                deadline,
                self.cfg.max_body_bytes,
            ))
        };
        let request = self.backend.request(&head, body)?;
        let client = if upgrading {
            &self.backend.http1
        } else {
            &self.backend.client
        };
        let mut response_head = if let Some(errors) = &mut failure {
            let request = client.request(request);
            tokio::pin!(request);
            tokio::select! {
                biased;
                changed = errors.changed() => {
                    if changed.is_ok() { return Err(errors.borrow().clone().unwrap_or_else(|| Status::cancelled("upload failed"))); }
                    bounded(&cancel, deadline, &mut request).await?.map_err(|e| Status::unavailable(format!("backend: {e}")))?
                }
                r = bounded(&cancel, deadline, &mut request) => r?.map_err(|e| Status::unavailable(format!("backend: {e}")))?,
            }
        } else {
            bounded(&cancel, deadline, client.request(request))
                .await?
                .map_err(|e| Status::unavailable(format!("backend: {e}")))?
        };
        let tunnel = upgrading && response_head.status() == http::StatusCode::SWITCHING_PROTOCOLS;
        let on_upgrade = tunnel.then(|| hyper::upgrade::on(&mut response_head));
        let head = wire::ResponseHead {
            status: response_head.status().as_u16() as u32,
            headers: encode_headers(&clean_headers(response_head.headers(), true)),
            upgrade: if tunnel { head.upgrade } else { String::new() },
            no_body: !tunnel && response_head.body().is_end_stream(),
        };
        out.send(Ok(packet(Payload::Response(head))))
            .await
            .map_err(|_| Status::cancelled("response cancelled"))?;
        tokio::spawn(async move {
            let r = match on_upgrade {
                Some(upgrade) => {
                    tunnel_backend(
                        upgrade,
                        incoming.unwrap(),
                        out.clone(),
                        cancel.clone(),
                        deadline,
                    )
                    .await
                }
                None => {
                    let (tx, mut rx) = mpsc::channel(4);
                    let encode = encode_body(
                        Body::new(response_head.into_body()),
                        tx,
                        cancel.clone(),
                        deadline,
                        usize::MAX,
                    );
                    let forward = async {
                        while let Some(p) = bounded(&cancel, deadline, rx.recv()).await? {
                            bounded(&cancel, deadline, out.send(Ok(p)))
                                .await?
                                .map_err(|_| Status::cancelled("response cancelled"))?;
                        }
                        Ok::<_, Status>(())
                    };
                    tokio::try_join!(encode, forward).map(|_| ())
                }
            };
            if let Err(e) = r {
                let _ = bounded(&cancel, deadline, out.send(Err(e))).await;
            }
        });
        Ok(tonic::Response::new(Box::pin(response)))
    }
}

async fn tunnel_backend(
    upgrade: hyper::upgrade::OnUpgrade,
    stream: Streaming<Packet>,
    out: mpsc::Sender<Result<Packet, Status>>,
    cancel: CancellationToken,
    deadline: Instant,
) -> Result<(), Status> {
    let upgraded = bounded(&cancel, deadline, upgrade)
        .await?
        .map_err(|e| Status::unavailable(format!("backend upgrade: {e}")))?;
    let (mut rd, mut wr) = tokio::io::split(TokioIo::new(upgraded));
    let (tx, body) = Body::channel(cancel.clone(), None);
    let mut body = body.without_cancellation();
    let decode = decode_body(stream, tx, cancel.clone(), deadline, usize::MAX);
    let up = async {
        while let Some(frame) = bounded(&cancel, deadline, body.frame()).await? {
            let frame = frame.map_err(|e| Status::cancelled(e.to_string()))?;
            if let Ok(data) = frame.into_data() {
                bounded(&cancel, deadline, wr.write_all(&data))
                    .await?
                    .map_err(|e| Status::cancelled(e.to_string()))?;
            }
        }
        bounded(&cancel, deadline, wr.shutdown())
            .await?
            .map_err(|e| Status::cancelled(e.to_string()))
    };
    let down = async {
        let mut buffer = vec![0; CHUNK];
        loop {
            let n = bounded(&cancel, deadline, rd.read(&mut buffer))
                .await?
                .map_err(|e| Status::cancelled(e.to_string()))?;
            let payload = if n == 0 {
                Payload::End(Empty {})
            } else {
                Payload::Data(Bytes::copy_from_slice(&buffer[..n]))
            };
            bounded(&cancel, deadline, out.send(Ok(packet(payload))))
                .await?
                .map_err(|_| Status::cancelled("tunnel receiver closed"))?;
            if n == 0 {
                return Ok::<_, Status>(());
            }
        }
    };
    tokio::try_join!(decode, up, down).map(|_| ())
}

pub async fn serve(cfg: GrpcListener) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let backend = Backend::new(&cfg)?;
    let token = config::token(&cfg.token_env)?;
    let permits = Arc::new(Semaphore::new(cfg.max_in_flight));
    let mut server =
        Server::builder().max_concurrent_streams(cfg.max_in_flight.min(u32::MAX as usize) as u32);
    if let (Some(cert), Some(key)) = (&cfg.cert_file, &cfg.key_file) {
        server = server.tls_config(ServerTlsConfig::new().identity(Identity::from_pem(
            std::fs::read(cert)?,
            std::fs::read(key)?,
        )))?;
    }
    let address = cfg.listen.parse()?;
    tracing::info!("gRPC bridge listening on {} -> {}", cfg.listen, cfg.backend);
    let service = Service {
        backend,
        cfg,
        token,
        permits,
    };
    server
        .add_service(
            BridgeServer::new(service)
                .max_decoding_message_size(MAX_PACKET)
                .max_encoding_message_size(MAX_PACKET),
        )
        .serve_with_shutdown(address, async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

pub fn http_error(status: &Status) -> http::StatusCode {
    use http::StatusCode as H;
    match status.code() {
        Code::InvalidArgument | Code::OutOfRange => H::BAD_REQUEST,
        Code::Unauthenticated => H::UNAUTHORIZED,
        Code::PermissionDenied => H::FORBIDDEN,
        Code::ResourceExhausted => H::PAYLOAD_TOO_LARGE,
        Code::DeadlineExceeded => H::GATEWAY_TIMEOUT,
        Code::Unavailable => H::SERVICE_UNAVAILABLE,
        Code::Unimplemented => H::NOT_IMPLEMENTED,
        _ => H::BAD_GATEWAY,
    }
}
