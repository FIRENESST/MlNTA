use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use base64::{engine::general_purpose::STANDARD, Engine};
use bytes::Bytes;
use http::{HeaderMap, Request, Response, StatusCode};
use http_body::Frame;
use http_body_util::{BodyExt, Limited};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tonic::Status;

use crate::body::{Body, BoxError, Lingering};
use crate::config::HttpListener;
use crate::transport::{self, bounded, Client, CHUNK};

pub struct State {
    pub cfg: HttpListener,
    pub clients: HashMap<String, Client>,
    permits: Arc<Semaphore>,
}

impl State {
    pub fn new(cfg: HttpListener, clients: HashMap<String, Client>) -> Self {
        let permits = Arc::new(Semaphore::new(cfg.max_in_flight));
        Self {
            cfg,
            clients,
            permits,
        }
    }
}

fn json_response(status: StatusCode, value: Value) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Body::full(value.to_string()))
        .unwrap()
}

fn error(e: Status) -> Response<Body> {
    let code = match e.code() {
        tonic::Code::InvalidArgument | tonic::Code::OutOfRange => "invalid_argument",
        tonic::Code::ResourceExhausted => "body_too_large",
        tonic::Code::DeadlineExceeded => "deadline_exceeded",
        tonic::Code::Unauthenticated => "unauthenticated",
        tonic::Code::PermissionDenied => "permission_denied",
        tonic::Code::Unavailable => "unavailable",
        tonic::Code::Unimplemented => "unimplemented",
        tonic::Code::Cancelled => "cancelled",
        _ => "upstream_error",
    };
    json_response(
        transport::http_error(&e),
        json!({"error": {"code": code, "message": e.message()}}),
    )
}

fn duration(ms: Option<u64>, default: u64) -> Result<Duration, Status> {
    let ms = ms.unwrap_or(default);
    if ms == 0 {
        return Err(Status::invalid_argument("timeout_ms must be positive"));
    }
    Ok(Duration::from_millis(ms.min(default)))
}

/// Prefixes match whole path segments: `/api` covers `/api`, `/api/x` and `/api?q`,
/// not `/apiary`. A prefix ending in `/` already marks its own boundary.
fn prefix_matches(prefix: &str, target: &str) -> bool {
    target.strip_prefix(prefix).is_some_and(|rest| {
        prefix.ends_with('/') || rest.is_empty() || rest.starts_with(['/', '?'])
    })
}

fn route<'a>(
    st: &'a State,
    explicit: Option<&str>,
    target: &mut String,
) -> Result<&'a Client, Status> {
    let mut name = explicit.unwrap_or(&st.cfg.upstream);
    if explicit.is_none() {
        if let Some(r) = st
            .cfg
            .routes
            .iter()
            .filter(|r| prefix_matches(&r.prefix, target))
            .max_by_key(|r| r.prefix.len())
        {
            name = &r.upstream;
            if r.strip_prefix {
                *target = format!("/{}", target[r.prefix.len()..].trim_start_matches('/'));
            }
        }
    }
    st.clients
        .get(name)
        .ok_or_else(|| Status::invalid_argument(format!("unknown upstream {name:?}")))
}

async fn dispatch(
    st: &State,
    client: &Client,
    request: Request<Body>,
    timeout: Duration,
    max: usize,
) -> Result<Response<Body>, Status> {
    let deadline = Instant::now() + timeout;
    let permit = tokio::time::timeout_at(deadline, st.permits.clone().acquire_owned())
        .await
        .map_err(|_| Status::deadline_exceeded("gateway admission timed out"))?
        .map_err(|_| Status::unavailable("gateway stopped"))?;
    client
        .exchange(
            request,
            deadline.saturating_duration_since(Instant::now()),
            max,
            permit,
        )
        .await
}

#[derive(Deserialize)]
#[serde(untagged)]
enum JsonHeaders {
    Map(BTreeMap<String, String>),
    Pairs(Vec<(String, String)>),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct JsonCall {
    #[serde(default = "get_method")]
    method: String,
    target: String,
    #[serde(default)]
    headers: Option<JsonHeaders>,
    #[serde(default)]
    body: Option<String>,
    #[serde(default)]
    body_base64: Option<String>,
    #[serde(default, deserialize_with = "present_json")]
    json: Option<Value>,
    #[serde(default)]
    upstream: Option<String>,
    #[serde(default)]
    timeout_ms: Option<u64>,
}
fn get_method() -> String {
    "GET".into()
}
fn present_json<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<Value>, D::Error> {
    Value::deserialize(d).map(Some)
}

async fn json_call(req: Request<Incoming>, st: &State) -> Result<Response<Body>, Status> {
    if req.method() != http::Method::POST {
        return Ok(Response::builder()
            .status(405)
            .header("allow", "POST")
            .body(Body::full(Bytes::new()))
            .unwrap());
    }
    let media = req
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .split(';')
        .next()
        .unwrap_or("")
        .trim();
    if !media.eq_ignore_ascii_case("application/json") {
        return Ok(json_response(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            json!({"error": {"code": "unsupported_media_type", "message": "use Content-Type: application/json"}}),
        ));
    }
    // Encoded JSON/base64 overhead is bounded separately from decoded body bytes.
    let max_wire = st
        .cfg
        .max_body_bytes
        .saturating_add(2)
        .saturating_div(3)
        .saturating_mul(4)
        .saturating_add(64 * 1024);
    let bytes = tokio::time::timeout(
        Duration::from_millis(st.cfg.timeout_ms),
        Limited::new(req.into_body(), max_wire).collect(),
    )
    .await
    .map_err(|_| Status::deadline_exceeded("reading JSON request timed out"))?
    .map_err(|e| {
        if e.downcast_ref::<http_body_util::LengthLimitError>()
            .is_some()
        {
            Status::resource_exhausted("JSON request too large")
        } else {
            Status::invalid_argument(format!("reading JSON request: {e}"))
        }
    })?
    .to_bytes();
    let call: JsonCall = serde_json::from_slice(&bytes)
        .map_err(|e| Status::invalid_argument(format!("invalid JSON request: {e}")))?;
    if !call.target.starts_with('/') || call.target.starts_with("//") {
        return Err(Status::invalid_argument(
            "target must start with / and contain a path, not a URL",
        ));
    }
    if usize::from(call.body.is_some())
        + usize::from(call.body_base64.is_some())
        + usize::from(call.json.is_some())
        > 1
    {
        return Err(Status::invalid_argument(
            "supply only one of body, body_base64, json",
        ));
    }
    let mut headers = HeaderMap::new();
    let pairs: Vec<_> = match call.headers {
        None => vec![],
        Some(JsonHeaders::Map(m)) => m.into_iter().collect(),
        Some(JsonHeaders::Pairs(p)) => p,
    };
    for (k, v) in pairs {
        let key = http::HeaderName::from_bytes(k.as_bytes())
            .map_err(|_| Status::invalid_argument("invalid header name"))?;
        let val = http::HeaderValue::from_str(&v)
            .map_err(|_| Status::invalid_argument("invalid header value"))?;
        headers.append(key, val);
    }
    let body = match (call.body, call.body_base64, call.json) {
        (Some(s), _, _) => Bytes::from(s),
        (_, Some(s), _) => Bytes::from(
            STANDARD
                .decode(s)
                .map_err(|_| Status::invalid_argument("invalid body_base64"))?,
        ),
        (_, _, Some(value)) => {
            headers
                .entry("content-type")
                .or_insert(http::HeaderValue::from_static("application/json"));
            Bytes::from(value.to_string())
        }
        _ => Bytes::new(),
    };
    if body.len() > st.cfg.max_body_bytes {
        return Err(Status::resource_exhausted(format!(
            "body exceeds {} bytes",
            st.cfg.max_body_bytes
        )));
    }
    let timeout = duration(call.timeout_ms, st.cfg.timeout_ms)?;
    let mut target = call.target;
    let client = route(st, call.upstream.as_deref(), &mut target)?;
    let mut request = Request::builder()
        .method(call.method.as_str())
        .uri(target)
        .body(Body::full(body))
        .map_err(|_| Status::invalid_argument("invalid method or target"))?;
    *request.headers_mut() = transport::clean_headers(&headers, false);
    // The JSON endpoint is a buffered request/response API; streaming/upgrades use ordinary HTTP.
    let deadline = Instant::now() + timeout;
    let response = dispatch(st, client, request, timeout, st.cfg.max_body_bytes).await?;
    let (parts, body) = response.into_parts();
    let collected = tokio::time::timeout_at(
        deadline,
        Limited::new(body, st.cfg.max_response_bytes).collect(),
    )
    .await
    .map_err(|_| Status::deadline_exceeded("JSON response deadline exceeded"))?
    .map_err(|e| {
        if let Some(status) = e.downcast_ref::<Status>() {
            return status.clone();
        }
        Status::internal(format!(
            "JSON response incomplete or exceeds {} bytes: {e}",
            st.cfg.max_response_bytes
        ))
    })?;
    let trailers = collected.trailers().map(json_headers).unwrap_or_default();
    let body = collected.to_bytes();
    let mut result = json!({"status": parts.status.as_u16(), "headers": json_headers(&parts.headers), "trailers": trailers});
    match std::str::from_utf8(&body) {
        Ok(text) => result["body"] = json!(text),
        Err(_) => result["body_base64"] = json!(STANDARD.encode(body)),
    }
    Ok(json_response(StatusCode::OK, result))
}

fn json_headers(headers: &HeaderMap) -> Vec<(String, String)> {
    headers
        .iter()
        .map(|(k, v)| {
            (
                k.to_string(),
                String::from_utf8_lossy(v.as_bytes()).into_owned(),
            )
        })
        .collect()
}

async fn proxy(mut req: Request<Incoming>, st: &State) -> Result<Response<Body>, Status> {
    let mut target = req
        .uri()
        .path_and_query()
        .map(|p| p.as_str())
        .unwrap_or("/")
        .to_string();
    // Old gateway SDKs remain usable; neither header is required by ordinary HTTP clients.
    let explicit = req
        .headers()
        .get("x-minta-upstream")
        .or_else(|| req.headers().get("x-beep-upstream"))
        .and_then(|v| v.to_str().ok());
    let client = route(st, explicit, &mut target)?;
    let timeout_header = req
        .headers()
        .get("x-minta-timeout-ms")
        .or_else(|| req.headers().get("x-beep-timeout-ms"));
    let timeout_ms = timeout_header
        .map(|v| {
            v.to_str()
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .ok_or_else(|| Status::invalid_argument("invalid timeout header"))
        })
        .transpose()?;
    let timeout = duration(timeout_ms, st.cfg.timeout_ms)?;
    let upgrading = transport::upgrade(req.headers()).is_some();
    if !upgrading
        && req
            .headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<usize>().ok())
            .is_some_and(|n| n > st.cfg.max_body_bytes)
    {
        return Err(Status::resource_exhausted(format!(
            "body exceeds {} bytes",
            st.cfg.max_body_bytes
        )));
    }
    let on_upgrade = upgrading.then(|| hyper::upgrade::on(&mut req));
    let (mut parts, incoming) = req.into_parts();
    parts.uri = target
        .parse()
        .map_err(|_| Status::invalid_argument("invalid target"))?;
    for key in [
        "x-minta-upstream",
        "x-minta-timeout-ms",
        "x-beep-upstream",
        "x-beep-timeout-ms",
    ] {
        parts.headers.remove(key);
    }
    if let Some(on_upgrade) = on_upgrade {
        let cancel = CancellationToken::new();
        let (tx, body) = Body::channel(cancel, None);
        let response = dispatch(
            st,
            client,
            Request::from_parts(parts, body.without_cancellation()),
            timeout,
            usize::MAX,
        )
        .await?;
        if response.status() == StatusCode::SWITCHING_PROTOCOLS {
            let (parts, mut body) = response.into_parts();
            tokio::spawn(async move {
                let deadline = Instant::now() + timeout;
                let cancel = CancellationToken::new();
                let Ok(Ok(io)) = bounded(&cancel, deadline, on_upgrade).await else {
                    return;
                };
                let (mut rd, mut wr) = tokio::io::split(TokioIo::new(io));
                let up = async {
                    let mut buffer = vec![0; CHUNK];
                    loop {
                        let n = tokio::select! {
                            _ = tx.closed() => return Ok::<_, BoxError>(()),
                            n = tokio::time::timeout_at(deadline, rd.read(&mut buffer)) => n??,
                        };
                        if n == 0 {
                            return Ok(());
                        }
                        tokio::time::timeout_at(
                            deadline,
                            tx.send(Ok(Frame::data(Bytes::copy_from_slice(&buffer[..n])))),
                        )
                        .await?
                        .map_err(|_| "tunnel closed")?;
                    }
                };
                let down = async {
                    while let Some(frame) = tokio::time::timeout_at(deadline, body.frame()).await? {
                        if let Ok(data) = frame?.into_data() {
                            tokio::time::timeout_at(deadline, wr.write_all(&data)).await??;
                        }
                    }
                    wr.shutdown().await?;
                    Ok::<_, BoxError>(())
                };
                let _ = tokio::try_join!(up, down);
            });
            return Ok(Response::from_parts(parts, Body::full(Bytes::new())));
        }
        drop(tx);
        return Ok(response);
    }
    dispatch(
        st,
        client,
        // The reply may end before the upload does (early reject, limits, errors).
        Request::from_parts(
            parts,
            Body::new(Lingering::new(incoming, st.cfg.max_body_bytes)),
        ),
        timeout,
        st.cfg.max_body_bytes,
    )
    .await
}

async fn handle(
    req: Request<Incoming>,
    st: Arc<State>,
    _peer: SocketAddr,
) -> Result<Response<Body>, Infallible> {
    let result = match req.uri().path() {
        "/healthz" => Ok(json_response(StatusCode::OK, json!({"status": "ok"}))),
        "/readyz" | "/__beep/health" => {
            let mut checks = tokio::task::JoinSet::new();
            for (name, client) in &st.clients {
                let (name, client) = (name.clone(), client.clone());
                checks.spawn(async move { (name, client.check().await.is_ok()) });
            }
            let mut upstreams = serde_json::Map::new();
            let mut healthy = true;
            while let Some(r) = checks.join_next().await {
                match r {
                    Ok((name, ok)) => {
                        healthy &= ok;
                        upstreams.insert(name, json!({"connected": ok}));
                    }
                    Err(_) => healthy = false,
                }
            }
            Ok(json_response(
                if healthy {
                    StatusCode::OK
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                },
                json!({"status": if healthy { "ok" } else { "degraded" }, "upstreams": upstreams}),
            ))
        }
        "/openapi.json" => Ok(Response::builder()
            .header("content-type", "application/json")
            .body(Body::full(include_str!(
                "../../../docs/HTTP_API.openapi.json"
            )))
            .unwrap()),
        "/v1/call" => json_call(req, &st).await,
        _ => proxy(req, &st).await,
    };
    Ok(result.unwrap_or_else(error))
}

pub async fn serve(st: State) -> Result<(), BoxError> {
    let listener = TcpListener::bind(&st.cfg.listen).await?;
    tracing::info!("HTTP/JSON gateway listening on {}", listener.local_addr()?);
    let mut builder = auto::Builder::new(TokioExecutor::new());
    // An h2c client multiplexes calls on one connection; see transport::connection_window.
    builder
        .http2()
        .max_concurrent_streams(st.cfg.max_in_flight.min(u32::MAX as usize) as u32)
        .initial_stream_window_size(transport::STREAM_WINDOW)
        .initial_connection_window_size(transport::connection_window(st.cfg.max_in_flight));
    let builder = Arc::new(builder);
    let state = Arc::new(st);
    loop {
        let (tcp, peer) = tokio::select! {
            r = listener.accept() => r?,
            _ = tokio::signal::ctrl_c() => return Ok(()),
        };
        let _ = tcp.set_nodelay(true);
        let state = state.clone();
        let builder = builder.clone();
        tokio::spawn(async move {
            let service = service_fn(move |req| handle(req, state.clone(), peer));
            if let Err(e) = builder
                .serve_connection_with_upgrades(TokioIo::new(tcp), service)
                .await
            {
                tracing::debug!(%peer, "HTTP connection: {e}");
            }
        });
    }
}
