//! The MINTA RPC profiles: HTTP-shaped requests over BEEP. See docs/PROTOCOL.md §2.
//!
//! * `urn:minta:beep:rpc:1` — one request MSG, one reply (RPY, or ANS…NUL for streams).
//! * `urn:minta:beep:rpc:2` — rpc:1 plus streamed (chunked) request bodies, replies that may
//!   start before the request is complete (full duplex), response trailers and protocol
//!   upgrades (WebSocket tunnels). Clients offer rpc:2 first and fall back to rpc:1.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};

use crate::chunked;
use crate::error::{code, Error};
use crate::mgmt::{Mgmt, ProfileEl};
use crate::mime::{header, Entity, HeadParser, HeadProgress, Headers};
use crate::session::{Channel, Config, IncomingMessage, Listener, MsgSink, ProfileHandler, Profiles, ReplyEvent, ReplyStream, Responder, Session};

pub const RPC_PROFILE: &str = "urn:minta:beep:rpc:1";
pub const RPC_PROFILE_V2: &str = "urn:minta:beep:rpc:2";

pub const H_METHOD: &str = "Rpc-Method";
pub const H_TARGET: &str = "Rpc-Target";
pub const H_ID: &str = "Rpc-Id";
pub const H_TIMEOUT: &str = "Rpc-Timeout";
pub const H_AUTHORITY: &str = "Rpc-Authority";
pub const H_STATUS: &str = "Rpc-Status";
pub const H_REASON: &str = "Rpc-Reason";
pub const H_ABORT: &str = "Rpc-Abort";
/// rpc:2: `chunked` marks a request body in HTTP/1.1 chunked coding.
pub const H_BODY: &str = "Rpc-Body";
/// rpc:2: marks the ANS that carries response trailer fields.
pub const H_TRAILERS: &str = "Rpc-Trailers";
/// rpc:2: protocol the requester wants to switch to (e.g. `websocket`).
pub const H_UPGRADE: &str = "Rpc-Upgrade";

/// Envelope headers that only the RPC layer itself may set.
const RESERVED: [&str; 8] = [H_METHOD, H_TARGET, H_STATUS, H_REASON, H_ABORT, H_BODY, H_TRAILERS, "Content-Transfer-Encoding"];

fn is_reserved(name: &str) -> bool {
    RESERVED.iter().any(|r| r.eq_ignore_ascii_case(name))
}

fn app_headers(headers: Headers) -> Headers {
    headers.into_iter().filter(|(k, _)| !is_reserved(k)).collect()
}

// ---------------------------------------------------------------------------
// Messages

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RpcRequest {
    pub method: String,
    pub target: String,
    /// Application headers. May include `Rpc-Id`, `Rpc-Timeout`, `Rpc-Authority`, `Rpc-Upgrade`.
    pub headers: Headers,
    pub body: Bytes,
}

impl RpcRequest {
    pub fn new(method: impl Into<String>, target: impl Into<String>) -> Self {
        RpcRequest { method: method.into(), target: target.into(), ..Default::default() }
    }
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }
    pub fn body(mut self, body: impl Into<Bytes>) -> Self {
        self.body = body.into();
        self
    }

    fn envelope(&self, extra: &[(&str, &str)]) -> Headers {
        let mut h: Headers = Vec::with_capacity(self.headers.len() + 2 + extra.len());
        h.push((H_METHOD.into(), self.method.clone()));
        h.push((H_TARGET.into(), self.target.clone()));
        h.extend(self.headers.iter().filter(|(k, _)| !is_reserved(k)).cloned());
        h.extend(extra.iter().map(|(k, v)| (k.to_string(), v.to_string())));
        h
    }

    pub fn to_payload(&self) -> Bytes {
        Entity { headers: self.envelope(&[]), body: self.body.clone() }.to_bytes()
    }

    pub fn from_entity(e: Entity) -> Result<RpcRequest, RpcFailure> {
        let (method, target, headers) = parse_request_head(e.headers)?;
        Ok(RpcRequest { method, target, headers, body: e.body })
    }
}

fn parse_request_head(headers: Headers) -> Result<(String, String, Headers), RpcFailure> {
    let method = header(&headers, H_METHOD).map(str::to_string);
    let target = header(&headers, H_TARGET).map(str::to_string);
    match (method, target) {
        (Some(method), Some(target)) if !method.is_empty() => Ok((method, target, app_headers(headers))),
        _ => Err(RpcFailure::new(code::PARAM_SYNTAX_ERROR, "request lacks Rpc-Method or Rpc-Target")),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponseHead {
    pub status: u16,
    /// Application headers (envelope headers removed).
    pub headers: Headers,
}

impl ResponseHead {
    fn from_headers(headers: Headers) -> Result<Self, RpcError> {
        let status = match header(&headers, H_STATUS) {
            None => 200,
            Some(s) => s.trim().parse::<u16>().ok().filter(|s| (100..=999).contains(s)).ok_or_else(|| RpcError::BadReply(format!("bad Rpc-Status {s:?}")))?,
        };
        Ok(ResponseHead { status, headers: app_headers(headers) })
    }
    pub fn header(&self, name: &str) -> Option<&str> {
        header(&self.headers, name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcResponse {
    pub status: u16,
    pub headers: Headers,
    pub body: Bytes,
}

impl RpcResponse {
    pub fn header(&self, name: &str) -> Option<&str> {
        header(&self.headers, name)
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum RpcError {
    /// The peer answered with ERR (no application-level response).
    #[error("remote error {code}: {message}")]
    Remote { code: u32, message: String },
    #[error(transparent)]
    Transport(#[from] Error),
    #[error("request timed out")]
    Timeout,
    #[error("malformed reply: {0}")]
    BadReply(String),
    /// A streamed response (or, server side, a streamed request) was abandoned midway.
    #[error("stream aborted: {0}")]
    Aborted(String),
    /// Server side: the request body is not valid chunked coding.
    #[error("malformed request: {0}")]
    BadRequest(String),
}

impl RpcError {
    /// BEEP reply code that best describes this error.
    pub fn beep_code(&self) -> u32 {
        match self {
            RpcError::Remote { code, .. } => *code,
            RpcError::Timeout | RpcError::Aborted(_) => code::ABORTED,
            RpcError::BadRequest(_) => code::PARAM_SYNTAX_ERROR,
            RpcError::Transport(Error::Refused { code, .. }) => *code,
            RpcError::Transport(Error::Closed(_) | Error::Io(_)) => code::SERVICE_NOT_AVAILABLE,
            _ => code::TRANSACTION_FAILED,
        }
    }
    pub fn http_status(&self) -> u16 {
        beep_code_to_http(self.beep_code())
    }
}

/// docs/PROTOCOL.md §2.4.
pub fn beep_code_to_http(code: u32) -> u16 {
    match code {
        421 | 450 => 503,
        451 => 504,
        500 | 501 | 553 => 400,
        504 => 501,
        530 | 534 | 535 => 401,
        537 | 538 => 403,
        _ => 502,
    }
}

/// Parse an ERR payload into (code, message).
pub fn parse_err_payload(payload: Bytes) -> (u32, String) {
    let e = Entity::parse(payload);
    match Mgmt::parse(&e.body) {
        Ok(Mgmt::Error { code, message }) => (code, message),
        _ => (code::TRANSACTION_FAILED, String::from_utf8_lossy(&e.body).trim().to_string()),
    }
}

// ---------------------------------------------------------------------------
// Client-side reply decoding

#[derive(Default)]
struct AnsBuf {
    head: Option<HeadParser>,
    data: VecDeque<Bytes>,
    complete: bool,
}

struct ReplyDecoder {
    stream: ReplyStream,
    max_err: usize,
    head: Option<ResponseHead>,
    rpy_head: Option<HeadParser>,
    ans: BTreeMap<u32, AnsBuf>,
    next_ans: u32,
    ready: VecDeque<Bytes>,
    trailers: Option<Headers>,
    /// The reply is ANS…NUL (a stream) rather than one RPY.
    streamed: bool,
    finished: bool,
}

impl ReplyDecoder {
    fn new(stream: ReplyStream) -> Self {
        ReplyDecoder {
            stream,
            max_err: 1 << 20,
            head: None,
            rpy_head: Some(HeadParser::default()),
            ans: BTreeMap::new(),
            next_ans: 0,
            ready: VecDeque::new(),
            trailers: None,
            streamed: false,
            finished: false,
        }
    }

    /// Pump events until the response head is known.
    async fn read_head(&mut self) -> Result<ResponseHead, RpcError> {
        while self.head.is_none() {
            if self.finished {
                return Err(RpcError::BadReply("reply ended before a response head".into()));
            }
            self.pump().await?;
        }
        Ok(self.head.clone().unwrap())
    }

    /// Next body piece, or `None` at the end.
    async fn next_body(&mut self) -> Option<Result<Bytes, RpcError>> {
        loop {
            if let Some(b) = self.ready.pop_front() {
                return Some(Ok(b));
            }
            if self.finished {
                return None;
            }
            if let Err(e) = self.pump().await {
                self.finished = true;
                return Some(Err(e));
            }
        }
    }

    async fn pump(&mut self) -> Result<(), RpcError> {
        let ev = match self.stream.next().await {
            None => {
                self.finished = true;
                return Ok(());
            }
            Some(ev) => ev?,
        };
        match ev {
            ReplyEvent::Rpy { data, last } => {
                let bytes = data.into_bytes();
                if let Some(hp) = self.rpy_head.as_mut() {
                    match hp.push(&bytes, last) {
                        HeadProgress::Pending => {}
                        HeadProgress::TooLarge => return Err(RpcError::BadReply("response head too large".into())),
                        HeadProgress::Done { headers, rest } => {
                            self.rpy_head = None;
                            self.head = Some(ResponseHead::from_headers(headers)?);
                            if !rest.is_empty() {
                                self.ready.push_back(rest);
                            }
                        }
                    }
                } else if !bytes.is_empty() {
                    self.ready.push_back(bytes);
                }
                if last {
                    self.finished = true;
                }
            }
            ReplyEvent::Err { data, last } => {
                let mut buf = BytesMut::from(&data.into_bytes()[..]);
                let mut last = last;
                while !last {
                    match self.stream.next().await {
                        Some(Ok(ReplyEvent::Err { data, last: l })) => {
                            if buf.len() + data.data.len() > self.max_err {
                                return Err(RpcError::BadReply("ERR payload too large".into()));
                            }
                            buf.extend_from_slice(&data.data);
                            last = l;
                        }
                        Some(Err(e)) => return Err(e.into()),
                        _ => return Err(RpcError::BadReply("truncated ERR".into())),
                    }
                }
                self.finished = true;
                let (code, message) = parse_err_payload(buf.freeze());
                return Err(RpcError::Remote { code, message });
            }
            ReplyEvent::Ans { ansno, data, last } => {
                let bytes = data.into_bytes();
                let buf = self.ans.entry(ansno).or_insert_with(|| AnsBuf { head: Some(HeadParser::default()), ..Default::default() });
                if let Some(hp) = buf.head.as_mut() {
                    match hp.push(&bytes, last) {
                        HeadProgress::Pending => {}
                        HeadProgress::TooLarge => return Err(RpcError::BadReply("ANS head too large".into())),
                        HeadProgress::Done { headers, rest } => {
                            buf.head = None;
                            if let Some(reason) = header(&headers, H_ABORT) {
                                return Err(RpcError::Aborted(reason.to_string()));
                            }
                            if ansno == 0 {
                                self.head = Some(ResponseHead::from_headers(headers)?);
                                self.streamed = true;
                            } else if header(&headers, H_TRAILERS).is_some() {
                                self.trailers = Some(app_headers(headers));
                                buf.complete = buf.complete || last;
                                self.flush_ans(false);
                                return Ok(());
                            }
                            if !rest.is_empty() {
                                buf.data.push_back(rest);
                            }
                        }
                    }
                } else if !bytes.is_empty() {
                    buf.data.push_back(bytes);
                }
                if last {
                    buf.complete = true;
                }
                self.flush_ans(false);
            }
            ReplyEvent::Nul => {
                self.flush_ans(true);
                self.finished = true;
                if self.head.is_none() {
                    return Err(RpcError::BadReply("ANS stream without a head (ansno 0)".into()));
                }
            }
        }
        Ok(())
    }

    /// Move ANS data to `ready` in ansno order. With `all`, ignore gaps (stream ended).
    fn flush_ans(&mut self, all: bool) {
        loop {
            let key = if all { self.ans.keys().next().copied() } else { Some(self.next_ans) };
            let Some(k) = key else { return };
            let Some(buf) = self.ans.get_mut(&k) else { return };
            if k == 0 && self.head.is_none() && !all {
                return;
            }
            self.ready.extend(buf.data.drain(..));
            if buf.complete || all {
                self.ans.remove(&k);
                self.next_ans = k + 1;
            } else {
                return;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Client

#[derive(Debug, Clone)]
pub struct ClientOptions {
    /// BEEP sessions (TCP connections) to keep.
    pub sessions: usize,
    /// Channels per session; each carries one request at a time.
    pub max_channels: usize,
    pub connect_timeout: Duration,
    /// Deadline for receiving the response head; also sent as `Rpc-Timeout`.
    pub request_timeout: Option<Duration>,
    /// Preferred profile offered when starting channels.
    pub profile: String,
    /// Offered after `profile`; the peer picks the first one it serves.
    pub fallback_profiles: Vec<String>,
    /// Authenticate every session with SASL PLAIN (use together with `tls`).
    pub sasl: Option<crate::sasl::PlainCredentials>,
    pub beep: Config,
    #[cfg(feature = "tls")]
    pub tls: Option<crate::tls::TlsClientOptions>,
}

impl Default for ClientOptions {
    fn default() -> Self {
        ClientOptions {
            sessions: 1,
            max_channels: 64,
            connect_timeout: Duration::from_secs(5),
            request_timeout: Some(Duration::from_secs(30)),
            profile: RPC_PROFILE_V2.to_string(),
            fallback_profiles: vec![RPC_PROFILE.to_string()],
            sasl: None,
            beep: Config::default(),
            #[cfg(feature = "tls")]
            tls: None,
        }
    }
}

#[derive(Default)]
struct PoolState {
    sessions: Vec<Session>,
    idle: Vec<Channel>,
    closed: bool,
}

struct ClientInner {
    addr: String,
    opts: ClientOptions,
    pool: Mutex<PoolState>,
    permits: Arc<Semaphore>,
    connect_lock: tokio::sync::Mutex<()>,
}

impl Drop for ClientInner {
    fn drop(&mut self) {
        for s in self.pool.lock().unwrap().sessions.drain(..) {
            s.abort();
        }
    }
}

/// Pooled RPC client: N sessions × M channels, one in-flight request per channel.
#[derive(Clone)]
pub struct RpcClient {
    inner: Arc<ClientInner>,
}

/// A channel borrowed from the pool. Shared (via `Arc`) by the request writer and the
/// response of a duplex call; the channel goes back to the pool once both are done.
struct Lease {
    ch: Option<Channel>,
    _permit: OwnedSemaphorePermit,
    client: Weak<ClientInner>,
}

impl Lease {
    fn channel(&self) -> &Channel {
        self.ch.as_ref().expect("leased channel")
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        if let (Some(ch), Some(c)) = (self.ch.take(), self.client.upgrade()) {
            if ch.is_open() && ch.pending() == 0 {
                let mut pool = c.pool.lock().unwrap();
                if !pool.closed {
                    pool.idle.push(ch);
                }
            }
        }
    }
}

/// Hand an unfinished reply to a background task so the channel can be reused afterwards.
fn spawn_drain(dec: ReplyDecoder, lease: Arc<Lease>) {
    let mut stream = dec.stream;
    if stream.is_done() {
        return drop(lease);
    }
    if let Ok(rt) = tokio::runtime::Handle::try_current() {
        rt.spawn(async move {
            while let Some(Ok(_)) = stream.next().await {}
            drop(lease);
        });
    }
}

/// Streaming response body.
pub struct ResponseBody {
    dec: Option<ReplyDecoder>,
    lease: Option<Arc<Lease>>,
    trailers: Option<Headers>,
}

impl ResponseBody {
    /// Next piece of the body; `None` at the end.
    pub async fn chunk(&mut self) -> Option<Result<Bytes, RpcError>> {
        let r = self.dec.as_mut()?.next_body().await;
        if matches!(r, None | Some(Err(_))) {
            self.trailers = self.dec.as_mut().and_then(|d| d.trailers.take());
            self.release();
        }
        r
    }

    /// Trailer fields (rpc:2), available once [`chunk`](Self::chunk) has returned `None`.
    pub fn trailers(&self) -> Option<&Headers> {
        self.trailers.as_ref()
    }

    /// True when the responder streams (ANS…NUL): forward pieces as they come instead of
    /// buffering, since the peer may be waiting on them (SSE, gRPC streams).
    pub fn is_streamed(&self) -> bool {
        self.dec.as_ref().map(|d| d.streamed).unwrap_or(false)
    }

    fn release(&mut self) {
        if let (Some(dec), Some(lease)) = (self.dec.take(), self.lease.take()) {
            spawn_drain(dec, lease);
        }
    }

    pub async fn collect(mut self, max: usize) -> Result<Bytes, RpcError> {
        let mut out = BytesMut::new();
        while let Some(c) = self.chunk().await {
            let c = c?;
            if out.len() + c.len() > max {
                return Err(RpcError::Transport(Error::TooLarge(max)));
            }
            out.extend_from_slice(&c);
        }
        Ok(out.freeze())
    }
}

impl Drop for ResponseBody {
    fn drop(&mut self) {
        self.release();
    }
}

enum WriterKind {
    /// rpc:2: the body goes out as chunked-coded MSG frames as it is written.
    Stream(MsgSink),
    /// rpc:1 peer: the body is buffered and the whole MSG sent on finish.
    Buffer { req: RpcRequest, buf: BytesMut, max: usize, reply: Option<oneshot::Sender<Result<ReplyStream, RpcError>>> },
}

/// Request-body side of [`RpcClient::call_duplex`]. Dropping it without
/// [`finish`](Self::finish) aborts the request.
pub struct RequestWriter {
    kind: WriterKind,
    _lease: Arc<Lease>,
}

impl RequestWriter {
    /// True when the body is streamed (rpc:2); false when it is buffered for an rpc:1 peer.
    pub fn is_streaming(&self) -> bool {
        matches!(self.kind, WriterKind::Stream(_))
    }

    pub async fn send(&mut self, data: Bytes) -> Result<(), RpcError> {
        if data.is_empty() {
            return Ok(());
        }
        match &mut self.kind {
            WriterKind::Stream(sink) => Ok(sink.send(chunked::chunk(&data)).await?),
            WriterKind::Buffer { buf, max, reply, .. } => {
                if buf.len() + data.len() > *max {
                    let e = RpcError::Transport(Error::TooLarge(*max));
                    if let Some(tx) = reply.take() {
                        let _ = tx.send(Err(e.clone()));
                    }
                    return Err(e);
                }
                buf.extend_from_slice(&data);
                Ok(())
            }
        }
    }

    /// End the body. Trailers are carried on rpc:2 and dropped for an rpc:1 peer.
    pub async fn finish(self, trailers: Headers) -> Result<(), RpcError> {
        let RequestWriter { kind, _lease: lease } = self;
        match kind {
            WriterKind::Stream(sink) => Ok(sink.finish(chunked::last_chunk(&trailers)).await?),
            WriterKind::Buffer { mut req, buf, reply, .. } => {
                let Some(tx) = reply else { return Err(RpcError::Aborted("request body rejected".into())) };
                req.body = buf.freeze();
                let r = lease.channel().send(req.to_payload()).await.map_err(RpcError::from);
                let err = r.as_ref().err().cloned();
                let _ = tx.send(r);
                err.map_or(Ok(()), Err)
            }
        }
    }
}

enum PendingKind {
    Ready(ReplyStream),
    Waiting(oneshot::Receiver<Result<ReplyStream, RpcError>>),
}

/// Response side of [`RpcClient::call_duplex`].
pub struct PendingResponse {
    kind: PendingKind,
    lease: Arc<Lease>,
    timeout: Option<Duration>,
}

impl PendingResponse {
    /// Wait for the response head. On rpc:2 this can complete before the request body is
    /// finished; for an rpc:1 peer only after [`RequestWriter::finish`].
    pub async fn response(self) -> Result<(ResponseHead, ResponseBody), RpcError> {
        let PendingResponse { kind, lease, timeout } = self;
        let stream = match kind {
            PendingKind::Ready(s) => s,
            PendingKind::Waiting(rx) => match rx.await {
                Ok(r) => r?,
                Err(_) => return Err(RpcError::Aborted("request body abandoned".into())),
            },
        };
        let mut dec = ReplyDecoder::new(stream);
        let head = match timeout {
            Some(t) => match tokio::time::timeout(t, dec.read_head()).await {
                Ok(r) => r,
                Err(_) => {
                    spawn_drain(dec, lease);
                    return Err(RpcError::Timeout);
                }
            },
            None => dec.read_head().await,
        };
        match head {
            Ok(h) => Ok((h, ResponseBody { dec: Some(dec), lease: Some(lease), trailers: None })),
            Err(e) => {
                spawn_drain(dec, lease);
                Err(e)
            }
        }
    }
}

impl RpcClient {
    pub fn new(addr: impl Into<String>, opts: ClientOptions) -> RpcClient {
        let permits = Arc::new(Semaphore::new(opts.sessions.max(1) * opts.max_channels.max(1)));
        RpcClient {
            inner: Arc::new(ClientInner {
                addr: addr.into(),
                opts,
                pool: Mutex::new(PoolState::default()),
                permits,
                connect_lock: tokio::sync::Mutex::new(()),
            }),
        }
    }

    pub fn addr(&self) -> &str {
        &self.inner.addr
    }

    /// Default head deadline ([`ClientOptions::request_timeout`]).
    pub fn request_timeout(&self) -> Option<Duration> {
        self.inner.opts.request_timeout
    }

    /// Live sessions and their open channel counts.
    pub fn status(&self) -> Vec<usize> {
        let pool = self.inner.pool.lock().unwrap();
        pool.sessions.iter().filter(|s| !s.is_closed()).map(|s| s.channel_count()).collect()
    }

    /// Make sure at least one session is up (useful for health checks / warm-up).
    pub async fn connect(&self) -> Result<(), RpcError> {
        self.session_for_channel().await.map(|_| ())
    }

    async fn new_session(&self) -> Result<Session, RpcError> {
        let o = &self.inner.opts;
        let fut = async {
            #[cfg(feature = "tls")]
            let s = match &o.tls {
                Some(tls) => crate::tls::connect(&self.inner.addr, o.beep.clone(), Profiles::new(), tls).await?,
                None => Session::connect(self.inner.addr.as_str(), o.beep.clone(), Profiles::new()).await?,
            };
            #[cfg(not(feature = "tls"))]
            let s = Session::connect(self.inner.addr.as_str(), o.beep.clone(), Profiles::new()).await?;
            if let Some(creds) = &o.sasl {
                if let Err(e) = crate::sasl::authenticate_plain(&s, creds).await {
                    s.abort();
                    return Err(e);
                }
            }
            Ok::<Session, Error>(s)
        };
        match tokio::time::timeout(o.connect_timeout, fut).await {
            Ok(r) => Ok(r?),
            Err(_) => Err(RpcError::Transport(Error::Timeout)),
        }
    }

    async fn session_for_channel(&self) -> Result<Session, RpcError> {
        {
            let mut pool = self.inner.pool.lock().unwrap();
            if pool.closed {
                return Err(RpcError::Transport(Error::Closed("client closed".into())));
            }
            pool.sessions.retain(|s| !s.is_closed());
            if pool.sessions.len() >= self.inner.opts.sessions.max(1) {
                return Ok(pool.sessions.iter().min_by_key(|s| s.channel_count()).unwrap().clone());
            }
        }
        let _g = self.inner.connect_lock.lock().await;
        {
            let pool = self.inner.pool.lock().unwrap();
            let live: Vec<&Session> = pool.sessions.iter().filter(|s| !s.is_closed()).collect();
            if live.len() >= self.inner.opts.sessions.max(1) {
                return Ok(live.into_iter().min_by_key(|s| s.channel_count()).unwrap().clone());
            }
        }
        let s = self.new_session().await?;
        self.inner.pool.lock().unwrap().sessions.push(s.clone());
        Ok(s)
    }

    async fn start_rpc_channel(&self, session: &Session) -> Result<Channel, Error> {
        let o = &self.inner.opts;
        let offer = std::iter::once(&o.profile).chain(o.fallback_profiles.iter()).map(ProfileEl::new).collect();
        session.start_channel_with(offer, None).await.map(|(c, _)| c)
    }

    async fn acquire(&self) -> Result<Lease, RpcError> {
        let permit = self.inner.permits.clone().acquire_owned().await.map_err(|_| RpcError::Transport(Error::Closed("client closed".into())))?;
        let client = Arc::downgrade(&self.inner);
        loop {
            let idle = {
                let mut pool = self.inner.pool.lock().unwrap();
                let mut found = None;
                while let Some(ch) = pool.idle.pop() {
                    if ch.is_open() && !ch.session().is_closed() {
                        found = Some(ch);
                        break;
                    }
                }
                found
            };
            if let Some(ch) = idle {
                return Ok(Lease { ch: Some(ch), _permit: permit, client });
            }
            let session = self.session_for_channel().await?;
            match self.start_rpc_channel(&session).await {
                Ok(ch) => return Ok(Lease { ch: Some(ch), _permit: permit, client }),
                Err(e) if session.is_closed() => {
                    tracing::debug!("beep session died while opening channel: {e}; retrying");
                    self.inner.pool.lock().unwrap().sessions.retain(|s| !s.is_closed());
                    // Retry once with a fresh session; a second failure is reported.
                    let session = self.session_for_channel().await?;
                    let ch = self.start_rpc_channel(&session).await?;
                    return Ok(Lease { ch: Some(ch), _permit: permit, client });
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    fn with_timeout_header(req: &mut RpcRequest, timeout: Option<Duration>) {
        if let Some(t) = timeout {
            if header(&req.headers, H_TIMEOUT).is_none() {
                req.headers.push((H_TIMEOUT.into(), t.as_millis().to_string()));
            }
        }
    }

    /// Send a request and return the head plus a streaming body.
    pub async fn call_streaming(&self, req: RpcRequest) -> Result<(ResponseHead, ResponseBody), RpcError> {
        self.call_streaming_timeout(req, self.inner.opts.request_timeout).await
    }

    /// Like [`call_streaming`](Self::call_streaming) with a per-request head deadline.
    pub async fn call_streaming_timeout(&self, mut req: RpcRequest, timeout: Option<Duration>) -> Result<(ResponseHead, ResponseBody), RpcError> {
        Self::with_timeout_header(&mut req, timeout);
        let lease = Arc::new(self.acquire().await?);
        let stream = lease.channel().send(req.to_payload()).await?;
        PendingResponse { kind: PendingKind::Ready(stream), lease, timeout }.response().await
    }

    /// Start a request whose body is written while the response is read (gRPC streams,
    /// uploads of unknown length, WebSocket tunnels). `req.body` is ignored.
    ///
    /// With an rpc:2 peer the body is streamed and the reply may begin before it ends;
    /// with an rpc:1 peer it is buffered (up to `beep.max_message_size`) and sent on
    /// [`RequestWriter::finish`]. Upgrades (`Rpc-Upgrade`) need rpc:2.
    pub async fn call_duplex(&self, mut req: RpcRequest, timeout: Option<Duration>) -> Result<(RequestWriter, PendingResponse), RpcError> {
        Self::with_timeout_header(&mut req, timeout);
        let lease = Arc::new(self.acquire().await?);
        if lease.channel().profile() == RPC_PROFILE_V2 {
            let head = Entity { headers: req.envelope(&[(H_BODY, "chunked")]), body: Bytes::new() }.to_bytes();
            let (sink, stream) = lease.channel().send_stream(head).await?;
            let writer = RequestWriter { kind: WriterKind::Stream(sink), _lease: lease.clone() };
            return Ok((writer, PendingResponse { kind: PendingKind::Ready(stream), lease, timeout }));
        }
        if header(&req.headers, H_UPGRADE).is_some() {
            return Err(RpcError::Remote { code: code::PARAM_NOT_IMPLEMENTED, message: format!("peer does not serve {RPC_PROFILE_V2}, which upgrades need") });
        }
        let (tx, rx) = oneshot::channel();
        let max = self.inner.opts.beep.max_message_size;
        req.body = Bytes::new();
        let writer = RequestWriter { kind: WriterKind::Buffer { req, buf: BytesMut::new(), max, reply: Some(tx) }, _lease: lease.clone() };
        Ok((writer, PendingResponse { kind: PendingKind::Waiting(rx), lease, timeout }))
    }

    /// Send a request and collect the whole response.
    pub async fn call(&self, req: RpcRequest) -> Result<RpcResponse, RpcError> {
        let max = self.inner.opts.beep.max_message_size;
        let (head, body) = self.call_streaming(req).await?;
        let body = body.collect(max).await?;
        Ok(RpcResponse { status: head.status, headers: head.headers, body })
    }

    /// Send an arbitrary payload on a channel of `profile` (raw passthrough, §3.3).
    pub async fn raw(&self, profile: &str, payload: Bytes) -> Result<crate::session::Reply, RpcError> {
        let session = self.session_for_channel().await?;
        let ch = session.start_channel(profile).await?;
        let reply = ch.request(payload).await;
        let _ = ch.close().await;
        Ok(reply?)
    }

    /// Release all sessions.
    pub async fn close(&self) {
        let sessions = {
            let mut pool = self.inner.pool.lock().unwrap();
            pool.closed = true;
            pool.idle.clear();
            std::mem::take(&mut pool.sessions)
        };
        for s in sessions {
            if s.close().await.is_err() {
                s.abort();
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Server

/// Failure reported to the client as BEEP ERR.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcFailure {
    pub code: u32,
    pub message: String,
}

impl RpcFailure {
    pub fn new(code: u32, message: impl Into<String>) -> Self {
        RpcFailure { code, message: message.into() }
    }
}

pub use chunked::Part as RequestPart;

/// A request body still arriving, handed to handlers whose [`RpcHandler::streaming`]
/// returns true. Pulling it is what returns flow-control credit to the requester, so a
/// slow consumer applies backpressure end to end.
pub struct RequestBody {
    msg: IncomingMessage,
    first: Option<Bytes>,
    decoder: Option<chunked::Decoder>,
    queue: VecDeque<RequestPart>,
    ended: bool,
}

impl std::fmt::Debug for RequestBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RequestBody").field("chunked", &self.decoder.is_some()).field("ended", &self.ended).finish()
    }
}

impl RequestBody {
    fn new(msg: IncomingMessage, first: Bytes, is_chunked: bool) -> Self {
        RequestBody { msg, first: Some(first), decoder: is_chunked.then(chunked::Decoder::default), queue: VecDeque::new(), ended: false }
    }

    /// True for an rpc:2 chunked body (which can carry trailers and signal an abort).
    pub fn is_chunked(&self) -> bool {
        self.decoder.is_some()
    }

    /// Next part; `Ok(None)` at the end. `Err(RpcError::Aborted)` means the requester gave
    /// up before the body was complete.
    pub async fn next(&mut self) -> Result<Option<RequestPart>, RpcError> {
        loop {
            if let Some(p) = self.queue.pop_front() {
                return Ok(Some(p));
            }
            if self.ended {
                return Ok(None);
            }
            let piece = match self.first.take() {
                Some(b) => Some(b),
                None => self.msg.next_chunk().await?.map(|c| c.into_bytes()),
            };
            match (piece, &mut self.decoder) {
                (Some(b), None) => {
                    if !b.is_empty() {
                        self.queue.push_back(RequestPart::Data(b));
                    }
                }
                (Some(b), Some(d)) => {
                    let mut out = Vec::new();
                    d.push(&b, &mut out).map_err(RpcError::BadRequest)?;
                    self.queue.extend(out);
                }
                (None, d) => {
                    self.ended = true;
                    if d.as_ref().is_some_and(|d| !d.is_done()) {
                        return Err(RpcError::Aborted("request body ended before its last chunk".into()));
                    }
                }
            }
        }
    }

    /// Read the whole body (and any trailers).
    pub async fn collect(mut self, max: usize) -> Result<(Bytes, Headers), RpcError> {
        let mut out = BytesMut::new();
        let mut trailers = Headers::new();
        while let Some(p) = self.next().await? {
            match p {
                RequestPart::Data(b) => {
                    if out.len() + b.len() > max {
                        return Err(RpcError::Transport(Error::TooLarge(max)));
                    }
                    out.extend_from_slice(&b);
                }
                RequestPart::Trailers(t) => trailers = t,
            }
        }
        Ok((out.freeze(), trailers))
    }
}

#[derive(Debug)]
pub struct RpcServerRequest {
    pub method: String,
    pub target: String,
    pub headers: Headers,
    /// The whole body; empty when [`stream`](Self::stream) is set.
    pub body: Bytes,
    /// The body as it arrives, for streaming handlers.
    pub stream: Option<RequestBody>,
    /// `Rpc-Upgrade` (rpc:2): the protocol the requester wants to switch to.
    pub upgrade: Option<String>,
    pub id: Option<String>,
    pub timeout: Option<Duration>,
    pub channel: u32,
    /// Profile of the channel the request arrived on.
    pub profile: &'static str,
    /// Identity the peer authenticated as on this session (SASL), if any.
    pub identity: Option<String>,
}

impl RpcServerRequest {
    pub fn header(&self, name: &str) -> Option<&str> {
        header(&self.headers, name)
    }
}

/// One item of a [`ServerBody::Frames`] response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BodyItem {
    Data(Bytes),
    /// Trailer fields; must be the last item (rpc:2; dropped on rpc:1 channels).
    Trailers(Headers),
    /// Abort the stream with this reason (`Rpc-Abort`).
    Abort(String),
}

pub enum ServerBody {
    /// Sent as a single RPY.
    Full(Bytes),
    /// Sent as ANS 0 (head), ANS 1..N (chunks), NUL. `Err(reason)` aborts the stream.
    Stream(mpsc::Receiver<Result<Bytes, String>>),
    /// Like `Stream`, with trailers.
    Frames(mpsc::Receiver<BodyItem>),
}

pub struct RpcServerResponse {
    pub status: u16,
    pub headers: Headers,
    pub body: ServerBody,
}

impl RpcServerResponse {
    pub fn full(status: u16, headers: Headers, body: impl Into<Bytes>) -> Self {
        RpcServerResponse { status, headers, body: ServerBody::Full(body.into()) }
    }
    /// A streaming response plus the sender that feeds it.
    pub fn stream(status: u16, headers: Headers) -> (Self, mpsc::Sender<Result<Bytes, String>>) {
        let (tx, rx) = mpsc::channel(16);
        (RpcServerResponse { status, headers, body: ServerBody::Stream(rx) }, tx)
    }
    /// A streaming response that may end with trailers.
    pub fn frames(status: u16, headers: Headers) -> (Self, mpsc::Sender<BodyItem>) {
        let (tx, rx) = mpsc::channel(16);
        (RpcServerResponse { status, headers, body: ServerBody::Frames(rx) }, tx)
    }
}

#[async_trait]
pub trait RpcHandler: Send + Sync + 'static {
    async fn handle(&self, req: RpcServerRequest) -> Result<RpcServerResponse, RpcFailure>;

    /// Return true to receive request bodies as they arrive ([`RpcServerRequest::stream`])
    /// and to be able to reply before the request is complete. Otherwise the body is
    /// collected first (up to `max_message_size`) into [`RpcServerRequest::body`].
    fn streaming(&self) -> bool {
        false
    }
}

/// Adapt an async closure into an [`RpcHandler`].
pub fn handler_fn<F, Fut>(f: F) -> impl RpcHandler
where
    F: Fn(RpcServerRequest) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<RpcServerResponse, RpcFailure>> + Send + 'static,
{
    struct FnHandler<F>(F);
    #[async_trait]
    impl<F, Fut> RpcHandler for FnHandler<F>
    where
        F: Fn(RpcServerRequest) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<RpcServerResponse, RpcFailure>> + Send + 'static,
    {
        async fn handle(&self, req: RpcServerRequest) -> Result<RpcServerResponse, RpcFailure> {
            (self.0)(req).await
        }
    }
    FnHandler(f)
}

/// [`ProfileHandler`] that decodes RPC envelopes and dispatches them to an [`RpcHandler`].
pub struct RpcProfile {
    handler: Arc<dyn RpcHandler>,
    v2: bool,
}

impl RpcProfile {
    /// Serves `urn:minta:beep:rpc:1`.
    pub fn new(handler: impl RpcHandler) -> Self {
        RpcProfile { handler: Arc::new(handler), v2: false }
    }
    pub fn from_arc(handler: Arc<dyn RpcHandler>) -> Self {
        RpcProfile { handler, v2: false }
    }
    /// Serves `urn:minta:beep:rpc:2`.
    pub fn v2(handler: Arc<dyn RpcHandler>) -> Self {
        RpcProfile { handler, v2: true }
    }
}

/// Register `handler` for both RPC profiles.
pub fn rpc_profiles(handler: impl RpcHandler) -> Profiles {
    let h: Arc<dyn RpcHandler> = Arc::new(handler);
    Profiles::new().with(RPC_PROFILE_V2, RpcProfile::v2(h.clone())).with(RPC_PROFILE, RpcProfile::from_arc(h))
}

fn response_headers(status: u16, headers: Headers, id: Option<&str>) -> Headers {
    let mut h: Headers = Vec::with_capacity(headers.len() + 2);
    h.push((H_STATUS.into(), status.to_string()));
    let has_id = header(&headers, H_ID).is_some();
    h.extend(headers.into_iter().filter(|(k, _)| !is_reserved(k)));
    if let (Some(id), false) = (id, has_id) {
        h.push((H_ID.into(), id.to_string()));
    }
    h
}

/// Read the entity head of an incoming MSG; the body may still be arriving.
async fn read_request_head(msg: &mut IncomingMessage) -> Result<(Headers, Bytes), Error> {
    let mut hp = HeadParser::default();
    loop {
        let (bytes, last) = match msg.next_chunk().await? {
            Some(c) => (c.into_bytes(), false),
            None => (Bytes::new(), true),
        };
        match hp.push(&bytes, last) {
            HeadProgress::Done { headers, rest } => return Ok((headers, rest)),
            HeadProgress::TooLarge => return Err(Error::TooLarge(crate::mime::MAX_HEAD)),
            HeadProgress::Pending => {}
        }
    }
}

#[async_trait]
impl ProfileHandler for RpcProfile {
    async fn on_message(&self, session: &Session, mut msg: IncomingMessage, reply: Responder) {
        let channel = msg.channel;
        let (headers, first) = match read_request_head(&mut msg).await {
            Ok(h) => h,
            Err(Error::TooLarge(_)) => {
                let _ = reply.err_code(code::TRANSACTION_FAILED, "request head too large").await;
                return;
            }
            Err(_) => return,
        };
        let is_chunked = header(&headers, H_BODY).is_some_and(|v| v.trim().eq_ignore_ascii_case("chunked"));
        let upgrade = if self.v2 { header(&headers, H_UPGRADE).map(str::to_string) } else { None };
        let (method, target, headers) = match parse_request_head(headers) {
            Ok(r) => r,
            Err(f) => {
                let _ = reply.err_code(f.code, &f.message).await;
                return;
            }
        };
        let body = RequestBody::new(msg, first, is_chunked);
        let (body, stream) = if self.handler.streaming() {
            (Bytes::new(), Some(body))
        } else {
            match body.collect(session.config().max_message_size).await {
                Ok((b, _trailers)) => (b, None),
                Err(RpcError::Transport(Error::TooLarge(n))) => {
                    let _ = reply.err_code(code::TRANSACTION_FAILED, &format!("request exceeds {n} bytes")).await;
                    return;
                }
                Err(e @ (RpcError::Aborted(_) | RpcError::BadRequest(_))) => {
                    let _ = reply.err_code(e.beep_code(), &e.to_string()).await;
                    return;
                }
                Err(_) => return,
            }
        };
        let id = header(&headers, H_ID).map(str::to_string);
        let timeout = header(&headers, H_TIMEOUT).and_then(|t| t.trim().parse::<u64>().ok()).map(Duration::from_millis);
        let profile = if self.v2 { RPC_PROFILE_V2 } else { RPC_PROFILE };
        let sreq = RpcServerRequest { method, target, headers, body, stream, upgrade, id: id.clone(), timeout, channel, profile, identity: session.identity() };
        let fut = self.handler.handle(sreq);
        let result = match timeout {
            Some(t) => tokio::time::timeout(t, fut).await.unwrap_or_else(|_| Err(RpcFailure::new(code::ABORTED, "deadline exceeded"))),
            None => fut.await,
        };
        let (status, headers, body) = match result {
            Err(f) => {
                let _ = reply.err_code(f.code, &f.message).await;
                return;
            }
            Ok(RpcServerResponse { status, headers, body }) => (status, headers, body),
        };
        let head = |body: Bytes| Entity { headers: response_headers(status, headers, id.as_deref()), body }.to_bytes();
        let mut frames = match body {
            ServerBody::Full(b) => {
                let _ = reply.rpy(head(b)).await;
                return;
            }
            ServerBody::Stream(mut rx) => {
                let (tx, frx) = mpsc::channel(16);
                tokio::spawn(async move {
                    while let Some(item) = rx.recv().await {
                        let item = match item {
                            Ok(b) => BodyItem::Data(b),
                            Err(reason) => BodyItem::Abort(reason),
                        };
                        if tx.send(item).await.is_err() {
                            return;
                        }
                    }
                });
                frx
            }
            ServerBody::Frames(rx) => rx,
        };
        if reply.ans(head(Bytes::new())).await.is_err() {
            return;
        }
        while let Some(item) = frames.recv().await {
            let payload = match item {
                BodyItem::Data(chunk) if chunk.is_empty() => continue,
                BodyItem::Data(chunk) => Entity { headers: vec![], body: chunk }.to_bytes(),
                BodyItem::Trailers(t) => {
                    if self.v2 {
                        let mut h: Headers = vec![(H_TRAILERS.into(), "1".into())];
                        h.extend(app_headers(t));
                        let _ = reply.ans(Entity { headers: h, body: Bytes::new() }.to_bytes()).await;
                    }
                    break;
                }
                BodyItem::Abort(reason) => {
                    let e = Entity { headers: vec![(H_ABORT.into(), reason)], body: Bytes::new() };
                    let _ = reply.ans(e.to_bytes()).await;
                    break;
                }
            };
            if reply.ans(payload).await.is_err() {
                return;
            }
        }
        let _ = reply.nul().await;
    }
}

/// Convenience server: a BEEP listener that serves the RPC profiles (rpc:2 and rpc:1).
pub struct RpcServer {
    listener: Listener,
}

impl RpcServer {
    pub async fn bind(addr: impl tokio::net::ToSocketAddrs, handler: impl RpcHandler, cfg: Config) -> Result<RpcServer, Error> {
        Ok(RpcServer { listener: Listener::bind(addr, cfg, rpc_profiles(handler)).await? })
    }

    #[cfg(feature = "tls")]
    pub async fn bind_tls(addr: impl tokio::net::ToSocketAddrs, handler: impl RpcHandler, cfg: Config, tls: &crate::tls::TlsServerOptions) -> Result<RpcServer, Error> {
        let listener = Listener::bind(addr, cfg, rpc_profiles(handler)).await?.with_tls(tls)?;
        Ok(RpcServer { listener })
    }

    pub fn local_addr(&self) -> Result<std::net::SocketAddr, Error> {
        self.listener.local_addr()
    }

    pub async fn serve(self) -> Result<(), Error> {
        self.listener.serve().await
    }
}
