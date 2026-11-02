//! The MINTA RPC profile (`urn:minta:beep:rpc:1`): HTTP-shaped requests over BEEP.
//! See docs/PROTOCOL.md §2.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};

use crate::error::{code, Error};
use crate::mgmt::Mgmt;
use crate::mime::{header, Entity, HeadParser, HeadProgress, Headers};
use crate::session::{Channel, Config, IncomingMessage, Listener, ProfileHandler, Profiles, ReplyEvent, ReplyStream, Responder, Session};

pub const RPC_PROFILE: &str = "urn:minta:beep:rpc:1";

pub const H_METHOD: &str = "Rpc-Method";
pub const H_TARGET: &str = "Rpc-Target";
pub const H_ID: &str = "Rpc-Id";
pub const H_TIMEOUT: &str = "Rpc-Timeout";
pub const H_AUTHORITY: &str = "Rpc-Authority";
pub const H_STATUS: &str = "Rpc-Status";
pub const H_REASON: &str = "Rpc-Reason";
pub const H_ABORT: &str = "Rpc-Abort";

/// Envelope headers that only the RPC layer itself may set.
const RESERVED: [&str; 6] = [H_METHOD, H_TARGET, H_STATUS, H_REASON, H_ABORT, "Content-Transfer-Encoding"];

fn is_reserved(name: &str) -> bool {
    RESERVED.iter().any(|r| r.eq_ignore_ascii_case(name))
}

// ---------------------------------------------------------------------------
// Messages

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RpcRequest {
    pub method: String,
    pub target: String,
    /// Application headers. May include `Rpc-Id`, `Rpc-Timeout`, `Rpc-Authority`.
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

    pub fn to_payload(&self) -> Bytes {
        let mut h: Headers = Vec::with_capacity(self.headers.len() + 2);
        h.push((H_METHOD.into(), self.method.clone()));
        h.push((H_TARGET.into(), self.target.clone()));
        h.extend(self.headers.iter().filter(|(k, _)| !is_reserved(k)).cloned());
        Entity { headers: h, body: self.body.clone() }.to_bytes()
    }

    pub fn from_entity(e: Entity) -> Result<RpcRequest, RpcFailure> {
        let method = e.header(H_METHOD).map(str::to_string);
        let target = e.header(H_TARGET).map(str::to_string);
        match (method, target) {
            (Some(method), Some(target)) if !method.is_empty() => Ok(RpcRequest {
                method,
                target,
                headers: e.headers.into_iter().filter(|(k, _)| !is_reserved(k)).collect(),
                body: e.body,
            }),
            _ => Err(RpcFailure::new(code::PARAM_SYNTAX_ERROR, "request lacks Rpc-Method or Rpc-Target")),
        }
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
        Ok(ResponseHead { status, headers: headers.into_iter().filter(|(k, _)| !is_reserved(k)).collect() })
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
    /// A streamed response was aborted by the responder (`Rpc-Abort`).
    #[error("response stream aborted: {0}")]
    Aborted(String),
}

impl RpcError {
    /// BEEP reply code that best describes this error.
    pub fn beep_code(&self) -> u32 {
        match self {
            RpcError::Remote { code, .. } => *code,
            RpcError::Timeout => code::ABORTED,
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
    pub profile: String,
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
            profile: RPC_PROFILE.to_string(),
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

struct Lease {
    ch: Option<Channel>,
    _permit: OwnedSemaphorePermit,
    client: Weak<ClientInner>,
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
fn spawn_drain(dec: ReplyDecoder, lease: Lease) {
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
    lease: Option<Lease>,
}

impl ResponseBody {
    /// Next piece of the body; `None` at the end.
    pub async fn chunk(&mut self) -> Option<Result<Bytes, RpcError>> {
        let r = self.dec.as_mut()?.next_body().await;
        if matches!(r, None | Some(Err(_))) {
            self.release();
        }
        r
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
            if let Some(tls) = &o.tls {
                return crate::tls::connect(&self.inner.addr, o.beep.clone(), Profiles::new(), tls).await;
            }
            Session::connect(self.inner.addr.as_str(), o.beep.clone(), Profiles::new()).await
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
            match session.start_channel(&self.inner.opts.profile).await {
                Ok(ch) => return Ok(Lease { ch: Some(ch), _permit: permit, client }),
                Err(e) if session.is_closed() => {
                    tracing::debug!("beep session died while opening channel: {e}; retrying");
                    self.inner.pool.lock().unwrap().sessions.retain(|s| !s.is_closed());
                    // Retry once with a fresh session; a second failure is reported.
                    let session = self.session_for_channel().await?;
                    let ch = session.start_channel(&self.inner.opts.profile).await?;
                    return Ok(Lease { ch: Some(ch), _permit: permit, client });
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    /// Send a request and return the head plus a streaming body.
    pub async fn call_streaming(&self, req: RpcRequest) -> Result<(ResponseHead, ResponseBody), RpcError> {
        self.call_streaming_timeout(req, self.inner.opts.request_timeout).await
    }

    /// Like [`call_streaming`](Self::call_streaming) with a per-request head deadline.
    pub async fn call_streaming_timeout(&self, mut req: RpcRequest, timeout: Option<Duration>) -> Result<(ResponseHead, ResponseBody), RpcError> {
        if let Some(t) = timeout {
            if header(&req.headers, H_TIMEOUT).is_none() {
                req.headers.push((H_TIMEOUT.into(), t.as_millis().to_string()));
            }
        }
        let lease = self.acquire().await?;
        let stream = lease.ch.as_ref().unwrap().send(req.to_payload()).await?;
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
            Ok(h) => Ok((h, ResponseBody { dec: Some(dec), lease: Some(lease) })),
            Err(e) => {
                spawn_drain(dec, lease);
                Err(e)
            }
        }
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

#[derive(Debug)]
pub struct RpcServerRequest {
    pub method: String,
    pub target: String,
    pub headers: Headers,
    pub body: Bytes,
    pub id: Option<String>,
    pub timeout: Option<Duration>,
    pub channel: u32,
}

impl RpcServerRequest {
    pub fn header(&self, name: &str) -> Option<&str> {
        header(&self.headers, name)
    }
}

pub enum ServerBody {
    /// Sent as a single RPY.
    Full(Bytes),
    /// Sent as ANS 0 (head), ANS 1..N (chunks), NUL. `Err(reason)` aborts the stream.
    Stream(mpsc::Receiver<Result<Bytes, String>>),
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
}

#[async_trait]
pub trait RpcHandler: Send + Sync + 'static {
    async fn handle(&self, req: RpcServerRequest) -> Result<RpcServerResponse, RpcFailure>;
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
}

impl RpcProfile {
    pub fn new(handler: impl RpcHandler) -> Self {
        RpcProfile { handler: Arc::new(handler) }
    }
    pub fn from_arc(handler: Arc<dyn RpcHandler>) -> Self {
        RpcProfile { handler }
    }
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

#[async_trait]
impl ProfileHandler for RpcProfile {
    async fn on_message(&self, session: &Session, msg: IncomingMessage, reply: Responder) {
        let channel = msg.channel;
        let body = match msg.collect(session.config().max_message_size).await {
            Ok(b) => b,
            Err(Error::TooLarge(n)) => {
                let _ = reply.err_code(code::TRANSACTION_FAILED, &format!("request exceeds {n} bytes")).await;
                return;
            }
            Err(_) => return,
        };
        let req = match RpcRequest::from_entity(Entity::parse(body)) {
            Ok(r) => r,
            Err(f) => {
                let _ = reply.err_code(f.code, &f.message).await;
                return;
            }
        };
        let id = header(&req.headers, H_ID).map(str::to_string);
        let timeout = header(&req.headers, H_TIMEOUT).and_then(|t| t.trim().parse::<u64>().ok()).map(Duration::from_millis);
        let sreq = RpcServerRequest { method: req.method, target: req.target, headers: req.headers, body: req.body, id: id.clone(), timeout, channel };
        let fut = self.handler.handle(sreq);
        let result = match timeout {
            Some(t) => tokio::time::timeout(t, fut).await.unwrap_or_else(|_| Err(RpcFailure::new(code::ABORTED, "deadline exceeded"))),
            None => fut.await,
        };
        match result {
            Err(f) => {
                let _ = reply.err_code(f.code, &f.message).await;
            }
            Ok(RpcServerResponse { status, headers, body: ServerBody::Full(b) }) => {
                let e = Entity { headers: response_headers(status, headers, id.as_deref()), body: b };
                let _ = reply.rpy(e.to_bytes()).await;
            }
            Ok(RpcServerResponse { status, headers, body: ServerBody::Stream(mut rx) }) => {
                let head = Entity { headers: response_headers(status, headers, id.as_deref()), body: Bytes::new() };
                if reply.ans(head.to_bytes()).await.is_err() {
                    return;
                }
                while let Some(item) = rx.recv().await {
                    let payload = match item {
                        Ok(chunk) if chunk.is_empty() => continue,
                        Ok(chunk) => Entity { headers: vec![], body: chunk }.to_bytes(),
                        Err(reason) => {
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
    }
}

/// Convenience server: a BEEP listener that serves only the RPC profile.
pub struct RpcServer {
    listener: Listener,
}

impl RpcServer {
    pub async fn bind(addr: impl tokio::net::ToSocketAddrs, handler: impl RpcHandler, cfg: Config) -> Result<RpcServer, Error> {
        let profiles = Profiles::new().with(RPC_PROFILE, RpcProfile::new(handler));
        Ok(RpcServer { listener: Listener::bind(addr, cfg, profiles).await? })
    }

    #[cfg(feature = "tls")]
    pub async fn bind_tls(addr: impl tokio::net::ToSocketAddrs, handler: impl RpcHandler, cfg: Config, tls: &crate::tls::TlsServerOptions) -> Result<RpcServer, Error> {
        let profiles = Profiles::new().with(RPC_PROFILE, RpcProfile::new(handler));
        let listener = Listener::bind(addr, cfg, profiles).await?.with_tls(tls)?;
        Ok(RpcServer { listener })
    }

    pub fn local_addr(&self) -> Result<std::net::SocketAddr, Error> {
        self.listener.local_addr()
    }

    pub async fn serve(self) -> Result<(), Error> {
        self.listener.serve().await
    }
}
