//! BEEP session engine: greeting, channel management, flow control, message exchange.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::sync::{mpsc, watch, Notify};

use crate::error::{code, Error, Result};
use crate::frame::{encode_data, encode_seq, DataFrame, Frame, FrameDecoder, FrameType, SeqFrame};
use crate::mgmt::{Mgmt, ProfileEl};
use crate::mime::Entity;

/// RFC 3081 initial window.
pub const INITIAL_WINDOW: u32 = 4096;
const MAX_CHANNEL: u32 = 2_147_483_647;

#[derive(Debug, Clone)]
pub struct Config {
    /// Receive window advertised for every channel.
    pub recv_window: u32,
    /// Largest frame payload we send.
    pub max_frame_size: u32,
    /// Upper bound when a whole message is collected into memory.
    pub max_message_size: usize,
    pub greeting_timeout: Duration,
    pub close_timeout: Duration,
    /// Listener side: refuse to start channels other than SASL ones (`ERR 530`) until the
    /// peer has authenticated (see [`crate::sasl`]).
    pub require_auth: bool,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            recv_window: 262_144,
            max_frame_size: 65_536,
            max_message_size: 64 << 20,
            greeting_timeout: Duration::from_secs(10),
            close_timeout: Duration::from_secs(10),
            require_auth: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Initiator,
    Listener,
}

// ---------------------------------------------------------------------------
// Public data types

/// A piece of payload. Dropping it returns its bytes to the channel's receive window,
/// so holding chunks unconsumed exerts real backpressure on the peer.
pub struct Chunk {
    pub data: Bytes,
    _credit: Credit,
}

impl Chunk {
    pub fn into_bytes(self) -> Bytes {
        self.data.clone()
    }
}

impl std::fmt::Debug for Chunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Chunk({} bytes)", self.data.len())
    }
}

struct Credit {
    shared: Weak<Shared>,
    channel: u32,
    n: u32,
}

impl Drop for Credit {
    fn drop(&mut self) {
        if self.n > 0 {
            if let Some(s) = self.shared.upgrade() {
                s.credit(self.channel, self.n);
            }
        }
    }
}

#[derive(Debug)]
pub enum ReplyEvent {
    Rpy { data: Chunk, last: bool },
    Err { data: Chunk, last: bool },
    Ans { ansno: u32, data: Chunk, last: bool },
    Nul,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply {
    Rpy(Bytes),
    Err(Bytes),
    /// ANS payloads in ansno order.
    Ans(Vec<(u32, Bytes)>),
}

/// Stream of reply events for one MSG.
pub struct ReplyStream {
    rx: mpsc::UnboundedReceiver<ReplyEvent>,
    done: bool,
    shared: Arc<Shared>,
}

impl ReplyStream {
    /// True once the terminal event (or the end of the session) has been observed.
    pub fn is_done(&self) -> bool {
        self.done
    }

    /// Next event; `None` after the terminal event (last RPY/ERR frame or NUL).
    pub async fn next(&mut self) -> Option<Result<ReplyEvent>> {
        if self.done {
            return None;
        }
        match self.rx.recv().await {
            Some(ev) => {
                self.done = matches!(
                    ev,
                    ReplyEvent::Rpy { last: true, .. } | ReplyEvent::Err { last: true, .. } | ReplyEvent::Nul
                );
                Some(Ok(ev))
            }
            None => {
                self.done = true;
                Some(Err(self.shared.closed_error()))
            }
        }
    }

    pub async fn collect(mut self, max: usize) -> Result<Reply> {
        let mut buf = BytesMut::new();
        let mut ans: Vec<(u32, BytesMut)> = vec![];
        let mut total = 0usize;
        let mut kind = None;
        while let Some(ev) = self.next().await {
            let (k, data) = match ev? {
                ReplyEvent::Rpy { data, .. } => (FrameType::Rpy, data),
                ReplyEvent::Err { data, .. } => (FrameType::Err, data),
                ReplyEvent::Ans { ansno, data, .. } => {
                    total += data.data.len();
                    if total > max {
                        return Err(Error::TooLarge(max));
                    }
                    match ans.iter_mut().find(|(n, _)| *n == ansno) {
                        Some((_, b)) => b.extend_from_slice(&data.data),
                        None => ans.push((ansno, BytesMut::from(&data.data[..]))),
                    }
                    kind = Some(FrameType::Ans);
                    continue;
                }
                ReplyEvent::Nul => {
                    kind = Some(FrameType::Ans);
                    continue;
                }
            };
            total += data.data.len();
            if total > max {
                return Err(Error::TooLarge(max));
            }
            buf.extend_from_slice(&data.data);
            kind = Some(k);
        }
        Ok(match kind {
            Some(FrameType::Err) => Reply::Err(buf.freeze()),
            Some(FrameType::Ans) => {
                ans.sort_by_key(|(n, _)| *n);
                Reply::Ans(ans.into_iter().map(|(n, b)| (n, b.freeze())).collect())
            }
            _ => Reply::Rpy(buf.freeze()),
        })
    }
}

enum Piece {
    Data(Chunk),
    End,
}

/// A MSG received from the peer, delivered to a profile handler.
pub struct IncomingMessage {
    pub channel: u32,
    pub msgno: u32,
    rx: mpsc::UnboundedReceiver<Piece>,
    done: bool,
    shared: Arc<Shared>,
}

impl IncomingMessage {
    /// Next body chunk; `Ok(None)` at end of message.
    pub async fn next_chunk(&mut self) -> Result<Option<Chunk>> {
        if self.done {
            return Ok(None);
        }
        match self.rx.recv().await {
            Some(Piece::Data(c)) => Ok(Some(c)),
            Some(Piece::End) => {
                self.done = true;
                Ok(None)
            }
            None => Err(self.shared.closed_error()),
        }
    }

    pub async fn collect(mut self, max: usize) -> Result<Bytes> {
        let mut buf = BytesMut::new();
        while let Some(c) = self.next_chunk().await? {
            if buf.len() + c.data.len() > max {
                return Err(Error::TooLarge(max));
            }
            buf.extend_from_slice(&c.data);
        }
        Ok(buf.freeze())
    }
}

/// Serves MSGs arriving on channels of one profile.
#[async_trait]
pub trait ProfileHandler: Send + Sync + 'static {
    /// The peer asks to start a channel with this profile. Return `Ok(reply_init)` to accept,
    /// or `Err((code, message))` to refuse.
    async fn on_start(&self, _session: &Session, _channel: u32, _init: Option<&str>) -> std::result::Result<Option<String>, (u32, String)> {
        Ok(None)
    }

    /// Handle one MSG. Replies on a channel are serialized, so the reply must be complete
    /// (RPY, ERR, or ANS…NUL) before this returns.
    async fn on_message(&self, session: &Session, msg: IncomingMessage, reply: Responder);
}

#[derive(Default, Clone)]
pub struct Profiles {
    map: HashMap<String, Arc<dyn ProfileHandler>>,
}

impl Profiles {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn with(mut self, uri: impl Into<String>, handler: impl ProfileHandler) -> Self {
        self.map.insert(uri.into(), Arc::new(handler));
        self
    }
    pub fn insert(&mut self, uri: impl Into<String>, handler: Arc<dyn ProfileHandler>) {
        self.map.insert(uri.into(), handler);
    }
    pub fn uris(&self) -> Vec<String> {
        let mut v: Vec<String> = self.map.keys().cloned().collect();
        v.sort();
        v
    }
    fn get(&self, uri: &str) -> Option<Arc<dyn ProfileHandler>> {
        self.map.get(uri).cloned()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Progress {
    Idle,
    Answering(u32),
    Done,
}

/// Sends the reply to one incoming MSG.
pub struct Responder {
    shared: Arc<Shared>,
    channel: u32,
    msgno: u32,
    progress: Arc<Mutex<Progress>>,
}

impl Responder {
    pub fn channel(&self) -> u32 {
        self.channel
    }
    pub fn msgno(&self) -> u32 {
        self.msgno
    }

    fn transition(&self, to_done: bool) -> Result<Option<u32>> {
        let mut p = self.progress.lock().unwrap();
        match (*p, to_done) {
            (Progress::Done, _) => Err(Error::Protocol("reply already completed".into())),
            (Progress::Answering(_), true) | (Progress::Idle, true) => {
                *p = Progress::Done;
                Ok(None)
            }
            (Progress::Idle, false) => {
                *p = Progress::Answering(1);
                Ok(Some(0))
            }
            (Progress::Answering(n), false) => {
                *p = Progress::Answering(n + 1);
                Ok(Some(n))
            }
        }
    }

    fn require_idle(&self) -> Result<()> {
        let mut p = self.progress.lock().unwrap();
        if *p != Progress::Idle {
            return Err(Error::Protocol("RPY/ERR not allowed after ANS or completion".into()));
        }
        *p = Progress::Done;
        Ok(())
    }

    pub async fn rpy(&self, payload: Bytes) -> Result<()> {
        self.require_idle()?;
        let r = self.shared.send_message(self.channel, FrameType::Rpy, self.msgno, 0, payload).await;
        self.shared.finish_inbound(self.channel, self.msgno);
        r
    }

    pub async fn err(&self, payload: Bytes) -> Result<()> {
        self.require_idle()?;
        let r = self.shared.send_message(self.channel, FrameType::Err, self.msgno, 0, payload).await;
        self.shared.finish_inbound(self.channel, self.msgno);
        r
    }

    /// ERR carrying a standard `<error code='…'>` element.
    pub async fn err_code(&self, code: u32, message: &str) -> Result<()> {
        let xml = Mgmt::Error { code, message: message.to_string() }.to_xml();
        self.err(Entity::xml(xml).to_bytes()).await
    }

    /// Send one ANS message; returns its ansno.
    pub async fn ans(&self, payload: Bytes) -> Result<u32> {
        let ansno = self.transition(false)?.expect("ansno");
        self.shared.send_message(self.channel, FrameType::Ans, self.msgno, ansno, payload).await?;
        Ok(ansno)
    }

    /// Terminate a one-to-many reply.
    pub async fn nul(&self) -> Result<()> {
        self.transition(true)?;
        let r = self.shared.send_message(self.channel, FrameType::Nul, self.msgno, 0, Bytes::new()).await;
        self.shared.finish_inbound(self.channel, self.msgno);
        r
    }
}

/// Writes the rest of a MSG started with [`Channel::send_stream`]. Replies may arrive (and
/// should be read) while the message is still being written.
pub struct MsgSink {
    shared: Arc<Shared>,
    tx: Arc<TxWin>,
    guard: Option<tokio::sync::OwnedMutexGuard<()>>,
    channel: u32,
    msgno: u32,
}

impl MsgSink {
    pub fn msgno(&self) -> u32 {
        self.msgno
    }

    /// Send more of the message (frames with `more='*'`).
    pub async fn send(&mut self, data: Bytes) -> Result<()> {
        self.shared.write_frames(&self.tx, self.channel, FrameType::Msg, self.msgno, 0, data, false).await
    }

    /// Send the final piece and complete the message.
    pub async fn finish(mut self, data: Bytes) -> Result<()> {
        let r = self.shared.write_frames(&self.tx, self.channel, FrameType::Msg, self.msgno, 0, data, true).await;
        self.guard = None;
        r
    }
}

impl Drop for MsgSink {
    fn drop(&mut self) {
        // Dropped mid-message: end it with an empty final frame so the channel stays usable.
        // The receiver sees the message as truncated (for rpc:2 bodies, an abort).
        if let Some(guard) = self.guard.take() {
            let (shared, tx, channel, msgno) = (self.shared.clone(), self.tx.clone(), self.channel, self.msgno);
            if let Ok(rt) = tokio::runtime::Handle::try_current() {
                rt.spawn(async move {
                    let _ = shared.write_frames(&tx, channel, FrameType::Msg, msgno, 0, Bytes::new(), true).await;
                    drop(guard);
                });
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Internal state

enum Out {
    Data(Bytes),
    Close,
}

struct TxWin {
    st: Mutex<TxState>,
    notify: Notify,
    /// Held while one message is being sent, so messages never interleave on a channel.
    lock: Arc<tokio::sync::Mutex<()>>,
}

struct TxState {
    seq: u32,
    /// ackno + window from the peer's latest SEQ.
    limit: u32,
    dead: bool,
}

impl TxWin {
    fn new() -> Arc<Self> {
        Arc::new(TxWin {
            st: Mutex::new(TxState { seq: 0, limit: INITIAL_WINDOW, dead: false }),
            notify: Notify::new(),
            lock: Arc::new(tokio::sync::Mutex::new(())),
        })
    }
}

struct Outstanding {
    sink: mpsc::UnboundedSender<ReplyEvent>,
    ans_open: HashSet<u32>,
}

struct Chan {
    profile: String,
    tx: Arc<TxWin>,
    recv_seq: u32,
    adv_ack: u32,
    adv_window: u32,
    consumed: u32,
    next_msgno: u32,
    outstanding: HashMap<u32, Outstanding>,
    rx_reply: Option<(FrameType, u32)>,
    rx_msg: Option<(u32, mpsc::UnboundedSender<Piece>)>,
    inbound: HashSet<u32>,
    queue: Option<mpsc::UnboundedSender<IncomingMessage>>,
}

impl Chan {
    fn new(profile: String, next_msgno: u32) -> Self {
        Chan {
            profile,
            tx: TxWin::new(),
            recv_seq: 0,
            adv_ack: 0,
            adv_window: INITIAL_WINDOW,
            consumed: 0,
            next_msgno,
            outstanding: HashMap::new(),
            rx_reply: None,
            rx_msg: None,
            inbound: HashSet::new(),
            queue: None,
        }
    }
    fn busy(&self) -> bool {
        !self.outstanding.is_empty() || !self.inbound.is_empty() || self.rx_msg.is_some()
    }
}

struct State {
    chans: HashMap<u32, Chan>,
    next_chan: u32,
    peer_profiles: Vec<String>,
    closed: Option<Error>,
    releasing: bool,
    /// Identity the peer authenticated as (SASL).
    identity: Option<String>,
}

pub(crate) struct Shared {
    role: Role,
    cfg: Config,
    out: mpsc::UnboundedSender<Out>,
    st: Mutex<State>,
    profiles: Profiles,
    closed_tx: watch::Sender<bool>,
    me: Weak<Shared>,
}

enum Deliver {
    Piece(mpsc::UnboundedSender<Piece>, Piece),
    Reply(mpsc::UnboundedSender<ReplyEvent>, ReplyEvent),
    Incoming(mpsc::UnboundedSender<IncomingMessage>, IncomingMessage),
}

impl Shared {
    fn closed_error(&self) -> Error {
        self.st.lock().unwrap().closed.clone().unwrap_or_else(|| Error::Closed("channel closed".into()))
    }

    fn enqueue(&self, b: Bytes) {
        let _ = self.out.send(Out::Data(b));
    }

    fn fail(&self, err: Error) {
        let chans = {
            let mut st = self.st.lock().unwrap();
            if st.closed.is_some() {
                return;
            }
            tracing::debug!(role = ?self.role, "beep session ended: {err}");
            st.closed = Some(err);
            std::mem::take(&mut st.chans)
        };
        for c in chans.values() {
            c.tx.st.lock().unwrap().dead = true;
            c.tx.notify.notify_waiters();
        }
        drop(chans);
        let _ = self.out.send(Out::Close);
        let _ = self.closed_tx.send(true);
    }

    fn credit(&self, channel: u32, n: u32) {
        let mut st = self.st.lock().unwrap();
        let window = self.cfg.recv_window;
        let Some(ch) = st.chans.get_mut(&channel) else { return };
        ch.consumed = ch.consumed.wrapping_add(n);
        if ch.consumed.wrapping_sub(ch.adv_ack) >= window / 2 {
            ch.adv_ack = ch.consumed;
            ch.adv_window = window;
            self.enqueue(encode_seq(&SeqFrame { channel, ackno: ch.consumed, window }));
        }
    }

    /// Advertise our full receive window on a freshly opened channel.
    fn open_window(&self, st: &mut State, channel: u32) {
        let window = self.cfg.recv_window;
        if let Some(ch) = st.chans.get_mut(&channel) {
            ch.adv_ack = ch.consumed;
            ch.adv_window = window;
            self.enqueue(encode_seq(&SeqFrame { channel, ackno: ch.consumed, window }));
        }
    }

    fn finish_inbound(&self, channel: u32, msgno: u32) {
        if let Some(ch) = self.st.lock().unwrap().chans.get_mut(&channel) {
            ch.inbound.remove(&msgno);
        }
    }

    fn on_frame(&self, f: Frame) -> Result<()> {
        match f {
            Frame::Seq(s) => {
                let tx = self.st.lock().unwrap().chans.get(&s.channel).map(|c| c.tx.clone());
                if let Some(tx) = tx {
                    tx.st.lock().unwrap().limit = s.ackno.wrapping_add(s.window);
                    tx.notify.notify_waiters();
                }
                Ok(())
            }
            Frame::Data(d) => {
                let mut deliveries = Vec::with_capacity(2);
                let r = self.on_data(d, &mut deliveries);
                // Deliver outside the lock: a failed send drops a Chunk, whose Credit locks state.
                for d in deliveries {
                    match d {
                        Deliver::Piece(tx, p) => drop(tx.send(p)),
                        Deliver::Reply(tx, ev) => drop(tx.send(ev)),
                        Deliver::Incoming(tx, m) => drop(tx.send(m)),
                    }
                }
                r
            }
        }
    }

    fn on_data(&self, d: DataFrame, out: &mut Vec<Deliver>) -> Result<()> {
        let mut st = self.st.lock().unwrap();
        let ch = st
            .chans
            .get_mut(&d.channel)
            .ok_or_else(|| Error::Protocol(format!("frame on unknown channel {}", d.channel)))?;
        if d.seqno != ch.recv_seq {
            return Err(Error::Protocol(format!(
                "channel {}: expected seqno {}, got {}",
                d.channel, ch.recv_seq, d.seqno
            )));
        }
        let size = d.payload.len() as u32;
        let end = ch.recv_seq.wrapping_add(size);
        if end.wrapping_sub(ch.adv_ack) > ch.adv_window {
            return Err(Error::Protocol(format!("channel {}: frame exceeds receive window", d.channel)));
        }
        ch.recv_seq = end;
        // Build the Chunk only once validation passed: dropping it under the lock would deadlock.
        let me = self.me.clone();
        let channel = d.channel;
        let payload = d.payload;
        let chunk = move || Chunk { data: payload, _credit: Credit { shared: me, channel, n: size } };
        let last = !d.more;
        match d.ftype {
            FrameType::Msg => {
                match &ch.rx_msg {
                    Some((m, _)) if *m != d.msgno => {
                        return Err(Error::Protocol(format!("channel {}: interleaved MSG frames", d.channel)))
                    }
                    Some(_) => {}
                    None => {
                        if !ch.inbound.insert(d.msgno) {
                            return Err(Error::Protocol(format!(
                                "channel {}: msgno {} reused while still outstanding",
                                d.channel, d.msgno
                            )));
                        }
                        let (tx, rx) = mpsc::unbounded_channel();
                        ch.rx_msg = Some((d.msgno, tx));
                        let shared = self.me.upgrade().expect("shared alive");
                        let msg = IncomingMessage { channel: d.channel, msgno: d.msgno, rx, done: false, shared };
                        if let Some(q) = &ch.queue {
                            out.push(Deliver::Incoming(q.clone(), msg));
                        }
                    }
                }
                let tx = ch.rx_msg.as_ref().unwrap().1.clone();
                out.push(Deliver::Piece(tx.clone(), Piece::Data(chunk())));
                if last {
                    ch.rx_msg = None;
                    out.push(Deliver::Piece(tx, Piece::End));
                }
            }
            FrameType::Rpy | FrameType::Err => {
                if let Some((t, m)) = ch.rx_reply {
                    if t != d.ftype || m != d.msgno {
                        return Err(Error::Protocol(format!("channel {}: interleaved reply frames", d.channel)));
                    }
                }
                let o = ch.outstanding.get(&d.msgno).ok_or_else(|| {
                    Error::Protocol(format!("channel {}: reply to unknown msgno {}", d.channel, d.msgno))
                })?;
                if !o.ans_open.is_empty() {
                    return Err(Error::Protocol("RPY/ERR while ANS in progress".into()));
                }
                let ev = if d.ftype == FrameType::Rpy {
                    ReplyEvent::Rpy { data: chunk(), last }
                } else {
                    ReplyEvent::Err { data: chunk(), last }
                };
                out.push(Deliver::Reply(o.sink.clone(), ev));
                if last {
                    ch.rx_reply = None;
                    ch.outstanding.remove(&d.msgno);
                } else {
                    ch.rx_reply = Some((d.ftype, d.msgno));
                }
            }
            FrameType::Ans => {
                if ch.rx_reply.is_some() {
                    return Err(Error::Protocol("ANS while RPY/ERR in progress".into()));
                }
                let o = ch.outstanding.get_mut(&d.msgno).ok_or_else(|| {
                    Error::Protocol(format!("channel {}: ANS to unknown msgno {}", d.channel, d.msgno))
                })?;
                if last {
                    o.ans_open.remove(&d.ansno);
                } else {
                    o.ans_open.insert(d.ansno);
                }
                out.push(Deliver::Reply(o.sink.clone(), ReplyEvent::Ans { ansno: d.ansno, data: chunk(), last }));
            }
            FrameType::Nul => {
                let o = ch.outstanding.remove(&d.msgno).ok_or_else(|| {
                    Error::Protocol(format!("channel {}: NUL to unknown msgno {}", d.channel, d.msgno))
                })?;
                if !o.ans_open.is_empty() {
                    return Err(Error::Protocol("NUL before all ANS messages completed".into()));
                }
                drop(chunk);
                out.push(Deliver::Reply(o.sink, ReplyEvent::Nul));
            }
        }
        Ok(())
    }

    fn tx_for(&self, channel: u32) -> Result<Arc<TxWin>> {
        let st = self.st.lock().unwrap();
        if let Some(e) = &st.closed {
            return Err(e.clone());
        }
        st.chans
            .get(&channel)
            .map(|c| c.tx.clone())
            .ok_or_else(|| Error::Closed(format!("channel {channel} is not open")))
    }

    /// Send a complete message, fragmenting it to fit the peer's window and our frame size.
    async fn send_message(&self, channel: u32, ftype: FrameType, msgno: u32, ansno: u32, payload: Bytes) -> Result<()> {
        let tx = self.tx_for(channel)?;
        let _guard = tx.lock.lock().await;
        self.write_frames(&tx, channel, ftype, msgno, ansno, payload, true).await
    }

    /// Send `payload` as frames of one message. With `last == false` every frame carries
    /// `more='*'` (the message continues); the caller must hold `tx.lock`.
    #[allow(clippy::too_many_arguments)]
    async fn write_frames(&self, tx: &TxWin, channel: u32, ftype: FrameType, msgno: u32, ansno: u32, payload: Bytes, last: bool) -> Result<()> {
        if !last && payload.is_empty() {
            return Ok(());
        }
        let max_frame = self.cfg.max_frame_size.max(1) as usize;
        let mut off = 0usize;
        loop {
            let notified = tx.notify.notified();
            {
                let mut ts = tx.st.lock().unwrap();
                if ts.dead {
                    drop(ts);
                    return Err(self.closed_error());
                }
                let avail = ts.limit.wrapping_sub(ts.seq) as i32;
                let remaining = payload.len() - off;
                if remaining == 0 || avail > 0 {
                    let n = remaining.min(avail.max(0) as usize).min(max_frame);
                    let frame = DataFrame {
                        ftype,
                        channel,
                        msgno,
                        more: off + n < payload.len() || !last,
                        seqno: ts.seq,
                        ansno,
                        payload: payload.slice(off..off + n),
                    };
                    ts.seq = ts.seq.wrapping_add(n as u32);
                    self.enqueue(encode_data(&frame));
                    off += n;
                    if off >= payload.len() {
                        return Ok(());
                    }
                    continue;
                }
            }
            notified.await;
        }
    }

    /// Reserve a msgno for a MSG that is sent incrementally; `head` goes out at once.
    async fn send_msg_stream(&self, channel: u32, head: Bytes) -> Result<(MsgSink, ReplyStream)> {
        let tx = self.tx_for(channel)?;
        let guard = tx.lock.clone().lock_owned().await;
        let (sink, rx) = mpsc::unbounded_channel();
        let msgno = {
            let mut st = self.st.lock().unwrap();
            if let Some(e) = &st.closed {
                return Err(e.clone());
            }
            let ch = st.chans.get_mut(&channel).ok_or_else(|| Error::Closed(format!("channel {channel} is not open")))?;
            let mut n = ch.next_msgno;
            while ch.outstanding.contains_key(&n) {
                n = if n >= MAX_CHANNEL { 0 } else { n + 1 };
            }
            ch.next_msgno = if n >= MAX_CHANNEL { 0 } else { n + 1 };
            ch.outstanding.insert(n, Outstanding { sink, ans_open: HashSet::new() });
            n
        };
        let shared = self.me.upgrade().expect("alive");
        let msg = MsgSink { shared: shared.clone(), tx, guard: Some(guard), channel, msgno };
        let stream = ReplyStream { rx, done: false, shared };
        if let Err(e) = self.write_frames(&msg.tx, channel, FrameType::Msg, msgno, 0, head, false).await {
            let mut msg = msg;
            msg.guard = None; // nothing to terminate: the session is gone
            let o = self.st.lock().unwrap().chans.get_mut(&channel).and_then(|c| c.outstanding.remove(&msgno));
            drop(o);
            return Err(e);
        }
        Ok((msg, stream))
    }

    /// Reserve a msgno, register the reply sink, and queue the MSG for transmission.
    ///
    /// Transmission runs in the background so a caller can pipeline large MSGs and read
    /// replies concurrently; waiting for each MSG to drain first could deadlock both windows.
    async fn send_msg(&self, channel: u32, payload: Bytes) -> Result<ReplyStream> {
        let (sink, rx) = mpsc::unbounded_channel();
        let msgno = {
            let mut st = self.st.lock().unwrap();
            if let Some(e) = &st.closed {
                return Err(e.clone());
            }
            let ch = st.chans.get_mut(&channel).ok_or_else(|| Error::Closed(format!("channel {channel} is not open")))?;
            let mut n = ch.next_msgno;
            while ch.outstanding.contains_key(&n) {
                n = if n >= MAX_CHANNEL { 0 } else { n + 1 };
            }
            ch.next_msgno = if n >= MAX_CHANNEL { 0 } else { n + 1 };
            ch.outstanding.insert(n, Outstanding { sink, ans_open: HashSet::new() });
            n
        };
        let shared = self.me.upgrade().expect("alive");
        let stream = ReplyStream { rx, done: false, shared: shared.clone() };
        tokio::spawn(async move {
            if shared.send_message(channel, FrameType::Msg, msgno, 0, payload).await.is_err() {
                // Unblock the waiting ReplyStream.
                let o = shared.st.lock().unwrap().chans.get_mut(&channel).and_then(|c| c.outstanding.remove(&msgno));
                drop(o);
            }
        });
        Ok(stream)
    }

    async fn mgmt_request(&self, m: &Mgmt) -> Result<Mgmt> {
        let stream = self.send_msg(0, Entity::xml(m.to_xml()).to_bytes()).await?;
        let reply = stream.collect(1 << 20).await?;
        match reply {
            Reply::Rpy(b) | Reply::Err(b) => Mgmt::parse(&Entity::parse(b).body),
            Reply::Ans(_) => Err(Error::Protocol("ANS on channel 0".into())),
        }
    }
}

// ---------------------------------------------------------------------------
// Session

/// A BEEP session. Cheap to clone; all clones refer to the same connection.
#[derive(Clone)]
pub struct Session {
    shared: Arc<Shared>,
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session").field("role", &self.shared.role).field("closed", &self.is_closed()).finish()
    }
}

impl Session {
    /// Connect as initiator over TCP.
    pub async fn connect(addr: impl ToSocketAddrs, cfg: Config, profiles: Profiles) -> Result<Session> {
        let tcp = TcpStream::connect(addr).await?;
        let _ = tcp.set_nodelay(true);
        Session::establish(tcp, Role::Initiator, cfg, profiles).await
    }

    /// Run BEEP over any byte stream (TCP, TLS, pipes…). Returns once greetings are exchanged.
    pub async fn establish<S>(stream: S, role: Role, cfg: Config, profiles: Profiles) -> Result<Session>
    where
        S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        let (out_tx, out_rx) = mpsc::unbounded_channel();
        let (closed_tx, _) = watch::channel(false);
        let mut chans = HashMap::new();
        chans.insert(0, Chan::new(String::new(), 1));
        let shared = Arc::new_cyclic(|me| Shared {
            role,
            cfg: cfg.clone(),
            out: out_tx,
            st: Mutex::new(State {
                chans,
                next_chan: if role == Role::Initiator { 1 } else { 2 },
                peer_profiles: vec![],
                closed: None,
                releasing: false,
                identity: None,
            }),
            profiles,
            closed_tx,
            me: me.clone(),
        });

        // Channel 0: our greeting answers the peer's implicit MSG 0; theirs answers ours.
        let (mgmt_tx, mgmt_rx) = mpsc::unbounded_channel();
        let (greet_sink, greet_rx) = mpsc::unbounded_channel();
        {
            let mut st = shared.st.lock().unwrap();
            let ch0 = st.chans.get_mut(&0).unwrap();
            ch0.queue = Some(mgmt_tx);
            ch0.outstanding.insert(0, Outstanding { sink: greet_sink, ans_open: HashSet::new() });
        }

        let (rd, wr) = tokio::io::split(stream);
        tokio::spawn(writer_task(wr, out_rx, Arc::downgrade(&shared)));
        tokio::spawn(reader_task(rd, shared.clone()));

        let greeting = Mgmt::Greeting { profiles: shared.profiles.uris(), features: None, localize: None };
        shared.send_message(0, FrameType::Rpy, 0, 0, Entity::xml(greeting.to_xml()).to_bytes()).await?;
        {
            let mut st = shared.st.lock().unwrap();
            shared.open_window(&mut st, 0);
        }

        let greet = ReplyStream { rx: greet_rx, done: false, shared: shared.clone() };
        let reply = match tokio::time::timeout(cfg.greeting_timeout, greet.collect(1 << 20)).await {
            Ok(r) => r,
            Err(_) => {
                shared.fail(Error::Timeout);
                return Err(Error::Timeout);
            }
        };
        let fail = |e: Error| {
            shared.fail(e.clone());
            Err(e)
        };
        match reply {
            Ok(Reply::Rpy(b)) => match Mgmt::parse(&Entity::parse(b).body) {
                Ok(Mgmt::Greeting { profiles, .. }) => shared.st.lock().unwrap().peer_profiles = profiles,
                Ok(other) => return fail(Error::Protocol(format!("expected greeting, got {other:?}"))),
                Err(e) => return fail(e),
            },
            Ok(Reply::Err(b)) => {
                let e = match Mgmt::parse(&Entity::parse(b).body) {
                    Ok(Mgmt::Error { code, message }) => Error::Refused { code, message },
                    _ => Error::Refused { code: code::SERVICE_NOT_AVAILABLE, message: "peer refused session".into() },
                };
                return fail(e);
            }
            Ok(Reply::Ans(_)) => return fail(Error::Protocol("greeting sent as ANS".into())),
            Err(e) => return fail(e),
        }

        let session = Session { shared };
        tokio::spawn(mgmt_task(session.clone(), mgmt_rx));
        Ok(session)
    }

    pub fn role(&self) -> Role {
        self.shared.role
    }

    pub fn config(&self) -> &Config {
        &self.shared.cfg
    }

    /// Profiles the peer advertised in its greeting.
    pub fn peer_profiles(&self) -> Vec<String> {
        self.shared.st.lock().unwrap().peer_profiles.clone()
    }

    /// Identity the peer authenticated as on this session (SASL), if any.
    pub fn identity(&self) -> Option<String> {
        self.shared.st.lock().unwrap().identity.clone()
    }

    pub(crate) fn set_identity(&self, id: String) {
        self.shared.st.lock().unwrap().identity = Some(id);
    }

    pub fn is_closed(&self) -> bool {
        self.shared.st.lock().unwrap().closed.is_some()
    }

    /// Resolves when the session has ended, with the reason.
    pub async fn closed(&self) -> Error {
        let mut rx = self.shared.closed_tx.subscribe();
        loop {
            if let Some(e) = self.shared.st.lock().unwrap().closed.clone() {
                return e;
            }
            if rx.changed().await.is_err() {
                return self.shared.closed_error();
            }
        }
    }

    /// Number of open channels, excluding channel 0.
    pub fn channel_count(&self) -> usize {
        self.shared.st.lock().unwrap().chans.len().saturating_sub(1)
    }

    /// Start a channel offering one profile.
    pub async fn start_channel(&self, uri: &str) -> Result<Channel> {
        self.start_channel_with(vec![ProfileEl::new(uri)], None).await.map(|(c, _)| c)
    }

    /// Start a channel offering several profiles (peer picks the first it supports),
    /// optionally with piggybacked init data. Returns the channel and the peer's init reply.
    pub async fn start_channel_with(&self, profiles: Vec<ProfileEl>, server_name: Option<String>) -> Result<(Channel, Option<String>)> {
        let number = {
            let mut st = self.shared.st.lock().unwrap();
            if let Some(e) = &st.closed {
                return Err(e.clone());
            }
            let mut n = st.next_chan;
            let mut tries = 0u32;
            while st.chans.contains_key(&n) {
                n = next_channel_number(n);
                tries += 1;
                if tries > 1_000_000 {
                    return Err(Error::Closed("no free channel numbers".into()));
                }
            }
            st.next_chan = next_channel_number(n);
            st.chans.insert(n, Chan::new(String::new(), 0));
            n
        };
        let req = Mgmt::Start { number, server_name, profiles };
        let res = self.shared.mgmt_request(&req).await;
        let mut st = self.shared.st.lock().unwrap();
        match res {
            Ok(Mgmt::Profile(p)) => {
                let handler = self.shared.profiles.get(&p.uri);
                let ch = st.chans.get_mut(&number).ok_or_else(|| self.shared.closed_error())?;
                ch.profile = p.uri.clone();
                if let Some(h) = handler {
                    let (qtx, qrx) = mpsc::unbounded_channel();
                    ch.queue = Some(qtx);
                    tokio::spawn(channel_task(self.clone(), number, h, qrx));
                } else {
                    let (qtx, qrx) = mpsc::unbounded_channel();
                    ch.queue = Some(qtx);
                    tokio::spawn(reject_task(self.clone(), qrx));
                }
                self.shared.open_window(&mut st, number);
                Ok((Channel { session: self.clone(), number, profile: p.uri }, p.data))
            }
            Ok(Mgmt::Error { code, message }) => {
                st.chans.remove(&number);
                Err(Error::Refused { code, message })
            }
            Ok(other) => {
                st.chans.remove(&number);
                Err(Error::Protocol(format!("unexpected reply to <start>: {other:?}")))
            }
            Err(e) => {
                st.chans.remove(&number);
                Err(e)
            }
        }
    }

    /// Gracefully release the session (close channel 0).
    pub async fn close(&self) -> Result<()> {
        if self.is_closed() {
            return Ok(());
        }
        let req = Mgmt::Close { number: 0, code: code::SUCCESS, message: None };
        match tokio::time::timeout(self.shared.cfg.close_timeout, self.shared.mgmt_request(&req)).await {
            Ok(Ok(Mgmt::Ok)) => {
                self.shared.fail(Error::Closed("session released".into()));
                Ok(())
            }
            Ok(Ok(Mgmt::Error { code, message })) => Err(Error::Refused { code, message }),
            Ok(Ok(other)) => Err(Error::Protocol(format!("unexpected reply to <close>: {other:?}"))),
            Ok(Err(e)) => {
                self.abort();
                Err(e)
            }
            Err(_) => {
                self.abort();
                Err(Error::Timeout)
            }
        }
    }

    /// Drop the connection immediately.
    pub fn abort(&self) {
        self.shared.fail(Error::Closed("session aborted".into()));
    }

    async fn close_channel(&self, number: u32, code: u32) -> Result<()> {
        if self.shared.st.lock().unwrap().chans.get(&number).map(|c| c.busy()).unwrap_or(false) {
            return Err(Error::Refused { code: code::ACTION_NOT_TAKEN, message: "still working".into() });
        }
        match self.shared.mgmt_request(&Mgmt::Close { number, code, message: None }).await? {
            Mgmt::Ok => {
                let removed = self.shared.st.lock().unwrap().chans.remove(&number);
                if let Some(c) = removed {
                    c.tx.st.lock().unwrap().dead = true;
                    c.tx.notify.notify_waiters();
                }
                Ok(())
            }
            Mgmt::Error { code, message } => Err(Error::Refused { code, message }),
            other => Err(Error::Protocol(format!("unexpected reply to <close>: {other:?}"))),
        }
    }
}

/// An open channel.
#[derive(Clone)]
pub struct Channel {
    session: Session,
    number: u32,
    profile: String,
}

impl std::fmt::Debug for Channel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Channel({}, {})", self.number, self.profile)
    }
}

impl Channel {
    pub fn number(&self) -> u32 {
        self.number
    }
    pub fn profile(&self) -> &str {
        &self.profile
    }
    pub fn session(&self) -> &Session {
        &self.session
    }
    pub fn is_open(&self) -> bool {
        self.session.shared.st.lock().unwrap().chans.contains_key(&self.number)
    }
    /// Messages sent on this channel still awaiting a complete reply.
    pub fn pending(&self) -> usize {
        self.session.shared.st.lock().unwrap().chans.get(&self.number).map(|c| c.outstanding.len()).unwrap_or(0)
    }

    /// Send a MSG; returns the stream of reply events.
    pub async fn send(&self, payload: Bytes) -> Result<ReplyStream> {
        self.session.shared.send_msg(self.number, payload).await
    }

    /// Start a MSG whose payload is written incrementally: `head` is sent now, the rest
    /// through the returned [`MsgSink`]. Replies arrive on the [`ReplyStream`] meanwhile.
    pub async fn send_stream(&self, head: Bytes) -> Result<(MsgSink, ReplyStream)> {
        self.session.shared.send_msg_stream(self.number, head).await
    }

    /// Send a MSG and collect the whole reply.
    pub async fn request(&self, payload: Bytes) -> Result<Reply> {
        let max = self.session.shared.cfg.max_message_size;
        self.send(payload).await?.collect(max).await
    }

    pub async fn close(&self) -> Result<()> {
        self.session.close_channel(self.number, code::SUCCESS).await
    }
}

/// Next number with the same parity, wrapping before 2^31.
fn next_channel_number(n: u32) -> u32 {
    if n + 2 > MAX_CHANNEL {
        2 - n % 2
    } else {
        n + 2
    }
}

// ---------------------------------------------------------------------------
// Tasks

async fn writer_task<W: AsyncWrite + Unpin>(w: W, mut rx: mpsc::UnboundedReceiver<Out>, shared: Weak<Shared>) {
    let mut w = BufWriter::with_capacity(64 * 1024, w);
    let result: std::io::Result<()> = async {
        while let Some(item) = rx.recv().await {
            let mut item = Some(item);
            loop {
                match item.take() {
                    Some(Out::Data(b)) => w.write_all(&b).await?,
                    Some(Out::Close) => {
                        w.flush().await?;
                        let _ = w.shutdown().await;
                        return Ok(());
                    }
                    None => break,
                }
                item = rx.try_recv().ok();
            }
            w.flush().await?;
        }
        Ok(())
    }
    .await;
    if let Err(e) = result {
        if let Some(s) = shared.upgrade() {
            s.fail(e.into());
        }
    }
}

async fn reader_task<R: AsyncRead + Unpin>(mut r: R, shared: Arc<Shared>) {
    let mut buf = BytesMut::with_capacity(64 * 1024);
    let mut dec = FrameDecoder::new(shared.cfg.recv_window.max(INITIAL_WINDOW) as usize);
    let mut closed_rx = shared.closed_tx.subscribe();
    loop {
        loop {
            match dec.decode(&mut buf) {
                Ok(Some(f)) => {
                    if let Err(e) = shared.on_frame(f) {
                        tracing::warn!("beep: {e}; dropping connection");
                        shared.fail(e);
                        return;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    tracing::warn!("beep: {e}; dropping connection");
                    shared.fail(e);
                    return;
                }
            }
        }
        tokio::select! {
            n = r.read_buf(&mut buf) => match n {
                Ok(0) => {
                    let released = shared.st.lock().unwrap().releasing;
                    shared.fail(Error::Closed(if released { "session released".into() } else { "connection closed by peer".into() }));
                    return;
                }
                Ok(_) => {}
                Err(e) => {
                    shared.fail(e.into());
                    return;
                }
            },
            _ = closed_rx.changed() => return,
        }
    }
}

/// Serves channel-0 requests (start/close) from the peer, in order.
async fn mgmt_task(session: Session, mut rx: mpsc::UnboundedReceiver<IncomingMessage>) {
    let shared = session.shared.clone();
    while let Some(msg) = rx.recv().await {
        let msgno = msg.msgno;
        let responder = Responder { shared: shared.clone(), channel: 0, msgno, progress: Arc::new(Mutex::new(Progress::Idle)) };
        let body = match msg.collect(1 << 20).await {
            Ok(b) => b,
            Err(_) => return,
        };
        let mut opened = None;
        let reply = match Mgmt::parse(&Entity::parse(body).body) {
            Ok(Mgmt::Start { number, profiles, .. }) => {
                let r = handle_start(&session, number, profiles).await;
                if r.is_ok() {
                    opened = Some(number);
                }
                r
            }
            Ok(Mgmt::Close { number, .. }) => handle_close(&session, number),
            Ok(other) => Err((code::PARAM_SYNTAX_ERROR, format!("unexpected element {other:?}"))),
            Err(e) => Err((code::SYNTAX_ERROR, e.to_string())),
        };
        let release = matches!(&reply, Ok(Mgmt::Ok)) && shared.st.lock().unwrap().releasing;
        let sent = match reply {
            Ok(m) => responder.rpy(Entity::xml(m.to_xml()).to_bytes()).await,
            Err((c, m)) => responder.err_code(c, &m).await,
        };
        if sent.is_err() {
            return;
        }
        if let Some(number) = opened {
            // The SEQ for a new channel must reach the peer after the RPY that creates it.
            let mut st = shared.st.lock().unwrap();
            shared.open_window(&mut st, number);
        }
        if release {
            // Flush our <ok/>, then half-close; the reader sees EOF when the peer closes.
            let _ = shared.out.send(Out::Close);
            return;
        }
    }
}

async fn handle_start(session: &Session, number: u32, profiles: Vec<ProfileEl>) -> std::result::Result<Mgmt, (u32, String)> {
    let shared = &session.shared;
    let peer_parity = if shared.role == Role::Listener { 1 } else { 0 };
    {
        let st = shared.st.lock().unwrap();
        if st.releasing {
            return Err((code::ACTION_NOT_TAKEN, "session is being released".into()));
        }
        if number == 0 || number % 2 != peer_parity || st.chans.contains_key(&number) {
            return Err((code::PARAM_INVALID, format!("channel number {number} is invalid or in use")));
        }
    }
    let Some((p, handler)) = profiles.iter().find_map(|p| shared.profiles.get(&p.uri).map(|h| (p, h))) else {
        return Err((code::ACTION_NOT_TAKEN, "no acceptable profile".into()));
    };
    if shared.cfg.require_auth && !p.uri.starts_with(crate::sasl::SASL_PREFIX) && session.identity().is_none() {
        return Err((code::AUTH_REQUIRED, "authentication required".into()));
    }
    let init_reply = handler.on_start(session, number, p.data.as_deref()).await?;
    let (qtx, qrx) = mpsc::unbounded_channel();
    {
        let mut st = shared.st.lock().unwrap();
        if st.chans.contains_key(&number) {
            return Err((code::PARAM_INVALID, format!("channel number {number} is in use")));
        }
        let mut ch = Chan::new(p.uri.clone(), 0);
        ch.queue = Some(qtx);
        st.chans.insert(number, ch);
    }
    tokio::spawn(channel_task(session.clone(), number, handler, qrx));
    Ok(Mgmt::Profile(ProfileEl { uri: p.uri.clone(), data: init_reply, base64: false }))
}

fn handle_close(session: &Session, number: u32) -> std::result::Result<Mgmt, (u32, String)> {
    let shared = &session.shared;
    let mut st = shared.st.lock().unwrap();
    if number == 0 {
        if st.chans.iter().any(|(n, c)| *n != 0 && c.busy()) {
            return Err((code::ACTION_NOT_TAKEN, "still working".into()));
        }
        st.releasing = true;
        return Ok(Mgmt::Ok);
    }
    match st.chans.get(&number) {
        None => Err((code::PARAM_INVALID, format!("channel {number} is not open"))),
        Some(c) if c.busy() => Err((code::ACTION_NOT_TAKEN, "still working".into())),
        Some(_) => {
            let c = st.chans.remove(&number).unwrap();
            c.tx.st.lock().unwrap().dead = true;
            c.tx.notify.notify_waiters();
            Ok(Mgmt::Ok)
        }
    }
}

/// Runs one channel's handler; MSGs are processed strictly in arrival order.
async fn channel_task(session: Session, channel: u32, handler: Arc<dyn ProfileHandler>, mut rx: mpsc::UnboundedReceiver<IncomingMessage>) {
    while let Some(msg) = rx.recv().await {
        let progress = Arc::new(Mutex::new(Progress::Idle));
        let responder = Responder { shared: session.shared.clone(), channel, msgno: msg.msgno, progress: progress.clone() };
        let msgno = msg.msgno;
        handler.on_message(&session, msg, responder).await;
        let state = *progress.lock().unwrap();
        let fallback = Responder { shared: session.shared.clone(), channel, msgno, progress };
        let r = match state {
            Progress::Idle => fallback.err_code(code::TRANSACTION_FAILED, "handler produced no reply").await,
            Progress::Answering(_) => fallback.nul().await,
            Progress::Done => Ok(()),
        };
        if r.is_err() && session.is_closed() {
            return;
        }
    }
}

/// Channels we started for a profile we cannot serve: refuse any MSG the peer sends.
async fn reject_task(session: Session, mut rx: mpsc::UnboundedReceiver<IncomingMessage>) {
    while let Some(msg) = rx.recv().await {
        let responder = Responder {
            shared: session.shared.clone(),
            channel: msg.channel,
            msgno: msg.msgno,
            progress: Arc::new(Mutex::new(Progress::Idle)),
        };
        drop(msg);
        if responder.err_code(code::ACTION_NOT_TAKEN, "this peer does not serve the profile").await.is_err() {
            return;
        }
    }
}

// ---------------------------------------------------------------------------
// Listener

/// Accepts BEEP sessions over TCP.
pub struct Listener {
    tcp: TcpListener,
    cfg: Config,
    profiles: Profiles,
    #[cfg(feature = "tls")]
    tls: Option<tokio_rustls::TlsAcceptor>,
}

impl Listener {
    pub async fn bind(addr: impl ToSocketAddrs, cfg: Config, profiles: Profiles) -> Result<Listener> {
        Ok(Listener {
            tcp: TcpListener::bind(addr).await?,
            cfg,
            profiles,
            #[cfg(feature = "tls")]
            tls: None,
        })
    }

    /// Require the TLS tuning profile on every session before anything else.
    #[cfg(feature = "tls")]
    pub fn with_tls(mut self, opts: &crate::tls::TlsServerOptions) -> Result<Listener> {
        self.tls = Some(tokio_rustls::TlsAcceptor::from(crate::tls::server_config(opts)?));
        Ok(self)
    }

    async fn handshake(tcp: TcpStream, cfg: Config, profiles: Profiles, #[cfg(feature = "tls")] tls: Option<tokio_rustls::TlsAcceptor>) -> Result<Session> {
        let _ = tcp.set_nodelay(true);
        #[cfg(feature = "tls")]
        if let Some(acceptor) = tls {
            let stream = crate::tls::start_tls_server(tcp, &acceptor).await?;
            return Session::establish(stream, Role::Listener, cfg, profiles).await;
        }
        Session::establish(tcp, Role::Listener, cfg, profiles).await
    }

    pub fn local_addr(&self) -> Result<std::net::SocketAddr> {
        Ok(self.tcp.local_addr()?)
    }

    /// Accept one TCP connection and run the greeting exchange.
    pub async fn accept(&self) -> Result<Session> {
        let (tcp, _) = self.tcp.accept().await?;
        Self::handshake(
            tcp,
            self.cfg.clone(),
            self.profiles.clone(),
            #[cfg(feature = "tls")]
            self.tls.clone(),
        )
        .await
    }

    /// Accept connections forever, each in its own task.
    pub async fn serve(self) -> Result<()> {
        loop {
            let (tcp, peer) = match self.tcp.accept().await {
                Ok(c) => c,
                Err(e) => {
                    // Transient accept errors (EMFILE, aborted handshakes) must not kill the server.
                    tracing::warn!("accept failed: {e}");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            };
            let cfg = self.cfg.clone();
            let profiles = self.profiles.clone();
            #[cfg(feature = "tls")]
            let tls = self.tls.clone();
            tokio::spawn(async move {
                let est = Self::handshake(
                    tcp,
                    cfg,
                    profiles,
                    #[cfg(feature = "tls")]
                    tls,
                );
                match est.await {
                    Ok(s) => {
                        tracing::debug!(%peer, "beep session established");
                        let reason = s.closed().await;
                        tracing::debug!(%peer, "beep session ended: {reason}");
                    }
                    Err(e) => tracing::debug!(%peer, "beep handshake failed: {e}"),
                }
            });
        }
    }
}
