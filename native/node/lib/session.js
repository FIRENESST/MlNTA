'use strict';
// BEEP session engine (RFC 3080/3081) for Node.js — no dependencies.

const net = require('net');
const tls = require('tls');
const fs = require('fs');
const { EventEmitter, once } = require('events');
const W = require('./wire');

const MASK = 0xffffffff;
const INITIAL_WINDOW = 4096;
const MAX_CHANNEL = 2147483647;
const TLS_PROFILE = 'http://iana.org/beep/TLS';

class BeepError extends Error {
  constructor(msg) { super(msg); this.name = 'BeepError'; }
}
class SessionClosed extends BeepError {
  constructor(msg) { super(msg); this.name = 'SessionClosed'; }
}
class Refused extends BeepError {
  constructor(code, message) {
    super(`refused by peer (${code}): ${message}`);
    this.name = 'Refused';
    this.code = code;
    this.beepMessage = message;
  }
}

const DEFAULT_CONFIG = {
  recvWindow: 262144,
  maxFrameSize: 65536,
  maxMessageSize: 64 * 1024 * 1024,
  greetingTimeout: 10000,
  closeTimeout: 10000,
};

class AsyncQueue {
  constructor() { this.items = []; this.waiters = []; }
  push(x) {
    const w = this.waiters.shift();
    if (w) w(x); else this.items.push(x);
  }
  shift() {
    if (this.items.length) return Promise.resolve(this.items.shift());
    return new Promise((r) => this.waiters.push(r));
  }
  get length() { return this.items.length; }
  drain() { const it = this.items; this.items = []; return it; }
}

function withTimeout(p, ms, what) {
  if (!ms) return p;
  let t;
  return Promise.race([
    p,
    new Promise((_, rej) => { t = setTimeout(() => rej(Object.assign(new BeepError(`${what} timed out`), { timeout: true })), ms); }),
  ]).finally(() => clearTimeout(t));
}

/** Events for one message; window credit is returned as items are consumed. */
class Sink {
  constructor(session, channel) {
    this.session = session;
    this.channel = channel;
    this.queue = new AsyncQueue();
    this.discarding = false;
    this.ansOpen = new Set();
  }
  deliver(item, n) {
    if (this.discarding) { this.session._credit(this.channel, n); return; }
    this.queue.push([item, n]);
  }
  discard() {
    this.discarding = true;
    for (const [, n] of this.queue.drain()) this.session._credit(this.channel, n);
  }
}

/** Reply events: {type:'RPY'|'ERR', data, last} | {type:'ANS', ansno, data, last} | {type:'NUL'} */
class ReplyStream {
  constructor(session, sink) {
    this._s = session;
    this._sink = sink;
    this.done = false;
  }
  async next() {
    if (this.done) return null;
    const [item, n] = await this._sink.queue.shift();
    this._s._credit(this._sink.channel, n);
    if (item.type === 'CLOSED') { this.done = true; throw this._s._closedError(); }
    if (item.type === 'NUL' || ((item.type === 'RPY' || item.type === 'ERR') && item.last)) this.done = true;
    return item;
  }
  async collect(max = 64 * 1024 * 1024) {
    const parts = [];
    const ans = new Map();
    let kind = 'RPY';
    let total = 0;
    for (let ev; (ev = await this.next()) !== null;) {
      if (ev.type === 'RPY' || ev.type === 'ERR') { kind = ev.type; parts.push(ev.data); total += ev.data.length; }
      else if (ev.type === 'ANS') {
        kind = 'ANS';
        if (!ans.has(ev.ansno)) ans.set(ev.ansno, []);
        ans.get(ev.ansno).push(ev.data);
        total += ev.data.length;
      } else if (ev.type === 'NUL') kind = 'ANS';
      if (total > max) { this.discard(); throw new BeepError(`reply exceeds ${max} bytes`); }
    }
    if (kind === 'ANS') return { kind, payload: [...ans.keys()].sort((a, b) => a - b).map((k) => [k, Buffer.concat(ans.get(k))]) };
    return { kind, payload: Buffer.concat(parts) };
  }
  discard() { this._sink.discard(); }
}

class IncomingMessage {
  constructor(session, channel, msgno) {
    this.channel = channel;
    this.msgno = msgno;
    this._s = session;
    this._sink = new Sink(session, channel);
    this._done = false;
  }
  async read() {
    if (this._done) return null;
    const [item, n] = await this._sink.queue.shift();
    this._s._credit(this.channel, n);
    if (item.type === 'END') { this._done = true; return null; }
    if (item.type === 'CLOSED') { this._done = true; throw this._s._closedError(); }
    return item.data;
  }
  async collect(max = 64 * 1024 * 1024) {
    const parts = [];
    let total = 0;
    for (let c; (c = await this.read()) !== null;) {
      parts.push(c);
      total += c.length;
      if (total > max) { this.discard(); throw new BeepError(`message exceeds ${max} bytes`); }
    }
    return Buffer.concat(parts);
  }
  discard() { this._done = true; this._sink.discard(); }
}

const IDLE = 0, ANSWERING = 1, DONE = 2;

class Responder {
  constructor(session, channel, msgno) {
    this._s = session;
    this.channel = channel;
    this.msgno = msgno;
    this.state = IDLE;
    this._nextAns = 0;
  }
  _final(kind) {
    if (this.state !== IDLE) throw new BeepError(`${kind} not allowed after ANS or completion`);
    this.state = DONE;
  }
  async rpy(payload) {
    this._final('RPY');
    try { await this._s._sendMessage(this.channel, 'RPY', this.msgno, 0, payload); }
    finally { this._s._finishInbound(this.channel, this.msgno); }
  }
  async err(payload) {
    this._final('ERR');
    try { await this._s._sendMessage(this.channel, 'ERR', this.msgno, 0, payload); }
    finally { this._s._finishInbound(this.channel, this.msgno); }
  }
  error(code, message) { return this.err(W.errorPayload(code, message)); }
  async ans(payload) {
    if (this.state === DONE) throw new BeepError('reply already completed');
    this.state = ANSWERING;
    const n = this._nextAns++;
    await this._s._sendMessage(this.channel, 'ANS', this.msgno, n, payload);
    return n;
  }
  async nul() {
    if (this.state === DONE) throw new BeepError('reply already completed');
    this.state = DONE;
    try { await this._s._sendMessage(this.channel, 'NUL', this.msgno, 0, Buffer.alloc(0)); }
    finally { this._s._finishInbound(this.channel, this.msgno); }
  }
}

class Chan {
  constructor(number, profile = '', nextMsgno = 0) {
    this.number = number;
    this.profile = profile;
    this.txSeq = 0;
    this.txLimit = INITIAL_WINDOW;
    this.txWaiters = [];
    this.txLock = Promise.resolve();
    this.dead = false;
    this.recvSeq = 0;
    this.advAck = 0;
    this.advWindow = INITIAL_WINDOW;
    this.consumed = 0;
    this.nextMsgno = nextMsgno;
    this.outstanding = new Map();
    this.rxReply = null;
    this.rxMsg = null;
    this.inbound = new Set();
    this.queue = null;
  }
  busy() { return this.outstanding.size > 0 || this.inbound.size > 0 || this.rxMsg !== null; }
  wake() { const w = this.txWaiters; this.txWaiters = []; for (const r of w) r(); }
  kill() { this.dead = true; this.wake(); }
}

class Channel {
  constructor(session, number, profile) {
    this.session = session;
    this.number = number;
    this.profile = profile;
  }
  get isOpen() { return !this.session.closed && this.session._chans.has(this.number); }
  get pending() { const c = this.session._chans.get(this.number); return c ? c.outstanding.size : 0; }
  send(payload) { return this.session._sendMsg(this.number, payload); }
  async request(payload) { return (await this.send(payload)).collect(this.session.config.maxMessageSize); }
  close(code = 200) { return this.session._closeChannel(this.number, code); }
}

const nextNumber = (n) => (n + 2 > MAX_CHANNEL ? 2 - (n % 2) : n + 2);

class Session extends EventEmitter {
  /**
   * @param {net.Socket} socket
   * @param {boolean} initiator
   * @param {object} [config]
   * @param {Object<string, {onMessage:Function, onStart?:Function}>} [profiles]
   */
  constructor(socket, initiator, config, profiles) {
    super();
    this.config = { ...DEFAULT_CONFIG, ...(config || {}) };
    this.profiles = { ...(profiles || {}) };
    this.initiator = initiator;
    this.peerProfiles = [];
    this._sock = socket;
    this._chans = new Map([[0, new Chan(0, '', 1)]]);
    this._nextChan = initiator ? 1 : 2;
    this._closed = null;
    this._releasing = false;
    this._dec = new W.FrameDecoder(Math.max(this.config.recvWindow, INITIAL_WINDOW));
    this._closedPromise = new Promise((r) => { this._resolveClosed = r; });
  }

  static async connect(host, port, opts = {}) {
    const sock = net.connect({ host, port, noDelay: true });
    await once(sock, 'connect');
    let stream = sock;
    if (opts.tls) stream = await tlsClientBootstrap(sock, opts.tls);
    return Session.establish(stream, true, opts.config, opts.profiles);
  }

  static async establish(socket, initiator, config, profiles) {
    const s = new Session(socket, initiator, config, profiles);
    const greet = new Sink(s, 0);
    s._chans.get(0).outstanding.set(0, greet);
    s._chans.get(0).queue = new AsyncQueue();
    socket.on('data', (chunk) => s._onBytes(chunk));
    socket.on('end', () => s._fail(new SessionClosed(s._releasing ? 'session released' : 'connection closed by peer')));
    socket.on('close', () => s._fail(new SessionClosed('connection closed')));
    socket.on('error', (e) => s._fail(new SessionClosed(`socket error: ${e.message}`)));
    const greeting = { kind: 'greeting', profiles: Object.keys(s.profiles).sort().map((uri) => ({ uri })) };
    await s._sendMessage(0, 'RPY', 0, 0, W.xmlEntity(W.mgmtXml(greeting)));
    s._openWindow(0);
    let reply;
    try {
      reply = await withTimeout(new ReplyStream(s, greet).collect(1 << 20), s.config.greetingTimeout, 'greeting');
    } catch (e) {
      s._fail(e instanceof BeepError ? e : new SessionClosed(String(e)));
      throw e;
    }
    let m;
    try {
      m = W.parseMgmt(W.parseEntity(reply.payload).body);
    } catch (e) {
      s._fail(e);
      throw e;
    }
    if (reply.kind === 'ERR' || m.kind === 'error') {
      const e = m.kind === 'error' ? new Refused(m.code, m.message) : new Refused(421, 'peer refused session');
      s._fail(e);
      throw e;
    }
    if (m.kind !== 'greeting') {
      const e = new W.ProtocolError(`expected greeting, got ${m.kind}`);
      s._fail(e);
      throw e;
    }
    s.peerProfiles = m.profiles.map((p) => p.uri);
    s._mgmtLoop().catch(() => {});
    return s;
  }

  get closed() { return this._closed !== null; }
  get channelCount() { return this._chans.size - 1; }
  waitClosed() { return this._closedPromise; }

  async startChannel(uri, serverName) {
    return (await this.startChannelWith(uri, serverName)).channel;
  }

  async startChannelWith(uri, serverName) {
    this._checkOpen();
    const profiles = typeof uri === 'string' ? [{ uri }] : uri;
    let n = this._nextChan;
    for (let i = 0; this._chans.has(n); i++) {
      if (i > 1e6) throw new SessionClosed('no free channel numbers');
      n = nextNumber(n);
    }
    this._nextChan = nextNumber(n);
    this._chans.set(n, new Chan(n));
    let m;
    try {
      m = await this._mgmtRequest({ kind: 'start', number: n, profiles, serverName });
    } catch (e) {
      this._chans.delete(n);
      throw e;
    }
    if (m.kind === 'error') { this._chans.delete(n); throw new Refused(m.code, m.message); }
    if (m.kind !== 'profile') { this._chans.delete(n); throw new W.ProtocolError(`unexpected reply to <start>: ${m.kind}`); }
    const c = this._chans.get(n);
    if (!c) throw this._closedError();
    const p = m.profiles[0];
    c.profile = p.uri;
    c.queue = new AsyncQueue();
    this._channelLoop(n, this.profiles[p.uri] || null, c.queue).catch(() => {});
    this._openWindow(n);
    return { channel: new Channel(this, n, p.uri), init: p.data };
  }

  async close() {
    if (this.closed) return;
    let m;
    try {
      m = await withTimeout(this._mgmtRequest({ kind: 'close', number: 0, code: 200 }), this.config.closeTimeout, 'close');
    } catch (e) {
      this.abort();
      throw e;
    }
    if (m.kind === 'ok') this._fail(new SessionClosed('session released'));
    else if (m.kind === 'error') throw new Refused(m.code, m.message);
  }

  abort() { this._fail(new SessionClosed('session aborted')); }

  // -- internals ------------------------------------------------------------

  _checkOpen() { if (this._closed) throw this._closedError(); }
  _closedError() { return this._closed instanceof BeepError ? this._closed : new SessionClosed(this._closed ? String(this._closed) : 'channel closed'); }

  _fail(err) {
    if (this._closed) return;
    this._closed = err;
    const chans = this._chans;
    this._chans = new Map();
    for (const c of chans.values()) {
      c.kill();
      for (const sink of c.outstanding.values()) sink.queue.push([{ type: 'CLOSED' }, 0]);
      if (c.rxMsg) c.rxMsg._sink.queue.push([{ type: 'CLOSED' }, 0]);
      if (c.queue) c.queue.push(null);
    }
    try { this._sock.end(); } catch (_) { /* ignore */ }
    setTimeout(() => { try { this._sock.destroy(); } catch (_) { /* ignore */ } }, 1000).unref();
    this._resolveClosed(err);
    this.emit('closed', err);
  }

  _write(buf) {
    if (this._closed) return true;
    return this._sock.write(buf);
  }

  _credit(channel, n) {
    if (n <= 0) return;
    const c = this._chans.get(channel);
    if (!c) return;
    c.consumed = (c.consumed + n) >>> 0;
    const w = this.config.recvWindow;
    if (((c.consumed - c.advAck) >>> 0) >= Math.floor(w / 2)) {
      c.advAck = c.consumed;
      c.advWindow = w;
      this._write(W.encodeSeq(channel, c.consumed, w));
    }
  }

  _openWindow(channel) {
    const c = this._chans.get(channel);
    if (!c) return;
    c.advAck = c.consumed;
    c.advWindow = this.config.recvWindow;
    this._write(W.encodeSeq(channel, c.consumed, c.advWindow));
  }

  _finishInbound(channel, msgno) {
    const c = this._chans.get(channel);
    if (c) c.inbound.delete(msgno);
  }

  _onBytes(chunk) {
    if (this._closed) return;
    this._dec.push(chunk);
    try {
      for (let f; (f = this._dec.next()) !== null;) {
        if (f.type === 'SEQ') {
          const c = this._chans.get(f.channel);
          if (c) { c.txLimit = (f.ackno + f.window) >>> 0; c.wake(); }
          continue;
        }
        this._onData(f);
        if (this._closed) return;
      }
    } catch (e) {
      this._fail(e instanceof BeepError || e instanceof W.ProtocolError ? e : new W.ProtocolError(String(e)));
      try { this._sock.destroy(); } catch (_) { /* ignore */ }
    }
  }

  _onData(f) {
    const c = this._chans.get(f.channel);
    if (!c) throw new W.ProtocolError(`frame on unknown channel ${f.channel}`);
    if (f.seqno !== c.recvSeq) throw new W.ProtocolError(`channel ${f.channel}: expected seqno ${c.recvSeq}, got ${f.seqno}`);
    const size = f.payload.length;
    const end = (c.recvSeq + size) >>> 0;
    if (((end - c.advAck) >>> 0) > c.advWindow) throw new W.ProtocolError(`channel ${f.channel}: frame exceeds receive window`);
    c.recvSeq = end;
    const last = !f.more;
    switch (f.type) {
      case 'MSG': {
        let m = c.rxMsg;
        if (m && m.msgno !== f.msgno) throw new W.ProtocolError(`channel ${f.channel}: interleaved MSG frames`);
        if (!m) {
          if (c.inbound.has(f.msgno)) throw new W.ProtocolError(`channel ${f.channel}: msgno ${f.msgno} reused while outstanding`);
          c.inbound.add(f.msgno);
          m = new IncomingMessage(this, f.channel, f.msgno);
          c.rxMsg = m;
          if (c.queue) c.queue.push(m);
        }
        m._sink.deliver({ type: 'MSG', data: f.payload }, size);
        if (last) { c.rxMsg = null; m._sink.deliver({ type: 'END' }, 0); }
        break;
      }
      case 'RPY':
      case 'ERR': {
        if (c.rxReply && (c.rxReply[0] !== f.type || c.rxReply[1] !== f.msgno)) throw new W.ProtocolError(`channel ${f.channel}: interleaved reply frames`);
        const sink = c.outstanding.get(f.msgno);
        if (!sink) throw new W.ProtocolError(`channel ${f.channel}: reply to unknown msgno ${f.msgno}`);
        if (sink.ansOpen.size) throw new W.ProtocolError('RPY/ERR while ANS in progress');
        sink.deliver({ type: f.type, data: f.payload, last }, size);
        if (last) { c.rxReply = null; c.outstanding.delete(f.msgno); } else c.rxReply = [f.type, f.msgno];
        break;
      }
      case 'ANS': {
        if (c.rxReply) throw new W.ProtocolError('ANS while RPY/ERR in progress');
        const sink = c.outstanding.get(f.msgno);
        if (!sink) throw new W.ProtocolError(`channel ${f.channel}: ANS to unknown msgno ${f.msgno}`);
        if (last) sink.ansOpen.delete(f.ansno); else sink.ansOpen.add(f.ansno);
        sink.deliver({ type: 'ANS', ansno: f.ansno, data: f.payload, last }, size);
        break;
      }
      case 'NUL': {
        const sink = c.outstanding.get(f.msgno);
        if (!sink) throw new W.ProtocolError(`channel ${f.channel}: NUL to unknown msgno ${f.msgno}`);
        if (sink.ansOpen.size) throw new W.ProtocolError('NUL before all ANS messages completed');
        c.outstanding.delete(f.msgno);
        sink.deliver({ type: 'NUL' }, 0);
        break;
      }
      default:
        throw new W.ProtocolError(`unexpected frame type ${f.type}`);
    }
  }

  async _sendMessage(channel, type, msgno, ansno, payload) {
    this._checkOpen();
    const c = this._chans.get(channel);
    if (!c) throw new SessionClosed(`channel ${channel} is not open`);
    // Per-channel lock: frames of different messages never interleave.
    const prev = c.txLock;
    let unlock;
    c.txLock = new Promise((r) => { unlock = r; });
    await prev;
    try {
      const maxFrame = Math.max(1, this.config.maxFrameSize);
      const total = payload.length;
      let off = 0;
      for (;;) {
        if (c.dead || this._closed) throw this._closedError();
        let avail = (c.txLimit - c.txSeq) >>> 0;
        if (avail >= 0x80000000) avail = 0;
        const remaining = total - off;
        if (remaining === 0 || avail > 0) {
          const n = Math.min(remaining, avail, maxFrame);
          const more = off + n < total;
          const ok = this._write(W.encodeFrame(type, channel, msgno, more, c.txSeq, payload.subarray(off, off + n), ansno));
          c.txSeq = (c.txSeq + n) >>> 0;
          off += n;
          if (!more) return;
          if (!ok && !this._closed) await Promise.race([once(this._sock, 'drain').catch(() => {}), this._closedPromise]);
          continue;
        }
        await new Promise((r) => c.txWaiters.push(r));
      }
    } finally {
      unlock();
    }
  }

  async _sendMsg(channel, payload) {
    this._checkOpen();
    const c = this._chans.get(channel);
    if (!c) throw new SessionClosed(`channel ${channel} is not open`);
    let n = c.nextMsgno;
    while (c.outstanding.has(n)) n = n >= MAX_CHANNEL ? 0 : n + 1;
    c.nextMsgno = n >= MAX_CHANNEL ? 0 : n + 1;
    const sink = new Sink(this, channel);
    c.outstanding.set(n, sink);
    // Transmit in the background so callers can pipeline large MSGs and read replies concurrently.
    this._sendMessage(channel, 'MSG', n, 0, payload).catch(() => {
      const ch = this._chans.get(channel);
      if (ch && ch.outstanding.delete(n)) sink.queue.push([{ type: 'CLOSED' }, 0]);
    });
    return new ReplyStream(this, sink);
  }

  async _mgmtRequest(m) {
    const st = await this._sendMsg(0, W.xmlEntity(W.mgmtXml(m)));
    const reply = await st.collect(1 << 20);
    if (reply.kind === 'ANS') throw new W.ProtocolError('ANS on channel 0');
    return W.parseMgmt(W.parseEntity(reply.payload).body);
  }

  async _closeChannel(number, code) {
    const c = this._chans.get(number);
    if (c && c.busy()) throw new Refused(550, 'still working');
    const m = await this._mgmtRequest({ kind: 'close', number, code });
    if (m.kind === 'ok') {
      const ch = this._chans.get(number);
      this._chans.delete(number);
      if (ch) { ch.kill(); if (ch.queue) ch.queue.push(null); }
    } else if (m.kind === 'error') throw new Refused(m.code, m.message);
    else throw new W.ProtocolError(`unexpected reply to <close>: ${m.kind}`);
  }

  async _mgmtLoop() {
    const q = this._chans.get(0).queue;
    for (;;) {
      const msg = await q.shift();
      if (msg === null) return;
      const r = new Responder(this, 0, msg.msgno);
      let body;
      try { body = await msg.collect(1 << 20); } catch (_) { return; }
      let reply;
      let opened = null;
      try {
        const m = W.parseMgmt(W.parseEntity(body).body);
        if (m.kind === 'start') { reply = await this._handleStart(m); opened = m.number; }
        else if (m.kind === 'close') reply = this._handleClose(m.number);
        else throw new Refused(501, `unexpected element <${m.kind}>`);
      } catch (e) {
        reply = e instanceof Refused ? { kind: 'error', code: e.code, message: e.beepMessage } : { kind: 'error', code: 500, message: String(e.message || e) };
        opened = null;
      }
      try {
        if (reply.kind === 'error') await r.error(reply.code, reply.message);
        else await r.rpy(W.xmlEntity(W.mgmtXml(reply)));
      } catch (_) { return; }
      if (opened !== null) this._openWindow(opened); // SEQ must follow the RPY that creates the channel
      if (reply.kind === 'ok' && this._releasing) {
        this._sock.end();
        return;
      }
    }
  }

  async _handleStart(m) {
    if (this._releasing) throw new Refused(550, 'session is being released');
    const peerParity = this.initiator ? 0 : 1;
    const n = m.number;
    if (n === 0 || n % 2 !== peerParity || this._chans.has(n)) throw new Refused(553, `channel number ${n} is invalid or in use`);
    const p = m.profiles.find((x) => this.profiles[x.uri]);
    if (!p) throw new Refused(550, 'no acceptable profile');
    const handler = this.profiles[p.uri];
    let init = null;
    if (typeof handler.onStart === 'function') init = await handler.onStart(this, n, p.data);
    if (this._chans.has(n)) throw new Refused(553, `channel number ${n} is in use`);
    const c = new Chan(n, p.uri);
    c.queue = new AsyncQueue();
    this._chans.set(n, c);
    this._channelLoop(n, handler, c.queue).catch(() => {});
    return { kind: 'profile', profiles: [{ uri: p.uri, data: init == null ? null : String(init) }] };
  }

  _handleClose(n) {
    if (n === 0) {
      for (const [k, c] of this._chans) if (k !== 0 && c.busy()) throw new Refused(550, 'still working');
      this._releasing = true;
      return { kind: 'ok' };
    }
    const c = this._chans.get(n);
    if (!c) throw new Refused(553, `channel ${n} is not open`);
    if (c.busy()) throw new Refused(550, 'still working');
    this._chans.delete(n);
    c.kill();
    if (c.queue) c.queue.push(null);
    return { kind: 'ok' };
  }

  async _channelLoop(number, handler, q) {
    for (;;) {
      const msg = await q.shift();
      if (msg === null) return;
      const r = new Responder(this, number, msg.msgno);
      try {
        if (!handler) {
          msg.discard();
          await r.error(550, 'this peer does not serve the profile');
          continue;
        }
        try {
          await handler.onMessage(this, msg, r);
        } catch (e) {
          if (this.closed) return;
          if (!(e instanceof BeepError)) {
            msg.discard();
            if (r.state === IDLE) await r.error(554, `handler failed: ${e.message || e}`);
            else if (r.state === ANSWERING) await r.nul();
            continue;
          }
          throw e;
        }
        msg.discard();
        if (r.state === IDLE) await r.error(554, 'handler produced no reply');
        else if (r.state === ANSWERING) await r.nul();
      } catch (e) {
        if (this.closed) return;
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Listener

class Listener extends EventEmitter {
  constructor(profiles, opts = {}) {
    super();
    this.profiles = profiles;
    this.config = opts.config;
    this.tls = opts.tls || null;
    this.sessions = new Set();
    this.server = net.createServer({ noDelay: true }, (sock) => this._accept(sock));
  }
  listen(port = 10288, host = '127.0.0.1') {
    return new Promise((resolve, reject) => {
      this.server.once('error', reject);
      this.server.listen(port, host, () => { this.server.off('error', reject); resolve(this); });
    });
  }
  get port() { return this.server.address().port; }
  async close() {
    this.server.close();
    for (const s of this.sessions) s.abort();
  }
  async _accept(sock) {
    sock.setNoDelay(true);
    try {
      let stream = sock;
      if (this.tls) stream = await tlsServerBootstrap(sock, this.tls);
      const s = await Session.establish(stream, false, this.config, this.profiles);
      this.sessions.add(s);
      this.emit('session', s);
      await s.waitClosed();
      this.sessions.delete(s);
    } catch (e) {
      sock.destroy();
    }
  }
}

// ---------------------------------------------------------------------------
// TLS tuning profile (RFC 3080 §3.1)

class Boot {
  constructor(sock) {
    this.sock = sock;
    this.dec = new W.FrameDecoder(65536);
    this.frames = new AsyncQueue();
    this.seq = new Map();
    this.onData = (c) => {
      this.dec.push(c);
      try { for (let f; (f = this.dec.next()) !== null;) this.frames.push(f); } catch (e) { this.frames.push(e); }
    };
    this.onEnd = () => this.frames.push(new SessionClosed('connection closed during TLS negotiation'));
    sock.on('data', this.onData);
    sock.on('end', this.onEnd);
    sock.on('error', this.onEnd);
  }
  send(type, channel, msgno, xml) {
    const payload = W.xmlEntity(xml);
    const s = this.seq.get(channel) || 0;
    this.sock.write(W.encodeFrame(type, channel, msgno, false, s, payload));
    this.seq.set(channel, (s + payload.length) >>> 0);
  }
  async recv() {
    const parts = [];
    for (;;) {
      const f = await this.frames.shift();
      if (f instanceof Error) throw f;
      if (f.type === 'SEQ') continue;
      parts.push(f.payload);
      if (!f.more) return { type: f.type, channel: f.channel, msgno: f.msgno, body: W.parseEntity(Buffer.concat(parts)).body };
    }
  }
  detach() {
    this.sock.off('data', this.onData);
    this.sock.off('end', this.onEnd);
    this.sock.off('error', this.onEnd);
    if (this.dec.pending || this.frames.length) throw new W.ProtocolError('unexpected data before TLS handshake');
  }
}

async function tlsClientBootstrap(sock, opts) {
  const b = new Boot(sock);
  b.send('RPY', 0, 0, '<greeting />');
  let r = await b.recv();
  const g = W.parseMgmt(r.body);
  if (r.type !== 'RPY' || g.kind !== 'greeting') throw new W.ProtocolError('expected greeting');
  if (!g.profiles.some((p) => p.uri === TLS_PROFILE)) throw new BeepError('peer does not offer the TLS profile');
  b.send('MSG', 0, 1, W.mgmtXml({ kind: 'start', number: 1, serverName: opts.servername, profiles: [{ uri: TLS_PROFILE, data: '<ready />' }] }));
  r = await b.recv();
  const m = W.parseMgmt(r.body);
  if (m.kind === 'error') throw new Refused(m.code, m.message);
  if (r.type !== 'RPY' || m.kind !== 'profile' || !String(m.profiles[0].data || '').includes('<proceed')) throw new BeepError('peer did not proceed with TLS');
  b.detach();
  const ca = opts.ca || (opts.caFile ? fs.readFileSync(opts.caFile) : undefined);
  const t = tls.connect({ socket: sock, servername: opts.servername, ca, ...(opts.tlsOptions || {}) });
  await once(t, 'secureConnect');
  return t;
}

async function tlsServerBootstrap(sock, opts) {
  const b = new Boot(sock);
  b.send('RPY', 0, 0, W.mgmtXml({ kind: 'greeting', profiles: [{ uri: TLS_PROFILE }] }));
  let r = await b.recv();
  if (r.type !== 'RPY' || r.channel !== 0) throw new W.ProtocolError('expected greeting');
  let tlsChannel = null;
  let proceeded = false;
  for (let i = 0; i < 16 && !proceeded; i++) {
    r = await b.recv();
    if (r.type !== 'MSG') throw new W.ProtocolError('expected MSG during TLS negotiation');
    if (r.channel === tlsChannel) {
      if (r.body.includes('<ready')) { b.send('RPY', r.channel, r.msgno, '<proceed />'); proceeded = true; }
      else b.send('ERR', r.channel, r.msgno, W.mgmtXml({ kind: 'error', code: 500, message: 'expected <ready />' }));
      continue;
    }
    const m = W.parseMgmt(r.body);
    const p = m.kind === 'start' && m.number % 2 === 1 ? m.profiles.find((x) => x.uri === TLS_PROFILE) : null;
    if (p) {
      if (p.data && p.data.includes('<ready')) {
        b.send('RPY', 0, r.msgno, W.mgmtXml({ kind: 'profile', profiles: [{ uri: TLS_PROFILE, data: '<proceed />' }] }));
        proceeded = true;
      } else {
        b.send('RPY', 0, r.msgno, W.mgmtXml({ kind: 'profile', profiles: [{ uri: TLS_PROFILE }] }));
        tlsChannel = m.number;
      }
    } else if (m.kind === 'close' && m.number === 0) {
      b.send('RPY', 0, r.msgno, '<ok />');
      throw new SessionClosed('peer released session before TLS');
    } else {
      b.send('ERR', 0, r.msgno, W.mgmtXml({ kind: 'error', code: 538, message: 'TLS required' }));
    }
  }
  if (!proceeded) throw new BeepError('peer never requested TLS');
  b.detach();
  const ctx = tls.createSecureContext({
    cert: opts.cert || fs.readFileSync(opts.certFile),
    key: opts.key || fs.readFileSync(opts.keyFile),
  });
  const t = new tls.TLSSocket(sock, { isServer: true, secureContext: ctx });
  await once(t, 'secure');
  return t;
}

module.exports = {
  TLS_PROFILE, DEFAULT_CONFIG, BeepError, SessionClosed, Refused, Session, Channel, Listener, ReplyStream,
  IncomingMessage, Responder, AsyncQueue, withTimeout,
};
