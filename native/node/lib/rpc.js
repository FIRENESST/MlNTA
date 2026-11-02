'use strict';
// MINTA RPC profile (urn:minta:beep:rpc:1): pooled client and server.

const W = require('./wire');
const { BeepError, SessionClosed, Session, Listener, AsyncQueue, withTimeout } = require('./session');

const RPC_PROFILE = 'urn:minta:beep:rpc:1';
const RESERVED = new Set(['rpc-method', 'rpc-target', 'rpc-status', 'rpc-reason', 'rpc-abort', 'content-transfer-encoding']);

class RemoteError extends BeepError {
  constructor(code, message) {
    super(`remote error ${code}: ${message}`);
    this.name = 'RemoteError';
    this.code = code;
    this.beepMessage = message;
  }
  get httpStatus() { return beepCodeToHttp(this.code); }
}
class RpcTimeout extends BeepError {
  constructor(msg) { super(msg); this.name = 'RpcTimeout'; }
}
class StreamAborted extends BeepError {
  constructor(msg) { super(msg); this.name = 'StreamAborted'; }
}
/** Throw from a server handler to answer with BEEP ERR. */
class RpcFailure extends BeepError {
  constructor(code, message) { super(`${code}: ${message}`); this.name = 'RpcFailure'; this.code = code; this.beepMessage = message; }
}

function beepCodeToHttp(code) {
  if (code === 421 || code === 450) return 503;
  if (code === 451) return 504;
  if (code === 500 || code === 501 || code === 553) return 400;
  if (code === 504) return 501;
  if (code === 530 || code === 534 || code === 535) return 401;
  if (code === 537 || code === 538) return 403;
  return 502;
}

function normHeaders(h) {
  if (!h) return [];
  if (Array.isArray(h)) return h.map(([k, v]) => [String(k), String(v)]);
  if (typeof h.entries === 'function' && !(h.constructor === Object)) return [...h.entries()].map(([k, v]) => [String(k), String(v)]);
  return Object.entries(h).map(([k, v]) => [k, String(v)]);
}
const appHeaders = (h) => h.filter(([k]) => !RESERVED.has(k.toLowerCase()));
const toBuf = (b) => (b == null ? Buffer.alloc(0) : Buffer.isBuffer(b) ? b : b instanceof Uint8Array ? Buffer.from(b) : Buffer.from(String(b)));

class Response {
  constructor(status, headers, body) { this.status = status; this.headers = headers; this.body = body; }
  header(name) { return W.getHeader(this.headers, name); }
  text() { return this.body.toString('utf8'); }
  json() { return JSON.parse(this.text()); }
}

/** Reply events → head + body chunks (docs/PROTOCOL.md §2.2–2.3). */
class Decoder {
  constructor(stream) {
    this.stream = stream;
    this.head = null;
    this.ready = [];
    this.finished = false;
    this.rpyBuf = Buffer.alloc(0);
    this.ans = new Map();
    this.nextAns = 0;
  }
  async readHead() {
    while (!this.head) {
      if (this.finished) throw new BeepError('reply ended before a response head');
      await this.pump();
    }
    return this.head;
  }
  async nextBody() {
    for (;;) {
      if (this.ready.length) return this.ready.shift();
      if (this.finished) return null;
      await this.pump();
    }
  }
  setHead(headers) {
    const st = W.getHeader(headers, 'Rpc-Status');
    const status = st === undefined ? 200 : Number(st);
    if (!Number.isInteger(status) || status < 100 || status > 999) throw new BeepError(`bad Rpc-Status ${st}`);
    this.head = { status, headers: appHeaders(headers) };
  }
  async pump() {
    const ev = await this.stream.next();
    if (ev === null) { this.finished = true; return; }
    if (ev.type === 'RPY') {
      if (this.rpyBuf) {
        this.rpyBuf = Buffer.concat([this.rpyBuf, ev.data]);
        const r = W.splitHead(this.rpyBuf, ev.last);
        if (r) {
          const rest = this.rpyBuf.subarray(r.offset);
          this.rpyBuf = null;
          this.setHead(r.headers);
          if (rest.length) this.ready.push(rest);
        } else if (this.rpyBuf.length > 65536) throw new BeepError('response head too large');
      } else if (ev.data.length) this.ready.push(ev.data);
      if (ev.last) this.finished = true;
    } else if (ev.type === 'ERR') {
      const parts = [ev.data];
      let last = ev.last;
      while (!last) {
        const n = await this.stream.next();
        if (!n || n.type !== 'ERR') throw new BeepError('truncated ERR');
        parts.push(n.data);
        last = n.last;
      }
      this.finished = true;
      const { code, message } = W.parseErrorPayload(Buffer.concat(parts));
      throw new RemoteError(code, message);
    } else if (ev.type === 'ANS') {
      let a = this.ans.get(ev.ansno);
      if (!a) { a = { head: Buffer.alloc(0), data: [], done: false, parsed: false }; this.ans.set(ev.ansno, a); }
      if (!a.parsed) {
        a.head = Buffer.concat([a.head, ev.data]);
        const r = W.splitHead(a.head, ev.last);
        if (r) {
          a.parsed = true;
          const abort = W.getHeader(r.headers, 'Rpc-Abort');
          if (abort !== undefined) throw new StreamAborted(abort);
          if (ev.ansno === 0) this.setHead(r.headers);
          const rest = a.head.subarray(r.offset);
          if (rest.length) a.data.push(rest);
        }
      } else if (ev.data.length) a.data.push(ev.data);
      if (ev.last) a.done = true;
      this.flush(false);
    } else if (ev.type === 'NUL') {
      this.flush(true);
      this.finished = true;
      if (!this.head) throw new BeepError('ANS stream without a head (ansno 0)');
    }
  }
  flush(all) {
    while (this.ans.size) {
      const k = all ? Math.min(...this.ans.keys()) : this.nextAns;
      const a = this.ans.get(k);
      if (!a || (k === 0 && !this.head && !all)) return;
      this.ready.push(...a.data);
      a.data = [];
      if (a.done || all) { this.ans.delete(k); this.nextAns = k + 1; } else return;
    }
  }
}

class StreamingResponse {
  constructor(status, headers, dec, release) {
    this.status = status;
    this.headers = headers;
    this._dec = dec;
    this._release = release;
    this._released = false;
  }
  header(name) { return W.getHeader(this.headers, name); }
  async *[Symbol.asyncIterator]() {
    try {
      for (let c; (c = await this._dec.nextBody()) !== null;) yield c;
    } finally {
      this.close();
    }
  }
  async read() {
    const parts = [];
    for await (const c of this) parts.push(c);
    return Buffer.concat(parts);
  }
  close() {
    if (!this._released) { this._released = true; this._release(this._dec.stream.done); }
  }
}

class Semaphore {
  constructor(n) { this.n = n; this.waiters = []; }
  acquire() {
    if (this.n > 0) { this.n--; return Promise.resolve(); }
    return new Promise((r) => this.waiters.push(r));
  }
  release() {
    const w = this.waiters.shift();
    if (w) w(); else this.n++;
  }
}

/** Pooled client: `sessions` connections × `maxChannels` channels, one request per channel. */
class RpcClient {
  constructor(host = '127.0.0.1', port = 10288, opts = {}) {
    this.host = host;
    this.port = port;
    this.sessionsMax = Math.max(1, opts.sessions || 1);
    this.maxChannels = Math.max(1, opts.maxChannels || 64);
    this.timeout = opts.timeout === undefined ? 30000 : opts.timeout;
    this.connectTimeout = opts.connectTimeout || 5000;
    this.tls = opts.tls || null;
    this.config = opts.config;
    this.profile = opts.profile || RPC_PROFILE;
    this._sessions = [];
    this._idle = [];
    this._sem = new Semaphore(this.sessionsMax * this.maxChannels);
    this._connecting = null;
    this._closed = false;
  }

  async _session() {
    if (this._closed) throw new SessionClosed('client closed');
    this._sessions = this._sessions.filter((s) => !s.closed);
    if (this._sessions.length >= this.sessionsMax) return this._sessions.reduce((a, b) => (a.channelCount <= b.channelCount ? a : b));
    if (!this._connecting) {
      this._connecting = withTimeout(Session.connect(this.host, this.port, { config: this.config, tls: this.tls }), this.connectTimeout, `connecting to ${this.host}:${this.port}`)
        .then((s) => { this._sessions.push(s); return s; })
        .catch((e) => { throw e.timeout ? new RpcTimeout(e.message) : e; })
        .finally(() => { this._connecting = null; });
    }
    return this._connecting;
  }

  connect() { return this._session().then(() => undefined); }

  async _acquire() {
    while (this._idle.length) {
      const ch = this._idle.pop();
      if (ch.isOpen) return ch;
    }
    let s = await this._session();
    try {
      return await s.startChannel(this.profile);
    } catch (e) {
      if (!s.closed) throw e;
      s = await this._session();
      return s.startChannel(this.profile);
    }
  }

  _giveBack(ch) {
    if (!this._closed && ch.isOpen && ch.pending === 0) this._idle.push(ch);
  }

  _releaser(ch, dec) {
    return (done) => {
      if (done) { this._giveBack(ch); this._sem.release(); return; }
      dec.stream.discard();
      (async () => {
        try { while ((await dec.stream.next()) !== null); } catch (_) { /* session gone */ }
        this._giveBack(ch);
        this._sem.release();
      })();
    };
  }

  /** Send a request; resolves once the response head has arrived. */
  async stream(method, target, opts = {}) {
    const t = opts.timeout === undefined ? this.timeout : opts.timeout;
    const h = appHeaders(normHeaders(opts.headers));
    if (t && W.getHeader(h, 'Rpc-Timeout') === undefined) h.push(['Rpc-Timeout', String(Math.round(t))]);
    const payload = W.encodeEntity([['Rpc-Method', method], ['Rpc-Target', target], ...h], toBuf(opts.body));
    await this._sem.acquire();
    let ch;
    try {
      ch = await this._acquire();
    } catch (e) {
      this._sem.release();
      throw e;
    }
    const dec = new Decoder(await ch.send(payload));
    const release = this._releaser(ch, dec);
    try {
      const head = await withTimeout(dec.readHead(), t, `${method} ${target}`);
      return new StreamingResponse(head.status, head.headers, dec, release);
    } catch (e) {
      release(dec.stream.done);
      if (e.timeout) throw new RpcTimeout(e.message);
      throw e;
    }
  }

  /** Send a request and read the whole response. opts: {headers, body, json, timeout} */
  async call(method, target, opts = {}) {
    if (opts.json !== undefined) {
      opts = { ...opts, body: JSON.stringify(opts.json), headers: [...normHeaders(opts.headers), ['Content-Type', 'application/json']] };
    }
    const r = await this.stream(method, target, opts);
    const body = await r.read();
    return new Response(r.status, r.headers, body);
  }

  get(target, opts) { return this.call('GET', target, opts); }
  post(target, body, opts = {}) { return this.call('POST', target, { ...opts, body }); }

  /** Verbatim payload on a fresh channel of any profile. */
  async raw(profile, payload) {
    const s = await this._session();
    const ch = await s.startChannel(profile);
    try { return await ch.request(toBuf(payload)); } finally { ch.close().catch(() => {}); }
  }

  status() { return this._sessions.filter((s) => !s.closed).map((s) => s.channelCount); }

  async close() {
    this._closed = true;
    this._idle = [];
    const ss = this._sessions;
    this._sessions = [];
    await Promise.all(ss.map((s) => s.close().catch(() => s.abort())));
  }
}

class Request {
  constructor(method, target, headers, body, channel) {
    Object.assign(this, { method, target, headers, body, channel });
  }
  header(name) { return W.getHeader(this.headers, name); }
  get path() { return this.target.split('?')[0]; }
  get id() { return this.header('Rpc-Id'); }
  get timeout() { const t = Number(this.header('Rpc-Timeout')); return Number.isFinite(t) && t > 0 ? t : undefined; }
  text() { return this.body.toString('utf8'); }
  json() { return JSON.parse(this.text()); }
}

/** Server handler result: {status, headers, body} where body is Buffer|string|AsyncIterable (streamed as ANS). */
function rpcProfile(handler) {
  return {
    async onMessage(session, msg, reply) {
      let payload;
      try {
        payload = await msg.collect(session.config.maxMessageSize);
      } catch (e) {
        if (!session.closed) await reply.error(554, e.message);
        return;
      }
      const { headers, offset } = W.splitHead(payload, true);
      const method = W.getHeader(headers, 'Rpc-Method');
      const target = W.getHeader(headers, 'Rpc-Target');
      if (!method || target === undefined) { await reply.error(501, 'request lacks Rpc-Method or Rpc-Target'); return; }
      const req = new Request(method, target, appHeaders(headers), payload.subarray(offset), msg.channel);
      let resp;
      try {
        resp = await withTimeout(Promise.resolve(handler(req)), req.timeout, 'handler');
      } catch (e) {
        if (e instanceof RpcFailure) await reply.error(e.code, e.beepMessage);
        else if (e.timeout) await reply.error(451, 'deadline exceeded');
        else await reply.error(554, `handler failed: ${e.message || e}`);
        return;
      }
      if (resp == null || typeof resp !== 'object' || Buffer.isBuffer(resp) || resp[Symbol.asyncIterator]) resp = { status: 200, body: resp };
      const h = [['Rpc-Status', String(resp.status || 200)], ...appHeaders(normHeaders(resp.headers))];
      if (req.id && W.getHeader(h, 'Rpc-Id') === undefined) h.push(['Rpc-Id', req.id]);
      const body = resp.body;
      if (body == null || typeof body === 'string' || Buffer.isBuffer(body) || body instanceof Uint8Array) {
        await reply.rpy(W.encodeEntity(h, toBuf(body)));
        return;
      }
      await reply.ans(W.encodeEntity(h, null));
      try {
        for await (const chunk of body) {
          const b = toBuf(chunk);
          if (b.length) await reply.ans(W.encodeEntity([], b));
        }
      } catch (e) {
        if (session.closed) return;
        await reply.ans(W.encodeEntity([['Rpc-Abort', e.message || String(e)]], null));
      }
      await reply.nul();
    },
  };
}

class RpcServer {
  constructor(handler, opts = {}) {
    this.listener = new Listener({ [RPC_PROFILE]: rpcProfile(handler), ...(opts.profiles || {}) }, opts);
  }
  async listen(port = 10288, host = '127.0.0.1') { await this.listener.listen(port, host); return this; }
  get port() { return this.listener.port; }
  close() { return this.listener.close(); }
}

module.exports = {
  RPC_PROFILE, RpcClient, RpcServer, rpcProfile, Request, Response, StreamingResponse, RemoteError, RpcTimeout,
  StreamAborted, RpcFailure, beepCodeToHttp, AsyncQueue,
};
