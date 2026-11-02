'use strict';
// Serve a standard Node request listener — (req, res) => …, i.e. Express, Koa's app.callback(),
// Fastify's server.routing, Connect, plain http handlers — natively over BEEP, no gateway needed.
//
//   const app = express(); …
//   await new RpcServer(httpHandler(app)).listen(10288, '0.0.0.0');
//
// A real http.ServerResponse writes to an in-memory socket; we parse its HTTP/1.1 output
// back into status/headers/body and stream long-running responses as ANS.

const http = require('http');
const { Duplex } = require('stream');
const { AsyncQueue } = require('./session');

const HOP = new Set(['connection', 'keep-alive', 'transfer-encoding', 'te', 'trailer', 'upgrade', 'content-length', 'rpc-id', 'rpc-timeout', 'rpc-authority']);

/** Decodes the raw HTTP/1.1 response that http.ServerResponse writes. */
class ResponseParser {
  constructor(onHead, onData, onEnd) {
    Object.assign(this, { onHead, onData, onEnd });
    this.buf = Buffer.alloc(0);
    this.state = 'head';
    this.chunked = false;
    this.remaining = -1;
  }
  push(b) {
    this.buf = Buffer.concat([this.buf, b]);
    for (;;) {
      if (this.state === 'head') {
        const p = this.buf.indexOf('\r\n\r\n');
        if (p < 0) return;
        const lines = this.buf.toString('latin1', 0, p).split('\r\n');
        this.buf = this.buf.subarray(p + 4);
        const status = Number(lines[0].split(' ')[1]);
        const headers = [];
        for (const l of lines.slice(1)) {
          const i = l.indexOf(':');
          if (i > 0) headers.push([l.slice(0, i).trim(), l.slice(i + 1).trim()]);
        }
        const te = headers.find(([k]) => k.toLowerCase() === 'transfer-encoding');
        const cl = headers.find(([k]) => k.toLowerCase() === 'content-length');
        this.chunked = !!te && /chunked/i.test(te[1]);
        this.remaining = !this.chunked && cl ? Number(cl[1]) : -1;
        this.state = this.chunked ? 'size' : 'raw';
        this.onHead(status, headers.filter(([k]) => !HOP.has(k.toLowerCase())));
        if (this.remaining === 0) { this.state = 'done'; this.onEnd(); return; }
      } else if (this.state === 'raw') {
        if (!this.buf.length) return;
        let b = this.buf;
        if (this.remaining >= 0) b = b.subarray(0, this.remaining);
        this.buf = this.buf.subarray(b.length);
        this.onData(b);
        if (this.remaining >= 0) {
          this.remaining -= b.length;
          if (this.remaining === 0) { this.state = 'done'; this.onEnd(); }
        }
        return;
      } else if (this.state === 'size') {
        const p = this.buf.indexOf('\r\n');
        if (p < 0) return;
        const size = parseInt(this.buf.toString('latin1', 0, p).split(';')[0], 16);
        this.buf = this.buf.subarray(p + 2);
        if (size === 0) { this.state = 'trailer'; continue; }
        this.remaining = size;
        this.state = 'chunk';
      } else if (this.state === 'chunk') {
        if (this.buf.length < this.remaining + 2) {
          if (this.buf.length > 0 && this.buf.length <= this.remaining) {
            const b = this.buf;
            this.remaining -= b.length;
            this.buf = Buffer.alloc(0);
            this.onData(b);
          }
          return;
        }
        const b = this.buf.subarray(0, this.remaining);
        this.buf = this.buf.subarray(this.remaining + 2);
        this.state = 'size';
        if (b.length) this.onData(b);
      } else if (this.state === 'trailer') {
        const p = this.buf.indexOf('\r\n');
        if (p < 0) return;
        const line = this.buf.subarray(0, p);
        this.buf = this.buf.subarray(p + 2);
        if (line.length === 0) { this.state = 'done'; this.onEnd(); return; }
      } else return;
    }
  }
}

/**
 * @param {(req: http.IncomingMessage, res: http.ServerResponse) => void} listener
 * @param {{streamAfterMs?: number}} [opts] responses not finished this long after their head are streamed (ANS)
 */
function httpHandler(listener, opts = {}) {
  const streamAfter = opts.streamAfterMs === undefined ? 25 : opts.streamAfterMs;
  return (rpcReq) => new Promise((resolve, reject) => {
    const queue = new AsyncQueue();
    const sock = new Duplex({
      read() {},
      write(chunk, _enc, cb) { parser.push(chunk); cb(); },
      final(cb) { cb(); },
    });
    sock.remoteAddress = '127.0.0.1';
    sock.remotePort = 0;
    sock.encrypted = false;
    sock.setTimeout = () => sock;
    sock.setNoDelay = () => sock;
    sock.setKeepAlive = () => sock;
    sock.destroySoon = () => sock.destroy();

    let decided = false;
    let head = null;
    const body = [];
    let timer = null;
    const decideStream = () => {
      if (decided) return;
      decided = true;
      for (const b of body) queue.push(b);
      async function* gen() {
        for (let b; (b = await queue.shift()) !== null;) {
          if (b instanceof Error) throw b;
          yield b;
        }
      }
      resolve({ status: head.status, headers: head.headers, body: gen() });
    };
    const parser = new ResponseParser(
      (status, headers) => {
        head = { status, headers };
        if (streamAfter >= 0) timer = setTimeout(decideStream, streamAfter);
      },
      (b) => (decided ? queue.push(b) : body.push(b)),
      () => {
        clearTimeout(timer);
        if (!decided) {
          decided = true;
          resolve({ status: head.status, headers: head.headers, body: Buffer.concat(body) });
        } else queue.push(null);
      },
    );

    const req = new http.IncomingMessage(sock);
    req.method = rpcReq.method.toUpperCase();
    req.url = rpcReq.target;
    req.httpVersionMajor = 1;
    req.httpVersionMinor = 1;
    req.httpVersion = '1.1';
    const host = rpcReq.header('Rpc-Authority') || 'beep';
    const raw = [];
    for (const [k, v] of rpcReq.headers) if (!HOP.has(k.toLowerCase())) raw.push(k, v);
    raw.push('Host', host, 'Content-Length', String(rpcReq.body.length));
    req.rawHeaders = raw;
    const hdrs = {};
    for (let i = 0; i < raw.length; i += 2) {
      const k = raw[i].toLowerCase();
      hdrs[k] = hdrs[k] === undefined ? raw[i + 1] : `${hdrs[k]}, ${raw[i + 1]}`;
    }
    req.headers = hdrs;
    if (rpcReq.body.length) req.push(rpcReq.body);
    req.push(null);
    req.complete = true;

    const res = new http.ServerResponse(req);
    res.shouldKeepAlive = false;
    res.assignSocket(sock);
    res.on('finish', () => { res.detachSocket(sock); sock.end(); });
    res.on('error', (e) => (decided ? queue.push(e) : reject(e)));
    try {
      listener(req, res);
    } catch (e) {
      reject(e);
    }
  });
}

module.exports = { httpHandler };
