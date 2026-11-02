'use strict';
// Node binding tests: node test.js ECHO_ADDR [TLS_ECHO_ADDR CA CERT KEY]
// Set EXPRESS_PATH to an installed express module to also test the Express adapter.
const assert = require('assert');
const crypto = require('crypto');
const http2 = require('http2');
const B = require('..');

let failures = 0;
function check(cond, msg) {
  if (!cond) { failures++; console.log('FAIL:', msg); }
}
const addr = (s) => { const i = s.lastIndexOf(':'); return [s.slice(0, i), Number(s.slice(i + 1))]; };

async function againstRustEcho(host, port) {
  const c = new B.RpcClient(host, port, { maxChannels: 8, timeout: 5000 });
  let r = await c.call('POST', '/echo?lang=js', { headers: { 'Content-Type': 'text/plain', 'X-Test': 'js' }, body: 'hello from node' });
  check(r.status === 200 && r.text() === 'hello from node', `echo ${r.status} ${r.text()}`);
  check(r.header('x-test') === 'js' && r.header('X-Echo-Method') === 'POST', 'echo headers');
  r = await c.call('PUT', '/inspect', { json: { k: [1, 2] } });
  const j = r.json();
  check(j.method === 'PUT' && j.headers['content-type'] === 'application/json', `inspect ${JSON.stringify(j)}`);
  r = await c.get('/stream/50');
  check(r.text() === Array.from({ length: 50 }, (_, i) => `line ${i}\n`).join(''), 'stream body');
  const sr = await c.stream('GET', '/stream/1000');
  for await (const chunk of sr) { check(chunk.toString().startsWith('line 0'), 'first chunk'); break; }
  r = await c.get('/status/418');
  check(r.status === 418 && r.text() === 'status 418', `status ${r.status}`);
  try { await c.get('/err/553'); check(false, 'expected RemoteError'); } catch (e) {
    check(e instanceof B.RemoteError && e.code === 553 && e.httpStatus === 400, `err ${e}`);
  }
  r = await c.get('/bytes/3000000');
  check(r.body.length === 3000000 && r.body.subarray(0, 3).toString() === 'abc', '3 MB body');
  const big = crypto.randomBytes(2500000);
  r = await c.post('/echo', big);
  check(r.body.equals(big), '2.5 MB upload');
  const t0 = Date.now();
  const rs = await Promise.all(Array.from({ length: 40 }, (_, i) => c.post('/sleep/50', String(i))));
  check(rs.every((x) => x.status === 200), 'concurrent');
  check(Date.now() - t0 < 2000, `concurrency too slow ${Date.now() - t0}ms`);
  check(c.status().length === 1 && c.status()[0] <= 8, `pool ${c.status()}`);
  try { await c.get('/sleep/2000', { timeout: 200 }); check(false, 'expected timeout'); } catch (e) {
    check(e instanceof B.RpcTimeout || (e instanceof B.RemoteError && e.code === 451), `timeout ${e}`);
  }
  r = await c.post('/echo', 'after-timeout');
  check(r.text() === 'after-timeout', 'pool usable after timeout');
  await c.close();
}

async function handler(req) {
  if (req.path.startsWith('/stream/')) {
    const n = Number(req.path.split('/')[2]);
    return { status: 200, headers: { 'Content-Type': 'text/plain' }, body: (async function* () { for (let i = 0; i < n; i++) yield `line ${i}\n`; })() };
  }
  if (req.path === '/abort') return { body: (async function* () { yield 'partial'; throw new Error('generator died'); })() };
  if (req.path.startsWith('/err/')) throw new B.RpcFailure(Number(req.path.split('/')[2]), 'js says no');
  return { status: 200, headers: [['Content-Type', req.header('Content-Type') || 'application/octet-stream'], ['X-Lang', 'node']], body: req.body };
}

async function nodeServerNodeClient() {
  const cfg = { recvWindow: 6000, maxFrameSize: 1999 };
  const server = await new B.RpcServer(handler, { config: cfg }).listen(0);
  const c = new B.RpcClient('127.0.0.1', server.port, { config: cfg });
  const data = crypto.randomBytes(1000000);
  let r = await c.post('/echo', data);
  check(r.body.equals(data) && r.header('X-Lang') === 'node', 'node<->node big echo with tiny windows');
  r = await c.get('/stream/20');
  check(r.text() === Array.from({ length: 20 }, (_, i) => `line ${i}\n`).join(''), 'node stream');
  try { await c.get('/abort'); check(false, 'expected abort'); } catch (e) {
    check(e instanceof B.StreamAborted && /generator died/.test(e.message), `abort ${e}`);
  }
  try { await c.get('/err/537'); check(false, 'expected 537'); } catch (e) { check(e.code === 537, `err ${e}`); }
  await c.close();

  const s = await B.Session.connect('127.0.0.1', server.port);
  check(JSON.stringify(s.peerProfiles) === JSON.stringify([B.RPC_PROFILE]), `greeting ${s.peerProfiles}`);
  try { await s.startChannel('urn:none'); check(false, 'expected refusal'); } catch (e) { check(e instanceof B.Refused && e.code === 550, `refused ${e}`); }
  const ch = await s.startChannel(B.RPC_PROFILE);
  const streams = [];
  for (let i = 0; i < 10; i++) streams.push(await ch.send(B.wire.encodeEntity([['Rpc-Method', 'GET'], ['Rpc-Target', '/echo']], String(i))));
  const bodies = await Promise.all(streams.map((st) => st.collect()));
  check(bodies.every((b, i) => b.payload.toString().endsWith(String(i))), 'pipelined replies in order');
  await ch.close();
  await s.close();
  check(s.closed, 'session closed');
  await server.close();
}

async function httpAdapter() {
  const plain = (req, res) => {
    let body = '';
    req.on('data', (d) => { body += d; });
    req.on('end', () => {
      if (req.url === '/slow-stream') {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        let i = 0;
        const t = setInterval(() => { res.write(`tick${i};`); if (++i === 3) { clearInterval(t); res.end(); } }, 40);
        return;
      }
      res.statusCode = 201;
      res.setHeader('X-Node-Http', 'yes');
      res.end(`${req.method} ${req.url} ${req.headers['x-a'] || ''} ${body}`);
    });
  };
  const server = await new B.RpcServer(B.httpHandler(plain)).listen(0);
  const c = new B.RpcClient('127.0.0.1', server.port);
  let r = await c.call('PUT', '/w?q=1', { headers: { 'X-A': 'hdr' }, body: 'body' });
  check(r.status === 201 && r.text() === 'PUT /w?q=1 hdr body' && r.header('X-Node-Http') === 'yes', `http adapter ${r.status} ${r.text()}`);
  r = await c.get('/slow-stream');
  check(r.text() === 'tick0;tick1;tick2;', `http adapter streaming ${r.text()}`);
  if (process.env.EXPRESS_PATH) {
    const express = require(process.env.EXPRESS_PATH);
    const app = express();
    app.use(express.json());
    app.post('/users/:id', (req, res) => res.status(202).json({ id: req.params.id, got: req.body, q: req.query.x }));
    const s2 = await new B.RpcServer(B.httpHandler(app)).listen(0);
    const c2 = new B.RpcClient('127.0.0.1', s2.port);
    r = await c2.call('POST', '/users/42?x=7', { json: { a: 1 } });
    const j = r.json();
    check(r.status === 202 && j.id === '42' && j.got.a === 1 && j.q === '7', `express ${r.status} ${r.text()}`);
    await c2.close();
    await s2.close();
    console.log('  (express adapter tested)');
  }
  await c.close();
  await server.close();
}

async function tlsTests(tlsAddr, ca, cert, key) {
  const [h, p] = addr(tlsAddr);
  const c = new B.RpcClient(h, p, { tls: { servername: 'localhost', caFile: ca } });
  let r = await c.post('/echo', 'node over tls to rust');
  check(r.text() === 'node over tls to rust', 'node tls -> rust');
  await c.close();
  const server = await new B.RpcServer(handler, { tls: { certFile: cert, keyFile: key } }).listen(0);
  const c2 = new B.RpcClient('127.0.0.1', server.port, { tls: { servername: 'localhost', caFile: ca } });
  r = await c2.post('/echo', 'node tls both ends');
  check(r.text() === 'node tls both ends', 'node tls -> node');
  await c2.close();
  await server.close();
}

/** h2c against the gateway's HTTP egress, if GATEWAY_HTTP is set. */
async function h2cGateway(gw) {
  const client = http2.connect(`http://${gw}`);
  const res = await new Promise((resolve, reject) => {
    const req = client.request({ ':method': 'POST', ':path': '/echo', 'content-type': 'text/plain' });
    let status;
    const parts = [];
    req.on('response', (h) => { status = h[':status']; });
    req.on('data', (d) => parts.push(d));
    req.on('end', () => resolve({ status, body: Buffer.concat(parts).toString() }));
    req.on('error', reject);
    req.end('via h2c');
  });
  client.close();
  check(res.status === 200 && res.body === 'via h2c', `h2c gateway ${JSON.stringify(res)}`);
}

(async () => {
  if (process.argv[2] === '--serve') {
    const s = await new B.RpcServer(handler).listen(Number(process.argv[3]));
    console.log('READY', s.port);
    return;
  }
  const [host, port] = addr(process.argv[2]);
  await againstRustEcho(host, port);
  await nodeServerNodeClient();
  await httpAdapter();
  if (process.argv.length >= 7) await tlsTests(...process.argv.slice(3, 7));
  if (process.env.GATEWAY_HTTP) await h2cGateway(process.env.GATEWAY_HTTP);
  console.log(failures ? `Node: ${failures} FAILURE(S)` : 'Node: all tests passed');
  process.exit(failures ? 1 : 0);
})().catch((e) => { console.log('FAIL (exception):', e); process.exit(1); });
void assert;
