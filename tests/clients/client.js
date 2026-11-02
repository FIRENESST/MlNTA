'use strict';
// Conformance client (Node native lib): node client.js ADDR → exit 1 on failure.
const B = require('../../native/node');

(async () => {
  const i = process.argv[2].lastIndexOf(':');
  const c = new B.RpcClient(process.argv[2].slice(0, i), Number(process.argv[2].slice(i + 1)), { timeout: 10000 });
  let r = await c.call('POST', '/echo', { headers: { 'Content-Type': 'text/plain' }, body: 'conformance' });
  if (r.status !== 200 || r.text() !== 'conformance') throw new Error(`echo ${r.status} ${r.text()}`);
  r = await c.get('/stream/5');
  if (r.text() !== [0, 1, 2, 3, 4].map((n) => `line ${n}\n`).join('')) throw new Error(`stream ${r.text()}`);
  const big = Buffer.from('0123456789'.repeat(200000));
  r = await c.post('/echo', big);
  if (!r.body.equals(big)) throw new Error('2MB echo');
  try {
    await c.get('/err/537');
    throw new Error('expected RemoteError');
  } catch (e) {
    if (!(e instanceof B.RemoteError) || e.code !== 537) throw e;
  }
  await c.close();
  console.log('ok');
})().catch((e) => { console.log('FAIL:', e.message); process.exit(1); });
