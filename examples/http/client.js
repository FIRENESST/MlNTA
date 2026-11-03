'use strict';
(async () => {
  const endpoint = process.env.MINTA_GATEWAY || 'http://127.0.0.1:8080';
  const response = await fetch(endpoint + '/v1/call', {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({method: 'POST', target: '/echo', json: {hello: 'node'}}),
    signal: AbortSignal.timeout(10000),
  });
  const reply = await response.json();
  if (!response.ok || reply.status !== 200 || JSON.parse(reply.body).hello !== 'node') throw new Error(JSON.stringify(reply));
  console.log('Node HTTP/JSON: all tests passed');
})().catch(error => { console.error(error); process.exit(1); });
