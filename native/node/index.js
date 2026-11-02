'use strict';
/**
 * @minta/beep — pure Node.js RFC 3080/3081 BEEP with the MINTA RPC profile (urn:minta:beep:rpc:1).
 *
 *   const { RpcClient } = require('@minta/beep');
 *   const c = new RpcClient('127.0.0.1', 10288);
 *   const r = await c.call('POST', '/orders', { json: { id: 7 } });
 *
 *   const { RpcServer } = require('@minta/beep');
 *   await new RpcServer(async (req) => ({ status: 200, body: 'hi ' + req.text() })).listen(10288);
 */
const wire = require('./lib/wire');
const session = require('./lib/session');
const rpc = require('./lib/rpc');
const http = require('./lib/http');

module.exports = {
  ...rpc,
  ...http,
  TLS_PROFILE: session.TLS_PROFILE,
  BeepError: session.BeepError,
  SessionClosed: session.SessionClosed,
  Refused: session.Refused,
  Session: session.Session,
  Channel: session.Channel,
  Listener: session.Listener,
  ProtocolError: wire.ProtocolError,
  wire,
};
