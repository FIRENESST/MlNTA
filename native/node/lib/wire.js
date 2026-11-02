'use strict';
// BEEP wire format: frames (RFC 3080 §2.2, RFC 3081 §3.1), MIME entities, channel-0 XML.

const MAX_HEADER_LINE = 128;
const MAX_I31 = 2147483647;
const MAX_U32 = 4294967295;
const BEEP_XML = 'application/beep+xml';
const CRLF = Buffer.from('\r\n');
const TRAILER = Buffer.from('END\r\n');

class ProtocolError extends Error {
  constructor(msg) {
    super(msg);
    this.name = 'ProtocolError';
  }
}

function num(s, max) {
  if (!/^[0-9]{1,10}$/.test(s)) throw new ProtocolError(`bad number ${JSON.stringify(s)} in frame header`);
  const v = Number(s);
  if (v > max) throw new ProtocolError(`number ${v} out of range`);
  return v;
}

/** Incremental frame decoder. push() bytes, then call next() until it returns null. */
class FrameDecoder {
  constructor(maxPayload) {
    this.maxPayload = maxPayload;
    this.buf = Buffer.alloc(0);
  }
  push(chunk) {
    this.buf = this.buf.length ? Buffer.concat([this.buf, chunk]) : chunk;
  }
  get pending() {
    return this.buf.length;
  }
  next() {
    const buf = this.buf;
    const scan = Math.min(buf.length, MAX_HEADER_LINE);
    const lineEnd = buf.subarray(0, scan).indexOf(CRLF);
    if (lineEnd < 0) {
      if (buf.length >= MAX_HEADER_LINE) throw new ProtocolError('frame header line too long');
      return null;
    }
    const line = buf.toString('latin1', 0, lineEnd);
    if (!/^[\x20-\x7e]*$/.test(line)) throw new ProtocolError('frame header is not ASCII');
    const parts = line.split(' ');
    const headLen = lineEnd + 2;
    const kw = parts[0];
    if (kw === 'SEQ') {
      if (parts.length !== 4) throw new ProtocolError(`malformed SEQ header ${JSON.stringify(line)}`);
      const f = { type: 'SEQ', channel: num(parts[1], MAX_I31), ackno: num(parts[2], MAX_U32), window: num(parts[3], MAX_I31) };
      this.buf = buf.subarray(headLen);
      return f;
    }
    if (!['MSG', 'RPY', 'ERR', 'ANS', 'NUL'].includes(kw)) throw new ProtocolError(`unknown frame keyword ${JSON.stringify(kw)}`);
    if (parts.length !== (kw === 'ANS' ? 7 : 6)) throw new ProtocolError(`malformed frame header ${JSON.stringify(line)}`);
    if (parts[3] !== '.' && parts[3] !== '*') throw new ProtocolError(`bad continuation indicator in ${JSON.stringify(line)}`);
    const f = {
      type: kw,
      channel: num(parts[1], MAX_I31),
      msgno: num(parts[2], MAX_I31),
      more: parts[3] === '*',
      seqno: num(parts[4], MAX_U32),
      ansno: kw === 'ANS' ? num(parts[6], MAX_I31) : 0,
      payload: null,
    };
    const size = num(parts[5], MAX_I31);
    if (kw === 'NUL' && (size !== 0 || f.more)) throw new ProtocolError('NUL frame must have size 0 and no continuation');
    if (size > this.maxPayload) throw new ProtocolError(`frame payload ${size} exceeds limit ${this.maxPayload}`);
    const total = headLen + size + 5;
    if (buf.length < total) return null;
    if (!buf.subarray(headLen + size, total).equals(TRAILER)) throw new ProtocolError('frame trailer is not END CRLF');
    f.payload = Buffer.from(buf.subarray(headLen, headLen + size));
    this.buf = buf.subarray(total);
    return f;
  }
}

function encodeFrame(type, channel, msgno, more, seqno, payload, ansno = 0) {
  const m = more ? '*' : '.';
  const head = type === 'ANS'
    ? `ANS ${channel} ${msgno} ${m} ${seqno} ${payload.length} ${ansno}\r\n`
    : `${type} ${channel} ${msgno} ${m} ${seqno} ${payload.length}\r\n`;
  return Buffer.concat([Buffer.from(head, 'latin1'), payload, TRAILER]);
}

function encodeSeq(channel, ackno, window) {
  return Buffer.from(`SEQ ${channel} ${ackno} ${window}\r\n`, 'latin1');
}

// ---------------------------------------------------------------------------
// MIME entities

const clean = (s) => String(s).replace(/[\r\n]/g, '');

/** headers: array of [name, value] */
function encodeEntity(headers, body) {
  const head = headers.map(([k, v]) => `${clean(k)}: ${clean(v)}\r\n`).join('') + '\r\n';
  const b = body == null ? Buffer.alloc(0) : Buffer.isBuffer(body) ? body : Buffer.from(body);
  return Buffer.concat([Buffer.from(head, 'utf8'), b]);
}

/** Returns {headers, offset} or null when undecidable yet (docs/PROTOCOL.md §1.2). */
function splitHead(buf, complete) {
  if (buf.length >= 2 && buf[0] === 13 && buf[1] === 10) return { headers: [], offset: 2 };
  const p = buf.indexOf(CRLF);
  if (p < 0) return complete ? { headers: [], offset: 0 } : null;
  if (buf.subarray(0, p).indexOf(0x3a) < 0) return { headers: [], offset: 0 };
  const q = buf.indexOf('\r\n\r\n');
  if (q < 0) return complete ? { headers: [], offset: 0 } : null;
  return { headers: parseHeaderBlock(buf.toString('utf8', 0, q + 2)), offset: q + 4 };
}

function parseHeaderBlock(text) {
  const out = [];
  for (const line of text.split('\r\n')) {
    if (!line) continue;
    if ((line[0] === ' ' || line[0] === '\t') && out.length) {
      out[out.length - 1][1] += ' ' + line.trim();
      continue;
    }
    const i = line.indexOf(':');
    if (i > 0) out.push([line.slice(0, i).trim(), line.slice(i + 1).trim()]);
  }
  return out;
}

function parseEntity(payload) {
  const { headers, offset } = splitHead(payload, true);
  return { headers, body: payload.subarray(offset) };
}

function getHeader(headers, name) {
  const n = name.toLowerCase();
  for (const [k, v] of headers) if (k.toLowerCase() === n) return v;
  return undefined;
}

// ---------------------------------------------------------------------------
// Minimal XML reader for channel 0 (DOCTYPE rejected → no entity expansion attacks).

function unescape(s) {
  if (!s.includes('&')) return s;
  return s.replace(/&([^;]*);/g, (m, e) => {
    switch (e) {
      case 'lt': return '<';
      case 'gt': return '>';
      case 'amp': return '&';
      case 'quot': return '"';
      case 'apos': return "'";
      default:
        if (/^#x[0-9a-f]+$/i.test(e)) return String.fromCodePoint(parseInt(e.slice(2), 16));
        if (/^#[0-9]+$/.test(e)) return String.fromCodePoint(parseInt(e.slice(1), 10));
        throw new ProtocolError(`unknown XML entity &${e};`);
    }
  });
}

function parseXml(src) {
  let i = 0;
  const starts = (p) => src.startsWith(p, i);
  const skipPast = (end) => {
    const p = src.indexOf(end, i);
    if (p < 0) throw new ProtocolError(`unterminated XML construct, expected ${end}`);
    const start = i;
    i = p + end.length;
    return start;
  };
  const ws = () => { while (i < src.length && /\s/.test(src[i])) i++; };
  const misc = () => {
    for (;;) {
      ws();
      if (starts('<?')) skipPast('?>');
      else if (starts('<!--')) skipPast('-->');
      else if (starts('<!')) throw new ProtocolError('DOCTYPE/declarations are not allowed');
      else return;
    }
  };
  const name = () => {
    const m = /^[A-Za-z0-9_.:\-\u0080-￿]+/.exec(src.slice(i, i + 256));
    if (!m) throw new ProtocolError('expected an XML name');
    i += m[0].length;
    return m[0];
  };
  const element = (depth) => {
    if (depth > 16) throw new ProtocolError('XML nested too deeply');
    if (!starts('<')) throw new ProtocolError("expected '<'");
    i++;
    const el = { name: name(), attrs: {}, children: [] };
    for (;;) {
      ws();
      if (starts('/>')) { i += 2; return el; }
      if (starts('>')) { i++; break; }
      const an = name();
      ws();
      if (!starts('=')) throw new ProtocolError(`attribute ${an} lacks '='`);
      i++;
      ws();
      const q = src[i];
      if (q !== "'" && q !== '"') throw new ProtocolError('attribute value must be quoted');
      const end = src.indexOf(q, i + 1);
      if (end < 0) throw new ProtocolError('unterminated attribute value');
      el.attrs[an] = unescape(src.slice(i + 1, end));
      i = end + 1;
    }
    for (;;) {
      if (i >= src.length) throw new ProtocolError(`unterminated element <${el.name}>`);
      if (starts('</')) {
        i += 2;
        const close = name();
        ws();
        if (close !== el.name || !starts('>')) throw new ProtocolError(`mismatched closing tag </${close}>`);
        i++;
        return el;
      } else if (starts('<![CDATA[')) {
        i += 9;
        const start = skipPast(']]>');
        el.children.push(src.slice(start, i - 3));
      } else if (starts('<!--')) skipPast('-->');
      else if (starts('<?')) skipPast('?>');
      else if (starts('<!')) throw new ProtocolError('declarations are not allowed');
      else if (starts('<')) el.children.push(element(depth + 1));
      else {
        const p = src.indexOf('<', i);
        const end = p < 0 ? src.length : p;
        el.children.push(unescape(src.slice(i, end)));
        i = end;
      }
    }
  };
  misc();
  const root = element(0);
  misc();
  if (i !== src.length) throw new ProtocolError('trailing content after XML root');
  return root;
}

const textOf = (el) => el.children.filter((c) => typeof c === 'string').join('');
const esc = (s) => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/'/g, '&apos;').replace(/"/g, '&quot;');

function profileXml(p) {
  const enc = p.base64 ? " encoding='base64'" : '';
  if (p.data == null) return `<profile uri='${esc(p.uri)}'${enc} />`;
  if (!p.data.includes(']]>')) return `<profile uri='${esc(p.uri)}'${enc}><![CDATA[${p.data}]]></profile>`;
  return `<profile uri='${esc(p.uri)}'${enc}>${esc(p.data)}</profile>`;
}

/** Management message → XML. m = {kind, profiles, number, code, message, serverName} */
function mgmtXml(m) {
  switch (m.kind) {
    case 'greeting':
      if (!m.profiles || !m.profiles.length) return '<greeting />';
      return '<greeting>' + m.profiles.map((p) => `<profile uri='${esc(p.uri)}' />`).join('') + '</greeting>';
    case 'start': {
      const sn = m.serverName ? ` serverName='${esc(m.serverName)}'` : '';
      return `<start number='${m.number}'${sn}>` + m.profiles.map(profileXml).join('') + '</start>';
    }
    case 'close':
      return m.message
        ? `<close number='${m.number}' code='${m.code}'>${esc(m.message)}</close>`
        : `<close number='${m.number}' code='${m.code}' />`;
    case 'profile': return profileXml(m.profiles[0]);
    case 'ok': return '<ok />';
    case 'error': return `<error code='${m.code}'>${esc(m.message)}</error>`;
    default: throw new Error(`unknown management kind ${m.kind}`);
  }
}

function parseMgmt(buf) {
  const root = parseXml(buf.toString('utf8'));
  const n = (attr, dflt) => {
    const v = root.attrs[attr];
    if (v === undefined) {
      if (dflt === undefined) throw new ProtocolError(`channel 0: <${root.name}> lacks ${attr}`);
      return dflt;
    }
    if (!/^\s*[0-9]+\s*$/.test(v)) throw new ProtocolError(`channel 0: bad ${attr} attribute`);
    return Number(v);
  };
  const profiles = () => root.children.filter((c) => typeof c === 'object' && c.name === 'profile').map(profileFrom);
  switch (root.name) {
    case 'greeting': return { kind: 'greeting', profiles: profiles() };
    case 'start': {
      const number = n('number');
      if (number > MAX_I31) throw new ProtocolError('channel number out of range');
      return { kind: 'start', number, serverName: root.attrs.serverName, profiles: profiles() };
    }
    case 'close': return { kind: 'close', number: n('number'), code: n('code', 200), message: textOf(root).trim() };
    case 'profile': return { kind: 'profile', profiles: [profileFrom(root)] };
    case 'ok': return { kind: 'ok' };
    case 'error': return { kind: 'error', code: n('code', 554), message: textOf(root).trim() };
    default: throw new ProtocolError(`channel 0: unknown element <${root.name}>`);
  }
}

function profileFrom(el) {
  const t = textOf(el).trim();
  return { uri: el.attrs.uri || '', data: t ? t : null, base64: el.attrs.encoding === 'base64' };
}

const xmlEntity = (xml) => encodeEntity([['Content-Type', BEEP_XML]], Buffer.from(xml, 'utf8'));
const errorPayload = (code, message) => xmlEntity(mgmtXml({ kind: 'error', code, message }));

function parseErrorPayload(payload) {
  const { body } = parseEntity(payload);
  try {
    const m = parseMgmt(body);
    if (m.kind === 'error') return { code: m.code, message: m.message };
  } catch (_) { /* not XML */ }
  return { code: 554, message: body.toString('utf8').trim() };
}

module.exports = {
  MAX_HEADER_LINE, BEEP_XML, ProtocolError, FrameDecoder, encodeFrame, encodeSeq, encodeEntity, splitHead,
  parseEntity, getHeader, parseXml, mgmtXml, parseMgmt, xmlEntity, errorPayload, parseErrorPayload,
};
