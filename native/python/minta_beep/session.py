"""asyncio BEEP session engine (RFC 3080/3081)."""
from __future__ import annotations

import asyncio
import logging
import ssl
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, Union

from .wire import (
    ANS, ERR, MAX_HEADER_LINE, MSG, NUL, RPY, SEQ, Frame, Mgmt, ProfileEl, ProtocolError,
    encode_frame, encode_seq, error_payload, parse_entity, parse_header, parse_mgmt, xml_entity,
)

log = logging.getLogger("minta_beep")
MASK = 0xFFFFFFFF
INITIAL_WINDOW = 4096
MAX_CHANNEL = 2_147_483_647
TLS_PROFILE = "http://iana.org/beep/TLS"


class BeepError(Exception):
    """Base class for BEEP failures."""


class SessionClosed(BeepError):
    pass


class Refused(BeepError):
    """The peer answered a management request with <error>."""

    def __init__(self, code: int, message: str):
        super().__init__(f"refused by peer ({code}): {message}")
        self.code = code
        self.message = message


@dataclass
class Config:
    recv_window: int = 262_144
    max_frame_size: int = 65_536
    max_message_size: int = 64 << 20
    greeting_timeout: float = 10.0
    close_timeout: float = 10.0


@dataclass
class TlsClient:
    server_name: str
    cafile: Optional[str] = None
    context: Optional[ssl.SSLContext] = None

    def ssl_context(self) -> ssl.SSLContext:
        if self.context:
            return self.context
        ctx = ssl.create_default_context(cafile=self.cafile)
        return ctx


@dataclass
class TlsServer:
    certfile: str
    keyfile: str

    def ssl_context(self) -> ssl.SSLContext:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(self.certfile, self.keyfile)
        return ctx


class Reply:
    """A complete reply: kind is 'RPY', 'ERR' or 'ANS' (payload is then a list of (ansno, bytes))."""

    def __init__(self, kind: str, payload: Union[bytes, List[Tuple[int, bytes]]]):
        self.kind = kind
        self.payload = payload

    def __repr__(self) -> str:
        return f"Reply({self.kind}, {self.payload!r})"


class _Sink:
    """Queue of events for one message, with consumption-based window credit."""

    def __init__(self, session: "Session", channel: int):
        self.session = session
        self.channel = channel
        self.queue: asyncio.Queue = asyncio.Queue()
        self.discarding = False
        self.ans_open: set = set()

    def deliver(self, item: tuple, nbytes: int) -> None:
        if self.discarding:
            self.session._credit(self.channel, nbytes)
            return
        self.queue.put_nowait((item, nbytes))

    def discard(self) -> None:
        self.discarding = True
        while not self.queue.empty():
            _, n = self.queue.get_nowait()
            self.session._credit(self.channel, n)


class ReplyStream:
    """Events for the reply to one MSG: ('RPY', data, last), ('ERR', data, last),
    ('ANS', ansno, data, last), ('NUL',)."""

    def __init__(self, session: "Session", sink: _Sink):
        self._s = session
        self._sink = sink
        self.done = False

    async def next(self) -> Optional[tuple]:
        if self.done:
            return None
        # Session failure pushes a CLOSED marker into every pending sink.
        item, n = await self._sink.queue.get()
        self._s._credit(self._sink.channel, n)
        if item[0] == "CLOSED":
            self.done = True
            raise self._s._closed_error()
        if item[0] == NUL or (item[0] in (RPY, ERR) and item[-1]):
            self.done = True
        return item

    async def collect(self, max_size: int = 64 << 20) -> Reply:
        buf = bytearray()
        ans: Dict[int, bytearray] = {}
        kind = RPY
        total = 0
        while True:
            ev = await self.next()
            if ev is None:
                break
            if ev[0] in (RPY, ERR):
                kind = ev[0]
                buf += ev[1]
                total += len(ev[1])
            elif ev[0] == ANS:
                kind = ANS
                ans.setdefault(ev[1], bytearray()).extend(ev[2])
                total += len(ev[2])
            elif ev[0] == NUL:
                kind = ANS
            if total > max_size:
                self.discard()
                raise BeepError(f"reply exceeds {max_size} bytes")
        if kind == ANS:
            return Reply(ANS, [(k, bytes(ans[k])) for k in sorted(ans)])
        return Reply(kind, bytes(buf))

    def discard(self) -> None:
        """Stop consuming; remaining data is acknowledged as it arrives."""
        self._sink.discard()


class IncomingMessage:
    def __init__(self, session: "Session", channel: int, msgno: int):
        self.channel = channel
        self.msgno = msgno
        self._s = session
        self._sink = _Sink(session, channel)
        self._done = False

    async def read(self) -> Optional[bytes]:
        """Next body chunk, or None at the end of the message."""
        if self._done:
            return None
        item, n = await self._sink.queue.get()
        self._s._credit(self.channel, n)
        if item[0] == "END":
            self._done = True
            return None
        if item[0] == "CLOSED":
            self._done = True
            raise self._s._closed_error()
        return item[1]

    async def collect(self, max_size: int = 64 << 20) -> bytes:
        buf = bytearray()
        while True:
            c = await self.read()
            if c is None:
                return bytes(buf)
            buf += c
            if len(buf) > max_size:
                self.discard()
                raise BeepError(f"message exceeds {max_size} bytes")

    def discard(self) -> None:
        self._done = True
        self._sink.discard()


_IDLE, _ANSWERING, _DONE = 0, 1, 2


class Responder:
    """Sends the reply to one incoming MSG (RPY, ERR, or ANS…NUL)."""

    def __init__(self, session: "Session", channel: int, msgno: int):
        self._s = session
        self.channel = channel
        self.msgno = msgno
        self.state = _IDLE
        self._next_ans = 0

    async def rpy(self, payload: bytes) -> None:
        self._final(RPY)
        try:
            await self._s._send_message(self.channel, RPY, self.msgno, 0, payload)
        finally:
            self._s._finish_inbound(self.channel, self.msgno)

    async def err(self, payload: bytes) -> None:
        self._final(ERR)
        try:
            await self._s._send_message(self.channel, ERR, self.msgno, 0, payload)
        finally:
            self._s._finish_inbound(self.channel, self.msgno)

    async def error(self, code: int, message: str) -> None:
        await self.err(error_payload(code, message))

    async def ans(self, payload: bytes) -> int:
        if self.state == _DONE:
            raise BeepError("reply already completed")
        self.state = _ANSWERING
        n = self._next_ans
        self._next_ans += 1
        await self._s._send_message(self.channel, ANS, self.msgno, n, payload)
        return n

    async def nul(self) -> None:
        if self.state == _DONE:
            raise BeepError("reply already completed")
        self.state = _DONE
        try:
            await self._s._send_message(self.channel, NUL, self.msgno, 0, b"")
        finally:
            self._s._finish_inbound(self.channel, self.msgno)

    def _final(self, kind: str) -> None:
        if self.state != _IDLE:
            raise BeepError(f"{kind} not allowed after ANS or completion")
        self.state = _DONE


class ProfileHandler:
    """Override on_message (and optionally on_start) to serve a profile."""

    async def on_start(self, session: "Session", channel: int, init: Optional[str]) -> Optional[str]:
        """Accept by returning init-reply data (or None); refuse by raising Refused."""
        return None

    async def on_message(self, session: "Session", msg: IncomingMessage, reply: Responder) -> None:
        raise NotImplementedError


class _Chan:
    def __init__(self, number: int, profile: str = "", next_msgno: int = 0):
        self.number = number
        self.profile = profile
        self.tx_seq = 0
        self.tx_limit = INITIAL_WINDOW
        self.tx_event = asyncio.Event()
        self.tx_lock = asyncio.Lock()
        self.dead = False
        self.recv_seq = 0
        self.adv_ack = 0
        self.adv_window = INITIAL_WINDOW
        self.consumed = 0
        self.next_msgno = next_msgno
        self.outstanding: Dict[int, _Sink] = {}
        self.rx_reply: Optional[Tuple[str, int]] = None
        self.rx_msg: Optional[IncomingMessage] = None
        self.inbound: set = set()
        self.queue: Optional[asyncio.Queue] = None

    def busy(self) -> bool:
        return bool(self.outstanding or self.inbound or self.rx_msg)

    def kill(self) -> None:
        self.dead = True
        self.tx_event.set()


class Channel:
    def __init__(self, session: "Session", number: int, profile: str):
        self.session = session
        self.number = number
        self.profile = profile

    def __repr__(self) -> str:
        return f"Channel({self.number}, {self.profile})"

    @property
    def is_open(self) -> bool:
        return self.number in self.session._chans and not self.session.closed

    @property
    def pending(self) -> int:
        c = self.session._chans.get(self.number)
        return len(c.outstanding) if c else 0

    async def send(self, payload: bytes) -> ReplyStream:
        return await self.session._send_msg(self.number, payload)

    async def request(self, payload: bytes) -> Reply:
        st = await self.send(payload)
        return await st.collect(self.session.config.max_message_size)

    async def close(self, code: int = 200) -> None:
        await self.session._close_channel(self.number, code)


Profiles = Dict[str, ProfileHandler]


class Session:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, initiator: bool,
                 config: Optional[Config] = None, profiles: Optional[Profiles] = None):
        self.config = config or Config()
        self.profiles: Profiles = dict(profiles or {})
        self.initiator = initiator
        self.peer_profiles: List[str] = []
        self._reader = reader
        self._writer = writer
        self._out: asyncio.Queue = asyncio.Queue()
        self._chans: Dict[int, _Chan] = {0: _Chan(0, "", 1)}
        self._next_chan = 1 if initiator else 2
        self._closed: Optional[BaseException] = None
        self._closed_evt = asyncio.Event()
        self._releasing = False
        self._tasks: List[asyncio.Task] = []

    # -- lifecycle -----------------------------------------------------------

    @classmethod
    async def connect(cls, host: str, port: int, config: Optional[Config] = None,
                      profiles: Optional[Profiles] = None, tls: Optional[TlsClient] = None) -> "Session":
        reader, writer = await asyncio.open_connection(host, port)
        _nodelay(writer)
        if tls is not None:
            await _tls_client_bootstrap(reader, writer, tls)
        return await cls.establish(reader, writer, True, config, profiles)

    @classmethod
    async def establish(cls, reader, writer, initiator: bool, config: Optional[Config] = None,
                        profiles: Optional[Profiles] = None) -> "Session":
        s = cls(reader, writer, initiator, config, profiles)
        greet = _Sink(s, 0)
        s._chans[0].outstanding[0] = greet
        s._chans[0].queue = asyncio.Queue()
        s._tasks.append(asyncio.ensure_future(s._writer_loop()))
        s._tasks.append(asyncio.ensure_future(s._reader_loop()))
        greeting = Mgmt("greeting", [ProfileEl(u) for u in sorted(s.profiles)])
        await s._send_message(0, RPY, 0, 0, xml_entity(greeting.xml()))
        s._open_window(0)
        try:
            reply = await asyncio.wait_for(ReplyStream(s, greet).collect(1 << 20), s.config.greeting_timeout)
        except asyncio.TimeoutError:
            s._fail(SessionClosed("greeting timed out"))
            raise SessionClosed("greeting timed out")
        except BaseException as e:
            s._fail(e if isinstance(e, BeepError) else SessionClosed(str(e)))
            raise
        _, body = parse_entity(reply.payload) if reply.kind != ANS else ([], b"")
        try:
            m = parse_mgmt(body)
        except ProtocolError as e:
            s._fail(e)
            raise
        if reply.kind == ERR or m.kind == "error":
            err = Refused(m.code, m.message) if m.kind == "error" else Refused(421, "peer refused session")
            s._fail(err)
            raise err
        if m.kind != "greeting":
            s._fail(ProtocolError(f"expected greeting, got {m.kind}"))
            raise ProtocolError(f"expected greeting, got {m.kind}")
        s.peer_profiles = [p.uri for p in m.profiles]
        s._tasks.append(asyncio.ensure_future(s._mgmt_loop()))
        return s

    @property
    def closed(self) -> bool:
        return self._closed is not None

    @property
    def channel_count(self) -> int:
        return len(self._chans) - 1

    async def wait_closed(self) -> BaseException:
        await self._closed_evt.wait()
        return self._closed  # type: ignore[return-value]

    async def start_channel(self, uri: Union[str, List[ProfileEl]], server_name: Optional[str] = None) -> Channel:
        ch, _ = await self.start_channel_with(uri, server_name)
        return ch

    async def start_channel_with(self, uri: Union[str, List[ProfileEl]], server_name: Optional[str] = None) -> Tuple[Channel, Optional[str]]:
        self._check_open()
        profiles = [ProfileEl(uri)] if isinstance(uri, str) else uri
        n = self._next_chan
        for _ in range(1_000_000):
            if n not in self._chans:
                break
            n = _next_number(n)
        else:
            raise SessionClosed("no free channel numbers")
        self._next_chan = _next_number(n)
        self._chans[n] = _Chan(n)
        try:
            m = await self._mgmt_request(Mgmt("start", profiles, number=n, server_name=server_name))
        except BaseException:
            self._chans.pop(n, None)
            raise
        if m.kind == "error":
            self._chans.pop(n, None)
            raise Refused(m.code, m.message)
        if m.kind != "profile":
            self._chans.pop(n, None)
            raise ProtocolError(f"unexpected reply to <start>: {m.kind}")
        chan = self._chans.get(n)
        if chan is None:
            raise self._closed_error()
        p = m.profiles[0]
        chan.profile = p.uri
        chan.queue = asyncio.Queue()
        handler = self.profiles.get(p.uri)
        self._spawn(self._channel_loop(n, handler, chan.queue))
        self._open_window(n)
        return Channel(self, n, p.uri), p.data

    async def close(self) -> None:
        """Gracefully release the session (close channel 0)."""
        if self.closed:
            return
        try:
            m = await asyncio.wait_for(self._mgmt_request(Mgmt("close", number=0, code=200)), self.config.close_timeout)
        except BaseException:
            self.abort()
            raise
        if m.kind == "ok":
            self._fail(SessionClosed("session released"))
        elif m.kind == "error":
            raise Refused(m.code, m.message)

    def abort(self) -> None:
        self._fail(SessionClosed("session aborted"))

    # -- internals -----------------------------------------------------------

    def _spawn(self, coro: Awaitable) -> None:
        t = asyncio.ensure_future(coro)
        self._tasks.append(t)
        t.add_done_callback(lambda t: self._tasks.remove(t) if t in self._tasks else None)

    def _check_open(self) -> None:
        if self._closed is not None:
            raise self._closed_error()

    def _closed_error(self) -> BeepError:
        e = self._closed
        if isinstance(e, BeepError):
            return e
        return SessionClosed(str(e) if e else "channel closed")

    def _fail(self, err: BaseException) -> None:
        if self._closed is not None:
            return
        log.debug("beep session ended: %s", err)
        self._closed = err
        chans, self._chans = self._chans, {}
        for c in chans.values():
            c.kill()
            for sink in c.outstanding.values():
                sink.queue.put_nowait((("CLOSED",), 0))
            if c.rx_msg is not None:
                c.rx_msg._sink.queue.put_nowait((("CLOSED",), 0))
            if c.queue is not None:
                c.queue.put_nowait(None)
        self._out.put_nowait(None)
        self._closed_evt.set()

    def _credit(self, channel: int, n: int) -> None:
        if n <= 0:
            return
        c = self._chans.get(channel)
        if c is None:
            return
        c.consumed = (c.consumed + n) & MASK
        w = self.config.recv_window
        if ((c.consumed - c.adv_ack) & MASK) >= w // 2:
            c.adv_ack = c.consumed
            c.adv_window = w
            self._out.put_nowait(encode_seq(channel, c.consumed, w))

    def _open_window(self, channel: int) -> None:
        c = self._chans.get(channel)
        if c is None:
            return
        c.adv_ack = c.consumed
        c.adv_window = self.config.recv_window
        self._out.put_nowait(encode_seq(channel, c.consumed, c.adv_window))

    def _finish_inbound(self, channel: int, msgno: int) -> None:
        c = self._chans.get(channel)
        if c:
            c.inbound.discard(msgno)

    async def _writer_loop(self) -> None:
        w = self._writer
        try:
            while True:
                item = await self._out.get()
                stop = item is None
                if not stop:
                    w.write(item)
                while not stop and not self._out.empty():
                    item = self._out.get_nowait()
                    if item is None:
                        stop = True
                    else:
                        w.write(item)
                await w.drain()
                if stop:
                    break
        except (ConnectionError, OSError) as e:
            self._fail(SessionClosed(f"write failed: {e}"))
        finally:
            try:
                w.close()
            except Exception:
                pass

    async def _reader_loop(self) -> None:
        r = self._reader
        limit = max(self.config.recv_window, INITIAL_WINDOW)
        try:
            while True:
                try:
                    line = await r.readuntil(b"\r\n")
                except asyncio.IncompleteReadError as e:
                    if e.partial:
                        raise ProtocolError("connection closed mid-frame")
                    self._fail(SessionClosed("session released" if self._releasing else "connection closed by peer"))
                    return
                except asyncio.LimitOverrunError:
                    raise ProtocolError("frame header line too long")
                if len(line) > MAX_HEADER_LINE:
                    raise ProtocolError("frame header line too long")
                f = parse_header(line[:-2])
                if f.type == SEQ:
                    c = self._chans.get(f.channel)
                    if c is not None:
                        c.tx_limit = (f.ackno + f.window) & MASK
                        c.tx_event.set()
                    continue
                size = f.window
                if size > limit:
                    raise ProtocolError(f"frame payload {size} exceeds limit")
                data = await r.readexactly(size + 5)
                if data[-5:] != b"END\r\n":
                    raise ProtocolError("frame trailer is not END CRLF")
                f.payload = data[:-5]
                self._on_data(f)
        except ProtocolError as e:
            log.warning("beep: %s; dropping connection", e)
            self._fail(e)
        except asyncio.IncompleteReadError:
            self._fail(SessionClosed("connection closed mid-frame"))
        except (ConnectionError, OSError) as e:
            self._fail(SessionClosed(f"read failed: {e}"))
        except asyncio.CancelledError:
            pass

    def _on_data(self, f: Frame) -> None:
        c = self._chans.get(f.channel)
        if c is None:
            raise ProtocolError(f"frame on unknown channel {f.channel}")
        if f.seqno != c.recv_seq:
            raise ProtocolError(f"channel {f.channel}: expected seqno {c.recv_seq}, got {f.seqno}")
        size = len(f.payload)
        end = (c.recv_seq + size) & MASK
        if ((end - c.adv_ack) & MASK) > c.adv_window:
            raise ProtocolError(f"channel {f.channel}: frame exceeds receive window")
        c.recv_seq = end
        last = not f.more
        if f.type == MSG:
            m = c.rx_msg
            if m is not None and m.msgno != f.msgno:
                raise ProtocolError(f"channel {f.channel}: interleaved MSG frames")
            if m is None:
                if f.msgno in c.inbound:
                    raise ProtocolError(f"channel {f.channel}: msgno {f.msgno} reused while outstanding")
                c.inbound.add(f.msgno)
                m = IncomingMessage(self, f.channel, f.msgno)
                c.rx_msg = m
                if c.queue is not None:
                    c.queue.put_nowait(m)
            m._sink.deliver((MSG, f.payload, last), size)
            if last:
                c.rx_msg = None
                m._sink.deliver(("END",), 0)
        elif f.type in (RPY, ERR):
            if c.rx_reply is not None and c.rx_reply != (f.type, f.msgno):
                raise ProtocolError(f"channel {f.channel}: interleaved reply frames")
            sink = c.outstanding.get(f.msgno)
            if sink is None:
                raise ProtocolError(f"channel {f.channel}: reply to unknown msgno {f.msgno}")
            if sink.ans_open:
                raise ProtocolError("RPY/ERR while ANS in progress")
            sink.deliver((f.type, f.payload, last), size)
            if last:
                c.rx_reply = None
                del c.outstanding[f.msgno]
            else:
                c.rx_reply = (f.type, f.msgno)
        elif f.type == ANS:
            if c.rx_reply is not None:
                raise ProtocolError("ANS while RPY/ERR in progress")
            sink = c.outstanding.get(f.msgno)
            if sink is None:
                raise ProtocolError(f"channel {f.channel}: ANS to unknown msgno {f.msgno}")
            (sink.ans_open.discard if last else sink.ans_open.add)(f.ansno)
            sink.deliver((ANS, f.ansno, f.payload, last), size)
        elif f.type == NUL:
            sink = c.outstanding.pop(f.msgno, None)
            if sink is None:
                raise ProtocolError(f"channel {f.channel}: NUL to unknown msgno {f.msgno}")
            if sink.ans_open:
                raise ProtocolError("NUL before all ANS messages completed")
            sink.deliver((NUL,), 0)

    async def _send_message(self, channel: int, ftype: str, msgno: int, ansno: int, payload: bytes) -> None:
        self._check_open()
        c = self._chans.get(channel)
        if c is None:
            raise SessionClosed(f"channel {channel} is not open")
        max_frame = max(1, self.config.max_frame_size)
        mv = memoryview(payload)
        total = len(payload)
        async with c.tx_lock:
            off = 0
            while True:
                if c.dead or self._closed is not None:
                    raise self._closed_error()
                avail = (c.tx_limit - c.tx_seq) & MASK
                if avail >= 1 << 31:
                    avail = 0
                remaining = total - off
                if remaining == 0 or avail > 0:
                    n = min(remaining, avail, max_frame)
                    more = off + n < total
                    self._out.put_nowait(encode_frame(ftype, channel, msgno, more, c.tx_seq, bytes(mv[off:off + n]), ansno))
                    c.tx_seq = (c.tx_seq + n) & MASK
                    off += n
                    if not more:
                        return
                    if self._out.qsize() > 64:
                        await asyncio.sleep(0)
                    continue
                c.tx_event.clear()
                await c.tx_event.wait()

    async def _send_msg(self, channel: int, payload: bytes) -> ReplyStream:
        self._check_open()
        c = self._chans.get(channel)
        if c is None:
            raise SessionClosed(f"channel {channel} is not open")
        n = c.next_msgno
        while n in c.outstanding:
            n = 0 if n >= MAX_CHANNEL else n + 1
        c.next_msgno = 0 if n >= MAX_CHANNEL else n + 1
        sink = _Sink(self, channel)
        c.outstanding[n] = sink

        async def transmit() -> None:
            try:
                await self._send_message(channel, MSG, n, 0, payload)
            except BeepError:
                ch = self._chans.get(channel)
                if ch is not None and ch.outstanding.pop(n, None) is not None:
                    sink.queue.put_nowait((("CLOSED",), 0))

        self._spawn(transmit())
        return ReplyStream(self, sink)

    async def _mgmt_request(self, m: Mgmt) -> Mgmt:
        st = await self._send_msg(0, xml_entity(m.xml()))
        reply = await st.collect(1 << 20)
        if reply.kind == ANS:
            raise ProtocolError("ANS on channel 0")
        return parse_mgmt(parse_entity(reply.payload)[1])

    async def _close_channel(self, number: int, code: int) -> None:
        c = self._chans.get(number)
        if c is not None and c.busy():
            raise Refused(550, "still working")
        m = await self._mgmt_request(Mgmt("close", number=number, code=code))
        if m.kind == "ok":
            c = self._chans.pop(number, None)
            if c is not None:
                c.kill()
                if c.queue is not None:
                    c.queue.put_nowait(None)
        elif m.kind == "error":
            raise Refused(m.code, m.message)
        else:
            raise ProtocolError(f"unexpected reply to <close>: {m.kind}")

    async def _mgmt_loop(self) -> None:
        q = self._chans[0].queue
        assert q is not None
        while True:
            msg = await q.get()
            if msg is None:
                return
            responder = Responder(self, 0, msg.msgno)
            try:
                body = await msg.collect(1 << 20)
            except BeepError:
                return
            opened = None
            try:
                m = parse_mgmt(parse_entity(body)[1])
                if m.kind == "start":
                    reply = await self._handle_start(m)
                    opened = m.number
                elif m.kind == "close":
                    reply = self._handle_close(m.number)
                else:
                    raise Refused(501, f"unexpected element <{m.kind}>")
            except Refused as e:
                reply = Mgmt("error", code=e.code, message=e.message)
                opened = None
            except ProtocolError as e:
                reply = Mgmt("error", code=500, message=str(e))
            try:
                if reply.kind == "error":
                    await responder.error(reply.code, reply.message)
                else:
                    await responder.rpy(xml_entity(reply.xml()))
            except BeepError:
                return
            if opened is not None:
                # The SEQ for a new channel must reach the peer after the RPY that creates it.
                self._open_window(opened)
            if reply.kind == "ok" and self._releasing:
                self._out.put_nowait(None)
                return

    async def _handle_start(self, m: Mgmt) -> Mgmt:
        if self._releasing:
            raise Refused(550, "session is being released")
        peer_parity = 0 if self.initiator else 1
        n = m.number
        if n == 0 or n % 2 != peer_parity or n in self._chans:
            raise Refused(553, f"channel number {n} is invalid or in use")
        for p in m.profiles:
            handler = self.profiles.get(p.uri)
            if handler is not None:
                break
        else:
            raise Refused(550, "no acceptable profile")
        init_reply = await handler.on_start(self, n, p.data)
        if n in self._chans:
            raise Refused(553, f"channel number {n} is in use")
        c = _Chan(n, p.uri)
        c.queue = asyncio.Queue()
        self._chans[n] = c
        self._spawn(self._channel_loop(n, handler, c.queue))
        return Mgmt("profile", [ProfileEl(p.uri, init_reply)])

    def _handle_close(self, n: int) -> Mgmt:
        if n == 0:
            if any(c.busy() for k, c in self._chans.items() if k != 0):
                raise Refused(550, "still working")
            self._releasing = True
            return Mgmt("ok")
        c = self._chans.get(n)
        if c is None:
            raise Refused(553, f"channel {n} is not open")
        if c.busy():
            raise Refused(550, "still working")
        del self._chans[n]
        c.kill()
        if c.queue is not None:
            c.queue.put_nowait(None)
        return Mgmt("ok")

    async def _channel_loop(self, number: int, handler: Optional[ProfileHandler], q: asyncio.Queue) -> None:
        while True:
            msg = await q.get()
            if msg is None:
                return
            r = Responder(self, number, msg.msgno)
            try:
                if handler is None:
                    msg.discard()
                    await r.error(550, "this peer does not serve the profile")
                    continue
                try:
                    await handler.on_message(self, msg, r)
                except BeepError:
                    if self.closed:
                        return
                    raise
                msg.discard()
                if r.state == _IDLE:
                    await r.error(554, "handler produced no reply")
                elif r.state == _ANSWERING:
                    await r.nul()
            except BeepError:
                if self.closed:
                    return
            except Exception as e:  # handler bug: answer ERR, keep serving
                log.exception("beep handler failed")
                msg.discard()
                try:
                    if r.state == _IDLE:
                        await r.error(554, f"handler failed: {e}")
                    elif r.state == _ANSWERING:
                        await r.nul()
                except BeepError:
                    if self.closed:
                        return


def _next_number(n: int) -> int:
    return 2 - n % 2 if n + 2 > MAX_CHANNEL else n + 2


def _nodelay(writer: asyncio.StreamWriter) -> None:
    import socket
    sock = writer.get_extra_info("socket")
    if sock is not None:
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Listener


class Listener:
    def __init__(self, profiles: Profiles, config: Optional[Config] = None, tls: Optional[TlsServer] = None,
                 on_session: Optional[Callable[[Session], Any]] = None):
        self.profiles = profiles
        self.config = config or Config()
        self.tls = tls
        self.on_session = on_session
        self.sessions: set = set()
        self._server: Optional[asyncio.base_events.Server] = None

    async def start(self, host: str = "127.0.0.1", port: int = 10288) -> "Listener":
        self._server = await asyncio.start_server(self._accept, host, port)
        return self

    @property
    def port(self) -> int:
        assert self._server is not None
        return self._server.sockets[0].getsockname()[1]

    async def serve_forever(self) -> None:
        assert self._server is not None
        await self._server.serve_forever()

    async def close(self) -> None:
        if self._server is not None:
            self._server.close()
        for s in list(self.sessions):
            s.abort()

    async def _accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        _nodelay(writer)
        try:
            if self.tls is not None:
                await _tls_server_bootstrap(reader, writer, self.tls)
            s = await Session.establish(reader, writer, False, self.config, self.profiles)
        except (BeepError, OSError, asyncio.IncompleteReadError, ssl.SSLError) as e:
            log.debug("beep handshake failed: %s", e)
            writer.close()
            return
        self.sessions.add(s)
        if self.on_session:
            self.on_session(s)
        await s.wait_closed()
        self.sessions.discard(s)


# ---------------------------------------------------------------------------
# TLS tuning profile (RFC 3080 §3.1) bootstrap


class _Boot:
    def __init__(self, reader, writer):
        self.r, self.w = reader, writer
        self.seq: Dict[int, int] = {}

    async def send(self, ftype: str, channel: int, msgno: int, xml: str) -> None:
        payload = xml_entity(xml)
        s = self.seq.get(channel, 0)
        self.w.write(encode_frame(ftype, channel, msgno, False, s, payload))
        self.seq[channel] = (s + len(payload)) & MASK
        await self.w.drain()

    async def recv(self) -> Tuple[str, int, int, bytes]:
        acc = bytearray()
        while True:
            line = await self.r.readuntil(b"\r\n")
            f = parse_header(line[:-2])
            if f.type == SEQ:
                continue
            data = await self.r.readexactly(f.window + 5)
            if data[-5:] != b"END\r\n":
                raise ProtocolError("bad trailer during TLS negotiation")
            acc += data[:-5]
            if len(acc) > 65536:
                raise ProtocolError("bootstrap message too large")
            if not f.more:
                return f.type, f.channel, f.msgno, parse_entity(bytes(acc))[1]


def _require_start_tls(writer) -> None:
    if not hasattr(writer, "start_tls"):
        raise BeepError("TLS tuning requires Python 3.11+ (StreamWriter.start_tls)")


async def _tls_client_bootstrap(reader, writer, tls: TlsClient) -> None:
    _require_start_tls(writer)
    b = _Boot(reader, writer)
    await b.send(RPY, 0, 0, "<greeting />")
    t, ch, _, body = await b.recv()
    g = parse_mgmt(body)
    if t != RPY or ch != 0 or g.kind != "greeting":
        raise ProtocolError("expected greeting")
    if TLS_PROFILE not in [p.uri for p in g.profiles]:
        raise BeepError("peer does not offer the TLS profile")
    await b.send(MSG, 0, 1, Mgmt("start", [ProfileEl(TLS_PROFILE, "<ready />")], number=1, server_name=tls.server_name).xml())
    t, ch, _, body = await b.recv()
    m = parse_mgmt(body)
    if m.kind == "error":
        raise Refused(m.code, m.message)
    if t != RPY or m.kind != "profile" or "<proceed" not in (m.profiles[0].data or ""):
        raise BeepError(f"peer did not proceed with TLS: {m}")
    await writer.start_tls(tls.ssl_context(), server_hostname=tls.server_name)


async def _tls_server_bootstrap(reader, writer, tls: TlsServer) -> None:
    _require_start_tls(writer)
    b = _Boot(reader, writer)
    await b.send(RPY, 0, 0, Mgmt("greeting", [ProfileEl(TLS_PROFILE)]).xml())
    t, ch, msgno, _ = await b.recv()
    if t != RPY or ch != 0 or msgno != 0:
        raise ProtocolError("expected greeting")
    tls_channel = None
    for _ in range(16):
        t, ch, msgno, body = await b.recv()
        if t != MSG:
            raise ProtocolError("expected MSG during TLS negotiation")
        if ch == tls_channel:
            if b"<ready" in body:
                await b.send(RPY, ch, msgno, "<proceed />")
                break
            await b.send(ERR, ch, msgno, Mgmt("error", code=500, message="expected <ready />").xml())
            continue
        m = parse_mgmt(body)
        if m.kind == "start" and m.number % 2 == 1 and any(p.uri == TLS_PROFILE for p in m.profiles):
            p = next(p for p in m.profiles if p.uri == TLS_PROFILE)
            if p.data and "<ready" in p.data:
                await b.send(RPY, 0, msgno, ProfileEl(TLS_PROFILE, "<proceed />").xml())
                break
            await b.send(RPY, 0, msgno, ProfileEl(TLS_PROFILE).xml())
            tls_channel = m.number
        elif m.kind == "close" and m.number == 0:
            await b.send(RPY, 0, msgno, "<ok />")
            raise SessionClosed("peer released session before TLS")
        else:
            await b.send(ERR, 0, msgno, Mgmt("error", code=538, message="TLS required").xml())
    else:
        raise BeepError("peer never requested TLS")
    await writer.start_tls(tls.ssl_context())
