"""The MINTA RPC profile (urn:minta:beep:rpc:1) — client pool and server."""
from __future__ import annotations

import asyncio
import json as _json
import logging
from dataclasses import dataclass, field
from typing import AsyncIterator, Awaitable, Callable, Dict, List, Mapping, Optional, Tuple, Union

from .session import (
    BeepError, Channel, Config, IncomingMessage, Listener, ProfileHandler, Refused, ReplyStream,
    Responder, Session, SessionClosed, TlsClient, TlsServer,
)
from .wire import ANS, ERR, NUL, RPY, Headers, encode_entity, get_header, parse_error_payload, split_head

log = logging.getLogger("minta_beep")

RPC_PROFILE = "urn:minta:beep:rpc:1"
_RESERVED = {"rpc-method", "rpc-target", "rpc-status", "rpc-reason", "rpc-abort", "content-transfer-encoding"}
HeadersLike = Union[None, Mapping[str, str], List[Tuple[str, str]]]


class RemoteError(BeepError):
    """The peer answered BEEP ERR: no application-level response exists."""

    def __init__(self, code: int, message: str):
        super().__init__(f"remote error {code}: {message}")
        self.code = code
        self.message = message

    @property
    def http_status(self) -> int:
        return beep_code_to_http(self.code)


class RpcTimeout(BeepError):
    pass


class StreamAborted(BeepError):
    pass


def beep_code_to_http(code: int) -> int:
    if code in (421, 450):
        return 503
    if code == 451:
        return 504
    if code in (500, 501, 553):
        return 400
    if code == 504:
        return 501
    if code in (530, 534, 535):
        return 401
    if code in (537, 538):
        return 403
    return 502


def _norm_headers(h: HeadersLike) -> Headers:
    if h is None:
        return []
    if isinstance(h, Mapping):
        return [(str(k), str(v)) for k, v in h.items()]
    return [(str(k), str(v)) for k, v in h]


def _app_headers(h: Headers) -> Headers:
    return [(k, v) for k, v in h if k.lower() not in _RESERVED]


@dataclass
class Response:
    status: int
    headers: Headers
    body: bytes = b""

    def header(self, name: str) -> Optional[str]:
        return get_header(self.headers, name)

    def text(self, encoding: str = "utf-8") -> str:
        return self.body.decode(encoding, "replace")

    def json(self):
        return _json.loads(self.body)


class _Decoder:
    """Turns reply events into (status, headers) + body chunks (docs/PROTOCOL.md §2.2–2.3)."""

    def __init__(self, stream: ReplyStream):
        self.stream = stream
        self.head: Optional[Tuple[int, Headers]] = None
        self.ready: List[bytes] = []
        self.finished = False
        self._rpy_buf: Optional[bytearray] = bytearray()
        self._ans: Dict[int, dict] = {}
        self._next_ans = 0

    async def read_head(self) -> Tuple[int, Headers]:
        while self.head is None:
            if self.finished:
                raise BeepError("reply ended before a response head")
            await self._pump()
        return self.head

    async def next_body(self) -> Optional[bytes]:
        while True:
            if self.ready:
                return self.ready.pop(0)
            if self.finished:
                return None
            await self._pump()

    def _set_head(self, headers: Headers) -> None:
        st = get_header(headers, "Rpc-Status")
        try:
            status = int(st) if st is not None else 200
        except ValueError:
            raise BeepError(f"bad Rpc-Status {st!r}")
        self.head = (status, _app_headers(headers))

    async def _pump(self) -> None:
        ev = await self.stream.next()
        if ev is None:
            self.finished = True
            return
        kind = ev[0]
        if kind == RPY:
            data, last = ev[1], ev[2]
            if self._rpy_buf is not None:
                self._rpy_buf += data
                r = split_head(bytes(self._rpy_buf), last)
                if r is not None:
                    headers, off = r
                    rest = bytes(self._rpy_buf[off:])
                    self._rpy_buf = None
                    self._set_head(headers)
                    if rest:
                        self.ready.append(rest)
                elif len(self._rpy_buf) > 65536:
                    raise BeepError("response head too large")
            elif data:
                self.ready.append(data)
            if last:
                self.finished = True
        elif kind == ERR:
            buf = bytearray(ev[1])
            last = ev[2]
            while not last:
                nxt = await self.stream.next()
                if nxt is None or nxt[0] != ERR:
                    raise BeepError("truncated ERR")
                buf += nxt[1]
                last = nxt[2]
            self.finished = True
            code, message = parse_error_payload(bytes(buf))
            raise RemoteError(code, message)
        elif kind == ANS:
            ansno, data, last = ev[1], ev[2], ev[3]
            a = self._ans.setdefault(ansno, {"head": bytearray(), "data": [], "done": False, "parsed": False})
            if not a["parsed"]:
                a["head"] += data
                r = split_head(bytes(a["head"]), last)
                if r is not None:
                    headers, off = r
                    a["parsed"] = True
                    abort = get_header(headers, "Rpc-Abort")
                    if abort is not None:
                        raise StreamAborted(abort)
                    if ansno == 0:
                        self._set_head(headers)
                    rest = bytes(a["head"][off:])
                    if rest:
                        a["data"].append(rest)
            elif data:
                a["data"].append(data)
            if last:
                a["done"] = True
            self._flush(False)
        elif kind == NUL:
            self._flush(True)
            self.finished = True
            if self.head is None:
                raise BeepError("ANS stream without a head (ansno 0)")

    def _flush(self, everything: bool) -> None:
        while self._ans:
            k = min(self._ans) if everything else self._next_ans
            a = self._ans.get(k)
            if a is None or (k == 0 and self.head is None and not everything):
                return
            self.ready.extend(a["data"])
            a["data"] = []
            if a["done"] or everything:
                del self._ans[k]
                self._next_ans = k + 1
            else:
                return


class StreamingResponse:
    """Response whose body is consumed incrementally (use as an async context manager)."""

    def __init__(self, status: int, headers: Headers, dec: _Decoder, release: Callable[[bool], None]):
        self.status = status
        self.headers = headers
        self._dec = dec
        self._release = release
        self._released = False

    def header(self, name: str) -> Optional[str]:
        return get_header(self.headers, name)

    async def iter_chunks(self) -> AsyncIterator[bytes]:
        try:
            while True:
                c = await self._dec.next_body()
                if c is None:
                    break
                yield c
        finally:
            self.close()

    async def read(self) -> bytes:
        return b"".join([c async for c in self.iter_chunks()])

    def close(self) -> None:
        if not self._released:
            self._released = True
            self._release(self._dec.stream.done)

    async def __aenter__(self) -> "StreamingResponse":
        return self

    async def __aexit__(self, *exc) -> None:
        self.close()


class RpcClient:
    """Pooled client: `sessions` connections × `max_channels` channels, one request per channel."""

    def __init__(self, host: str = "127.0.0.1", port: int = 10288, *, sessions: int = 1, max_channels: int = 64,
                 timeout: Optional[float] = 30.0, connect_timeout: float = 5.0, tls: Optional[TlsClient] = None,
                 config: Optional[Config] = None, profile: str = RPC_PROFILE):
        self.host, self.port = host, port
        self.sessions_max = max(1, sessions)
        self.max_channels = max(1, max_channels)
        self.timeout = timeout
        self.connect_timeout = connect_timeout
        self.tls = tls
        self.config = config or Config()
        self.profile = profile
        self._sessions: List[Session] = []
        self._idle: List[Channel] = []
        self._sem: Optional[asyncio.Semaphore] = None
        self._connect_lock: Optional[asyncio.Lock] = None
        self._closed = False

    def _lazy(self) -> None:
        if self._sem is None:
            self._sem = asyncio.Semaphore(self.sessions_max * self.max_channels)
            self._connect_lock = asyncio.Lock()

    async def _session(self) -> Session:
        if self._closed:
            raise SessionClosed("client closed")
        self._sessions = [s for s in self._sessions if not s.closed]
        if len(self._sessions) >= self.sessions_max:
            return min(self._sessions, key=lambda s: s.channel_count)
        assert self._connect_lock is not None
        async with self._connect_lock:
            self._sessions = [s for s in self._sessions if not s.closed]
            if len(self._sessions) >= self.sessions_max:
                return min(self._sessions, key=lambda s: s.channel_count)
            try:
                s = await asyncio.wait_for(Session.connect(self.host, self.port, self.config, None, self.tls), self.connect_timeout)
            except asyncio.TimeoutError:
                raise RpcTimeout(f"connecting to {self.host}:{self.port} timed out")
            self._sessions.append(s)
            return s

    async def connect(self) -> None:
        self._lazy()
        await self._session()

    async def _acquire(self) -> Channel:
        while self._idle:
            ch = self._idle.pop()
            if ch.is_open:
                return ch
        s = await self._session()
        try:
            return await s.start_channel(self.profile)
        except BeepError:
            if not s.closed:
                raise
            s = await self._session()
            return await s.start_channel(self.profile)

    def _give_back(self, ch: Channel) -> None:
        if not self._closed and ch.is_open and ch.pending == 0:
            self._idle.append(ch)

    def _releaser(self, ch: Channel, dec: _Decoder) -> Callable[[bool], None]:
        sem = self._sem
        assert sem is not None

        def release(done: bool) -> None:
            if done:
                self._give_back(ch)
                sem.release()
                return
            dec.stream.discard()

            async def drain() -> None:
                try:
                    while await dec.stream.next() is not None:
                        pass
                except BeepError:
                    pass
                self._give_back(ch)
                sem.release()

            asyncio.ensure_future(drain())

        return release

    async def stream(self, method: str, target: str, headers: HeadersLike = None, body: bytes = b"",
                     timeout: Optional[float] = -1.0) -> StreamingResponse:
        """Send a request; return as soon as the response head arrives."""
        self._lazy()
        assert self._sem is not None
        t = self.timeout if timeout == -1.0 else timeout
        h = [(k, v) for k, v in _norm_headers(headers) if k.lower() not in _RESERVED]
        if t is not None and get_header(h, "Rpc-Timeout") is None:
            h.append(("Rpc-Timeout", str(int(t * 1000))))
        payload = encode_entity([("Rpc-Method", method), ("Rpc-Target", target)] + h, bytes(body))
        await self._sem.acquire()
        try:
            ch = await self._acquire()
        except BaseException:
            self._sem.release()
            raise
        dec = _Decoder(await ch.send(payload))
        release = self._releaser(ch, dec)
        try:
            status, rh = await (asyncio.wait_for(dec.read_head(), t) if t is not None else dec.read_head())
        except asyncio.TimeoutError:
            release(False)
            raise RpcTimeout(f"{method} {target} timed out")
        except BaseException:
            release(dec.stream.done)
            raise
        return StreamingResponse(status, rh, dec, release)

    async def call(self, method: str, target: str, headers: HeadersLike = None, body: Union[bytes, str] = b"",
                   timeout: Optional[float] = -1.0, json=None) -> Response:
        """Send a request and read the whole response."""
        if json is not None:
            body = _json.dumps(json).encode()
            headers = _norm_headers(headers) + [("Content-Type", "application/json")]
        if isinstance(body, str):
            body = body.encode("utf-8")
        r = await self.stream(method, target, headers, body, timeout)
        data = await r.read()
        return Response(r.status, r.headers, data)

    async def get(self, target: str, **kw) -> Response:
        return await self.call("GET", target, **kw)

    async def post(self, target: str, body: Union[bytes, str] = b"", **kw) -> Response:
        return await self.call("POST", target, body=body, **kw)

    async def raw(self, profile: str, payload: bytes):
        """Send a verbatim payload on a fresh channel of any profile; returns session.Reply."""
        self._lazy()
        s = await self._session()
        ch = await s.start_channel(profile)
        try:
            return await ch.request(payload)
        finally:
            try:
                await ch.close()
            except BeepError:
                pass

    def status(self) -> List[int]:
        return [s.channel_count for s in self._sessions if not s.closed]

    async def close(self) -> None:
        self._closed = True
        self._idle.clear()
        sessions, self._sessions = self._sessions, []
        for s in sessions:
            try:
                await s.close()
            except BaseException:
                s.abort()

    async def __aenter__(self) -> "RpcClient":
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()


# ---------------------------------------------------------------------------
# Server


class RpcFailure(BeepError):
    """Raise from a handler to answer with BEEP ERR."""

    def __init__(self, code: int, message: str):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


@dataclass
class Request:
    method: str
    target: str
    headers: Headers
    body: bytes
    channel: int = 0

    def header(self, name: str) -> Optional[str]:
        return get_header(self.headers, name)

    @property
    def path(self) -> str:
        return self.target.split("?", 1)[0]

    @property
    def id(self) -> Optional[str]:
        return self.header("Rpc-Id")

    @property
    def timeout(self) -> Optional[float]:
        t = self.header("Rpc-Timeout")
        try:
            return int(t) / 1000.0 if t else None
        except ValueError:
            return None

    def text(self) -> str:
        return self.body.decode("utf-8", "replace")

    def json(self):
        return _json.loads(self.body)


@dataclass
class ServerResponse:
    """`body` may be bytes/str (sent as one RPY) or an async iterator of bytes (sent as ANS…NUL)."""

    status: int = 200
    headers: HeadersLike = None
    body: Union[bytes, str, AsyncIterator[bytes], None] = b""


Handler = Callable[[Request], Awaitable[ServerResponse]]


class RpcProfile(ProfileHandler):
    def __init__(self, handler: Handler):
        self.handler = handler

    async def on_message(self, session: Session, msg: IncomingMessage, reply: Responder) -> None:
        try:
            payload = await msg.collect(session.config.max_message_size)
        except BeepError as e:
            if session.closed:
                return
            await reply.error(554, str(e))
            return
        r = split_head(payload, True)
        headers, off = r if r else ([], 0)
        method, target = get_header(headers, "Rpc-Method"), get_header(headers, "Rpc-Target")
        if not method or target is None:
            await reply.error(501, "request lacks Rpc-Method or Rpc-Target")
            return
        req = Request(method, target, _app_headers(headers), payload[off:], msg.channel)
        try:
            coro = self.handler(req)
            resp = await (asyncio.wait_for(coro, req.timeout) if req.timeout else coro)
        except asyncio.TimeoutError:
            await reply.error(451, "deadline exceeded")
            return
        except RpcFailure as f:
            await reply.error(f.code, f.message)
            return
        except Exception as e:
            log.exception("rpc handler failed")
            await reply.error(554, f"handler failed: {e}")
            return
        if not isinstance(resp, ServerResponse):
            resp = ServerResponse(200, None, resp)  # bare bytes/str/iterator
        h = [("Rpc-Status", str(resp.status))] + _app_headers(_norm_headers(resp.headers))
        if req.id and get_header(h, "Rpc-Id") is None:
            h.append(("Rpc-Id", req.id))
        body = resp.body
        if body is None or isinstance(body, (bytes, bytearray, str)):
            data = body.encode("utf-8") if isinstance(body, str) else bytes(body or b"")
            await reply.rpy(encode_entity(h, data))
            return
        await reply.ans(encode_entity(h, b""))
        try:
            async for chunk in body:
                if isinstance(chunk, str):
                    chunk = chunk.encode("utf-8")
                if chunk:
                    await reply.ans(encode_entity([], chunk))
        except BeepError:
            if session.closed:
                return
            raise
        except Exception as e:
            await reply.ans(encode_entity([("Rpc-Abort", str(e) or type(e).__name__)], b""))
        await reply.nul()


class RpcServer:
    """BEEP listener serving the RPC profile with an async handler."""

    def __init__(self, handler: Handler, config: Optional[Config] = None, tls: Optional[TlsServer] = None,
                 extra_profiles: Optional[Dict[str, ProfileHandler]] = None):
        profiles: Dict[str, ProfileHandler] = {RPC_PROFILE: RpcProfile(handler)}
        profiles.update(extra_profiles or {})
        self.listener = Listener(profiles, config, tls)

    async def start(self, host: str = "127.0.0.1", port: int = 10288) -> "RpcServer":
        await self.listener.start(host, port)
        return self

    @property
    def port(self) -> int:
        return self.listener.port

    async def serve_forever(self) -> None:
        await self.listener.serve_forever()

    async def close(self) -> None:
        await self.listener.close()
