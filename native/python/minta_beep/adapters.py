"""Serve existing Python web apps natively over BEEP — no gateway process needed.

    from minta_beep.adapters import asgi_handler, wsgi_handler
    from minta_beep import RpcServer
    server = await RpcServer(asgi_handler(fastapi_app)).start("0.0.0.0", 10288)
    server = await RpcServer(wsgi_handler(flask_app)).start("0.0.0.0", 10288)
"""
from __future__ import annotations

import asyncio
import io
import sys
from typing import Any, AsyncIterator, Callable, List, Optional, Tuple

from .rpc import Handler, Request, ServerResponse

_HOP = {"connection", "keep-alive", "transfer-encoding", "te", "trailer", "upgrade", "content-length", "rpc-id", "rpc-timeout", "rpc-authority"}


def _split(target: str) -> Tuple[str, str]:
    path, _, query = target.partition("?")
    return path or "/", query


def asgi_handler(app: Callable, root_path: str = "") -> Handler:
    """Adapt an ASGI 3 application (FastAPI, Starlette, Django ASGI, Quart, …)."""

    async def handle(req: Request) -> ServerResponse:
        path, query = _split(req.target)
        host = req.header("Rpc-Authority") or "beep"
        headers = [(k.lower().encode("latin-1"), v.encode("latin-1", "replace")) for k, v in req.headers if k.lower() not in _HOP]
        headers.append((b"host", host.encode("latin-1", "replace")))
        headers.append((b"content-length", str(len(req.body)).encode()))
        scope = {
            "type": "http", "asgi": {"version": "3.0", "spec_version": "2.3"}, "http_version": "1.1",
            "method": req.method.upper(), "scheme": "http", "path": path, "raw_path": path.encode("latin-1", "replace"),
            "query_string": query.encode("latin-1", "replace"), "root_path": root_path, "headers": headers,
            "server": (host, 0), "client": None,
        }
        sent_body = False

        async def receive():
            nonlocal sent_body
            if not sent_body:
                sent_body = True
                return {"type": "http.request", "body": req.body, "more_body": False}
            await asyncio.Event().wait()  # never: no disconnects on a BEEP message

        start: asyncio.Future = asyncio.get_running_loop().create_future()
        chunks: asyncio.Queue = asyncio.Queue()

        async def send(message):
            t = message["type"]
            if t == "http.response.start":
                start.set_result(message)
            elif t == "http.response.body":
                chunks.put_nowait(bytes(message.get("body", b"")))
                if not message.get("more_body", False):
                    chunks.put_nowait(None)

        task = asyncio.ensure_future(app(scope, receive, send))

        def done(t: asyncio.Task) -> None:
            if not start.done():
                start.set_exception(t.exception() or RuntimeError("ASGI app returned without a response"))
            chunks.put_nowait(None)

        task.add_done_callback(done)
        msg = await start
        status = int(msg["status"])
        rh = [(k.decode("latin-1"), v.decode("latin-1")) for k, v in msg.get("headers", [])]

        # Peek: a single complete body → RPY; otherwise stream as ANS.
        first = await chunks.get()
        if first is None:
            return ServerResponse(status, rh, b"")
        second_ready = not chunks.empty()
        if second_ready:
            nxt = chunks.get_nowait()
            if nxt is None:
                return ServerResponse(status, rh, first)
            pending: List[Optional[bytes]] = [first, nxt]
        else:
            pending = [first]

        async def body() -> AsyncIterator[bytes]:
            for p in pending:
                if p is None:
                    return
                yield p
            while True:
                c = await chunks.get()
                if c is None:
                    if task.done() and task.exception():
                        raise task.exception()
                    return
                yield c

        return ServerResponse(status, rh, body())

    return handle


def wsgi_handler(app: Callable, threads: Optional[Any] = None) -> Handler:
    """Adapt a WSGI application (Flask, Django, Bottle, Pyramid, …). Runs in a thread pool."""

    def run(req: Request) -> Tuple[int, list, bytes]:
        path, query = _split(req.target)
        host = req.header("Rpc-Authority") or "beep"
        environ = {
            "REQUEST_METHOD": req.method.upper(), "SCRIPT_NAME": "", "PATH_INFO": path, "QUERY_STRING": query,
            "SERVER_NAME": host.split(":")[0], "SERVER_PORT": (host.split(":") + ["80"])[1], "SERVER_PROTOCOL": "HTTP/1.1",
            "HTTP_HOST": host, "CONTENT_LENGTH": str(len(req.body)),
            "wsgi.version": (1, 0), "wsgi.url_scheme": "http", "wsgi.input": io.BytesIO(req.body),
            "wsgi.errors": sys.stderr, "wsgi.multithread": True, "wsgi.multiprocess": False, "wsgi.run_once": False,
        }
        for k, v in req.headers:
            lk = k.lower()
            if lk in _HOP:
                continue
            if lk == "content-type":
                environ["CONTENT_TYPE"] = v
            else:
                environ["HTTP_" + k.upper().replace("-", "_")] = v
        state = {}

        def start_response(status, headers, exc_info=None):
            state["status"] = int(status.split(" ", 1)[0])
            state["headers"] = headers
            return lambda data: state.setdefault("written", []).append(data)

        result = app(environ, start_response)
        try:
            body = b"".join(state.get("written", [])) + b"".join(result)
        finally:
            if hasattr(result, "close"):
                result.close()
        return state["status"], state["headers"], body

    async def handle(req: Request) -> ServerResponse:
        status, headers, body = await asyncio.get_running_loop().run_in_executor(threads, run, req)
        return ServerResponse(status, headers, body)

    return handle
