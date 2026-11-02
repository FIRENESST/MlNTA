"""Blocking facade over the asyncio client, for scripts, Django/Flask views, Celery tasks, etc."""
from __future__ import annotations

import asyncio
import threading
from typing import Optional, Union

from .rpc import HeadersLike, Response, RpcClient
from .session import TlsClient


class _Loop:
    """One background event loop shared by all blocking clients in the process."""

    _lock = threading.Lock()
    _loop: Optional[asyncio.AbstractEventLoop] = None

    @classmethod
    def get(cls) -> asyncio.AbstractEventLoop:
        with cls._lock:
            if cls._loop is None:
                loop = asyncio.new_event_loop()
                t = threading.Thread(target=loop.run_forever, name="minta-beep-loop", daemon=True)
                t.start()
                cls._loop = loop
            return cls._loop


class Client:
    """Thread-safe blocking RPC client.

    >>> c = Client("127.0.0.1", 10288)
    >>> c.call("GET", "/echo").status
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 10288, *, sessions: int = 1, max_channels: int = 64,
                 timeout: Optional[float] = 30.0, tls: Optional[TlsClient] = None):
        self._loop = _Loop.get()
        self._timeout = timeout

        async def make() -> RpcClient:
            return RpcClient(host, port, sessions=sessions, max_channels=max_channels, timeout=timeout, tls=tls)

        self._client = self._run(make())

    def _run(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result()

    def call(self, method: str, target: str, headers: HeadersLike = None, body: Union[bytes, str] = b"",
             timeout: Optional[float] = -1.0, json=None) -> Response:
        return self._run(self._client.call(method, target, headers, body, timeout, json))

    def get(self, target: str, **kw) -> Response:
        return self.call("GET", target, **kw)

    def post(self, target: str, body: Union[bytes, str] = b"", **kw) -> Response:
        return self.call("POST", target, body=body, **kw)

    def close(self) -> None:
        self._run(self._client.close())

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
