"""Python binding tests. Usage:

    python test_minta_beep.py ECHO_ADDR [TLS_ECHO_ADDR CA_FILE CERT_FILE KEY_FILE]

ECHO_ADDR is a `beepgw echo` server (Rust), which makes this an interop test.
"""
import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from minta_beep import (  # noqa: E402
    Config, ProfileHandler, Refused, RemoteError, Request, RpcClient, RpcServer, ServerResponse, Session,
    StreamAborted, TlsClient, TlsServer, RPC_PROFILE,
)
from minta_beep.adapters import asgi_handler, wsgi_handler  # noqa: E402
from minta_beep.sync import Client  # noqa: E402

failures = 0


def check(cond, msg):
    global failures
    if not cond:
        failures += 1
        print("FAIL:", msg)


def addr(s):
    h, p = s.rsplit(":", 1)
    return h, int(p)


async def test_against_rust_echo(host, port):
    async with RpcClient(host, port, max_channels=8, timeout=5) as c:
        r = await c.call("POST", "/echo?lang=py", {"Content-Type": "text/plain", "X-Test": "py"}, b"hello from python")
        check(r.status == 200 and r.body == b"hello from python", f"echo {r}")
        check(r.header("x-test") == "py" and r.header("X-Echo-Method") == "POST", f"echo headers {r.headers}")
        r = await c.call("PUT", "/inspect", json={"k": [1, 2]})
        j = r.json()
        check(j["method"] == "PUT" and j["headers"]["content-type"] == "application/json" and '"k"' in j["body"], f"inspect {j}")
        r = await c.get("/stream/50")
        check(r.text() == "".join(f"line {i}\n" for i in range(50)), "stream body")
        async with await c.stream("GET", "/stream/1000") as sr:
            first = None
            async for chunk in sr.iter_chunks():
                first = chunk
                break
            check(first is not None and first.startswith(b"line 0"), "streaming first chunk")
        r = await c.get("/status/418")
        check(r.status == 418 and r.text() == "status 418", f"status {r.status}")
        try:
            await c.get("/err/553")
            check(False, "expected RemoteError")
        except RemoteError as e:
            check(e.code == 553 and e.http_status == 400, f"err {e}")
        r = await c.get("/bytes/3000000")
        check(len(r.body) == 3_000_000 and r.body[:3] == b"abc", "3 MB body")
        big = os.urandom(2_500_000)
        r = await c.call("POST", "/echo", body=big)
        check(r.body == big, "2.5 MB upload echo")
        t0 = time.time()
        rs = await asyncio.gather(*[c.call("POST", f"/sleep/{50}", body=str(i)) for i in range(40)])
        check(all(r.status == 200 for r in rs), "concurrent")
        check(time.time() - t0 < 2.0, f"concurrency too slow: {time.time() - t0:.2f}s")
        check(len(c.status()) == 1 and c.status()[0] <= 8, f"pool {c.status()}")
        try:
            await c.call("GET", "/sleep/2000", timeout=0.2)
            check(False, "expected timeout")
        except Exception as e:
            check(type(e).__name__ in ("RpcTimeout", "RemoteError"), f"timeout type {e!r}")
        r = await c.get("/echo", body=b"after-timeout")
        check(r.body == b"after-timeout", "pool usable after timeout")


def test_sync_client(host, port):
    with Client(host, port) as c:
        r = c.post("/echo", "sync works", headers=[("Content-Type", "text/plain")])
        check(r.status == 200 and r.text() == "sync works", f"sync {r}")


async def handler(req: Request) -> ServerResponse:
    if req.path.startswith("/stream/"):
        n = int(req.path.rsplit("/", 1)[1])

        async def gen():
            for i in range(n):
                yield f"line {i}\n"
        return ServerResponse(200, {"Content-Type": "text/plain"}, gen())
    if req.path == "/abort":
        async def bad():
            yield b"partial"
            raise RuntimeError("generator died")
        return ServerResponse(200, None, bad())
    if req.path.startswith("/err/"):
        from minta_beep import RpcFailure
        raise RpcFailure(int(req.path.rsplit("/", 1)[1]), "py says no")
    return ServerResponse(200, [("Content-Type", req.header("Content-Type") or "application/octet-stream"), ("X-Lang", "python")], req.body)


async def test_python_server_python_client():
    cfg = Config(recv_window=6000, max_frame_size=1999)
    server = await RpcServer(handler, cfg).start("127.0.0.1", 0)
    async with RpcClient("127.0.0.1", server.port, config=cfg) as c:
        data = os.urandom(1_000_000)
        r = await c.call("POST", "/echo", body=data)
        check(r.body == data and r.header("X-Lang") == "python", "py<->py big echo with tiny windows")
        r = await c.get("/stream/20")
        check(r.text() == "".join(f"line {i}\n" for i in range(20)), "py stream")
        try:
            await c.get("/abort")
            check(False, "expected abort")
        except StreamAborted as e:
            check("generator died" in str(e), f"abort {e}")
        try:
            await c.get("/err/537")
            check(False, "expected 537")
        except RemoteError as e:
            check(e.code == 537, f"err {e}")
    # raw session API + refused profile + pipelining on one channel
    s = await Session.connect("127.0.0.1", server.port)
    check(s.peer_profiles == [RPC_PROFILE], f"greeting {s.peer_profiles}")
    try:
        await s.start_channel("urn:none")
        check(False, "expected refusal")
    except Refused as e:
        check(e.code == 550, f"refused {e}")
    ch = await s.start_channel(RPC_PROFILE)
    from minta_beep.wire import encode_entity
    streams = [await ch.send(encode_entity([("Rpc-Method", "GET"), ("Rpc-Target", "/echo")], str(i).encode())) for i in range(10)]
    bodies = [(await st.collect()).payload for st in streams]
    check(all(b.endswith(str(i).encode()) for i, b in enumerate(bodies)), "pipelined replies in order")
    await ch.close()
    await s.close()
    check(s.closed, "session closed")
    await server.close()
    return server


async def asgi_app(scope, receive, send):
    assert scope["type"] == "http"
    body = (await receive())["body"]
    if scope["path"] == "/chunks":
        await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/plain")]})
        for i in range(3):
            await send({"type": "http.response.body", "body": f"c{i};".encode(), "more_body": True})
        await send({"type": "http.response.body", "body": b"", "more_body": False})
        return
    out = f"{scope['method']} {scope['path']}?{scope['query_string'].decode()} {body.decode()}".encode()
    await send({"type": "http.response.start", "status": 201, "headers": [(b"x-asgi", b"yes")]})
    await send({"type": "http.response.body", "body": out})


def wsgi_app(environ, start_response):
    body = environ["wsgi.input"].read()
    start_response("202 Accepted", [("X-Wsgi", "yes"), ("Content-Type", "text/plain")])
    return [f"{environ['REQUEST_METHOD']} {environ['PATH_INFO']} {environ.get('HTTP_X_A', '')} ".encode(), body]


async def test_adapters():
    s1 = await RpcServer(asgi_handler(asgi_app)).start("127.0.0.1", 0)
    s2 = await RpcServer(wsgi_handler(wsgi_app)).start("127.0.0.1", 0)
    async with RpcClient("127.0.0.1", s1.port) as c:
        r = await c.call("POST", "/a/b?x=1", body=b"zz")
        check(r.status == 201 and r.text() == "POST /a/b?x=1 zz" and r.header("x-asgi") == "yes", f"asgi {r}")
        r = await c.get("/chunks")
        check(r.text() == "c0;c1;c2;", f"asgi chunks {r.body}")
    async with RpcClient("127.0.0.1", s2.port) as c:
        r = await c.call("PUT", "/w", {"X-A": "hdr"}, b"body")
        check(r.status == 202 and r.text() == "PUT /w hdr body" and r.header("X-Wsgi") == "yes", f"wsgi {r}")
    await s1.close()
    await s2.close()


async def test_tls(tls_addr, ca, cert, key):
    host, port = addr(tls_addr)
    async with RpcClient(host, port, tls=TlsClient("localhost", ca)) as c:
        r = await c.call("POST", "/echo", body=b"py over tls to rust")
        check(r.body == b"py over tls to rust", "py tls -> rust")
    server = await RpcServer(handler, tls=TlsServer(cert, key)).start("127.0.0.1", 0)
    async with RpcClient("127.0.0.1", server.port, tls=TlsClient("localhost", ca)) as c:
        r = await c.call("POST", "/echo", body=b"py tls both ends")
        check(r.body == b"py tls both ends", "py tls -> py")
    await server.close()


async def main():
    host, port = addr(sys.argv[1])
    await test_against_rust_echo(host, port)
    await asyncio.get_running_loop().run_in_executor(None, test_sync_client, host, port)
    await test_python_server_python_client()
    await test_adapters()
    if len(sys.argv) >= 6:
        await test_tls(*sys.argv[2:6])


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--serve":
        # Conformance server for cross-language tests: python test_minta_beep.py --serve PORT
        async def serve():
            s = await RpcServer(handler).start("127.0.0.1", int(sys.argv[2]))
            print("READY", s.port, flush=True)
            await s.serve_forever()
        asyncio.run(serve())
    else:
        asyncio.run(main())
        print("Python: all tests passed" if not failures else f"Python: {failures} FAILURE(S)")
        sys.exit(1 if failures else 0)
