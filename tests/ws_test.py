"""WebSocket through the BEEP bridge (HTTP/1.1 Upgrade tunnelled over an rpc:2 request/response).

    python ws_test.py serve PORT    # WebSocket echo server (the backend)
    python ws_test.py client URL    # e.g. ws://127.0.0.1:8080/chat via the gateway

The handshake is end to end: the backend computes Sec-WebSocket-Accept from the client's key.
"""
import asyncio
import sys

import websockets


async def echo(ws, *_path):
    async for msg in ws:
        await ws.send(msg)


async def serve(port):
    async with websockets.serve(echo, "127.0.0.1", port, max_size=None):
        await asyncio.Future()


async def client(url):
    failures = 0

    def check(cond, msg):
        nonlocal failures
        if not cond:
            failures += 1
            print("FAIL:", msg, flush=True)

    async with websockets.connect(url, max_size=None, open_timeout=15) as ws:
        await ws.send("hello")
        check(await asyncio.wait_for(ws.recv(), 15) == "hello", "text echo")
        blob = bytes(range(256)) * 4096  # 1 MiB, binary frames
        await ws.send(blob)
        check(await asyncio.wait_for(ws.recv(), 30) == blob, "binary echo")
        for i in range(20):
            await ws.send(f"m{i}")
            got = await asyncio.wait_for(ws.recv(), 15)
            check(got == f"m{i}", f"ping-pong {i}: {got!r}")
        await ws.close()
        check(ws.close_code == 1000, f"close code {ws.close_code}")
    print("WebSocket via BEEP: all tests passed" if failures == 0 else f"WebSocket via BEEP: {failures} FAILURE(S)", flush=True)
    return failures


if __name__ == "__main__":
    if sys.argv[1] == "serve":
        asyncio.run(serve(int(sys.argv[2])))
    else:
        sys.exit(1 if asyncio.run(client(sys.argv[2])) else 0)
