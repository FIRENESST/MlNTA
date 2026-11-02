"""Conformance client (Python native lib): python client.py ADDR → exit 1 on failure."""
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "native", "python"))
from minta_beep import RemoteError, RpcClient  # noqa: E402


async def main(addr: str) -> None:
    host, port = addr.rsplit(":", 1)
    async with RpcClient(host, int(port), timeout=10) as c:
        r = await c.call("POST", "/echo", {"Content-Type": "text/plain"}, b"conformance")
        assert r.status == 200 and r.body == b"conformance", f"echo {r}"
        r = await c.get("/stream/5")
        assert r.body == b"".join(b"line %d\n" % i for i in range(5)), f"stream {r.body!r}"
        big = b"0123456789" * 200_000
        r = await c.call("POST", "/echo", body=big)
        assert r.body == big, "2MB echo"
        try:
            await c.get("/err/537")
            raise AssertionError("expected RemoteError")
        except RemoteError as e:
            assert e.code == 537, f"err {e}"


try:
    asyncio.run(main(sys.argv[1]))
    print("ok")
except Exception as e:  # noqa: BLE001
    print("FAIL:", repr(e))
    sys.exit(1)
