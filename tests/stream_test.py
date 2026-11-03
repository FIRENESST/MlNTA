"""Streamed upload through the gateway: HTTP/1.1 chunked PUT of 80 MiB → rpc:2 chunked MSG →
echo server's /count, which reads the body without buffering. Proves request bodies are no
longer capped at 64 MiB and arrive intact (CRC-32 compared end to end).

    python stream_test.py 127.0.0.1:8080
"""
import http.client
import json
import sys
import zlib

MIB = 1 << 20
BLOCKS = 80


def main(addr):
    host, port = addr.rsplit(":", 1)
    crc = 0

    def body():
        nonlocal crc
        for i in range(BLOCKS):
            block = bytes((i + j) % 251 for j in range(256)) * (MIB // 256)
            crc = zlib.crc32(block, crc)
            yield block

    conn = http.client.HTTPConnection(host, int(port), timeout=120)
    conn.request("PUT", "/count", body=body(), encode_chunked=True, headers={"Content-Type": "application/octet-stream"})
    resp = conn.getresponse()
    raw = resp.read()
    ok = resp.status == 200
    doc = json.loads(raw) if ok else {}
    ok = ok and doc.get("bytes") == BLOCKS * MIB and doc.get("crc32") == f"{crc:08x}"
    print(f"streamed upload: status {resp.status}, {doc}")
    print("Streamed upload: all tests passed" if ok else f"Streamed upload: FAILURE {raw[:200]!r}")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main(sys.argv[1])
