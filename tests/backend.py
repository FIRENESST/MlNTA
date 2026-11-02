"""Tiny HTTP backend used by the interop tests (stands in for "an existing service in any language").

Routes:
  GET  /hello            -> 200 text "hello from python backend"
  POST /json             -> 200 JSON {"method","path","headers","body"}
  GET  /chunked/{n}      -> 200, chunked transfer encoding, n chunks "part i\n"
  GET  /big/{n}          -> 200, n bytes with Content-Length
  GET  /status/{code}    -> that status
"""
import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def _send(self, status, body, ctype="text/plain; charset=utf-8", extra=None):
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def _body(self):
        n = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(n) if n else b""

    def handle_any(self):
        path = self.path.split("?")[0]
        body = self._body()
        if path == "/hello":
            return self._send(200, b"hello from python backend", extra={"X-Backend": "python"})
        if path == "/json":
            doc = {
                "method": self.command,
                "path": self.path,
                "headers": {k.lower(): v for k, v in self.headers.items()},
                "body": body.decode("utf-8", "replace"),
            }
            return self._send(200, json.dumps(doc).encode(), "application/json")
        if path.startswith("/chunked/"):
            n = int(path.rsplit("/", 1)[1])
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            for i in range(n):
                part = f"part {i}\n".encode()
                self.wfile.write(b"%x\r\n%s\r\n" % (len(part), part))
            self.wfile.write(b"0\r\n\r\n")
            return
        if path.startswith("/big/"):
            n = int(path.rsplit("/", 1)[1])
            return self._send(200, bytes((65 + i % 26) for i in range(n)), "application/octet-stream")
        if path.startswith("/status/"):
            code = int(path.rsplit("/", 1)[1])
            return self._send(code, f"backend status {code}".encode())
        return self._send(404, b"not found")

    do_GET = do_POST = do_PUT = do_DELETE = do_PATCH = handle_any


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 3000
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()
