#!/usr/bin/env python3
"""HTTP/JSON -> streaming gRPC -> HTTP integration and language-client checks.

    cargo build --manifest-path core/Cargo.toml --workspace --locked
    python tests/standard_test.py --strict

Only the gRPC/WebSocket application tests require tests/requirements.txt.
Missing optional language tools are reported as SKIP; --strict requires grpcio/websockets.
"""
import argparse
import base64
import concurrent.futures
import datetime
import http.client
import importlib.util
import json
import os
from pathlib import Path
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = Path(__file__).resolve().parent.parent
BUILD = ROOT / "tests/.build"
GW = Path(os.environ.get("MINTAGW", ROOT / "core/target/debug" / ("mintagw.exe" if os.name == "nt" else "mintagw")))
PROCESSES = []
LOGS = []
PASSED = []
SKIPPED = []
ACTIVE_STREAMS = 0
LOCK = threading.Lock()
STALL = threading.Event()  # /stall-read waits for this before reading its body


def port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def process(args, ready, env=None):
    log = tempfile.TemporaryFile()
    LOGS.append(log)
    child = subprocess.Popen([str(x) for x in args], stdout=log, stderr=subprocess.STDOUT, env={**os.environ, **(env or {})})
    PROCESSES.append(child)
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if child.poll() is not None:
            log.seek(0)
            raise AssertionError(log.read().decode(errors="replace"))
        try:
            with socket.create_connection(("127.0.0.1", ready), timeout=0.2):
                return child
        except OSError:
            time.sleep(0.03)
    raise AssertionError(f"process did not listen on {ready}")


def chain(backend, *, limit=1 << 20, response_limit=1 << 20, timeout=5000, concurrency=8, server_limit=None, extra=(), client_extra=(), env=None):
    pg, ph = port(), port()
    process([GW, "expose", "--listen", f"127.0.0.1:{pg}", "--backend", backend,
             "--max-body-bytes", str(server_limit or limit), "--timeout-ms", str(timeout), "--max-in-flight", str(concurrency), *extra], pg, env)
    scheme = "https" if "--cert-file" in extra else "http"
    host = "localhost" if scheme == "https" else "127.0.0.1"
    process([GW, "proxy", "--listen", f"127.0.0.1:{ph}", "--upstream", f"{scheme}://{host}:{pg}",
             "--max-body-bytes", str(limit), "--max-response-bytes", str(response_limit), "--timeout-ms", str(timeout), "--max-in-flight", str(concurrency), *client_extra], ph, env)
    return ph, pg


def request(ph, path, body=None, method="GET", headers=None, chunks=False):
    c = http.client.HTTPConnection("127.0.0.1", ph, timeout=10)
    try:
        c.request(method, path, body=body, headers=headers or {}, encode_chunked=chunks)
        r = c.getresponse()
        return r.status, dict(r.getheaders()), r.read()
    finally:
        c.close()


def call(ph, payload):
    status, headers, body = request(ph, "/v1/call", json.dumps(payload).encode(), "POST", {"Content-Type": "application/json"})
    return status, json.loads(body)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def log_message(self, *_): pass

    def handle(self):
        try: super().handle()
        except OSError: pass  # Expected TCP resets in cancellation tests.

    def read_body(self):
        if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
            parts = []
            while True:
                line = self.rfile.readline()
                if not line: raise ConnectionError("aborted upload")
                n = int(line.split(b";", 1)[0].strip(), 16)
                if n == 0:
                    while self.rfile.readline() not in (b"\r\n", b"", b"\n"): pass
                    return b"".join(parts)
                data = self.rfile.read(n)
                if len(data) != n or self.rfile.read(2) != b"\r\n": raise ConnectionError("short chunk")
                parts.append(data)
        return self.rfile.read(int(self.headers.get("Content-Length", "0")))

    def send(self, data, status=200, headers=()):
        self.send_response(status)
        self.send_header("Content-Length", str(len(data)))
        for k, v in headers: self.send_header(k, v)
        self.end_headers()
        if self.command != "HEAD": self.wfile.write(data)

    def handle_any(self):
        global ACTIVE_STREAMS
        path = self.path.split("?", 1)[0]
        if path == "/early-reject":
            self.close_connection = True
            self.send(b"denied", 403, (("Connection", "close"),))
            self.wfile.flush()
            # Lingering close: closing with unread upload bytes sends a TCP reset, and
            # Windows then discards the 403 before the gateway has read it.
            try: self.read_body()
            except (OSError, ValueError): pass
            return
        if path == "/stall-read":
            STALL.wait(30)
        body = self.read_body()
        if path == "/echo":
            return self.send(body, headers=(("Content-Type", self.headers.get("Content-Type", "application/octet-stream")), ("Set-Cookie", "a=1"), ("Set-Cookie", "b=2")))
        if path == "/inspect":
            return self.send(json.dumps({"path": self.path, "headers": dict(self.headers.items()), "body": body.decode()}).encode())
        if path.startswith("/status/"): return self.send(b"backend status", int(path.rsplit("/", 1)[1]))
        if path == "/broken":
            self.send_response(200)
            self.send_header("Content-Length", "10000")
            self.end_headers()
            self.wfile.write(b"short")
            self.close_connection = True
            return
        if path.startswith("/bytes/"): return self.send(bytes(range(256)) * int(path.rsplit("/", 1)[1]))
        if path == "/slow":
            time.sleep(1)
            return self.send(b"late")
        if path == "/forever":
            with LOCK: ACTIVE_STREAMS += 1
            try:
                self.send_response(200)
                self.send_header("Transfer-Encoding", "chunked")
                self.end_headers()
                while True:
                    self.wfile.write(b"5\r\nfirst\r\n")
                    self.wfile.flush()
                    time.sleep(0.01)
            finally:
                with LOCK: ACTIVE_STREAMS -= 1
        if path == "/trailers":
            self.send_response(200)
            self.send_header("Transfer-Encoding", "chunked")
            self.send_header("Trailer", "X-Trailer")
            self.end_headers()
            self.wfile.write(b"5\r\nhello\r\n0\r\nX-Trailer: done\r\n\r\n")
            return
        return self.send(b"hello")

    def safe_handle(self):
        try: self.handle_any()
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, ConnectionError, OSError): self.close_connection = True

    do_GET = do_POST = do_PUT = do_HEAD = safe_handle


def check(name, fn):
    pattern = os.environ.get("MINTA_TEST_FILTER")
    if pattern and pattern.lower() not in name.lower(): return
    fn()
    PASSED.append(name)
    print(f"[PASS] {name}", flush=True)


def skip(name, why):
    SKIPPED.append(name)
    print(f"[SKIP] {name}: {why}", flush=True)


def run_client(args, env=None, cwd=None):
    p = subprocess.run([str(x) for x in args], cwd=cwd or ROOT, env={**os.environ, "NO_PROXY": "127.0.0.1,localhost", **(env or {})}, capture_output=True, timeout=180)
    if p.returncode:
        for log in LOGS[-3:]:
            position = log.tell()
            log.seek(max(0, position - 4000))
            print(log.read().decode(errors="replace"), flush=True)
    assert p.returncode == 0, (args, p.stdout.decode(errors="replace"), p.stderr.decode(errors="replace"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args()
    assert GW.is_file(), f"build mintagw first: {GW}"
    BUILD.mkdir(exist_ok=True)
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    backend = f"http://127.0.0.1:{server.server_port}"
    temp = tempfile.TemporaryDirectory(prefix="standard-", dir=BUILD)
    work = Path(temp.name).resolve()
    assert work.is_relative_to(BUILD.resolve())
    try:
        ph, _ = chain(backend)

        def raw():
            blob = bytes(range(256)) * 3000
            assert request(ph, "/echo", blob, "POST")[2] == blob
            assert request(ph, "/echo", iter([blob[:10000], blob[10000:]]), "POST", chunks=True)[2] == blob
            status, _, data = request(ph, "/inspect?x=1", b"hi", "POST", {"Connection": "x-secret", "X-Secret": "drop", "X-Keep": "yes"})
            doc = json.loads(data)
            assert status == 200 and doc["path"] == "/inspect?x=1" and doc["body"] == "hi"
            assert "x-secret" not in {k.lower() for k in doc["headers"]}
            assert request(ph, "/status/404")[0] == 404
            assert request(ph, "/status/204")[0] == 204
            assert request(ph, "/", method="HEAD")[2] == b""
            assert request(ph, "/early-reject", iter([b"a" * 4096] * 20), "POST", chunks=True)[0] == 403
            try: request(ph, "/broken")
            except http.client.IncompleteRead: pass
            else: raise AssertionError("truncated response was accepted")
        check("HTTP passthrough, binary/chunked upload, hop headers, status and HEAD", raw)

        def json_api():
            for value in ({"message": "你好"}, None, [1, True, "x"]):
                status, reply = call(ph, {"method": "POST", "target": "/echo", "json": value})
                assert status == 200 and reply["status"] == 200 and json.loads(reply["body"]) == value, reply
                assert ["content-type", "application/json"] in reply["headers"]
                assert len([h for h in reply["headers"] if h[0] == "set-cookie"]) == 2
            blob = bytes(range(256))
            _, reply = call(ph, {"method": "POST", "target": "/echo", "body_base64": base64.b64encode(blob).decode()})
            assert base64.b64decode(reply["body_base64"]) == blob
            _, reply = call(ph, {"target": "/status/404"})
            assert reply["status"] == 404
            _, reply = call(ph, {"target": "/trailers"})
            assert reply["body"] == "hello" and ["x-trailer", "done"] in reply["trailers"], reply
            spec = json.loads(request(ph, "/openapi.json")[2])
            assert spec["openapi"] == "3.1.0"
            assert request(ph, "/healthz")[0] == request(ph, "/readyz")[0] == 200
        check("JSON API, Unicode/null/binary, duplicate headers, trailers, OpenAPI and health", json_api)

        def invalid():
            for doc in ({}, {"target": "http://evil/x"}, {"target": "//evil/x"}, {"target": "/", "body": "x", "json": {}}, {"target": "/", "body_base64": "???"}, {"target": "/", "headers": {"X-Test": "x\r\ny"}}, {"target": "/", "timeout_ms": 0}, {"target": "/", "upstream": "missing"}, {"target": "/", "typo": 1}):
                status, reply = call(ph, doc)
                assert status == 400 and "error" in reply, (doc, status, reply)
            assert request(ph, "/v1/call", b"{}", "POST")[0] == 415
            assert request(ph, "/v1/call")[0] == 405
            # Dot segments would let a client climb out of a backend base path.
            for target in ("/../inspect", "/a/./inspect", "/a/%2e%2E/inspect", "/a/.%2e?x=1"):
                assert request(ph, target)[0] == 400, target
            assert call(ph, {"target": "/x/../inspect"})[0] == 400
            assert request(ph, "/files/..hidden")[0] == 200
        check("invalid envelopes, invalid headers, SSRF target rejection, dot segments and media type", invalid)

        def limits():
            small, _ = chain(backend, limit=1024, response_limit=1024)
            assert request(small, "/echo", b"a" * 1025, "POST")[0] == 413
            assert request(small, "/echo", iter([b"a" * 512] * 3), "POST", chunks=True)[0] == 413
            assert call(small, {"target": "/echo", "body": "a" * 1025})[0] == 413
            assert call(small, {"target": "/bytes/8"})[0] == 502
            assert len(request(small, "/bytes/8")[2]) == 2048
            remote, _ = chain(backend, limit=4096, server_limit=1024)
            assert request(remote, "/echo", b"a" * 2048, "POST")[0] == 413
            assert request(remote, "/")[0] == 200
        check("consistent known/chunked/JSON/server body limits and bounded JSON responses", limits)

        def cancellation():
            single, _ = chain(backend, timeout=2000, concurrency=1)
            for _ in range(5):
                started = time.monotonic()
                status, reply = call(single, {"target": "/slow", "timeout_ms": 50})
                assert status == 504, (status, reply)
                assert time.monotonic() - started < 0.8
                assert request(single, "/")[0] == 200
            c = http.client.HTTPConnection("127.0.0.1", single, timeout=3)
            c.request("GET", "/forever")
            r = c.getresponse()
            assert r.read(5) == b"first"
            r.close()
            c.close()
            assert request(single, "/")[0] == 200
            until = time.monotonic() + 2
            while time.monotonic() < until:
                with LOCK:
                    if ACTIVE_STREAMS == 0: break
                time.sleep(0.02)
            with LOCK: assert ACTIVE_STREAMS == 0, "backend stream did not cancel"
            assert call(single, {"target": "/forever", "timeout_ms": 50})[0] == 504
            assert request(single, "/")[0] == 200
        check("deadlines and abandoned infinite streams release capacity and backend connections", cancellation)

        def concurrent_calls():
            with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
                replies = list(pool.map(lambda i: call(ph, {"target": "/echo", "method": "POST", "body": str(i)})[1]["body"], range(64)))
            assert replies == [str(i) for i in range(64)]
        check("64 concurrent independent calls", concurrent_calls)

        def stalled_readers():
            # Every call between two gateways shares one HTTP/2 connection. Streams whose
            # reader stalls may fill their own window, never the whole connection's.
            wide, _ = chain(backend, limit=64 << 20, timeout=20_000, concurrency=16)

            def probe(what):
                started = time.monotonic()
                c = http.client.HTTPConnection("127.0.0.1", wide, timeout=5)
                try:
                    c.request("GET", "/")
                    assert c.getresponse().read() == b"hello"
                except TimeoutError:
                    raise AssertionError(f"an unrelated call froze while {what}")
                finally:
                    c.close()
                assert time.monotonic() - started < 2, what

            stalled = []
            for _ in range(4):
                s = socket.create_connection(("127.0.0.1", wide))
                s.sendall(b"GET /bytes/40960 HTTP/1.1\r\nHost: x\r\n\r\n")  # 10 MiB, never read
                stalled.append(s)
            time.sleep(1.5)
            try: probe("4 clients stall reading 10 MiB downloads")
            finally:
                for s in stalled: s.close()

            def upload():
                c = http.client.HTTPConnection("127.0.0.1", wide, timeout=30)
                try:
                    c.request("POST", "/stall-read", b"u" * (16 << 20))
                    assert c.getresponse().status == 200
                finally:
                    c.close()
            STALL.clear()
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
                uploads = [pool.submit(upload) for _ in range(4)]
                time.sleep(1.5)
                try: probe("4 backends stall reading 16 MiB uploads")
                finally: STALL.set()
                for u in uploads: u.result()
        check("stalled readers in either direction do not block unrelated calls", stalled_readers)

        def routes():
            pp, pg = port(), port()
            conf = work / "routes.toml"
            conf.write_text(f'[[upstream]]\nname="one"\naddress="http://127.0.0.1:{pg}"\n[[http]]\nlisten="127.0.0.1:{pp}"\nupstream="one"\nroutes=[{{prefix="/api/",upstream="one",strip_prefix=true}},{{prefix="/v2",upstream="one",strip_prefix=true}}]\n[[grpc]]\nlisten="127.0.0.1:{pg}"\nbackend="{backend}"\n')
            process([GW, "run", "-c", conf], pp)
            assert json.loads(request(pp, "/api/inspect?a=1")[2])["path"] == "/inspect?a=1"
            # A prefix matches whole path segments: /v2 covers /v2 and /v2/..., not /v2inspect.
            assert json.loads(request(pp, "/v2/inspect")[2])["path"] == "/inspect"
            assert request(pp, "/v2inspect")[2] == b"hello"
        check("TOML configuration and path routing", routes)

        clients = ROOT / "examples/http"
        env = {"MINTA_GATEWAY": f"http://127.0.0.1:{ph}"}
        check("Python standard-library HTTP/JSON client", lambda: run_client([sys.executable, clients / "client.py"], env))
        tools = [
            ("Node.js fetch", "node", lambda exe: [exe, clients / "client.js"]),
            ("PowerShell Invoke-RestMethod", "pwsh", lambda exe: [exe, "-NoProfile", "-File", clients / "client.ps1"]),
            ("Java HttpClient", "java", lambda exe: [exe, clients / "Client.java"]),
            ("Go net/http", "go", lambda exe: [exe, "run", clients / "client.go"]),
            ("C# HttpClient", "dotnet", lambda exe: [exe, "run", "--project", clients / "dotnet/Client.csproj", "--configuration", "Release"]),
        ]
        for name, tool, command in tools:
            exe = os.environ.get(tool.upper()) or shutil.which(tool)
            if exe: check(name + " HTTP/JSON client", lambda exe=exe, command=command: run_client(command(exe), env))
            else: skip(name, "toolchain missing")
        curl = shutil.which("curl")
        if curl:
            def curl_client():
                p = subprocess.run([curl, "--noproxy", "*", "-fsS", "-H", "Content-Type: application/json", "--data", '{"method":"POST","target":"/echo","body":"curl"}', env["MINTA_GATEWAY"] + "/v1/call"], capture_output=True, timeout=10)
                assert p.returncode == 0 and json.loads(p.stdout)["body"] == "curl"
            check("curl HTTP/JSON client (usable from any language with process execution)", curl_client)

        for module, script, scheme, name in [("grpc", "grpc_test.py", "h2c", "real gRPC: unary, both streaming directions, interleaved bidi, trailers and errors"), ("websockets", "ws_test.py", "http", "real WebSocket: text, 1 MiB binary, ping-pong and clean close")]:
            if not importlib.util.find_spec(module):
                assert not args.strict, f"install tests/requirements.txt: missing {module}"
                skip(name, "optional Python dependency missing")
                continue
            def application(script=script, scheme=scheme):
                pb = port()
                process([sys.executable, ROOT / "tests" / script, "serve", pb], pb)
                edge, _ = chain(f"{scheme}://127.0.0.1:{pb}", limit=8 << 20, timeout=60_000)
                address = f"127.0.0.1:{edge}" if scheme == "h2c" else f"ws://127.0.0.1:{edge}/chat"
                run_client([sys.executable, ROOT / "tests" / script, "client", address])
            check(name, application)

        if importlib.util.find_spec("cryptography"):
            def tls_auth():
                from cryptography import x509
                from cryptography.hazmat.primitives import hashes, serialization
                from cryptography.hazmat.primitives.asymmetric import rsa
                from cryptography.x509.oid import NameOID
                cert, key = work / "cert.pem", work / "key.pem"
                private = rsa.generate_private_key(public_exponent=65537, key_size=2048)
                subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
                now = datetime.datetime.now(datetime.timezone.utc)
                certificate = (x509.CertificateBuilder().subject_name(subject).issuer_name(subject).public_key(private.public_key()).serial_number(x509.random_serial_number())
                    .not_valid_before(now - datetime.timedelta(minutes=1)).not_valid_after(now + datetime.timedelta(days=1))
                    .add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost")]), critical=False)
                    .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True).sign(private, hashes.SHA256()))
                cert.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
                key.write_bytes(private.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
                token_env = {"MINTA_TEST_TOKEN": "test-only-token"}
                edge, pg = chain(backend, extra=("--cert-file", cert, "--key-file", key, "--token-env", "MINTA_TEST_TOKEN"), client_extra=("--ca-file", cert, "--token-env", "MINTA_TEST_TOKEN"), env=token_env)
                assert request(edge, "/")[0] == 200
                invalid = port()
                process([GW, "proxy", "--listen", f"127.0.0.1:{invalid}", "--upstream", f"https://localhost:{pg}", "--ca-file", cert], invalid)
                assert request(invalid, "/")[0] == 401
                untrusted = port()
                process([GW, "proxy", "--listen", f"127.0.0.1:{untrusted}", "--upstream", f"https://localhost:{pg}"], untrusted)
                assert request(untrusted, "/")[0] == 503
                https = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
                https.daemon_threads = True
                ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
                ctx.load_cert_chain(cert, key)
                https.socket = ctx.wrap_socket(https.socket, server_side=True)
                threading.Thread(target=https.serve_forever, daemon=True).start()
                try:
                    edge, _ = chain(f"https://localhost:{https.server_port}", extra=("--backend-ca-file", cert))
                    assert request(edge, "/")[0] == 200
                finally:
                    https.shutdown()
                    https.server_close()
            check("gRPC TLS, CA validation, bearer authentication and HTTPS backend", tls_auth)
        else:
            assert not args.strict, "install tests/requirements.txt: missing cryptography"
            skip("TLS/authentication", "optional cryptography dependency missing")
        print(f"\n{len(PASSED)} passed, 0 failed, {len(SKIPPED)} skipped", flush=True)
    finally:
        for child in reversed(PROCESSES):
            if child.poll() is None:
                child.terminate()
                try: child.wait(timeout=3)
                except subprocess.TimeoutExpired: child.kill(); child.wait()
        for log in LOGS: log.close()
        server.shutdown()
        server.server_close()
        temp.cleanup()


if __name__ == "__main__":
    main()
