#!/usr/bin/env python3
"""Build (optionally) and run every binding's tests plus a cross-language interop matrix.

    python tests/run_all.py --build          # build everything, then test
    python tests/run_all.py                  # test with existing builds

Toolchains are found on PATH; override with env vars: CARGO, GO, PHP, RUBY, ZIG, JAVA, JAVAC,
DOTNET, NODE, GCC, GXX, GFORTRAN, PERL, TCLSH, PWSH, CURL, OPENSSL, LUAJIT, NIM, LDC2, FPC, JULIA,
DART, SWIFT, COBC, RSCRIPT, ELIXIR, ERLC, ERL, RUNGHC, CRYSTAL, LUA, OCAMLFIND, KOTLINC, SCALAC,
GROOVY, CLOJURE. Missing tools → SKIP.
"""
import argparse
import os
import platform
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BUILD = ROOT / "tests" / ".build"
WIN = platform.system() == "Windows"
EXE = ".exe" if WIN else ""
LIBNAME = "beep.dll" if WIN else ("libbeep.dylib" if platform.system() == "Darwin" else "libbeep.so")
RELEASE = ROOT / "core" / "target" / "release"

results = []  # (group, name, status, detail)


def tool(name, *fallbacks):
    env = os.environ.get(name.upper())
    if env and Path(env).exists():
        return env
    for n in (name, *fallbacks):
        p = shutil.which(n)
        if p:
            return p
    return None


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def run(cmd, cwd=None, env=None, timeout=300):
    e = dict(os.environ)
    e.update(env or {})
    try:
        p = subprocess.run(cmd, cwd=cwd, env=e, capture_output=True, timeout=timeout)
        out = (p.stdout + p.stderr).decode("utf-8", "replace")
        return p.returncode, out
    except subprocess.TimeoutExpired:
        return -1, "TIMEOUT"
    except FileNotFoundError as ex:
        return -2, str(ex)


def record(group, name, rc, out, ok_marker=None):
    ok = rc == 0 and (ok_marker is None or ok_marker in out)
    detail = "" if ok else out.strip().splitlines()[-1][:160] if out.strip() else f"rc={rc}"
    results.append((group, name, "PASS" if ok else "FAIL", detail))
    print(f"[{'PASS' if ok else 'FAIL'}] {group}: {name}" + ("" if ok else f"  -- {detail}"), flush=True)
    if not ok and os.environ.get("VERBOSE"):
        print(out)


def skip(group, name, why):
    results.append((group, name, "SKIP", why))
    print(f"[SKIP] {group}: {name}  -- {why}", flush=True)


class Proc:
    def __init__(self, cmd, cwd=None, env=None, ready=None):
        e = dict(os.environ)
        e.update(env or {})
        self.p = subprocess.Popen(cmd, cwd=cwd, env=e, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        self.ready = ready

    def wait_port(self, port, timeout=30):
        t0 = time.time()
        while time.time() - t0 < timeout:
            if self.p.poll() is not None:
                raise RuntimeError(f"server exited: {self.p.stdout.read().decode(errors='replace')[-500:]}")
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                    return self
            except OSError:
                time.sleep(0.1)
        raise RuntimeError(f"port {port} not ready")

    def stop(self):
        if self.p.poll() is None:
            self.p.kill()
            self.p.wait()


def build(t):
    BUILD.mkdir(parents=True, exist_ok=True)
    steps = []
    if t["cargo"]:
        steps.append(("rust", [t["cargo"], "build", "--release"], ROOT / "core"))
    if t["javac"]:
        srcs = [str(p) for p in (ROOT / "native/java/src").rglob("*.java")]
        steps.append(("java", [t["javac"], "--release", "11", "-d", str(BUILD / "java"), *srcs], None))
    if t["dotnet"]:
        for proj in ("native/dotnet/tests/Minta.Beep.Tests", "examples/fsharp", "examples/vbnet"):
            steps.append(("dotnet " + proj, [t["dotnet"], "build", "-c", "Release", "-nologo", "-v", "q"], ROOT / proj))
    if t["go"]:
        steps.append(("go beepconf", [t["go"], "build", "-o", str(BUILD / f"beepconf{EXE}"), "./cmd/beepconf"], ROOT / "native/go"))
    for name, cmd, cwd in steps:
        rc, out = run(cmd, cwd=cwd, timeout=1200)
        print(f"[build] {name}: {'ok' if rc == 0 else 'FAILED'}", flush=True)
        if rc != 0:
            print(out[-3000:])
    lib = RELEASE / LIBNAME
    inc = ROOT / "core/ffi/include"
    link = [str(lib)] if WIN else [f"-L{RELEASE}", "-lbeep", f"-Wl,-rpath,{RELEASE}"]
    if lib.exists():
        shutil.copy(lib, BUILD / LIBNAME)
        if t["gcc"]:
            run([t["gcc"], "-I", str(inc), str(ROOT / "bindings/c/test_beep.c"), *link, "-o", str(BUILD / f"test_c{EXE}")])
        if t["gxx"]:
            run([t["gxx"], "-std=c++17", "-I", str(inc), "-I", str(ROOT / "bindings/cpp"), str(ROOT / "bindings/cpp/test_beep.cpp"), *link,
                 "-o", str(BUILD / f"test_cpp{EXE}")])
        if t["gfortran"]:
            static = ["-static-libgfortran", "-static-libgcc"] if WIN else []
            run([t["gfortran"], *static, "-J", str(BUILD), "-c", str(ROOT / "bindings/fortran/beep.f90"), "-o", str(BUILD / "beep_f.o")])
            run([t["gfortran"], *static, "-I", str(BUILD), str(ROOT / "bindings/fortran/test_beep.f90"), str(BUILD / "beep_f.o"), *link,
                 "-o", str(BUILD / f"test_fortran{EXE}")])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--build", action="store_true")
    args = ap.parse_args()
    t = {k: tool(k) for k in ("cargo", "go", "php", "ruby", "zig", "java", "javac", "dotnet", "node", "gcc", "perl", "tclsh", "pwsh", "curl", "openssl")}
    t["gxx"] = tool("gxx", "g++")
    t["gfortran"] = tool("gfortran")
    t["sh"] = tool("sh", "bash")
    for k in ("luajit", "nim", "fpc", "julia", "dart", "swift", "cobc", "elixir", "erlc", "erl", "runghc", "crystal",
              "ocamlfind", "kotlinc", "scalac", "groovy", "clojure"):
        t[k] = tool(k)
    t["ldc2"] = tool("ldc2", "ldmd2")
    t["rscript"] = tool("rscript", "Rscript")
    t["lua"] = tool("lua", "lua5.4", "lua5.3", "lua5.1")
    py = sys.executable
    if args.build:
        build(t)

    gw = RELEASE / f"beepgw{EXE}"
    if not gw.exists():
        sys.exit("beepgw not built: run with --build (needs cargo)")
    certs = BUILD / "certs"
    certs.mkdir(parents=True, exist_ok=True)
    cert, key = certs / "cert.pem", certs / "key.pem"
    if not cert.exists() and t["openssl"]:
        run([t["openssl"], "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", str(key), "-out", str(cert),
             "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost", "-days", "30"],
            env={"MSYS_NO_PATHCONV": "1"})
    have_tls = cert.exists()

    procs = []
    try:
        p_echo, p_tls, p_http, p_jsonl, p_backend, p_ingress, p_http2 = (free_port() for _ in range(7))
        echo = f"127.0.0.1:{p_echo}"
        tls_echo = f"127.0.0.1:{p_tls}"
        procs.append(Proc([str(gw), "echo", "--listen", echo]).wait_port(p_echo))
        if have_tls:
            procs.append(Proc([str(gw), "echo", "--listen", tls_echo, "--tls-cert", str(cert), "--tls-key", str(key)]).wait_port(p_tls))
        procs.append(Proc([str(gw), "proxy", "--listen", f"127.0.0.1:{p_http}", "--upstream", echo, "--jsonl", f"127.0.0.1:{p_jsonl}"]).wait_port(p_http))
        procs.append(Proc([py, str(ROOT / "tests/backend.py"), str(p_backend)]).wait_port(p_backend))
        procs.append(Proc([str(gw), "expose", "--listen", f"127.0.0.1:{p_ingress}", "--backend", f"http://127.0.0.1:{p_backend}"]).wait_port(p_ingress))
        procs.append(Proc([str(gw), "proxy", "--listen", f"127.0.0.1:{p_http2}", "--upstream", f"127.0.0.1:{p_ingress}"]).wait_port(p_http2))
        gateway = f"http://127.0.0.1:{p_http}"
        tls_args = [tls_echo, str(cert), str(cert), str(key)] if have_tls else []
        libpath = {"PATH": f"{BUILD}{os.pathsep}{os.environ.get('PATH', '')}", "LD_LIBRARY_PATH": str(RELEASE), "BEEP_LIB": str(RELEASE / LIBNAME)}

        # ---- Rust core: unit + integration tests (frames, flow control, RPC, TLS) ------------
        if t["cargo"]:
            rc, out = run([t["cargo"], "test", "--release", "-p", "minta-beep"], cwd=ROOT / "core", timeout=1200)
            record("native", "Rust core (cargo test)", rc, out, "test result: ok")
        else:
            skip("native", "Rust core", "cargo not found")

        # ---- native implementations against the Rust echo server -------------------------
        rc, out = run([py, str(ROOT / "native/python/tests/test_minta_beep.py"), echo, *tls_args])
        record("native", "Python", rc, out, "all tests passed")
        if t["node"]:
            rc, out = run([t["node"], str(ROOT / "native/node/test/test.js"), echo, *tls_args], env={"GATEWAY_HTTP": f"127.0.0.1:{p_http}"})
            record("native", "Node.js (+h2c via gateway)", rc, out, "all tests passed")
        else:
            skip("native", "Node.js", "node not found")
        if t["java"] and (BUILD / "java").exists():
            if t["javac"]:
                run([t["javac"], "--release", "11", "-cp", str(BUILD / "java"), "-d", str(BUILD / "java"), str(ROOT / "native/java/src/test/java/io/minta/beep/BeepTest.java")])
            rc, out = run([t["java"], "-cp", str(BUILD / "java"), "io.minta.beep.BeepTest", echo, *tls_args])
            record("native", "Java", rc, out, "all tests passed")
        else:
            skip("native", "Java", "java/javac not found or not built")
        dll = ROOT / "native/dotnet/tests/Minta.Beep.Tests/bin/Release/net8.0/Minta.Beep.Tests.dll"
        if t["dotnet"] and dll.exists():
            rc, out = run([t["dotnet"], str(dll), echo, *tls_args])
            record("native", ".NET (C#)", rc, out, "all tests passed")
            for name, proj in (("F#", "examples/fsharp/bin/Release/net8.0/BeepFs.dll"), ("VB.NET", "examples/vbnet/bin/Release/net8.0/BeepVb.dll")):
                rc, out = run([t["dotnet"], str(ROOT / proj), echo])
                record("native", name, rc, out, "all tests passed")
        else:
            skip("native", ".NET", "dotnet not found or not built")
        if t["go"]:
            env = {"BEEP_ECHO": echo}
            if have_tls:
                env.update(BEEP_TLS_ECHO=tls_echo, BEEP_CA=str(cert), BEEP_CERT=str(cert), BEEP_KEY=str(key))
            rc, out = run([t["go"], "test", "-count=1", "./..."], cwd=ROOT / "native/go", env=env)
            record("native", "Go (+net/http adapter, RoundTripper)", rc, out, "ok")
        else:
            skip("native", "Go", "go not found")

        # ---- C ABI bindings -------------------------------------------------------------------
        for name, exe in (("C", "test_c"), ("C++", "test_cpp"), ("Fortran", "test_fortran")):
            p = BUILD / f"{exe}{EXE}"
            if p.exists():
                rc, out = run([str(p), echo], env=libpath)
                record("ffi", name, rc, out, "all tests passed")
            else:
                skip("ffi", name, "not built (needs gcc/g++/gfortran)")
        if t["ruby"]:
            rc, out = run([t["ruby"], str(ROOT / "bindings/ruby/test_beep.rb"), echo], env=libpath)
            record("ffi", "Ruby (Fiddle)", rc, out, "all tests passed")
        else:
            skip("ffi", "Ruby (Fiddle)", "ruby not found")
        if t["php"]:
            ext = Path(t["php"]).parent / "ext"
            cmd = [t["php"], "-n", "-d", f"extension_dir={ext}", "-d", "extension=ffi"] if WIN else [t["php"], "-d", "ffi.enable=true"]
            rc, out = run([*cmd, str(ROOT / "bindings/php/test_beep.php"), echo], env=libpath)
            record("ffi", "PHP (ext-ffi)", rc, out, "all tests passed")
        else:
            skip("ffi", "PHP (ext-ffi)", "php not found")
        if t["zig"]:
            implib = RELEASE / "beep.dll.lib" if WIN else None
            linkargs = [str(implib)] if WIN else [f"-L{RELEASE}", "-lbeep"]
            rc, out = run([t["zig"], "test", "test_beep.zig", "-lc", *linkargs, "--global-cache-dir", str(BUILD / "zigcache")],
                          cwd=ROOT / "bindings/zig", env={**libpath, "BEEP_ECHO": echo}, timeout=600)
            record("ffi", "Zig", rc, out, "passed")
        else:
            skip("ffi", "Zig", "zig not found")

        out_dir = BUILD / "ffi"
        out_dir.mkdir(parents=True, exist_ok=True)
        implib = [str(RELEASE / "beep.dll.lib")] if WIN else [f"-L{RELEASE}", "-lbeep"]

        def case(group, name, key, steps, cmd, cwd=None, marker="all tests passed", timeout=900):
            """Build with `steps` (each a command), then run `cmd`; skipped when tool `key` is missing."""
            if not t[key]:
                skip(group, name, f"{key} not found")
                return
            env = {**libpath, "BEEP_ECHO": echo, "BEEP_GATEWAY": gateway}
            for step in steps:
                rc, out = run(step, cwd=cwd, env=env, timeout=timeout)
                if rc != 0:
                    record(group, name, rc, out)
                    return
            rc, out = run(cmd, cwd=cwd, env=env, timeout=timeout)
            record(group, name, rc, out, marker)

        b = ROOT / "bindings"
        case("ffi", "LuaJIT", "luajit", [], [t["luajit"], str(b / "luajit/test_beep.lua"), echo])
        case("ffi", "Nim", "nim", [[t["nim"], "c", "--hints:off", f"--nimcache:{out_dir / 'nimcache'}",
                                    f"-o:{out_dir / ('test_nim' + EXE)}", "test_beep.nim"]],
             [str(out_dir / f"test_nim{EXE}"), echo], cwd=b / "nim")
        case("ffi", "D (LDC)", "ldc2", [[t["ldc2"], "test_beep.d", "beep.d", f"-of={out_dir / ('test_d' + EXE)}",
                                         f"-od={out_dir / 'd_obj'}", *(implib if WIN else [f"-L-L{RELEASE}", "-L-lbeep"])]],
             [str(out_dir / f"test_d{EXE}"), echo], cwd=b / "d")
        (out_dir / "fpc").mkdir(exist_ok=True)
        case("ffi", "Free Pascal", "fpc", [[t["fpc"], "-Fu.", f"-FU{out_dir / 'fpc'}", f"-o{out_dir / ('test_pascal' + EXE)}",
                                            *([] if WIN else [f"-Fl{RELEASE}"]), "test_beep.pas"]],
             [str(out_dir / f"test_pascal{EXE}"), echo], cwd=b / "pascal")
        case("ffi", "Julia", "julia", [], [t["julia"], "--startup-file=no", str(b / "julia/test_beep.jl"), echo])
        case("ffi", "Dart", "dart", [[t["dart"], "pub", "get"]], [t["dart"], "run", "test_beep.dart", echo], cwd=b / "dart")
        case("ffi", "Swift", "swift", [[t["swift"], "build", "--package-path", str(b / "swift"), "--scratch-path", str(out_dir / "swift"),
                                        "-Xlinker", f"-L{RELEASE}"]],
             [str(out_dir / "swift/debug" / f"beep-test{EXE}"), echo])
        case("ffi", "COBOL (GnuCOBOL)", "cobc", [[t["cobc"], "-x", "-o", str(out_dir / f"test_cobol{EXE}"), "beep-demo.cob", *implib]],
             [str(out_dir / f"test_cobol{EXE}")], cwd=b / "cobol")

        # ---- gateway (HTTP / JSON-lines) SDKs ------------------------------------------------
        genv = {"BEEP_GATEWAY": gateway, "MSYS_NO_PATHCONV": "1"}
        sdk = [
            ("Shell/curl", [t["sh"], str(ROOT / "sdk/shell/test.sh")] if t["sh"] and t["curl"] else None),
            ("PowerShell", [t["pwsh"], "-NoProfile", "-File", str(ROOT / "sdk/powershell/test.ps1")] if t["pwsh"] else None),
            ("Perl", [t["perl"], "-Ilib", "test.pl"] if t["perl"] else None),
            ("Tcl", [t["tclsh"], str(ROOT / "sdk/tcl/test.tcl")] if t["tclsh"] else None),
            ("PHP", [t["php"], "-n", str(ROOT / "sdk/php/test_gateway.php")] if t["php"] else None),
            ("Ruby", [t["ruby"], str(ROOT / "sdk/ruby/test_gateway.rb")] if t["ruby"] else None),
        ]
        for name, cmd in sdk:
            if cmd is None:
                skip("gateway", name, "interpreter not found")
                continue
            # MSYS perl cannot use Windows-style paths in @INC, so run it from its own directory.
            rc, out = run(cmd, env=genv, cwd=ROOT / "sdk/perl" if name == "Perl" else None)
            record("gateway", name, rc, out, "all tests passed")
        s = ROOT / "sdk"
        case("gateway", "R (curl)", "rscript", [], [t["rscript"], str(s / "r/test.R")])
        case("gateway", "Elixir (:httpc)", "elixir", [], [t["elixir"], str(s / "elixir/test.exs")])
        (out_dir / "erlang").mkdir(exist_ok=True)
        if t["erl"]:
            case("gateway", "Erlang (httpc)", "erlc", [[t["erlc"], "-o", str(out_dir / "erlang"), "beep.erl", "beep_test.erl"]],
                 [t["erl"], "-noshell", "-pa", str(out_dir / "erlang"), "-s", "beep_test", "run"], cwd=s / "erlang")
        else:
            skip("gateway", "Erlang (httpc)", "erl not found")
        case("gateway", "Haskell (http-client)", "runghc", [], [t["runghc"], "Test.hs"], cwd=s / "haskell")
        case("gateway", "Crystal", "crystal", [], [t["crystal"], "run", "test.cr"], cwd=s / "crystal")
        case("gateway", "Lua (LuaSocket)", "lua", [], [t["lua"], str(s / "lua/test.lua")])
        ml = out_dir / "ocaml"
        ml.mkdir(exist_ok=True)
        for f in ("beep.ml", "test.ml"):
            shutil.copy(s / "ocaml" / f, ml / f)
        case("gateway", "OCaml (cohttp)", "ocamlfind",
             [[t["ocamlfind"], "ocamlopt", "-thread", "-package", "cohttp-lwt-unix", "-linkpkg", "beep.ml", "test.ml", "-o", f"test_ocaml{EXE}"]],
             [str(ml / f"test_ocaml{EXE}")], cwd=ml)

        # ---- other JVM languages on the pure-Java library --------------------------------------
        jcp = str(BUILD / "java")
        jvm = BUILD / "jvm"
        (jvm / "scala").mkdir(parents=True, exist_ok=True)
        ex = ROOT / "examples/jvm"
        if t["java"] and (BUILD / "java").exists():
            case("jvm", "Kotlin", "kotlinc", [[t["kotlinc"], "-cp", jcp, str(ex / "Kotlin.kt"), "-include-runtime", "-d", str(jvm / "kotlin.jar")]],
                 [t["java"], "-cp", f"{jvm / 'kotlin.jar'}{os.pathsep}{jcp}", "KotlinKt"], marker="200 kotlin saw POST /echo")
            scala_lib = Path(t["scalac"]).resolve().parent.parent / "lib" / "*" if t["scalac"] else ""
            case("jvm", "Scala 3", "scalac", [[t["scalac"], "-classpath", jcp, "-d", str(jvm / "scala"), str(ex / "Scala.scala")]],
                 [t["java"], "-cp", os.pathsep.join([str(scala_lib), str(jvm / "scala"), jcp]), "BeepScala"], marker="200 scala saw GET /scala")
            case("jvm", "Groovy", "groovy", [], [t["groovy"], "-cp", jcp, str(ex / "groovy.groovy")], marker="200 groovy saw GET /groovy")
            case("jvm", "Clojure", "clojure", [], [t["clojure"], "-cp", jcp, str(ex / "clojure.clj")], marker="200 clojure saw GET /clj")
        else:
            skip("jvm", "Kotlin/Scala/Groovy/Clojure", "java not found or Java library not built")
        # JSON-lines
        with socket.create_connection(("127.0.0.1", p_jsonl), timeout=10) as s:
            s.sendall(b'{"id":1,"method":"POST","target":"/echo","body":"jsonl"}\n{"id":2,"target":"/err/550"}\n')
            s.shutdown(socket.SHUT_WR)
            data = b""
            while chunk := s.recv(65536):
                data += chunk
        ok = b'"body":"jsonl"' in data and b'"code":550' in data
        record("gateway", "TCP JSON-lines", 0 if ok else 1, data.decode(errors="replace"))
        # ingress chain: HTTP → gateway → BEEP → expose → Python HTTP backend
        if t["curl"]:
            rc, out = run([t["curl"], "-s", "-X", "POST", "--data", "x=1", f"http://127.0.0.1:{p_http2}/json?q=2"], env={"NO_PROXY": "*"})
            record("ingress", "HTTP→BEEP→HTTP backend (any language)", rc, out, '"path": "/json?q=2"')
            rc, out = run([t["curl"], "-s", f"http://127.0.0.1:{p_http2}/chunked/3"], env={"NO_PROXY": "*"})
            record("ingress", "chunked backend → ANS stream", rc, out, "part 2")

        # ---- cross-language matrix ---------------------------------------------------------------
        servers = {"Rust": echo}
        def serve(name, cmd, port, cwd=None):
            try:
                procs.append(Proc(cmd, cwd=cwd).wait_port(port))
                servers[name] = f"127.0.0.1:{port}"
            except RuntimeError as e:
                skip("matrix", f"{name} server", str(e)[:120])
        port = free_port(); serve("Python", [py, str(ROOT / "native/python/tests/test_minta_beep.py"), "--serve", str(port)], port)
        if t["node"]:
            port = free_port(); serve("Node", [t["node"], str(ROOT / "native/node/test/test.js"), "--serve", str(port)], port)
        if t["java"] and (BUILD / "java/io/minta/beep/BeepTest.class").exists():
            port = free_port(); serve("Java", [t["java"], "-cp", str(BUILD / "java"), "io.minta.beep.BeepTest", "--serve", str(port)], port)
        if t["dotnet"] and dll.exists():
            port = free_port(); serve(".NET", [t["dotnet"], str(dll), "--serve", str(port)], port)
        conf = BUILD / f"beepconf{EXE}"
        if conf.exists():
            port = free_port(); serve("Go", [str(conf), "serve", str(port)], port)

        def rust_client(addr):
            rc1, o1 = run([str(gw), "call", "--upstream", addr, "POST", "/echo", "-d", "conformance"], env={"MSYS_NO_PATHCONV": "1"})
            rc2, o2 = run([str(gw), "call", "--upstream", addr, "GET", "/stream/5"], env={"MSYS_NO_PATHCONV": "1"})
            rc3, o3 = run([str(gw), "call", "--upstream", addr, "GET", "/err/537"], env={"MSYS_NO_PATHCONV": "1"})
            ok = rc1 == 0 and o1.strip() == "conformance" and rc2 == 0 and "line 4" in o2 and rc3 != 0 and "537" in o3
            return (0 if ok else 1), f"{o1}|{o2}|{o3}"
        clients = {"Rust": rust_client, "Python": lambda a: run([py, str(ROOT / "tests/clients/client.py"), a])}
        if t["node"]:
            clients["Node"] = lambda a: run([t["node"], str(ROOT / "tests/clients/client.js"), a])
        if conf.exists():
            clients["Go"] = lambda a: run([str(conf), "client", a])
        for sname, saddr in servers.items():
            for cname, fn in clients.items():
                rc, out = fn(saddr)
                record("matrix", f"{cname} client → {sname} server", rc, out)
    finally:
        for p in procs:
            p.stop()

    passed = sum(1 for r in results if r[2] == "PASS")
    failed = [r for r in results if r[2] == "FAIL"]
    skipped = sum(1 for r in results if r[2] == "SKIP")
    print(f"\n==== {passed} passed, {len(failed)} failed, {skipped} skipped ====")
    for g, n, _, d in failed:
        print(f"  FAIL {g}: {n} -- {d}")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
