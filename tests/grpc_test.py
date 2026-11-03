"""gRPC through the BEEP bridge, using real grpcio on both ends (no protoc: raw-bytes methods).

    python grpc_test.py serve PORT     # gRPC server (the backend)
    python grpc_test.py client ADDR    # gRPC client; ADDR is the gateway's h2c egress

Chain under test: grpc client → beepgw proxy (h2c) → BEEP rpc:2 → beepgw expose → h2c → grpc server.
Covers unary + metadata + trailing metadata, server/client streaming, interleaved bidi (needs
full duplex), status errors (trailers-only responses), deadlines and a 3 MB message.
"""
import queue
import sys
import time
from concurrent import futures

import grpc

SVC = "minta.Echo"


def unary(req, ctx):
    md = dict(ctx.invocation_metadata())
    ctx.set_trailing_metadata((("x-trailer", "t-" + md.get("x-meta", "")),))
    return b"echo:" + req


def server_stream(req, ctx):
    for i in range(int(req)):
        yield f"item {i}".encode()


def client_stream(it, ctx):
    return str(sum(len(m) for m in it)).encode()


def bidi(it, ctx):
    for m in it:
        yield b"re:" + m


def fail(req, ctx):
    ctx.abort(grpc.StatusCode.NOT_FOUND, "no such thing")


def slow(req, ctx):
    time.sleep(3)
    return b"late"


HANDLERS = {
    "Unary": grpc.unary_unary_rpc_method_handler(unary),
    "ServerStream": grpc.unary_stream_rpc_method_handler(server_stream),
    "ClientStream": grpc.stream_unary_rpc_method_handler(client_stream),
    "Bidi": grpc.stream_stream_rpc_method_handler(bidi),
    "Fail": grpc.unary_unary_rpc_method_handler(fail),
    "Slow": grpc.unary_unary_rpc_method_handler(slow),
}


def serve(port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler(SVC, HANDLERS),))
    server.add_insecure_port(f"127.0.0.1:{port}")
    server.start()
    server.wait_for_termination()


failures = 0


def check(cond, msg):
    global failures
    if not cond:
        failures += 1
        print("FAIL:", msg, flush=True)


def client(addr):
    opts = [("grpc.max_receive_message_length", 16 << 20), ("grpc.max_send_message_length", 16 << 20), ("grpc.enable_http_proxy", 0)]
    ch = grpc.insecure_channel(addr, options=opts)
    grpc.channel_ready_future(ch).result(timeout=15)
    m = lambda name: f"/{SVC}/{name}"

    resp, call = ch.unary_unary(m("Unary")).with_call(b"hi", metadata=(("x-meta", "m1"),), timeout=15)
    check(resp == b"echo:hi", f"unary {resp!r}")
    check(dict(call.trailing_metadata()).get("x-trailer") == "t-m1", f"trailing metadata {call.trailing_metadata()}")

    items = list(ch.unary_stream(m("ServerStream"))(b"5", timeout=15))
    check(items == [f"item {i}".encode() for i in range(5)], f"server stream {items}")

    total = ch.stream_unary(m("ClientStream"))(iter([b"a" * 1000] * 50), timeout=15)
    check(total == b"50000", f"client stream {total!r}")

    # Each reply must arrive before the next request is sent: impossible without full duplex.
    q = queue.Queue()

    def gen():
        while True:
            item = q.get()
            if item is None:
                return
            yield item

    replies = ch.stream_stream(m("Bidi"))(gen(), timeout=15)
    for i in range(3):
        q.put(f"m{i}".encode())
        r = next(replies)
        check(r == f"re:m{i}".encode(), f"bidi reply {i}: {r!r}")
    q.put(None)
    check(list(replies) == [], "bidi end")

    try:
        ch.unary_unary(m("Fail"))(b"x", timeout=15)
        check(False, "expected NOT_FOUND")
    except grpc.RpcError as e:
        check(e.code() == grpc.StatusCode.NOT_FOUND and e.details() == "no such thing", f"status {e.code()} {e.details()!r}")

    try:
        ch.unary_unary(m("Slow"))(b"x", timeout=0.5)
        check(False, "expected DEADLINE_EXCEEDED")
    except grpc.RpcError as e:
        check(e.code() == grpc.StatusCode.DEADLINE_EXCEEDED, f"deadline {e.code()}")

    big = bytes(range(256)) * (3 * 4096)
    check(ch.unary_unary(m("Unary"))(big, timeout=30) == b"echo:" + big, "3 MB message")

    ch.close()
    print("gRPC via BEEP: all tests passed" if failures == 0 else f"gRPC via BEEP: {failures} FAILURE(S)", flush=True)
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    if sys.argv[1] == "serve":
        serve(int(sys.argv[2]))
    else:
        client(sys.argv[2])
