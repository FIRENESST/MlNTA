package io.minta.beep;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/** java io.minta.beep.BeepTest ECHO_ADDR [TLS_ECHO_ADDR CA CERT KEY] — or --serve PORT. */
public final class BeepTest {
    static int failures = 0;

    static void check(boolean cond, String msg) {
        if (!cond) {
            failures++;
            System.out.println("FAIL: " + msg);
        }
    }

    static byte[] utf8(String s) { return s.getBytes(StandardCharsets.UTF_8); }

    static String lines(int n) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++) sb.append("line ").append(i).append('\n');
        return sb.toString();
    }

    static void againstRustEcho(String host, int port) throws Exception {
        try (RpcClient c = new RpcClient(host, port, new RpcClient.Options().maxChannels(8).timeoutMs(5000))) {
            Rpc.Response r = c.call("POST", "/echo?lang=java", Headers.of("Content-Type", "text/plain", "X-Test", "java"), utf8("hello from java"));
            check(r.status == 200 && r.text().equals("hello from java"), "echo " + r.status + " " + r.text());
            check("java".equals(r.header("x-test")) && "POST".equals(r.header("X-Echo-Method")), "echo headers " + r.headers);
            r = c.get("/stream/50");
            check(r.text().equals(lines(50)), "stream body");
            try (RpcClient.StreamingResponse sr = c.stream("GET", "/stream/1000", null, null, -1)) {
                byte[] first = sr.next();
                check(first != null && new String(first, StandardCharsets.UTF_8).startsWith("line 0"), "first chunk");
            }
            r = c.get("/status/418");
            check(r.status == 418 && r.text().equals("status 418"), "status " + r.status);
            try {
                c.get("/err/553");
                check(false, "expected Remote");
            } catch (BeepException.Remote e) {
                check(e.code == 553 && e.httpStatus() == 400, "err " + e);
            }
            r = c.get("/bytes/3000000");
            check(r.body.length == 3000000 && r.body[0] == 'a', "3 MB body");
            byte[] big = new byte[2_500_000];
            new Random(1).nextBytes(big);
            r = c.call("POST", "/echo", null, big);
            check(Arrays.equals(r.body, big), "2.5 MB upload");
            ExecutorService pool = Executors.newFixedThreadPool(40);
            long t0 = System.nanoTime();
            List<Future<Rpc.Response>> fs = new ArrayList<>();
            for (int i = 0; i < 40; i++) {
                final int n = i;
                fs.add(pool.submit(() -> c.call("POST", "/sleep/50", null, utf8(Integer.toString(n)))));
            }
            for (Future<Rpc.Response> f : fs) check(f.get().status == 200, "concurrent");
            long ms = (System.nanoTime() - t0) / 1_000_000;
            check(ms < 2000, "concurrency too slow " + ms + "ms");
            pool.shutdown();
            check(c.status().size() == 1 && c.status().get(0) <= 8, "pool " + c.status());
            try {
                c.call("GET", "/sleep/2000", null, null, 200);
                check(false, "expected timeout");
            } catch (BeepException.Timeout | BeepException.Remote e) {
                check(!(e instanceof BeepException.Remote) || ((BeepException.Remote) e).code == 451, "timeout " + e);
            }
            r = c.call("POST", "/echo", null, utf8("after-timeout"));
            check(r.text().equals("after-timeout"), "pool usable after timeout");
        }
    }

    static Rpc.Reply handler(Rpc.Request req) throws Exception {
        String path = req.path();
        if (path.startsWith("/stream/")) {
            int n = Integer.parseInt(path.substring(8));
            Iterator<byte[]> it = new Iterator<byte[]>() {
                int i = 0;
                public boolean hasNext() { return i < n; }
                public byte[] next() { return utf8("line " + (i++) + "\n"); }
            };
            return Rpc.Reply.stream(200, Headers.of("Content-Type", "text/plain"), it);
        }
        if (path.equals("/abort")) {
            Iterator<byte[]> it = new Iterator<byte[]>() {
                int i = 0;
                public boolean hasNext() { return true; }
                public byte[] next() {
                    if (i++ == 0) return utf8("partial");
                    throw new IllegalStateException("iterator died");
                }
            };
            return Rpc.Reply.stream(200, null, it);
        }
        if (path.startsWith("/err/")) throw new Rpc.Failure(Integer.parseInt(path.substring(5)), "java says no");
        String ct = req.header("Content-Type");
        return Rpc.Reply.of(200, Headers.of("Content-Type", ct == null ? "application/octet-stream" : ct, "X-Lang", "java"), req.body);
    }

    static void javaServerJavaClient() throws Exception {
        Config cfg = new Config().recvWindow(6000).maxFrameSize(1999);
        try (RpcServer server = new RpcServer(BeepTest::handler, cfg, null).start("127.0.0.1", 0);
             RpcClient c = new RpcClient("127.0.0.1", server.port(), new RpcClient.Options().config(cfg))) {
            byte[] data = new byte[1_000_000];
            new Random(2).nextBytes(data);
            Rpc.Response r = c.call("POST", "/echo", null, data);
            check(Arrays.equals(r.body, data) && "java".equals(r.header("X-Lang")), "java<->java big echo with tiny windows");
            r = c.get("/stream/20");
            check(r.text().equals(lines(20)), "java stream");
            try {
                c.get("/abort");
                check(false, "expected abort");
            } catch (BeepException.Aborted e) {
                check(e.getMessage().contains("iterator died"), "abort " + e);
            }
            try {
                c.get("/err/537");
                check(false, "expected 537");
            } catch (BeepException.Remote e) {
                check(e.code == 537, "err " + e);
            }
            Session s = Session.connect("127.0.0.1", server.port());
            check(s.peerProfiles().equals(java.util.Collections.singletonList(Rpc.PROFILE)), "greeting " + s.peerProfiles());
            try {
                s.startChannel("urn:none");
                check(false, "expected refusal");
            } catch (BeepException.Refused e) {
                check(e.code == 550, "refused " + e);
            }
            Channel ch = s.startChannel(Rpc.PROFILE);
            List<ReplyStream> streams = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                streams.add(ch.send(Wire.encodeEntity(Headers.of("Rpc-Method", "GET", "Rpc-Target", "/echo"), utf8(Integer.toString(i)))));
            }
            for (int i = 0; i < 10; i++) {
                String body = new String(streams.get(i).collect(1 << 20).payload(), StandardCharsets.UTF_8);
                check(body.endsWith(Integer.toString(i)), "pipelined replies in order");
            }
            ch.close();
            s.close();
            check(s.isClosed(), "session closed");
        }
    }

    static void tls(String tlsAddr, String ca, String cert, String key) throws Exception {
        String[] hp = tlsAddr.split(":");
        try (RpcClient c = new RpcClient(hp[0], Integer.parseInt(hp[1]), new RpcClient.Options().tls(new Tls.Client("localhost", ca)))) {
            Rpc.Response r = c.call("POST", "/echo", null, utf8("java over tls to rust"));
            check(r.text().equals("java over tls to rust"), "java tls -> rust");
        }
        try (RpcServer server = new RpcServer(BeepTest::handler, null, new Tls.Server(cert, key)).start("127.0.0.1", 0);
             RpcClient c = new RpcClient("127.0.0.1", server.port(), new RpcClient.Options().tls(new Tls.Client("localhost", ca)))) {
            Rpc.Response r = c.call("POST", "/echo", null, utf8("java tls both ends"));
            check(r.text().equals("java tls both ends"), "java tls -> java");
        }
    }

    public static void main(String[] args) throws Exception {
        if (args[0].equals("--serve")) {
            RpcServer s = new RpcServer(BeepTest::handler).start("127.0.0.1", Integer.parseInt(args[1]));
            System.out.println("READY " + s.port());
            Thread.currentThread().join();
            return;
        }
        String[] hp = args[0].split(":");
        againstRustEcho(hp[0], Integer.parseInt(hp[1]));
        javaServerJavaClient();
        if (args.length >= 5) tls(args[1], args[2], args[3], args[4]);
        System.out.println(failures == 0 ? "Java: all tests passed" : "Java: " + failures + " FAILURE(S)");
        System.exit(failures == 0 ? 0 : 1);
    }
}
