package io.minta.beep;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Pooled RPC client: {@code sessions} TCP connections × {@code maxChannels} channels,
 * one in-flight request per channel. Thread-safe.
 *
 * <pre>
 * try (RpcClient c = new RpcClient("127.0.0.1", 10288)) {
 *     Rpc.Response r = c.call("POST", "/orders", Headers.of("Content-Type", "application/json"), "{}".getBytes());
 * }
 * </pre>
 */
public final class RpcClient implements AutoCloseable {
    public static final class Options {
        public int sessions = 1;
        public int maxChannels = 64;
        /** Deadline for the response head; also sent as Rpc-Timeout. 0 disables. */
        public long timeoutMs = 30_000;
        public Tls.Client tls;
        public Config config = new Config();
        public String profile = Rpc.PROFILE;

        public Options sessions(int v) { sessions = v; return this; }
        public Options maxChannels(int v) { maxChannels = v; return this; }
        public Options timeoutMs(long v) { timeoutMs = v; return this; }
        public Options tls(Tls.Client v) { tls = v; return this; }
        public Options config(Config v) { config = v; return this; }
    }

    private final String host;
    private final int port;
    private final Options opts;
    private final List<Session> sessions = new ArrayList<>();
    private final Deque<Channel> idle = new ArrayDeque<>();
    private final Semaphore permits;
    private final Object connectLock = new Object();
    private volatile boolean closed;

    public RpcClient(String host, int port) {
        this(host, port, new Options());
    }

    public RpcClient(String host, int port, Options opts) {
        this.host = host;
        this.port = port;
        this.opts = opts;
        this.permits = new Semaphore(Math.max(1, opts.sessions) * Math.max(1, opts.maxChannels), true);
    }

    private Session session() {
        if (closed) throw new BeepException.Closed("client closed");
        synchronized (this) {
            sessions.removeIf(Session::isClosed);
            if (sessions.size() >= Math.max(1, opts.sessions)) return leastLoaded();
        }
        synchronized (connectLock) {
            synchronized (this) {
                sessions.removeIf(Session::isClosed);
                if (sessions.size() >= Math.max(1, opts.sessions)) return leastLoaded();
            }
            Session s = Session.connect(host, port, opts.config, null, opts.tls);
            synchronized (this) {
                sessions.add(s);
            }
            return s;
        }
    }

    private Session leastLoaded() {
        Session best = sessions.get(0);
        for (Session s : sessions) if (s.channelCount() < best.channelCount()) best = s;
        return best;
    }

    /** Open a session now (warm-up / health check). */
    public void connect() {
        session();
    }

    private Channel acquire() {
        synchronized (this) {
            while (!idle.isEmpty()) {
                Channel ch = idle.pop();
                if (ch.isOpen()) return ch;
            }
        }
        Session s = session();
        try {
            return s.startChannel(opts.profile);
        } catch (BeepException e) {
            if (!s.isClosed()) throw e;
            return session().startChannel(opts.profile);
        }
    }

    private void giveBack(Channel ch) {
        synchronized (this) {
            if (!closed && ch.isOpen() && ch.pending() == 0) idle.push(ch);
        }
    }

    void release(Channel ch, ReplyStream st) {
        if (st.isDone()) {
            giveBack(ch);
            permits.release();
            return;
        }
        st.discard();
        Session.EXEC.execute(() -> {
            try {
                while (st.next() != null) {
                    // drain
                }
            } catch (BeepException ignored) {
                // session gone
            }
            giveBack(ch);
            permits.release();
        });
    }

    /** Send a request; returns once the response head arrives. timeoutMs: -1 = client default, 0 = none. */
    public StreamingResponse stream(String method, String target, Headers headers, byte[] body, long timeoutMs) {
        long t = timeoutMs < 0 ? opts.timeoutMs : timeoutMs;
        Headers h = Rpc.appHeaders(headers);
        if (t > 0 && h.get("Rpc-Timeout") == null) h.add("Rpc-Timeout", Long.toString(t));
        Headers all = new Headers().add("Rpc-Method", method).add("Rpc-Target", target);
        for (java.util.Map.Entry<String, String> e : h) all.add(e.getKey(), e.getValue());
        byte[] payload = Wire.encodeEntity(all, body);
        long deadline = t > 0 ? System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(t) : Long.MAX_VALUE;
        try {
            if (t > 0) {
                if (!permits.tryAcquire(t, TimeUnit.MILLISECONDS)) throw new BeepException.Timeout(method + " " + target + " timed out waiting for a channel");
            } else {
                permits.acquire();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BeepException.Closed("interrupted");
        }
        Channel ch;
        try {
            ch = acquire();
        } catch (RuntimeException e) {
            permits.release();
            throw e;
        }
        ReplyStream st = ch.send(payload);
        Decoder dec = new Decoder(st);
        try {
            dec.readHead(deadline);
        } catch (BeepException.Timeout e) {
            release(ch, st);
            throw new BeepException.Timeout(method + " " + target + " timed out");
        } catch (RuntimeException e) {
            release(ch, st);
            throw e;
        }
        return new StreamingResponse(this, ch, dec);
    }

    public Rpc.Response call(String method, String target, Headers headers, byte[] body, long timeoutMs) {
        StreamingResponse r = stream(method, target, headers, body, timeoutMs);
        return new Rpc.Response(r.status, r.headers, r.readAll());
    }

    public Rpc.Response call(String method, String target, Headers headers, byte[] body) {
        return call(method, target, headers, body, -1);
    }

    public Rpc.Response call(String method, String target) {
        return call(method, target, null, null, -1);
    }

    public Rpc.Response get(String target) {
        return call("GET", target);
    }

    public Rpc.Response post(String target, String contentType, String body) {
        return call("POST", target, Headers.of("Content-Type", contentType), body.getBytes(StandardCharsets.UTF_8));
    }

    /** Verbatim payload on a fresh channel of any profile. */
    public Reply raw(String profile, byte[] payload) {
        Channel ch = session().startChannel(profile);
        try {
            return ch.request(payload);
        } finally {
            try {
                ch.close();
            } catch (BeepException ignored) {
                // best effort
            }
        }
    }

    /** Open channel counts of the live sessions. */
    public synchronized List<Integer> status() {
        List<Integer> out = new ArrayList<>();
        for (Session s : sessions) if (!s.isClosed()) out.add(s.channelCount());
        return out;
    }

    @Override
    public void close() {
        List<Session> ss;
        synchronized (this) {
            closed = true;
            idle.clear();
            ss = new ArrayList<>(sessions);
            sessions.clear();
        }
        for (Session s : ss) {
            try {
                s.close();
            } catch (RuntimeException e) {
                s.abort();
            }
        }
    }

    // ------------------------------------------------------------------ streaming response

    /** Response whose body is read incrementally; {@link #close()} returns the channel to the pool. */
    public static final class StreamingResponse implements AutoCloseable {
        public final int status;
        public final Headers headers;
        private final RpcClient client;
        private final Channel ch;
        private final Decoder dec;
        private boolean released;

        StreamingResponse(RpcClient client, Channel ch, Decoder dec) {
            this.client = client;
            this.ch = ch;
            this.dec = dec;
            this.status = dec.status;
            this.headers = dec.headers;
        }

        public String header(String name) { return headers.get(name); }

        /** Next body chunk or null at the end. */
        public byte[] next() {
            try {
                byte[] c = dec.nextBody();
                if (c == null) close();
                return c;
            } catch (RuntimeException e) {
                close();
                throw e;
            }
        }

        public byte[] readAll() {
            ByteArrayOutputStream bo = new ByteArrayOutputStream();
            byte[] c;
            while ((c = next()) != null) bo.write(c, 0, c.length);
            return bo.toByteArray();
        }

        @Override
        public synchronized void close() {
            if (!released) {
                released = true;
                client.release(ch, dec.stream);
            }
        }
    }

    /** Reply events → head + body chunks (docs/PROTOCOL.md §2.2–2.3). */
    static final class Decoder {
        final ReplyStream stream;
        int status;
        Headers headers;
        boolean haveHead;
        boolean finished;
        final Deque<byte[]> ready = new ArrayDeque<>();
        private ByteArrayOutputStream rpyBuf = new ByteArrayOutputStream();
        private final TreeMap<Integer, Object[]> ans = new TreeMap<>(); // ansno → {ByteArrayOutputStream head|null, Deque<byte[]> data, Boolean done}
        private int nextAns;

        Decoder(ReplyStream stream) {
            this.stream = stream;
        }

        void readHead(long deadline) {
            while (!haveHead) {
                if (finished) throw new BeepException("reply ended before a response head");
                pump(deadline);
            }
        }

        byte[] nextBody() {
            while (true) {
                if (!ready.isEmpty()) return ready.pop();
                if (finished) return null;
                pump(Long.MAX_VALUE);
            }
        }

        private void setHead(Headers h) {
            String st = h.get("Rpc-Status");
            try {
                status = st == null ? 200 : Integer.parseInt(st.trim());
            } catch (NumberFormatException e) {
                throw new BeepException("bad Rpc-Status " + st);
            }
            headers = Rpc.appHeaders(h);
            haveHead = true;
        }

        @SuppressWarnings("unchecked")
        private void pump(long deadline) {
            ReplyStream.Event ev = stream.next(deadline);
            if (ev == null) {
                finished = true;
                return;
            }
            switch (ev.type) {
                case "RPY":
                    if (rpyBuf != null) {
                        rpyBuf.write(ev.data, 0, ev.data.length);
                        byte[] b = rpyBuf.toByteArray();
                        Wire.Head h = Wire.splitHead(b, b.length, ev.last);
                        if (h != null) {
                            rpyBuf = null;
                            setHead(h.headers);
                            if (b.length > h.offset) ready.add(java.util.Arrays.copyOfRange(b, h.offset, b.length));
                        } else if (b.length > 65536) {
                            throw new BeepException("response head too large");
                        }
                    } else if (ev.data.length > 0) {
                        ready.add(ev.data);
                    }
                    if (ev.last) finished = true;
                    break;
                case "ERR": {
                    ByteArrayOutputStream bo = new ByteArrayOutputStream();
                    bo.write(ev.data, 0, ev.data.length);
                    boolean last = ev.last;
                    while (!last) {
                        ReplyStream.Event n = stream.next(deadline);
                        if (n == null || !n.type.equals("ERR")) throw new BeepException("truncated ERR");
                        bo.write(n.data, 0, n.data.length);
                        last = n.last;
                    }
                    finished = true;
                    Object[] ce = Wire.parseErrorPayload(bo.toByteArray());
                    throw new BeepException.Remote((Integer) ce[0], (String) ce[1]);
                }
                case "ANS": {
                    Object[] a = ans.computeIfAbsent(ev.ansno, k -> new Object[] {new ByteArrayOutputStream(), new ArrayDeque<byte[]>(), false});
                    Deque<byte[]> data = (Deque<byte[]>) a[1];
                    if (a[0] != null) {
                        ByteArrayOutputStream hb = (ByteArrayOutputStream) a[0];
                        hb.write(ev.data, 0, ev.data.length);
                        byte[] b = hb.toByteArray();
                        Wire.Head h = Wire.splitHead(b, b.length, ev.last);
                        if (h != null) {
                            a[0] = null;
                            String abort = h.headers.get("Rpc-Abort");
                            if (abort != null) throw new BeepException.Aborted(abort);
                            if (ev.ansno == 0) setHead(h.headers);
                            if (b.length > h.offset) data.add(java.util.Arrays.copyOfRange(b, h.offset, b.length));
                        }
                    } else if (ev.data.length > 0) {
                        data.add(ev.data);
                    }
                    if (ev.last) a[2] = true;
                    flush(false);
                    break;
                }
                case "NUL":
                    flush(true);
                    finished = true;
                    if (!haveHead) throw new BeepException("ANS stream without a head (ansno 0)");
                    break;
                default:
                    throw new BeepException.Protocol("unexpected reply event " + ev.type);
            }
        }

        @SuppressWarnings("unchecked")
        private void flush(boolean all) {
            while (!ans.isEmpty()) {
                int k = all ? ans.firstKey() : nextAns;
                Object[] a = ans.get(k);
                if (a == null || (k == 0 && !haveHead && !all)) return;
                Deque<byte[]> data = (Deque<byte[]>) a[1];
                ready.addAll(data);
                data.clear();
                if ((Boolean) a[2] || all) {
                    ans.remove(k);
                    nextAns = k + 1;
                } else {
                    return;
                }
            }
        }
    }
}
