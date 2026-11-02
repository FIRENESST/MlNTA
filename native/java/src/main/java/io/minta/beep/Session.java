package io.minta.beep;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/** A BEEP session (RFC 3080/3081) over a socket. Thread-safe. */
public final class Session implements AutoCloseable {
    static final Logger LOG = Logger.getLogger("io.minta.beep");
    static final long MASK = 0xffffffffL;
    static final int INITIAL_WINDOW = 4096;
    static final int MAX_CHANNEL = 2147483647;

    /** Shared daemon pool for channel handlers and background transmissions. */
    static final ExecutorService EXEC = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "minta-beep");
        t.setDaemon(true);
        return t;
    });

    static final Object POISON = new Object();
    static final byte[] CLOSE = new byte[0];

    // ------------------------------------------------------------------ per-message sink

    static final class Sink {
        final Session session;
        final int channel;
        final LinkedBlockingQueue<Object[]> queue = new LinkedBlockingQueue<>();
        final Set<Integer> ansOpen = new HashSet<>();
        volatile boolean discarding;

        Sink(Session s, int channel) {
            this.session = s;
            this.channel = channel;
        }

        void deliver(Object item, int n) {
            if (discarding) {
                session.credit(channel, n);
                return;
            }
            queue.add(new Object[] {item, n});
        }

        void discard() {
            discarding = true;
            Object[] it;
            while ((it = queue.poll()) != null) session.credit(channel, (Integer) it[1]);
        }
    }

    static final class Chan {
        final int number;
        String profile;
        long txSeq = 0;
        long txLimit = INITIAL_WINDOW;
        boolean dead;
        final ReentrantLock txLock = new ReentrantLock(true);
        long recvSeq = 0, advAck = 0, advWindow = INITIAL_WINDOW, consumed = 0;
        int nextMsgno;
        final Map<Integer, Sink> outstanding = new HashMap<>();
        String rxReplyType;
        int rxReplyMsgno;
        IncomingMessage rxMsg;
        final Set<Integer> inbound = new HashSet<>();
        LinkedBlockingQueue<Object> queue;

        Chan(int number, String profile, int nextMsgno) {
            this.number = number;
            this.profile = profile;
            this.nextMsgno = nextMsgno;
        }

        boolean busy() {
            return !outstanding.isEmpty() || !inbound.isEmpty() || rxMsg != null;
        }

        void kill() {
            synchronized (this) {
                dead = true;
                notifyAll();
            }
        }
    }

    // ------------------------------------------------------------------ state

    final Config config;
    final Map<String, ProfileHandler> profiles;
    final boolean initiator;
    private final Socket socket;
    private final InputStream in;
    private final OutputStream out;
    private final LinkedBlockingQueue<byte[]> outq = new LinkedBlockingQueue<>();
    private final Map<Integer, Chan> chans = new HashMap<>();
    private int nextChan;
    private volatile BeepException closed;
    private volatile boolean releasing;
    private final Object closedLock = new Object();
    private volatile List<String> peerProfiles = Collections.emptyList();

    private Session(Socket socket, InputStream in, OutputStream out, boolean initiator, Config config, Map<String, ProfileHandler> profiles) {
        this.socket = socket;
        this.in = in;
        this.out = out;
        this.initiator = initiator;
        this.config = config == null ? new Config() : config;
        this.profiles = profiles == null ? new HashMap<>() : new HashMap<>(profiles);
        this.nextChan = initiator ? 1 : 2;
        chans.put(0, new Chan(0, "", 1));
    }

    public static Session connect(String host, int port) {
        return connect(host, port, null, null, null);
    }

    public static Session connect(String host, int port, Config config, Map<String, ProfileHandler> profiles, Tls.Client tls) {
        Config cfg = config == null ? new Config() : config;
        Socket s = new Socket();
        try {
            s.setTcpNoDelay(true);
            s.connect(new InetSocketAddress(host, port), cfg.connectTimeoutMs);
            if (tls != null) s = Tls.clientBootstrap(s, host, port, tls);
        } catch (IOException e) {
            closeQuietly(s);
            throw new BeepException.Closed("connect " + host + ":" + port + ": " + e.getMessage(), e);
        }
        return establish(s, true, cfg, profiles);
    }

    /** Run BEEP on an already-connected (optionally TLS) socket; returns after greetings. */
    public static Session establish(Socket s, boolean initiator, Config config, Map<String, ProfileHandler> profiles) {
        Session session;
        try {
            session = new Session(s, new BufferedInputStream(s.getInputStream(), 65536), s.getOutputStream(), initiator, config, profiles);
        } catch (IOException e) {
            closeQuietly(s);
            throw new BeepException.Closed("socket: " + e.getMessage(), e);
        }
        session.start();
        return session;
    }

    private void start() {
        Sink greet = new Sink(this, 0);
        synchronized (this) {
            Chan c0 = chans.get(0);
            c0.outstanding.put(0, greet);
            c0.queue = new LinkedBlockingQueue<>();
        }
        Thread w = new Thread(this::writerLoop, "minta-beep-writer");
        w.setDaemon(true);
        w.start();
        Thread r = new Thread(this::readerLoop, "minta-beep-reader");
        r.setDaemon(true);
        r.start();
        Wire.Mgmt g = Wire.Mgmt.of("greeting");
        for (String uri : new TreeSet<>(profiles.keySet())) g.profiles.add(new Wire.ProfileEl(uri, null));
        sendMessage(0, "RPY", 0, 0, Wire.xmlEntity(g.xml()));
        openWindow(0);
        Reply reply;
        try {
            reply = new ReplyStream(this, greet).collect(1 << 20, System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.greetingTimeoutMs));
        } catch (BeepException e) {
            fail(e);
            throw e;
        }
        Wire.Mgmt m;
        try {
            m = Wire.Mgmt.parse(Wire.parseEntity(reply.payload()).getValue());
        } catch (BeepException e) {
            fail(e);
            throw e;
        }
        if (reply.kind == Reply.Kind.ERR || m.kind.equals("error")) {
            BeepException e = m.kind.equals("error") ? new BeepException.Refused(m.code, m.message) : new BeepException.Refused(421, "peer refused session");
            fail(e);
            throw e;
        }
        if (!m.kind.equals("greeting")) {
            BeepException e = new BeepException.Protocol("expected greeting, got " + m.kind);
            fail(e);
            throw e;
        }
        List<String> peer = new ArrayList<>();
        for (Wire.ProfileEl p : m.profiles) peer.add(p.uri);
        peerProfiles = Collections.unmodifiableList(peer);
        EXEC.execute(this::mgmtLoop);
    }

    // ------------------------------------------------------------------ public API

    public List<String> peerProfiles() {
        return peerProfiles;
    }

    public boolean isClosed() {
        return closed != null;
    }

    public boolean isInitiator() {
        return initiator;
    }

    public Config config() {
        return config;
    }

    public synchronized int channelCount() {
        return chans.size() - 1;
    }

    /** Block until the session ends; returns the reason. */
    public BeepException awaitClosed() throws InterruptedException {
        synchronized (closedLock) {
            while (closed == null) closedLock.wait();
        }
        return closed;
    }

    public Channel startChannel(String uri) {
        return startChannel(uri, null, null).getKey();
    }

    /** Start a channel, optionally with piggybacked init data; returns (channel, peer's init reply). */
    public Map.Entry<Channel, String> startChannel(String uri, String initData, String serverName) {
        checkOpen();
        int n;
        synchronized (this) {
            n = nextChan;
            int tries = 0;
            while (chans.containsKey(n)) {
                n = nextNumber(n);
                if (++tries > 1_000_000) throw new BeepException.Closed("no free channel numbers");
            }
            nextChan = nextNumber(n);
            chans.put(n, new Chan(n, "", 0));
        }
        Wire.Mgmt req = Wire.Mgmt.of("start");
        req.number = n;
        req.serverName = serverName;
        req.profiles.add(new Wire.ProfileEl(uri, initData));
        Wire.Mgmt m;
        try {
            m = mgmtRequest(req);
        } catch (RuntimeException e) {
            synchronized (this) {
                chans.remove(n);
            }
            throw e;
        }
        if (!m.kind.equals("profile")) {
            synchronized (this) {
                chans.remove(n);
            }
            if (m.kind.equals("error")) throw new BeepException.Refused(m.code, m.message);
            throw new BeepException.Protocol("unexpected reply to <start>: " + m.kind);
        }
        Wire.ProfileEl p = m.profiles.get(0);
        LinkedBlockingQueue<Object> q = new LinkedBlockingQueue<>();
        synchronized (this) {
            Chan c = chans.get(n);
            if (c == null) throw closedError();
            c.profile = p.uri;
            c.queue = q;
        }
        ProfileHandler h = profiles.get(p.uri);
        final int num = n;
        EXEC.execute(() -> channelLoop(num, h, q));
        openWindow(n);
        return new java.util.AbstractMap.SimpleImmutableEntry<>(new Channel(this, n, p.uri), p.data);
    }

    /** Gracefully release the session. */
    @Override
    public void close() {
        if (closed != null) return;
        Wire.Mgmt req = Wire.Mgmt.of("close");
        req.number = 0;
        Wire.Mgmt m;
        try {
            m = mgmtRequest(req, System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.closeTimeoutMs));
        } catch (RuntimeException e) {
            abort();
            throw e;
        }
        if (m.kind.equals("ok")) fail(new BeepException.Closed("session released"));
        else if (m.kind.equals("error")) throw new BeepException.Refused(m.code, m.message);
    }

    public void abort() {
        fail(new BeepException.Closed("session aborted"));
    }

    // ------------------------------------------------------------------ internals

    private static int nextNumber(int n) {
        return n > MAX_CHANNEL - 2 ? 2 - n % 2 : n + 2;
    }

    static void closeQuietly(Socket s) {
        try {
            s.close();
        } catch (IOException ignored) {
            // nothing to do
        }
    }

    void checkOpen() {
        if (closed != null) throw closedError();
    }

    BeepException closedError() {
        BeepException e = closed;
        return e != null ? e : new BeepException.Closed("channel closed");
    }

    void fail(BeepException err) {
        List<Chan> old;
        synchronized (this) {
            if (closed != null) return;
            closed = err;
            old = new ArrayList<>(chans.values());
            chans.clear();
        }
        LOG.fine("beep session ended: " + err.getMessage());
        for (Chan c : old) {
            c.kill();
            for (Sink s : c.outstanding.values()) s.queue.add(new Object[] {"CLOSED", 0});
            if (c.rxMsg != null) c.rxMsg.sink.queue.add(new Object[] {"CLOSED", 0});
            if (c.queue != null) c.queue.add(POISON);
        }
        outq.add(CLOSE);
        synchronized (closedLock) {
            closedLock.notifyAll();
        }
    }

    synchronized void credit(int channel, int n) {
        if (n <= 0) return;
        Chan c = chans.get(channel);
        if (c == null) return;
        c.consumed = (c.consumed + n) & MASK;
        long w = config.recvWindow;
        if (((c.consumed - c.advAck) & MASK) >= w / 2) {
            c.advAck = c.consumed;
            c.advWindow = w;
            outq.add(Wire.encodeSeq(channel, c.consumed, w));
        }
    }

    synchronized void openWindow(int channel) {
        Chan c = chans.get(channel);
        if (c == null) return;
        c.advAck = c.consumed;
        c.advWindow = config.recvWindow;
        outq.add(Wire.encodeSeq(channel, c.consumed, c.advWindow));
    }

    synchronized void finishInbound(int channel, int msgno) {
        Chan c = chans.get(channel);
        if (c != null) c.inbound.remove(msgno);
    }

    synchronized int pending(int channel) {
        Chan c = chans.get(channel);
        return c == null ? 0 : c.outstanding.size();
    }

    synchronized boolean hasChannel(int channel) {
        return closed == null && chans.containsKey(channel);
    }

    private void writerLoop() {
        try (OutputStream o = new BufferedOutputStream(out, 65536)) {
            while (true) {
                byte[] b = outq.take();
                if (b == CLOSE) break;
                o.write(b);
                boolean stop = false;
                while ((b = outq.poll()) != null) {
                    if (b == CLOSE) {
                        stop = true;
                        break;
                    }
                    o.write(b);
                }
                o.flush();
                if (stop) break;
            }
            o.flush();
            try {
                socket.shutdownOutput();
            } catch (IOException | UnsupportedOperationException ignored) {
                // TLS sockets do not support half-close
            }
        } catch (IOException e) {
            fail(new BeepException.Closed("write failed: " + e.getMessage(), e));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            if (closed != null) {
                EXEC.execute(() -> {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ignored) {
                        // closing anyway
                    }
                    closeQuietly(socket);
                });
            }
        }
    }

    private void readerLoop() {
        int limit = Math.max(config.recvWindow, INITIAL_WINDOW);
        try {
            while (closed == null) {
                Wire.Frame f = Wire.readFrame(in, limit);
                if (f == null) {
                    fail(new BeepException.Closed(releasing ? "session released" : "connection closed by peer"));
                    return;
                }
                if (f.type.equals("SEQ")) {
                    Chan c;
                    synchronized (this) {
                        c = chans.get(f.channel);
                    }
                    if (c != null) {
                        synchronized (c) {
                            c.txLimit = (f.ackno + f.window) & MASK;
                            c.notifyAll();
                        }
                    }
                    continue;
                }
                onData(f);
            }
        } catch (BeepException.Protocol e) {
            LOG.warning("beep: " + e.getMessage() + "; dropping connection");
            fail(e);
            closeQuietly(socket);
        } catch (IOException e) {
            fail(new BeepException.Closed("read failed: " + e.getMessage(), e));
        }
    }

    private synchronized void onData(Wire.Frame f) {
        Chan c = chans.get(f.channel);
        if (c == null) throw new BeepException.Protocol("frame on unknown channel " + f.channel);
        if (f.seqno != c.recvSeq) throw new BeepException.Protocol("channel " + f.channel + ": expected seqno " + c.recvSeq + ", got " + f.seqno);
        int size = f.payload.length;
        long end = (c.recvSeq + size) & MASK;
        if (((end - c.advAck) & MASK) > c.advWindow) throw new BeepException.Protocol("channel " + f.channel + ": frame exceeds receive window");
        c.recvSeq = end;
        boolean last = !f.more;
        switch (f.type) {
            case "MSG": {
                IncomingMessage m = c.rxMsg;
                if (m != null && m.msgno != f.msgno) throw new BeepException.Protocol("channel " + f.channel + ": interleaved MSG frames");
                if (m == null) {
                    if (!c.inbound.add(f.msgno)) throw new BeepException.Protocol("channel " + f.channel + ": msgno " + f.msgno + " reused while outstanding");
                    m = new IncomingMessage(this, f.channel, f.msgno);
                    c.rxMsg = m;
                    if (c.queue != null) c.queue.add(m);
                }
                m.sink.deliver(f.payload, size);
                if (last) {
                    c.rxMsg = null;
                    m.sink.deliver("END", 0);
                }
                break;
            }
            case "RPY":
            case "ERR": {
                if (c.rxReplyType != null && (!c.rxReplyType.equals(f.type) || c.rxReplyMsgno != f.msgno)) {
                    throw new BeepException.Protocol("channel " + f.channel + ": interleaved reply frames");
                }
                Sink s = c.outstanding.get(f.msgno);
                if (s == null) throw new BeepException.Protocol("channel " + f.channel + ": reply to unknown msgno " + f.msgno);
                if (!s.ansOpen.isEmpty()) throw new BeepException.Protocol("RPY/ERR while ANS in progress");
                s.deliver(new ReplyStream.Event(f.type, 0, f.payload, last), size);
                if (last) {
                    c.rxReplyType = null;
                    c.outstanding.remove(f.msgno);
                } else {
                    c.rxReplyType = f.type;
                    c.rxReplyMsgno = f.msgno;
                }
                break;
            }
            case "ANS": {
                if (c.rxReplyType != null) throw new BeepException.Protocol("ANS while RPY/ERR in progress");
                Sink s = c.outstanding.get(f.msgno);
                if (s == null) throw new BeepException.Protocol("channel " + f.channel + ": ANS to unknown msgno " + f.msgno);
                if (last) s.ansOpen.remove(f.ansno);
                else s.ansOpen.add(f.ansno);
                s.deliver(new ReplyStream.Event("ANS", f.ansno, f.payload, last), size);
                break;
            }
            case "NUL": {
                Sink s = c.outstanding.get(f.msgno);
                if (s == null) throw new BeepException.Protocol("channel " + f.channel + ": NUL to unknown msgno " + f.msgno);
                if (!s.ansOpen.isEmpty()) throw new BeepException.Protocol("NUL before all ANS messages completed");
                c.outstanding.remove(f.msgno);
                s.deliver(new ReplyStream.Event("NUL", 0, new byte[0], true), 0);
                break;
            }
            default:
                throw new BeepException.Protocol("unexpected frame type " + f.type);
        }
    }

    void sendMessage(int channel, String type, int msgno, int ansno, byte[] payload) {
        checkOpen();
        Chan c;
        synchronized (this) {
            c = chans.get(channel);
        }
        if (c == null) throw new BeepException.Closed("channel " + channel + " is not open");
        int maxFrame = Math.max(1, config.maxFrameSize);
        c.txLock.lock();
        try {
            int off = 0;
            synchronized (c) {
                while (true) {
                    if (c.dead || closed != null) throw closedError();
                    long avail = (c.txLimit - c.txSeq) & MASK;
                    if (avail >= 0x80000000L) avail = 0;
                    int remaining = payload.length - off;
                    if (remaining == 0 || avail > 0) {
                        int n = (int) Math.min(Math.min(remaining, avail), maxFrame);
                        boolean more = off + n < payload.length;
                        outq.add(Wire.encodeFrame(type, channel, msgno, more, c.txSeq, payload, off, n, ansno));
                        c.txSeq = (c.txSeq + n) & MASK;
                        off += n;
                        if (!more) return;
                        continue;
                    }
                    try {
                        c.wait();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new BeepException.Closed("interrupted while sending");
                    }
                }
            }
        } finally {
            c.txLock.unlock();
        }
    }

    ReplyStream sendMsg(int channel, byte[] payload) {
        checkOpen();
        Sink sink = new Sink(this, channel);
        int n;
        synchronized (this) {
            Chan c = chans.get(channel);
            if (c == null) throw new BeepException.Closed("channel " + channel + " is not open");
            n = c.nextMsgno;
            while (c.outstanding.containsKey(n)) n = n >= MAX_CHANNEL ? 0 : n + 1;
            c.nextMsgno = n >= MAX_CHANNEL ? 0 : n + 1;
            c.outstanding.put(n, sink);
        }
        final int msgno = n;
        // Transmit in the background so callers can pipeline big MSGs while reading replies.
        EXEC.execute(() -> {
            try {
                sendMessage(channel, "MSG", msgno, 0, payload);
            } catch (BeepException e) {
                boolean removed;
                synchronized (this) {
                    Chan c = chans.get(channel);
                    removed = c != null && c.outstanding.remove(msgno) != null;
                }
                if (removed) sink.queue.add(new Object[] {"CLOSED", 0});
            }
        });
        return new ReplyStream(this, sink);
    }

    Wire.Mgmt mgmtRequest(Wire.Mgmt m) {
        return mgmtRequest(m, Long.MAX_VALUE);
    }

    Wire.Mgmt mgmtRequest(Wire.Mgmt m, long deadline) {
        Reply r = sendMsg(0, Wire.xmlEntity(m.xml())).collect(1 << 20, deadline);
        if (r.kind == Reply.Kind.ANS) throw new BeepException.Protocol("ANS on channel 0");
        return Wire.Mgmt.parse(Wire.parseEntity(r.payload()).getValue());
    }

    void closeChannel(int number, int code) {
        synchronized (this) {
            Chan c = chans.get(number);
            if (c != null && c.busy()) throw new BeepException.Refused(550, "still working");
        }
        Wire.Mgmt req = Wire.Mgmt.of("close");
        req.number = number;
        req.code = code;
        Wire.Mgmt m = mgmtRequest(req);
        if (m.kind.equals("ok")) {
            Chan c;
            synchronized (this) {
                c = chans.remove(number);
            }
            if (c != null) {
                c.kill();
                if (c.queue != null) c.queue.add(POISON);
            }
        } else if (m.kind.equals("error")) {
            throw new BeepException.Refused(m.code, m.message);
        } else {
            throw new BeepException.Protocol("unexpected reply to <close>: " + m.kind);
        }
    }

    private void mgmtLoop() {
        LinkedBlockingQueue<Object> q;
        synchronized (this) {
            Chan c0 = chans.get(0);
            if (c0 == null) return;
            q = c0.queue;
        }
        try {
            while (true) {
                Object o = q.take();
                if (o == POISON) return;
                IncomingMessage msg = (IncomingMessage) o;
                Responder r = new Responder(this, 0, msg.msgno);
                byte[] body;
                try {
                    body = msg.collect(1 << 20);
                } catch (BeepException e) {
                    return;
                }
                Wire.Mgmt reply;
                Integer opened = null;
                try {
                    Wire.Mgmt m = Wire.Mgmt.parse(Wire.parseEntity(body).getValue());
                    if (m.kind.equals("start")) {
                        reply = handleStart(m);
                        opened = m.number;
                    } else if (m.kind.equals("close")) {
                        reply = handleClose(m.number);
                    } else {
                        reply = Wire.Mgmt.error(501, "unexpected element <" + m.kind + ">");
                    }
                } catch (BeepException.Refused e) {
                    reply = Wire.Mgmt.error(e.code, e.beepMessage);
                } catch (BeepException.Protocol e) {
                    reply = Wire.Mgmt.error(500, e.getMessage());
                }
                if (reply.kind.equals("error")) opened = null;
                try {
                    if (reply.kind.equals("error")) r.error(reply.code, reply.message);
                    else r.rpy(Wire.xmlEntity(reply.xml()));
                } catch (BeepException e) {
                    return;
                }
                if (opened != null) openWindow(opened); // SEQ must follow the RPY that creates the channel
                if (reply.kind.equals("ok") && releasing) {
                    outq.add(CLOSE);
                    return;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private Wire.Mgmt handleStart(Wire.Mgmt m) {
        if (releasing) throw new BeepException.Refused(550, "session is being released");
        int peerParity = initiator ? 0 : 1;
        int n = m.number;
        synchronized (this) {
            if (n == 0 || n % 2 != peerParity || chans.containsKey(n)) throw new BeepException.Refused(553, "channel number " + n + " is invalid or in use");
        }
        Wire.ProfileEl chosen = null;
        ProfileHandler handler = null;
        for (Wire.ProfileEl p : m.profiles) {
            handler = profiles.get(p.uri);
            if (handler != null) {
                chosen = p;
                break;
            }
        }
        if (chosen == null) throw new BeepException.Refused(550, "no acceptable profile");
        String init = handler.onStart(this, n, chosen.data);
        LinkedBlockingQueue<Object> q = new LinkedBlockingQueue<>();
        synchronized (this) {
            if (chans.containsKey(n)) throw new BeepException.Refused(553, "channel number " + n + " is in use");
            Chan c = new Chan(n, chosen.uri, 0);
            c.queue = q;
            chans.put(n, c);
        }
        final ProfileHandler h = handler;
        EXEC.execute(() -> channelLoop(n, h, q));
        Wire.Mgmt reply = Wire.Mgmt.of("profile");
        reply.profiles.add(new Wire.ProfileEl(chosen.uri, init));
        return reply;
    }

    private Wire.Mgmt handleClose(int n) {
        Chan c;
        synchronized (this) {
            if (n == 0) {
                for (Map.Entry<Integer, Chan> e : chans.entrySet()) {
                    if (e.getKey() != 0 && e.getValue().busy()) throw new BeepException.Refused(550, "still working");
                }
                releasing = true;
                return Wire.Mgmt.of("ok");
            }
            c = chans.get(n);
            if (c == null) throw new BeepException.Refused(553, "channel " + n + " is not open");
            if (c.busy()) throw new BeepException.Refused(550, "still working");
            chans.remove(n);
        }
        c.kill();
        if (c.queue != null) c.queue.add(POISON);
        return Wire.Mgmt.of("ok");
    }

    private void channelLoop(int number, ProfileHandler handler, LinkedBlockingQueue<Object> q) {
        try {
            while (true) {
                Object o = q.take();
                if (o == POISON) return;
                IncomingMessage msg = (IncomingMessage) o;
                Responder r = new Responder(this, number, msg.msgno);
                try {
                    if (handler == null) {
                        msg.discard();
                        r.error(550, "this peer does not serve the profile");
                        continue;
                    }
                    try {
                        handler.onMessage(this, msg, r);
                    } catch (BeepException e) {
                        if (closed != null) return;
                        throw e;
                    } catch (Exception e) {
                        LOG.log(Level.WARNING, "beep handler failed", e);
                        msg.discard();
                        if (r.state == Responder.IDLE) r.error(554, "handler failed: " + e);
                        else if (r.state == Responder.ANSWERING) r.nul();
                        continue;
                    }
                    msg.discard();
                    if (r.state == Responder.IDLE) r.error(554, "handler produced no reply");
                    else if (r.state == Responder.ANSWERING) r.nul();
                } catch (BeepException e) {
                    if (closed != null) return;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
