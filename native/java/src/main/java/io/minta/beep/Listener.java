package io.minta.beep;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/** Accepts BEEP sessions over TCP (optionally requiring the TLS tuning profile). */
public final class Listener implements AutoCloseable {
    private final Map<String, ProfileHandler> profiles;
    private final Config config;
    private final Tls.Server tls;
    private final Set<Session> sessions = ConcurrentHashMap.newKeySet();
    private ServerSocket server;
    private volatile boolean closed;
    private Consumer<Session> onSession = s -> {};

    public Listener(Map<String, ProfileHandler> profiles, Config config, Tls.Server tls) {
        this.profiles = profiles;
        this.config = config == null ? new Config() : config;
        this.tls = tls;
    }

    public Listener onSession(Consumer<Session> cb) {
        this.onSession = cb;
        return this;
    }

    /** Bind and start accepting in a background thread. Port 0 picks an ephemeral port. */
    public Listener start(String host, int port) throws IOException {
        server = new ServerSocket(port, 128, InetAddress.getByName(host));
        Thread t = new Thread(this::acceptLoop, "minta-beep-accept");
        t.setDaemon(true);
        t.start();
        return this;
    }

    public int port() {
        return server.getLocalPort();
    }

    private void acceptLoop() {
        while (!closed) {
            Socket s;
            try {
                s = server.accept();
            } catch (IOException e) {
                if (closed) return;
                try {
                    Thread.sleep(50);
                } catch (InterruptedException ie) {
                    return;
                }
                continue;
            }
            Session.EXEC.execute(() -> handle(s));
        }
    }

    private void handle(Socket s) {
        try {
            s.setTcpNoDelay(true);
            Socket sock = tls != null ? Tls.serverBootstrap(s, tls) : s;
            Session session = Session.establish(sock, false, config, profiles);
            sessions.add(session);
            onSession.accept(session);
            session.awaitClosed();
            sessions.remove(session);
        } catch (Exception e) {
            Session.LOG.fine("beep handshake failed: " + e);
            Session.closeQuietly(s);
        }
    }

    @Override
    public void close() {
        closed = true;
        try {
            if (server != null) server.close();
        } catch (IOException ignored) {
            // closing
        }
        for (Session s : sessions) s.abort();
    }
}
