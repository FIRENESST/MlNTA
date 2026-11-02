package io.minta.beep;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;

/**
 * BEEP listener serving the RPC profile.
 *
 * <pre>
 * RpcServer s = new RpcServer(req -&gt; Rpc.Reply.of(200, Headers.of("Content-Type", "text/plain"), "hi"))
 *         .start("0.0.0.0", 10288);
 * </pre>
 */
public final class RpcServer implements AutoCloseable {
    private final Listener listener;

    public RpcServer(Rpc.Handler handler) {
        this(handler, null, null);
    }

    public RpcServer(Rpc.Handler handler, Config config, Tls.Server tls) {
        Map<String, ProfileHandler> profiles = new HashMap<>();
        profiles.put(Rpc.PROFILE, profile(handler));
        listener = new Listener(profiles, config, tls);
    }

    public RpcServer start(String host, int port) throws IOException {
        listener.start(host, port);
        return this;
    }

    public int port() {
        return listener.port();
    }

    @Override
    public void close() {
        listener.close();
    }

    /** A {@link ProfileHandler} that decodes RPC envelopes and dispatches to {@code handler}. */
    public static ProfileHandler profile(Rpc.Handler handler) {
        return (session, msg, reply) -> {
            byte[] payload;
            try {
                payload = msg.collect(session.config.maxMessageSize);
            } catch (BeepException e) {
                if (!session.isClosed()) reply.error(554, e.getMessage());
                return;
            }
            Wire.Head h = Wire.splitHead(payload, payload.length, true);
            String method = h.headers.get("Rpc-Method");
            String target = h.headers.get("Rpc-Target");
            if (method == null || method.isEmpty() || target == null) {
                reply.error(501, "request lacks Rpc-Method or Rpc-Target");
                return;
            }
            byte[] body = java.util.Arrays.copyOfRange(payload, h.offset, payload.length);
            Rpc.Request req = new Rpc.Request(method, target, Rpc.appHeaders(h.headers), body, msg.channel);
            Rpc.Reply r;
            try {
                long t = req.timeoutMs();
                if (t > 0) {
                    Future<Rpc.Reply> f = Session.EXEC.submit(() -> handler.handle(req));
                    try {
                        r = f.get(t, TimeUnit.MILLISECONDS);
                    } catch (TimeoutException e) {
                        f.cancel(true);
                        reply.error(451, "deadline exceeded");
                        return;
                    } catch (ExecutionException e) {
                        throw e.getCause() instanceof Exception ? (Exception) e.getCause() : e;
                    }
                } else {
                    r = handler.handle(req);
                }
            } catch (Rpc.Failure f) {
                reply.error(f.code, f.getMessage());
                return;
            } catch (Exception e) {
                Session.LOG.log(Level.WARNING, "rpc handler failed", e);
                reply.error(554, "handler failed: " + e);
                return;
            }
            if (r == null) r = Rpc.Reply.of(204, null, new byte[0]);
            Headers rh = new Headers().add("Rpc-Status", Integer.toString(r.status));
            for (Map.Entry<String, String> e : Rpc.appHeaders(r.headers)) rh.add(e.getKey(), e.getValue());
            if (req.id() != null && rh.get("Rpc-Id") == null) rh.add("Rpc-Id", req.id());
            if (r.stream == null) {
                reply.rpy(Wire.encodeEntity(rh, r.body));
                return;
            }
            reply.ans(Wire.encodeEntity(rh, null));
            try {
                while (r.stream.hasNext()) {
                    byte[] c = r.stream.next();
                    if (c != null && c.length > 0) reply.ans(Wire.encodeEntity(null, c));
                }
            } catch (BeepException e) {
                if (session.isClosed()) return;
                throw e;
            } catch (RuntimeException e) {
                reply.ans(Wire.encodeEntity(Headers.of("Rpc-Abort", String.valueOf(e.getMessage())), null));
            }
            reply.nul();
        };
    }
}
