package io.minta.beep;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/** Types of the MINTA RPC profile (urn:minta:beep:rpc:1). See docs/PROTOCOL.md §2. */
public final class Rpc {
    private Rpc() {}

    public static final String PROFILE = "urn:minta:beep:rpc:1";

    static boolean reserved(String name) {
        String n = name.toLowerCase();
        return n.equals("rpc-method") || n.equals("rpc-target") || n.equals("rpc-status") || n.equals("rpc-reason")
                || n.equals("rpc-abort") || n.equals("content-transfer-encoding");
    }

    static Headers appHeaders(Headers h) {
        Headers out = new Headers();
        if (h != null) for (Map.Entry<String, String> e : h) if (!reserved(e.getKey())) out.add(e.getKey(), e.getValue());
        return out;
    }

    public static int beepCodeToHttp(int code) {
        switch (code) {
            case 421: case 450: return 503;
            case 451: return 504;
            case 500: case 501: case 553: return 400;
            case 504: return 501;
            case 530: case 534: case 535: return 401;
            case 537: case 538: return 403;
            default: return 502;
        }
    }

    /** Client-side complete response. */
    public static final class Response {
        public final int status;
        public final Headers headers;
        public final byte[] body;

        Response(int status, Headers headers, byte[] body) {
            this.status = status;
            this.headers = headers;
            this.body = body;
        }

        public String header(String name) { return headers.get(name); }
        public String text() { return new String(body, StandardCharsets.UTF_8); }
    }

    /** Server-side incoming request. */
    public static final class Request {
        public final String method;
        public final String target;
        public final Headers headers;
        public final byte[] body;
        public final int channel;

        Request(String method, String target, Headers headers, byte[] body, int channel) {
            this.method = method;
            this.target = target;
            this.headers = headers;
            this.body = body;
            this.channel = channel;
        }

        public String header(String name) { return headers.get(name); }
        public String path() { int q = target.indexOf('?'); return q < 0 ? target : target.substring(0, q); }
        public String text() { return new String(body, StandardCharsets.UTF_8); }
        public String id() { return headers.get("Rpc-Id"); }

        /** Rpc-Timeout in milliseconds, or -1. */
        public long timeoutMs() {
            String t = headers.get("Rpc-Timeout");
            try {
                return t == null ? -1 : Long.parseLong(t.trim());
            } catch (NumberFormatException e) {
                return -1;
            }
        }
    }

    /** Server-side result: a full body (one RPY) or a chunk iterator (ANS…NUL). */
    public static final class Reply {
        public final int status;
        public final Headers headers;
        public final byte[] body;
        public final Iterator<byte[]> stream;

        private Reply(int status, Headers headers, byte[] body, Iterator<byte[]> stream) {
            this.status = status;
            this.headers = headers == null ? new Headers() : headers;
            this.body = body;
            this.stream = stream;
        }

        public static Reply of(int status, Headers headers, byte[] body) {
            return new Reply(status, headers, body == null ? new byte[0] : body, null);
        }

        public static Reply of(int status, Headers headers, String body) {
            return of(status, headers, body.getBytes(StandardCharsets.UTF_8));
        }

        public static Reply stream(int status, Headers headers, Iterator<byte[]> chunks) {
            return new Reply(status, headers, null, chunks == null ? Collections.emptyIterator() : chunks);
        }
    }

    /** Throw from a handler to answer with BEEP ERR. */
    public static final class Failure extends Exception {
        public final int code;

        public Failure(int code, String message) {
            super(message);
            this.code = code;
        }
    }

    @FunctionalInterface
    public interface Handler {
        Reply handle(Request req) throws Exception;
    }
}
