package io.minta.beep;

/** Base class of all BEEP failures. Subclasses are nested for a compact API. */
public class BeepException extends RuntimeException {
    public BeepException(String message) {
        super(message);
    }

    public BeepException(String message, Throwable cause) {
        super(message, cause);
    }

    /** The peer violated RFC 3080/3081; the session is dropped. */
    public static class Protocol extends BeepException {
        public Protocol(String m) { super(m); }
    }

    /** The session is gone. */
    public static class Closed extends BeepException {
        public Closed(String m) { super(m); }
        public Closed(String m, Throwable cause) { super(m, cause); }
    }

    public static class Timeout extends BeepException {
        public Timeout(String m) { super(m); }
    }

    /** The peer refused a channel-management request (start/close). */
    public static class Refused extends BeepException {
        public final int code;
        public final String beepMessage;
        public Refused(int code, String message) {
            super("refused by peer (" + code + "): " + message);
            this.code = code;
            this.beepMessage = message;
        }
    }

    /** The peer answered with BEEP ERR: no application-level response. */
    public static class Remote extends BeepException {
        public final int code;
        public final String beepMessage;
        public Remote(int code, String message) {
            super("remote error " + code + ": " + message);
            this.code = code;
            this.beepMessage = message;
        }
        public int httpStatus() { return Rpc.beepCodeToHttp(code); }
    }

    /** A streamed response was aborted by the responder (Rpc-Abort). */
    public static class Aborted extends BeepException {
        public Aborted(String m) { super(m); }
    }
}
