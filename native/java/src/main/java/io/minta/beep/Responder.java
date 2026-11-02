package io.minta.beep;

/** Sends the reply to one incoming MSG: RPY, ERR, or ANS…NUL. */
public final class Responder {
    static final int IDLE = 0, ANSWERING = 1, DONE = 2;

    private final Session session;
    public final int channel;
    public final int msgno;
    volatile int state = IDLE;
    private int nextAns;

    Responder(Session session, int channel, int msgno) {
        this.session = session;
        this.channel = channel;
        this.msgno = msgno;
    }

    private synchronized void finalState(String kind) {
        if (state != IDLE) throw new BeepException(kind + " not allowed after ANS or completion");
        state = DONE;
    }

    public void rpy(byte[] payload) {
        finalState("RPY");
        try {
            session.sendMessage(channel, "RPY", msgno, 0, payload);
        } finally {
            session.finishInbound(channel, msgno);
        }
    }

    public void err(byte[] payload) {
        finalState("ERR");
        try {
            session.sendMessage(channel, "ERR", msgno, 0, payload);
        } finally {
            session.finishInbound(channel, msgno);
        }
    }

    /** ERR carrying a standard error element with this code. */
    public void error(int code, String message) {
        err(Wire.errorPayload(code, message));
    }

    public int ans(byte[] payload) {
        int n;
        synchronized (this) {
            if (state == DONE) throw new BeepException("reply already completed");
            state = ANSWERING;
            n = nextAns++;
        }
        session.sendMessage(channel, "ANS", msgno, n, payload);
        return n;
    }

    public void nul() {
        synchronized (this) {
            if (state == DONE) throw new BeepException("reply already completed");
            state = DONE;
        }
        try {
            session.sendMessage(channel, "NUL", msgno, 0, new byte[0]);
        } finally {
            session.finishInbound(channel, msgno);
        }
    }
}
