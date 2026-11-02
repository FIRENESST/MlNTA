package io.minta.beep;

/** An open BEEP channel. */
public final class Channel {
    private final Session session;
    private final int number;
    private final String profile;

    Channel(Session session, int number, String profile) {
        this.session = session;
        this.number = number;
        this.profile = profile;
    }

    public Session session() { return session; }
    public int number() { return number; }
    public String profile() { return profile; }
    public boolean isOpen() { return session.hasChannel(number); }
    /** MSGs sent on this channel still awaiting a complete reply. */
    public int pending() { return session.pending(number); }

    /** Send a MSG; returns the stream of reply events. */
    public ReplyStream send(byte[] payload) {
        return session.sendMsg(number, payload);
    }

    /** Send a MSG and collect the complete reply. */
    public Reply request(byte[] payload) {
        return send(payload).collect(session.config.maxMessageSize, Long.MAX_VALUE);
    }

    public void close() {
        session.closeChannel(number, 200);
    }

    @Override
    public String toString() {
        return "Channel(" + number + ", " + profile + ")";
    }
}
