package io.minta.beep;

import java.io.ByteArrayOutputStream;

/** A MSG received from the peer. Body chunks are acknowledged (window credit) as they are read. */
public final class IncomingMessage {
    public final int channel;
    public final int msgno;
    final Session.Sink sink;
    private final Session session;
    private boolean done;

    IncomingMessage(Session session, int channel, int msgno) {
        this.session = session;
        this.channel = channel;
        this.msgno = msgno;
        this.sink = new Session.Sink(session, channel);
    }

    /** Next body chunk, or null at the end of the message. */
    public byte[] read() {
        if (done) return null;
        Object[] it;
        try {
            it = sink.queue.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BeepException.Closed("interrupted");
        }
        session.credit(channel, (Integer) it[1]);
        if ("END".equals(it[0])) {
            done = true;
            return null;
        }
        if ("CLOSED".equals(it[0])) {
            done = true;
            throw session.closedError();
        }
        return (byte[]) it[0];
    }

    public byte[] collect(int max) {
        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        byte[] c;
        while ((c = read()) != null) {
            bo.write(c, 0, c.length);
            if (bo.size() > max) {
                discard();
                throw new BeepException("message exceeds " + max + " bytes");
            }
        }
        return bo.toByteArray();
    }

    /** Stop reading; remaining data is acknowledged as it arrives. */
    public void discard() {
        done = true;
        sink.discard();
    }
}
