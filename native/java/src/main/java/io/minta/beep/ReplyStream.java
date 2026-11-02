package io.minta.beep;

import java.io.ByteArrayOutputStream;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/** Events of the reply to one MSG. Data is acknowledged (window credit) as it is consumed. */
public final class ReplyStream {
    /** type is one of RPY, ERR, ANS, NUL. */
    public static final class Event {
        public final String type;
        public final int ansno;
        public final byte[] data;
        public final boolean last;

        Event(String type, int ansno, byte[] data, boolean last) {
            this.type = type;
            this.ansno = ansno;
            this.data = data;
            this.last = last;
        }
    }

    private final Session session;
    private final Session.Sink sink;
    private volatile boolean done;

    ReplyStream(Session session, Session.Sink sink) {
        this.session = session;
        this.sink = sink;
    }

    public boolean isDone() {
        return done;
    }

    /** Next event, or null after the terminal one. */
    public Event next() {
        return next(Long.MAX_VALUE);
    }

    /** Next event before {@code deadline} (System.nanoTime based); throws Timeout. */
    public Event next(long deadline) {
        if (done) return null;
        Object[] it;
        try {
            if (deadline == Long.MAX_VALUE) {
                it = sink.queue.take();
            } else {
                long wait = deadline - System.nanoTime();
                it = wait > 0 ? sink.queue.poll(wait, TimeUnit.NANOSECONDS) : sink.queue.poll();
                if (it == null) throw new BeepException.Timeout("reply timed out");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BeepException.Closed("interrupted");
        }
        session.credit(sink.channel, (Integer) it[1]);
        if ("CLOSED".equals(it[0])) {
            done = true;
            throw session.closedError();
        }
        Event ev = (Event) it[0];
        if (ev.type.equals("NUL") || ((ev.type.equals("RPY") || ev.type.equals("ERR")) && ev.last)) done = true;
        return ev;
    }

    public Reply collect(int max) {
        return collect(max, Long.MAX_VALUE);
    }

    Reply collect(int max, long deadline) {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        TreeMap<Integer, ByteArrayOutputStream> ans = new TreeMap<>();
        Reply.Kind kind = Reply.Kind.RPY;
        long total = 0;
        Event ev;
        while ((ev = next(deadline)) != null) {
            switch (ev.type) {
                case "RPY":
                case "ERR":
                    kind = ev.type.equals("RPY") ? Reply.Kind.RPY : Reply.Kind.ERR;
                    buf.write(ev.data, 0, ev.data.length);
                    break;
                case "ANS":
                    kind = Reply.Kind.ANS;
                    ans.computeIfAbsent(ev.ansno, k -> new ByteArrayOutputStream()).write(ev.data, 0, ev.data.length);
                    break;
                default:
                    kind = Reply.Kind.ANS;
            }
            total += ev.data.length;
            if (total > max) {
                discard();
                throw new BeepException("reply exceeds " + max + " bytes");
            }
        }
        if (kind == Reply.Kind.ANS) {
            List<Map.Entry<Integer, byte[]>> parts = new ArrayList<>();
            ans.forEach((k, v) -> parts.add(new AbstractMap.SimpleImmutableEntry<>(k, v.toByteArray())));
            return new Reply(kind, null, parts);
        }
        return new Reply(kind, buf.toByteArray(), null);
    }

    /** Stop consuming; remaining data is acknowledged as it arrives. */
    public void discard() {
        sink.discard();
    }
}
