package io.minta.beep;

import java.util.List;
import java.util.Map;

/** A complete reply to a MSG. */
public final class Reply {
    public enum Kind { RPY, ERR, ANS }

    public final Kind kind;
    private final byte[] payload;
    /** For ANS: (ansno, payload) in ansno order. */
    public final List<Map.Entry<Integer, byte[]>> answers;

    Reply(Kind kind, byte[] payload, List<Map.Entry<Integer, byte[]>> answers) {
        this.kind = kind;
        this.payload = payload;
        this.answers = answers;
    }

    /** Payload of an RPY or ERR. */
    public byte[] payload() {
        return payload;
    }
}
