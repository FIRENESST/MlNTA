package io.minta.beep;

/** Serves MSGs on channels of one profile. MSGs on a channel arrive strictly in order. */
@FunctionalInterface
public interface ProfileHandler {
    /** The peer asks to start a channel. Return init-reply data (or null); throw {@link BeepException.Refused} to refuse. */
    default String onStart(Session session, int channel, String init) {
        return null;
    }

    /** Handle one MSG; the reply (RPY, ERR, or ANS…NUL) should be complete when this returns. */
    void onMessage(Session session, IncomingMessage msg, Responder reply) throws Exception;
}
