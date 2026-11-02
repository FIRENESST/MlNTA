package io.minta.beep;

/** Session tuning. Defaults follow docs/PROTOCOL.md §1.6. */
public final class Config {
    public int recvWindow = 262_144;
    public int maxFrameSize = 65_536;
    public int maxMessageSize = 64 << 20;
    public int connectTimeoutMs = 5_000;
    public int greetingTimeoutMs = 10_000;
    public int closeTimeoutMs = 10_000;

    public Config recvWindow(int v) { recvWindow = v; return this; }
    public Config maxFrameSize(int v) { maxFrameSize = v; return this; }
    public Config maxMessageSize(int v) { maxMessageSize = v; return this; }
    public Config connectTimeoutMs(int v) { connectTimeoutMs = v; return this; }
}
