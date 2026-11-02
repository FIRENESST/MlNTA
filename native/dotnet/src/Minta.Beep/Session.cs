using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Minta.Beep
{
    /// <summary>Serves MSGs on channels of one profile. MSGs on a channel are delivered strictly in order.</summary>
    public interface IProfileHandler
    {
        /// <summary>Accept by returning init-reply data (or null); throw <see cref="RefusedException"/> to refuse.</summary>
        Task<string?> OnStartAsync(Session session, int channel, string? init) => Task.FromResult<string?>(null);

        /// <summary>Handle one MSG; the reply (RPY, ERR, or ANS…NUL) should be complete when the task finishes.</summary>
        Task OnMessageAsync(Session session, IncomingMessage msg, Responder reply);
    }

    public enum ReplyKind { Rpy, Err, Ans }

    /// <summary>A complete reply.</summary>
    public sealed class Reply
    {
        public ReplyKind Kind { get; }
        public byte[] Payload { get; }
        public IReadOnlyList<(int Ansno, byte[] Payload)> Answers { get; }
        internal Reply(ReplyKind kind, byte[] payload, IReadOnlyList<(int, byte[])> answers) { Kind = kind; Payload = payload; Answers = answers; }
    }

    /// <summary>Type is RPY, ERR, ANS or NUL.</summary>
    public sealed record ReplyEvent(string Type, int Ansno, byte[] Data, bool Last);

    internal sealed class Sink
    {
        internal readonly Session S;
        internal readonly int ChannelNo;
        internal readonly Channel<(object Item, int N)> Q = Channel.CreateUnbounded<(object, int)>(new UnboundedChannelOptions { SingleReader = true });
        internal readonly HashSet<int> AnsOpen = new();
        internal volatile bool Discarding;

        internal Sink(Session s, int ch) { S = s; ChannelNo = ch; }

        internal void Deliver(object item, int n)
        {
            if (Discarding) { S.Credit(ChannelNo, n); return; }
            Q.Writer.TryWrite((item, n));
        }

        internal void Discard()
        {
            Discarding = true;
            while (Q.Reader.TryRead(out var it)) S.Credit(ChannelNo, it.N);
        }
    }

    /// <summary>Events of the reply to one MSG; data is acknowledged as it is consumed.</summary>
    public sealed class ReplyStream
    {
        private readonly Session _s;
        private readonly Sink _sink;
        public bool IsDone { get; private set; }

        internal ReplyStream(Session s, Sink sink) { _s = s; _sink = sink; }

        /// <summary>Next event, or null after the terminal one.</summary>
        public async Task<ReplyEvent?> NextAsync(CancellationToken ct = default)
        {
            if (IsDone) return null;
            var (item, n) = await _sink.Q.Reader.ReadAsync(ct).ConfigureAwait(false);
            _s.Credit(_sink.ChannelNo, n);
            if (item is string marker && marker == "CLOSED") { IsDone = true; throw _s.ClosedError(); }
            var ev = (ReplyEvent)item;
            if (ev.Type == "NUL" || ((ev.Type == "RPY" || ev.Type == "ERR") && ev.Last)) IsDone = true;
            return ev;
        }

        public async Task<Reply> CollectAsync(int max = 64 << 20, CancellationToken ct = default)
        {
            var buf = new MemoryStream();
            var ans = new SortedDictionary<int, MemoryStream>();
            var kind = ReplyKind.Rpy;
            long total = 0;
            ReplyEvent? ev;
            while ((ev = await NextAsync(ct).ConfigureAwait(false)) != null)
            {
                switch (ev.Type)
                {
                    case "RPY": kind = ReplyKind.Rpy; buf.Write(ev.Data); break;
                    case "ERR": kind = ReplyKind.Err; buf.Write(ev.Data); break;
                    case "ANS":
                        kind = ReplyKind.Ans;
                        if (!ans.TryGetValue(ev.Ansno, out var ms)) ans[ev.Ansno] = ms = new MemoryStream();
                        ms.Write(ev.Data);
                        break;
                    default: kind = ReplyKind.Ans; break;
                }
                total += ev.Data.Length;
                if (total > max) { Discard(); throw new BeepException($"reply exceeds {max} bytes"); }
            }
            return kind == ReplyKind.Ans
                ? new Reply(kind, Array.Empty<byte>(), ans.Select(kv => (kv.Key, kv.Value.ToArray())).ToList())
                : new Reply(kind, buf.ToArray(), Array.Empty<(int, byte[])>());
        }

        /// <summary>Stop consuming; remaining data is acknowledged as it arrives.</summary>
        public void Discard() => _sink.Discard();
    }

    /// <summary>A MSG received from the peer.</summary>
    public sealed class IncomingMessage
    {
        public int Channel { get; }
        public int Msgno { get; }
        internal readonly Sink Sink;
        private readonly Session _s;
        private bool _done;

        internal IncomingMessage(Session s, int channel, int msgno) { _s = s; Channel = channel; Msgno = msgno; Sink = new Sink(s, channel); }

        /// <summary>Next body chunk, or null at the end.</summary>
        public async Task<byte[]?> ReadAsync(CancellationToken ct = default)
        {
            if (_done) return null;
            var (item, n) = await Sink.Q.Reader.ReadAsync(ct).ConfigureAwait(false);
            _s.Credit(Channel, n);
            if (item is string m)
            {
                _done = true;
                if (m == "END") return null;
                throw _s.ClosedError();
            }
            return (byte[])item;
        }

        public async Task<byte[]> CollectAsync(int max = 64 << 20, CancellationToken ct = default)
        {
            var ms = new MemoryStream();
            byte[]? c;
            while ((c = await ReadAsync(ct).ConfigureAwait(false)) != null)
            {
                ms.Write(c);
                if (ms.Length > max) { Discard(); throw new BeepException($"message exceeds {max} bytes"); }
            }
            return ms.ToArray();
        }

        public void Discard() { _done = true; Sink.Discard(); }
    }

    /// <summary>Sends the reply to one incoming MSG.</summary>
    public sealed class Responder
    {
        internal const int Idle = 0, Answering = 1, Done = 2;
        private readonly Session _s;
        public int Channel { get; }
        public int Msgno { get; }
        internal int State = Idle;
        private int _nextAns;

        internal Responder(Session s, int channel, int msgno) { _s = s; Channel = channel; Msgno = msgno; }

        private void Final(string kind)
        {
            if (Interlocked.CompareExchange(ref State, Done, Idle) != Idle) throw new BeepException($"{kind} not allowed after ANS or completion");
        }

        public async Task RpyAsync(byte[] payload)
        {
            Final("RPY");
            try { await _s.SendMessageAsync(Channel, "RPY", Msgno, 0, payload).ConfigureAwait(false); }
            finally { _s.FinishInbound(Channel, Msgno); }
        }

        public async Task ErrAsync(byte[] payload)
        {
            Final("ERR");
            try { await _s.SendMessageAsync(Channel, "ERR", Msgno, 0, payload).ConfigureAwait(false); }
            finally { _s.FinishInbound(Channel, Msgno); }
        }

        public Task ErrorAsync(int code, string message) => ErrAsync(Wire.ErrorPayload(code, message));

        public async Task<int> AnsAsync(byte[] payload)
        {
            int n;
            lock (this)
            {
                if (State == Done) throw new BeepException("reply already completed");
                State = Answering;
                n = _nextAns++;
            }
            await _s.SendMessageAsync(Channel, "ANS", Msgno, n, payload).ConfigureAwait(false);
            return n;
        }

        public async Task NulAsync()
        {
            lock (this)
            {
                if (State == Done) throw new BeepException("reply already completed");
                State = Done;
            }
            try { await _s.SendMessageAsync(Channel, "NUL", Msgno, 0, Array.Empty<byte>()).ConfigureAwait(false); }
            finally { _s.FinishInbound(Channel, Msgno); }
        }
    }

    /// <summary>An open channel.</summary>
    public sealed class BeepChannel
    {
        public Session Session { get; }
        public int Number { get; }
        public string Profile { get; }
        internal BeepChannel(Session s, int n, string p) { Session = s; Number = n; Profile = p; }
        public bool IsOpen => Session.HasChannel(Number);
        public int Pending => Session.PendingOn(Number);
        public Task<ReplyStream> SendAsync(byte[] payload) => Task.FromResult(Session.SendMsg(Number, payload));
        public async Task<Reply> RequestAsync(byte[] payload, CancellationToken ct = default) =>
            await Session.SendMsg(Number, payload).CollectAsync(Session.Config.MaxMessageSize, ct).ConfigureAwait(false);
        public Task CloseAsync() => Session.CloseChannelAsync(Number, 200);
        public override string ToString() => $"Channel({Number}, {Profile})";
    }

    /// <summary>A BEEP session (RFC 3080/3081). Thread-safe.</summary>
    public sealed class Session : IAsyncDisposable
    {
        internal const long Mask = 0xffffffffL;
        internal const int InitialWindow = 4096;
        private const int MaxChannel = 2147483647;
        private const string Closed = "CLOSED";

        private sealed class Chan
        {
            public readonly int Number;
            public string Profile;
            public long TxSeq, TxLimit = InitialWindow;
            public bool Dead;
            public TaskCompletionSource Signal = new(TaskCreationOptions.RunContinuationsAsynchronously);
            public readonly SemaphoreSlim TxLock = new(1, 1);
            public long RecvSeq, AdvAck, AdvWindow = InitialWindow, Consumed;
            public int NextMsgno;
            public readonly Dictionary<int, Sink> Outstanding = new();
            public string? RxReplyType;
            public int RxReplyMsgno;
            public IncomingMessage? RxMsg;
            public readonly HashSet<int> Inbound = new();
            public Channel<object?>? Queue;

            public Chan(int number, string profile, int nextMsgno) { Number = number; Profile = profile; NextMsgno = nextMsgno; }
            public bool Busy => Outstanding.Count > 0 || Inbound.Count > 0 || RxMsg != null;

            public void Wake()
            {
                var s = Signal;
                Signal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                s.TrySetResult();
            }

            public void Kill() { lock (this) { Dead = true; Wake(); } }
        }

        public BeepConfig Config { get; }
        public bool Initiator { get; }
        public IReadOnlyList<string> PeerProfiles { get; private set; } = Array.Empty<string>();
        internal readonly Dictionary<string, IProfileHandler> Profiles;

        private readonly Stream _stream;
        private readonly IDisposable? _owner;
        private readonly Channel<byte[]?> _out = Channel.CreateUnbounded<byte[]?>(new UnboundedChannelOptions { SingleReader = true });
        private readonly object _lock = new();
        private readonly Dictionary<int, Chan> _chans = new();
        private int _nextChan;
        private volatile Exception? _closed;
        private volatile bool _releasing;
        private readonly TaskCompletionSource<Exception> _closedTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        private Session(Stream stream, IDisposable? owner, bool initiator, BeepConfig? config, IDictionary<string, IProfileHandler>? profiles)
        {
            _stream = stream;
            _owner = owner;
            Initiator = initiator;
            Config = config ?? new BeepConfig();
            Profiles = profiles == null ? new() : new Dictionary<string, IProfileHandler>(profiles);
            _nextChan = initiator ? 1 : 2;
            _chans[0] = new Chan(0, "", 1);
        }

        public static async Task<Session> ConnectAsync(string host, int port, BeepConfig? config = null,
            IDictionary<string, IProfileHandler>? profiles = null, TlsClientOptions? tls = null, CancellationToken ct = default)
        {
            config ??= new BeepConfig();
            var client = new TcpClient { NoDelay = true };
            try
            {
                using (var cts = CancellationTokenSource.CreateLinkedTokenSource(ct))
                {
                    cts.CancelAfter(config.ConnectTimeout);
                    await client.ConnectAsync(host, port, cts.Token).ConfigureAwait(false);
                }
                Stream stream = client.GetStream();
                if (tls != null) stream = await BeepTls.ClientBootstrapAsync(stream, tls, ct).ConfigureAwait(false);
                return await EstablishAsync(stream, client, true, config, profiles, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                client.Dispose();
                throw new BeepTimeoutException($"connecting to {host}:{port} timed out");
            }
            catch (SocketException e)
            {
                client.Dispose();
                throw new SessionClosedException($"connect {host}:{port}: {e.Message}", e);
            }
            catch
            {
                client.Dispose();
                throw;
            }
        }

        /// <summary>Run BEEP over any duplex stream; completes after greetings.</summary>
        public static async Task<Session> EstablishAsync(Stream stream, IDisposable? owner, bool initiator, BeepConfig? config = null,
            IDictionary<string, IProfileHandler>? profiles = null, CancellationToken ct = default)
        {
            var s = new Session(stream, owner, initiator, config, profiles);
            var greet = new Sink(s, 0);
            lock (s._lock)
            {
                s._chans[0].Outstanding[0] = greet;
                s._chans[0].Queue = Channel.CreateUnbounded<object?>();
            }
            _ = Task.Run(s.WriterLoop);
            _ = Task.Run(s.ReaderLoop);
            var g = Wire.Mgmt.Of("greeting");
            foreach (var uri in s.Profiles.Keys.OrderBy(k => k, StringComparer.Ordinal)) g.Profiles.Add(new Wire.ProfileEl(uri));
            await s.SendMessageAsync(0, "RPY", 0, 0, Wire.XmlEntity(g.Xml())).ConfigureAwait(false);
            s.OpenWindow(0);
            Reply reply;
            try
            {
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                cts.CancelAfter(s.Config.GreetingTimeout);
                reply = await new ReplyStream(s, greet).CollectAsync(1 << 20, cts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                var e = new BeepTimeoutException("greeting timed out");
                s.Fail(e);
                throw e;
            }
            catch (Exception e)
            {
                s.Fail(e);
                throw;
            }
            Wire.Mgmt m;
            try { m = Wire.Mgmt.Parse(Wire.ParseEntity(reply.Payload).body); }
            catch (Exception e) { s.Fail(e); throw; }
            if (reply.Kind == ReplyKind.Err || m.Kind == "error")
            {
                var e = m.Kind == "error" ? new RefusedException(m.Code, m.Message) : new RefusedException(421, "peer refused session");
                s.Fail(e);
                throw e;
            }
            if (m.Kind != "greeting")
            {
                var e = new BeepProtocolException($"expected greeting, got {m.Kind}");
                s.Fail(e);
                throw e;
            }
            s.PeerProfiles = m.Profiles.Select(p => p.Uri).ToList();
            _ = Task.Run(s.MgmtLoop);
            return s;
        }

        public bool IsClosed => _closed != null;
        public int ChannelCount { get { lock (_lock) return _chans.Count - 1; } }
        public Task<Exception> Completion => _closedTcs.Task;

        public async Task<BeepChannel> StartChannelAsync(string uri, string? initData = null, string? serverName = null)
            => (await StartChannelWithAsync(uri, initData, serverName).ConfigureAwait(false)).Channel;

        public async Task<(BeepChannel Channel, string? Init)> StartChannelWithAsync(string uri, string? initData = null, string? serverName = null)
        {
            CheckOpen();
            int n;
            lock (_lock)
            {
                n = _nextChan;
                int tries = 0;
                while (_chans.ContainsKey(n))
                {
                    n = NextNumber(n);
                    if (++tries > 1_000_000) throw new SessionClosedException("no free channel numbers");
                }
                _nextChan = NextNumber(n);
                _chans[n] = new Chan(n, "", 0);
            }
            var req = Wire.Mgmt.Of("start");
            req.Number = n;
            req.ServerName = serverName;
            req.Profiles.Add(new Wire.ProfileEl(uri, initData));
            Wire.Mgmt m;
            try { m = await MgmtRequestAsync(req).ConfigureAwait(false); }
            catch { lock (_lock) _chans.Remove(n); throw; }
            if (m.Kind != "profile")
            {
                lock (_lock) _chans.Remove(n);
                if (m.Kind == "error") throw new RefusedException(m.Code, m.Message);
                throw new BeepProtocolException($"unexpected reply to <start>: {m.Kind}");
            }
            var p = m.Profiles[0];
            var q = Channel.CreateUnbounded<object?>();
            lock (_lock)
            {
                if (!_chans.TryGetValue(n, out var c)) throw ClosedError();
                c.Profile = p.Uri;
                c.Queue = q;
            }
            Profiles.TryGetValue(p.Uri, out var h);
            _ = Task.Run(() => ChannelLoop(n, h, q));
            OpenWindow(n);
            return (new BeepChannel(this, n, p.Uri), p.Data);
        }

        /// <summary>Gracefully release the session.</summary>
        public async Task CloseAsync()
        {
            if (_closed != null) return;
            var req = Wire.Mgmt.Of("close");
            req.Number = 0;
            Wire.Mgmt m;
            try
            {
                var t = MgmtRequestAsync(req);
                if (await Task.WhenAny(t, Task.Delay(Config.CloseTimeout)).ConfigureAwait(false) != t) throw new BeepTimeoutException("close timed out");
                m = await t.ConfigureAwait(false);
            }
            catch
            {
                Abort();
                throw;
            }
            if (m.Kind == "ok") Fail(new SessionClosedException("session released"));
            else if (m.Kind == "error") throw new RefusedException(m.Code, m.Message);
        }

        public void Abort() => Fail(new SessionClosedException("session aborted"));

        public async ValueTask DisposeAsync()
        {
            try { await CloseAsync().ConfigureAwait(false); } catch { Abort(); }
        }

        // ------------------------------------------------------------- internals

        private static int NextNumber(int n) => n > MaxChannel - 2 ? 2 - n % 2 : n + 2;

        internal void CheckOpen() { if (_closed != null) throw ClosedError(); }

        internal Exception ClosedError() => _closed is BeepException b ? b : new SessionClosedException(_closed?.Message ?? "channel closed");

        internal void Fail(Exception err)
        {
            List<Chan> old;
            lock (_lock)
            {
                if (_closed != null) return;
                _closed = err;
                old = _chans.Values.ToList();
                _chans.Clear();
            }
            foreach (var c in old)
            {
                c.Kill();
                foreach (var s in c.Outstanding.Values) s.Q.Writer.TryWrite((Closed, 0));
                c.RxMsg?.Sink.Q.Writer.TryWrite((Closed, 0));
                c.Queue?.Writer.TryWrite(null);
            }
            _out.Writer.TryWrite(null);
            _closedTcs.TrySetResult(err);
        }

        internal void Credit(int channel, int n)
        {
            if (n <= 0) return;
            lock (_lock)
            {
                if (!_chans.TryGetValue(channel, out var c)) return;
                c.Consumed = (c.Consumed + n) & Mask;
                long w = Config.RecvWindow;
                if (((c.Consumed - c.AdvAck) & Mask) >= w / 2)
                {
                    c.AdvAck = c.Consumed;
                    c.AdvWindow = w;
                    _out.Writer.TryWrite(Wire.EncodeSeq(channel, c.Consumed, w));
                }
            }
        }

        internal void OpenWindow(int channel)
        {
            lock (_lock)
            {
                if (!_chans.TryGetValue(channel, out var c)) return;
                c.AdvAck = c.Consumed;
                c.AdvWindow = Config.RecvWindow;
                _out.Writer.TryWrite(Wire.EncodeSeq(channel, c.Consumed, c.AdvWindow));
            }
        }

        internal void FinishInbound(int channel, int msgno)
        {
            lock (_lock) if (_chans.TryGetValue(channel, out var c)) c.Inbound.Remove(msgno);
        }

        internal bool HasChannel(int n) { lock (_lock) return _closed == null && _chans.ContainsKey(n); }
        internal int PendingOn(int n) { lock (_lock) return _chans.TryGetValue(n, out var c) ? c.Outstanding.Count : 0; }

        private async Task WriterLoop()
        {
            try
            {
                var reader = _out.Reader;
                while (await reader.WaitToReadAsync().ConfigureAwait(false))
                {
                    bool stop = false;
                    while (reader.TryRead(out var b))
                    {
                        if (b == null) { stop = true; break; }
                        await _stream.WriteAsync(b).ConfigureAwait(false);
                    }
                    await _stream.FlushAsync().ConfigureAwait(false);
                    if (stop) break;
                }
            }
            catch (Exception e)
            {
                Fail(new SessionClosedException("write failed: " + e.Message, e));
            }
            finally
            {
                if (_closed != null)
                {
                    try { (_stream as NetworkStream)?.Socket.Shutdown(SocketShutdown.Send); } catch { }
                    _ = Task.Delay(1000).ContinueWith(_ => { try { _stream.Dispose(); _owner?.Dispose(); } catch { } });
                }
            }
        }

        private async Task ReaderLoop()
        {
            var dec = new Wire.Decoder(Math.Max(Config.RecvWindow, InitialWindow));
            try
            {
                while (_closed == null)
                {
                    int n = await _stream.ReadAsync(dec.WriteSpace(65536)).ConfigureAwait(false);
                    if (n == 0)
                    {
                        Fail(new SessionClosedException(_releasing ? "session released" : "connection closed by peer"));
                        return;
                    }
                    dec.Advance(n);
                    Wire.Frame? f;
                    while ((f = dec.Next()) != null)
                    {
                        if (f.Type == "SEQ")
                        {
                            Chan? c;
                            lock (_lock) _chans.TryGetValue(f.Channel, out c);
                            if (c != null) lock (c) { c.TxLimit = (f.Ackno + f.Window) & Mask; c.Wake(); }
                            continue;
                        }
                        OnData(f);
                    }
                }
            }
            catch (BeepProtocolException e)
            {
                Fail(e);
                try { _stream.Dispose(); _owner?.Dispose(); } catch { }
            }
            catch (Exception e)
            {
                Fail(new SessionClosedException("read failed: " + e.Message, e));
            }
        }

        private void OnData(Wire.Frame f)
        {
            lock (_lock)
            {
                if (!_chans.TryGetValue(f.Channel, out var c)) throw new BeepProtocolException($"frame on unknown channel {f.Channel}");
                if (f.Seqno != c.RecvSeq) throw new BeepProtocolException($"channel {f.Channel}: expected seqno {c.RecvSeq}, got {f.Seqno}");
                int size = f.Payload.Length;
                long end = (c.RecvSeq + size) & Mask;
                if (((end - c.AdvAck) & Mask) > c.AdvWindow) throw new BeepProtocolException($"channel {f.Channel}: frame exceeds receive window");
                c.RecvSeq = end;
                bool last = !f.More;
                switch (f.Type)
                {
                    case "MSG":
                        {
                            var m = c.RxMsg;
                            if (m != null && m.Msgno != f.Msgno) throw new BeepProtocolException($"channel {f.Channel}: interleaved MSG frames");
                            if (m == null)
                            {
                                if (!c.Inbound.Add(f.Msgno)) throw new BeepProtocolException($"channel {f.Channel}: msgno {f.Msgno} reused while outstanding");
                                m = new IncomingMessage(this, f.Channel, f.Msgno);
                                c.RxMsg = m;
                                c.Queue?.Writer.TryWrite(m);
                            }
                            m.Sink.Deliver(f.Payload, size);
                            if (last) { c.RxMsg = null; m.Sink.Deliver("END", 0); }
                            break;
                        }
                    case "RPY":
                    case "ERR":
                        {
                            if (c.RxReplyType != null && (c.RxReplyType != f.Type || c.RxReplyMsgno != f.Msgno))
                                throw new BeepProtocolException($"channel {f.Channel}: interleaved reply frames");
                            if (!c.Outstanding.TryGetValue(f.Msgno, out var s)) throw new BeepProtocolException($"channel {f.Channel}: reply to unknown msgno {f.Msgno}");
                            if (s.AnsOpen.Count > 0) throw new BeepProtocolException("RPY/ERR while ANS in progress");
                            s.Deliver(new ReplyEvent(f.Type, 0, f.Payload, last), size);
                            if (last) { c.RxReplyType = null; c.Outstanding.Remove(f.Msgno); }
                            else { c.RxReplyType = f.Type; c.RxReplyMsgno = f.Msgno; }
                            break;
                        }
                    case "ANS":
                        {
                            if (c.RxReplyType != null) throw new BeepProtocolException("ANS while RPY/ERR in progress");
                            if (!c.Outstanding.TryGetValue(f.Msgno, out var s)) throw new BeepProtocolException($"channel {f.Channel}: ANS to unknown msgno {f.Msgno}");
                            if (last) s.AnsOpen.Remove(f.Ansno); else s.AnsOpen.Add(f.Ansno);
                            s.Deliver(new ReplyEvent("ANS", f.Ansno, f.Payload, last), size);
                            break;
                        }
                    case "NUL":
                        {
                            if (!c.Outstanding.TryGetValue(f.Msgno, out var s)) throw new BeepProtocolException($"channel {f.Channel}: NUL to unknown msgno {f.Msgno}");
                            if (s.AnsOpen.Count > 0) throw new BeepProtocolException("NUL before all ANS messages completed");
                            c.Outstanding.Remove(f.Msgno);
                            s.Deliver(new ReplyEvent("NUL", 0, Array.Empty<byte>(), true), 0);
                            break;
                        }
                    default:
                        throw new BeepProtocolException($"unexpected frame type {f.Type}");
                }
            }
        }

        internal async Task SendMessageAsync(int channel, string type, int msgno, int ansno, byte[] payload)
        {
            CheckOpen();
            Chan? c;
            lock (_lock) _chans.TryGetValue(channel, out c);
            if (c == null) throw new SessionClosedException($"channel {channel} is not open");
            int maxFrame = Math.Max(1, Config.MaxFrameSize);
            await c.TxLock.WaitAsync().ConfigureAwait(false);
            try
            {
                int off = 0;
                while (true)
                {
                    Task wait;
                    lock (c)
                    {
                        while (true)
                        {
                            if (c.Dead || _closed != null) throw ClosedError();
                            long avail = (c.TxLimit - c.TxSeq) & Mask;
                            if (avail >= 0x80000000L) avail = 0;
                            int remaining = payload.Length - off;
                            if (remaining == 0 || avail > 0)
                            {
                                int n = (int)Math.Min(Math.Min(remaining, avail), maxFrame);
                                bool more = off + n < payload.Length;
                                _out.Writer.TryWrite(Wire.EncodeFrame(type, channel, msgno, more, c.TxSeq, payload, off, n, ansno));
                                c.TxSeq = (c.TxSeq + n) & Mask;
                                off += n;
                                if (!more) return;
                                continue;
                            }
                            wait = c.Signal.Task;
                            break;
                        }
                    }
                    await wait.ConfigureAwait(false);
                }
            }
            finally
            {
                c.TxLock.Release();
            }
        }

        internal ReplyStream SendMsg(int channel, byte[] payload)
        {
            CheckOpen();
            var sink = new Sink(this, channel);
            int msgno;
            lock (_lock)
            {
                if (!_chans.TryGetValue(channel, out var c)) throw new SessionClosedException($"channel {channel} is not open");
                msgno = c.NextMsgno;
                while (c.Outstanding.ContainsKey(msgno)) msgno = msgno >= MaxChannel ? 0 : msgno + 1;
                c.NextMsgno = msgno >= MaxChannel ? 0 : msgno + 1;
                c.Outstanding[msgno] = sink;
            }
            // Transmit in the background so callers can pipeline big MSGs while reading replies.
            _ = Task.Run(async () =>
            {
                try { await SendMessageAsync(channel, "MSG", msgno, 0, payload).ConfigureAwait(false); }
                catch (Exception)
                {
                    bool removed;
                    lock (_lock) removed = _chans.TryGetValue(channel, out var ch) && ch.Outstanding.Remove(msgno);
                    if (removed) sink.Q.Writer.TryWrite((Closed, 0));
                }
            });
            return new ReplyStream(this, sink);
        }

        private async Task<Wire.Mgmt> MgmtRequestAsync(Wire.Mgmt m)
        {
            var r = await SendMsg(0, Wire.XmlEntity(m.Xml())).CollectAsync(1 << 20).ConfigureAwait(false);
            if (r.Kind == ReplyKind.Ans) throw new BeepProtocolException("ANS on channel 0");
            return Wire.Mgmt.Parse(Wire.ParseEntity(r.Payload).body);
        }

        internal async Task CloseChannelAsync(int number, int code)
        {
            lock (_lock) if (_chans.TryGetValue(number, out var c) && c.Busy) throw new RefusedException(550, "still working");
            var req = Wire.Mgmt.Of("close");
            req.Number = number;
            req.Code = code;
            var m = await MgmtRequestAsync(req).ConfigureAwait(false);
            if (m.Kind == "ok")
            {
                Chan? c;
                lock (_lock) { _chans.TryGetValue(number, out c); _chans.Remove(number); }
                if (c != null) { c.Kill(); c.Queue?.Writer.TryWrite(null); }
            }
            else if (m.Kind == "error") throw new RefusedException(m.Code, m.Message);
            else throw new BeepProtocolException($"unexpected reply to <close>: {m.Kind}");
        }

        private async Task MgmtLoop()
        {
            Channel<object?>? q;
            lock (_lock) q = _chans.TryGetValue(0, out var c0) ? c0.Queue : null;
            if (q == null) return;
            while (true)
            {
                object? o;
                try { o = await q.Reader.ReadAsync().ConfigureAwait(false); } catch { return; }
                if (o is not IncomingMessage msg) return;
                var r = new Responder(this, 0, msg.Msgno);
                byte[] body;
                try { body = await msg.CollectAsync(1 << 20).ConfigureAwait(false); } catch { return; }
                Wire.Mgmt reply;
                int? opened = null;
                try
                {
                    var m = Wire.Mgmt.Parse(Wire.ParseEntity(body).body);
                    if (m.Kind == "start") { reply = await HandleStartAsync(m).ConfigureAwait(false); opened = m.Number; }
                    else if (m.Kind == "close") reply = HandleClose(m.Number);
                    else reply = Wire.Mgmt.Error(501, $"unexpected element <{m.Kind}>");
                }
                catch (RefusedException e) { reply = Wire.Mgmt.Error(e.Code, e.BeepMessage); }
                catch (BeepProtocolException e) { reply = Wire.Mgmt.Error(500, e.Message); }
                if (reply.Kind == "error") opened = null;
                try
                {
                    if (reply.Kind == "error") await r.ErrorAsync(reply.Code, reply.Message).ConfigureAwait(false);
                    else await r.RpyAsync(Wire.XmlEntity(reply.Xml())).ConfigureAwait(false);
                }
                catch { return; }
                if (opened != null) OpenWindow(opened.Value); // SEQ must follow the RPY that creates the channel
                if (reply.Kind == "ok" && _releasing)
                {
                    _out.Writer.TryWrite(null);
                    return;
                }
            }
        }

        private async Task<Wire.Mgmt> HandleStartAsync(Wire.Mgmt m)
        {
            if (_releasing) throw new RefusedException(550, "session is being released");
            int peerParity = Initiator ? 0 : 1;
            int n = m.Number;
            lock (_lock)
                if (n == 0 || n % 2 != peerParity || _chans.ContainsKey(n)) throw new RefusedException(553, $"channel number {n} is invalid or in use");
            Wire.ProfileEl? chosen = null;
            IProfileHandler? h = null;
            foreach (var p in m.Profiles)
                if (Profiles.TryGetValue(p.Uri, out h)) { chosen = p; break; }
            if (chosen == null || h == null) throw new RefusedException(550, "no acceptable profile");
            string? init = await h.OnStartAsync(this, n, chosen.Data).ConfigureAwait(false);
            var q = Channel.CreateUnbounded<object?>();
            lock (_lock)
            {
                if (_chans.ContainsKey(n)) throw new RefusedException(553, $"channel number {n} is in use");
                _chans[n] = new Chan(n, chosen.Uri, 0) { Queue = q };
            }
            var handler = h;
            _ = Task.Run(() => ChannelLoop(n, handler, q));
            var reply = Wire.Mgmt.Of("profile");
            reply.Profiles.Add(new Wire.ProfileEl(chosen.Uri, init));
            return reply;
        }

        private Wire.Mgmt HandleClose(int n)
        {
            Chan? c;
            lock (_lock)
            {
                if (n == 0)
                {
                    if (_chans.Any(kv => kv.Key != 0 && kv.Value.Busy)) throw new RefusedException(550, "still working");
                    _releasing = true;
                    return Wire.Mgmt.Of("ok");
                }
                if (!_chans.TryGetValue(n, out c)) throw new RefusedException(553, $"channel {n} is not open");
                if (c.Busy) throw new RefusedException(550, "still working");
                _chans.Remove(n);
            }
            c.Kill();
            c.Queue?.Writer.TryWrite(null);
            return Wire.Mgmt.Of("ok");
        }

        private async Task ChannelLoop(int number, IProfileHandler? handler, Channel<object?> q)
        {
            while (true)
            {
                object? o;
                try { o = await q.Reader.ReadAsync().ConfigureAwait(false); } catch { return; }
                if (o is not IncomingMessage msg) return;
                var r = new Responder(this, number, msg.Msgno);
                try
                {
                    if (handler == null)
                    {
                        msg.Discard();
                        await r.ErrorAsync(550, "this peer does not serve the profile").ConfigureAwait(false);
                        continue;
                    }
                    try
                    {
                        await handler.OnMessageAsync(this, msg, r).ConfigureAwait(false);
                    }
                    catch (BeepException) when (_closed != null) { return; }
                    catch (Exception e) when (e is not BeepException)
                    {
                        msg.Discard();
                        if (r.State == Responder.Idle) await r.ErrorAsync(554, "handler failed: " + e.Message).ConfigureAwait(false);
                        else if (r.State == Responder.Answering) await r.NulAsync().ConfigureAwait(false);
                        continue;
                    }
                    msg.Discard();
                    if (r.State == Responder.Idle) await r.ErrorAsync(554, "handler produced no reply").ConfigureAwait(false);
                    else if (r.State == Responder.Answering) await r.NulAsync().ConfigureAwait(false);
                }
                catch (BeepException)
                {
                    if (_closed != null) return;
                }
            }
        }
    }
}
