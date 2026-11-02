using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Minta.Beep
{
    /// <summary>MINTA RPC profile (urn:minta:beep:rpc:1) helpers. See docs/PROTOCOL.md §2.</summary>
    public static class Rpc
    {
        public const string Profile = "urn:minta:beep:rpc:1";

        private static readonly HashSet<string> Reserved = new(StringComparer.OrdinalIgnoreCase)
            { "Rpc-Method", "Rpc-Target", "Rpc-Status", "Rpc-Reason", "Rpc-Abort", "Content-Transfer-Encoding" };

        internal static Headers AppHeaders(IEnumerable<KeyValuePair<string, string>>? h) =>
            new(h?.Where(kv => !Reserved.Contains(kv.Key)));

        public static int BeepCodeToHttp(int code) => code switch
        {
            421 or 450 => 503,
            451 => 504,
            500 or 501 or 553 => 400,
            504 => 501,
            530 or 534 or 535 => 401,
            537 or 538 => 403,
            _ => 502,
        };
    }

    /// <summary>Client-side complete response.</summary>
    public sealed class RpcResponse
    {
        public int Status { get; }
        public Headers Headers { get; }
        public byte[] Body { get; }
        internal RpcResponse(int status, Headers headers, byte[] body) { Status = status; Headers = headers; Body = body; }
        public string? Header(string name) => Headers.Get(name);
        public string Text() => Encoding.UTF8.GetString(Body);
    }

    /// <summary>Server-side incoming request.</summary>
    public sealed class RpcRequest
    {
        public string Method { get; }
        public string Target { get; }
        public Headers Headers { get; }
        public byte[] Body { get; }
        public int Channel { get; }
        internal RpcRequest(string method, string target, Headers headers, byte[] body, int channel) { Method = method; Target = target; Headers = headers; Body = body; Channel = channel; }
        public string? Header(string name) => Headers.Get(name);
        public string Path { get { int q = Target.IndexOf('?'); return q < 0 ? Target : Target.Substring(0, q); } }
        public string? Id => Headers.Get("Rpc-Id");
        public TimeSpan? Timeout => long.TryParse(Headers.Get("Rpc-Timeout"), out var ms) && ms > 0 ? TimeSpan.FromMilliseconds(ms) : null;
        public string Text() => Encoding.UTF8.GetString(Body);
    }

    /// <summary>Server handler result: full body (one RPY) or an async chunk stream (ANS…NUL).</summary>
    public sealed class RpcReply
    {
        public int Status { get; init; } = 200;
        public Headers Headers { get; init; } = new();
        public byte[]? Body { get; init; }
        public IAsyncEnumerable<byte[]>? Stream { get; init; }

        public static RpcReply Of(int status, Headers? headers, byte[]? body) => new() { Status = status, Headers = headers ?? new(), Body = body ?? Array.Empty<byte>() };
        public static RpcReply Of(int status, Headers? headers, string body) => Of(status, headers, Encoding.UTF8.GetBytes(body));
        public static RpcReply Streamed(int status, Headers? headers, IAsyncEnumerable<byte[]> chunks) => new() { Status = status, Headers = headers ?? new(), Stream = chunks };
    }

    public sealed class RpcClientOptions
    {
        public int Sessions { get; set; } = 1;
        public int MaxChannels { get; set; } = 64;
        /// <summary>Deadline for the response head; also sent as Rpc-Timeout. Null disables.</summary>
        public TimeSpan? Timeout { get; set; } = TimeSpan.FromSeconds(30);
        public TlsClientOptions? Tls { get; set; }
        public BeepConfig Config { get; set; } = new();
        public string Profile { get; set; } = Rpc.Profile;
    }

    /// <summary>Pooled RPC client: Sessions × MaxChannels channels, one in-flight request per channel. Thread-safe.</summary>
    public sealed class RpcClient : IAsyncDisposable
    {
        private readonly string _host;
        private readonly int _port;
        private readonly RpcClientOptions _opts;
        private readonly List<Session> _sessions = new();
        private readonly Stack<BeepChannel> _idle = new();
        private readonly SemaphoreSlim _permits;
        private readonly SemaphoreSlim _connect = new(1, 1);
        private volatile bool _closed;

        public RpcClient(string host, int port, RpcClientOptions? options = null)
        {
            _host = host;
            _port = port;
            _opts = options ?? new RpcClientOptions();
            _permits = new SemaphoreSlim(Math.Max(1, _opts.Sessions) * Math.Max(1, _opts.MaxChannels));
        }

        private async Task<Session> SessionAsync()
        {
            if (_closed) throw new SessionClosedException("client closed");
            lock (_sessions)
            {
                _sessions.RemoveAll(s => s.IsClosed);
                if (_sessions.Count >= Math.Max(1, _opts.Sessions)) return _sessions.OrderBy(s => s.ChannelCount).First();
            }
            await _connect.WaitAsync().ConfigureAwait(false);
            try
            {
                lock (_sessions)
                {
                    _sessions.RemoveAll(s => s.IsClosed);
                    if (_sessions.Count >= Math.Max(1, _opts.Sessions)) return _sessions.OrderBy(s => s.ChannelCount).First();
                }
                var s = await Session.ConnectAsync(_host, _port, _opts.Config, null, _opts.Tls).ConfigureAwait(false);
                lock (_sessions) _sessions.Add(s);
                return s;
            }
            finally { _connect.Release(); }
        }

        public Task ConnectAsync() => SessionAsync();

        private async Task<BeepChannel> AcquireAsync()
        {
            lock (_idle)
                while (_idle.Count > 0) { var ch = _idle.Pop(); if (ch.IsOpen) return ch; }
            var s = await SessionAsync().ConfigureAwait(false);
            try { return await s.StartChannelAsync(_opts.Profile).ConfigureAwait(false); }
            catch (BeepException) when (s.IsClosed) { return await (await SessionAsync().ConfigureAwait(false)).StartChannelAsync(_opts.Profile).ConfigureAwait(false); }
        }

        private void GiveBack(BeepChannel ch)
        {
            lock (_idle) if (!_closed && ch.IsOpen && ch.Pending == 0) _idle.Push(ch);
        }

        internal void Release(BeepChannel ch, ReplyStream st)
        {
            if (st.IsDone) { GiveBack(ch); _permits.Release(); return; }
            st.Discard();
            _ = Task.Run(async () =>
            {
                try { while (await st.NextAsync().ConfigureAwait(false) != null) { } } catch (BeepException) { }
                GiveBack(ch);
                _permits.Release();
            });
        }

        /// <summary>Send a request; completes when the response head arrives.</summary>
        public async Task<RpcStreamingResponse> StreamAsync(string method, string target, Headers? headers = null, byte[]? body = null, TimeSpan? timeout = null, CancellationToken ct = default)
        {
            var t = timeout ?? _opts.Timeout;
            var h = Rpc.AppHeaders(headers);
            if (t != null && h.Get("Rpc-Timeout") == null) h.Add("Rpc-Timeout", ((long)t.Value.TotalMilliseconds).ToString());
            var all = new Headers { { "Rpc-Method", method }, { "Rpc-Target", target } };
            foreach (var kv in h) all.Add(kv.Key, kv.Value);
            var payload = Wire.EncodeEntity(all, body);
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            if (t != null) cts.CancelAfter(t.Value);
            try { await _permits.WaitAsync(cts.Token).ConfigureAwait(false); }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested) { throw new BeepTimeoutException($"{method} {target} timed out waiting for a channel"); }
            BeepChannel ch;
            try { ch = await AcquireAsync().ConfigureAwait(false); }
            catch { _permits.Release(); throw; }
            var st = ch.Session.SendMsg(ch.Number, payload);
            var dec = new RpcDecoder(st);
            try
            {
                await dec.ReadHeadAsync(cts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                Release(ch, st);
                throw new BeepTimeoutException($"{method} {target} timed out");
            }
            catch
            {
                Release(ch, st);
                throw;
            }
            return new RpcStreamingResponse(this, ch, dec);
        }

        public async Task<RpcResponse> CallAsync(string method, string target, Headers? headers = null, byte[]? body = null, TimeSpan? timeout = null, CancellationToken ct = default)
        {
            await using var r = await StreamAsync(method, target, headers, body, timeout, ct).ConfigureAwait(false);
            var data = await r.ReadAllAsync(ct).ConfigureAwait(false);
            return new RpcResponse(r.Status, r.Headers, data);
        }

        public Task<RpcResponse> GetAsync(string target, Headers? headers = null) => CallAsync("GET", target, headers);
        public Task<RpcResponse> PostAsync(string target, string contentType, string body) =>
            CallAsync("POST", target, new Headers { { "Content-Type", contentType } }, Encoding.UTF8.GetBytes(body));

        /// <summary>Verbatim payload on a fresh channel of any profile.</summary>
        public async Task<Reply> RawAsync(string profile, byte[] payload)
        {
            var ch = await (await SessionAsync().ConfigureAwait(false)).StartChannelAsync(profile).ConfigureAwait(false);
            try { return await ch.RequestAsync(payload).ConfigureAwait(false); }
            finally { try { await ch.CloseAsync().ConfigureAwait(false); } catch (BeepException) { } }
        }

        public IReadOnlyList<int> Status() { lock (_sessions) return _sessions.Where(s => !s.IsClosed).Select(s => s.ChannelCount).ToList(); }

        public async ValueTask DisposeAsync()
        {
            _closed = true;
            List<Session> ss;
            lock (_idle) _idle.Clear();
            lock (_sessions) { ss = _sessions.ToList(); _sessions.Clear(); }
            foreach (var s in ss)
            {
                try { await s.CloseAsync().ConfigureAwait(false); } catch { s.Abort(); }
            }
        }
    }

    /// <summary>Response whose body is read incrementally; disposing returns the channel to the pool.</summary>
    public sealed class RpcStreamingResponse : IAsyncDisposable
    {
        private readonly RpcClient _client;
        private readonly BeepChannel _ch;
        private readonly RpcDecoder _dec;
        private int _released;
        public int Status => _dec.Status;
        public Headers Headers => _dec.Headers;

        internal RpcStreamingResponse(RpcClient c, BeepChannel ch, RpcDecoder dec) { _client = c; _ch = ch; _dec = dec; }

        public string? Header(string name) => Headers.Get(name);

        public async IAsyncEnumerable<byte[]> ReadChunksAsync([EnumeratorCancellation] CancellationToken ct = default)
        {
            try
            {
                byte[]? c;
                while ((c = await _dec.NextBodyAsync(ct).ConfigureAwait(false)) != null) yield return c;
            }
            finally { Release(); }
        }

        public async Task<byte[]> ReadAllAsync(CancellationToken ct = default)
        {
            var ms = new MemoryStream();
            await foreach (var c in ReadChunksAsync(ct).ConfigureAwait(false)) ms.Write(c);
            return ms.ToArray();
        }

        private void Release()
        {
            if (Interlocked.Exchange(ref _released, 1) == 0) _client.Release(_ch, _dec.Stream);
        }

        public ValueTask DisposeAsync() { Release(); return ValueTask.CompletedTask; }
    }

    /// <summary>Reply events → head + body chunks (docs/PROTOCOL.md §2.2–2.3).</summary>
    internal sealed class RpcDecoder
    {
        internal readonly ReplyStream Stream;
        internal int Status;
        internal Headers Headers = new();
        private bool _haveHead, _finished;
        private readonly Queue<byte[]> _ready = new();
        private MemoryStream? _rpyBuf = new();
        private readonly SortedDictionary<int, AnsBuf> _ans = new();
        private int _nextAns;

        private sealed class AnsBuf { public MemoryStream? Head = new(); public readonly Queue<byte[]> Data = new(); public bool Done; }

        internal RpcDecoder(ReplyStream s) { Stream = s; }

        internal async Task ReadHeadAsync(CancellationToken ct)
        {
            while (!_haveHead)
            {
                if (_finished) throw new BeepException("reply ended before a response head");
                await PumpAsync(ct).ConfigureAwait(false);
            }
        }

        internal async Task<byte[]?> NextBodyAsync(CancellationToken ct)
        {
            while (true)
            {
                if (_ready.Count > 0) return _ready.Dequeue();
                if (_finished) return null;
                await PumpAsync(ct).ConfigureAwait(false);
            }
        }

        private void SetHead(Headers h)
        {
            var st = h.Get("Rpc-Status");
            if (st == null) Status = 200;
            else if (!int.TryParse(st.Trim(), out Status)) throw new BeepException($"bad Rpc-Status {st}");
            Headers = Rpc.AppHeaders(h);
            _haveHead = true;
        }

        private async Task PumpAsync(CancellationToken ct)
        {
            var ev = await Stream.NextAsync(ct).ConfigureAwait(false);
            if (ev == null) { _finished = true; return; }
            switch (ev.Type)
            {
                case "RPY":
                    if (_rpyBuf != null)
                    {
                        _rpyBuf.Write(ev.Data);
                        var b = _rpyBuf.ToArray();
                        var h = Wire.SplitHead(b, b.Length, ev.Last);
                        if (h != null)
                        {
                            _rpyBuf = null;
                            SetHead(h.Value.headers);
                            if (b.Length > h.Value.offset) _ready.Enqueue(b.AsSpan(h.Value.offset).ToArray());
                        }
                        else if (b.Length > 65536) throw new BeepException("response head too large");
                    }
                    else if (ev.Data.Length > 0) _ready.Enqueue(ev.Data);
                    if (ev.Last) _finished = true;
                    break;
                case "ERR":
                    {
                        var ms = new MemoryStream();
                        ms.Write(ev.Data);
                        bool last = ev.Last;
                        while (!last)
                        {
                            var n = await Stream.NextAsync(ct).ConfigureAwait(false);
                            if (n == null || n.Type != "ERR") throw new BeepException("truncated ERR");
                            ms.Write(n.Data);
                            last = n.Last;
                        }
                        _finished = true;
                        var (code, message) = Wire.ParseErrorPayload(ms.ToArray());
                        throw new RemoteException(code, message);
                    }
                case "ANS":
                    {
                        if (!_ans.TryGetValue(ev.Ansno, out var a)) _ans[ev.Ansno] = a = new AnsBuf();
                        if (a.Head != null)
                        {
                            a.Head.Write(ev.Data);
                            var b = a.Head.ToArray();
                            var h = Wire.SplitHead(b, b.Length, ev.Last);
                            if (h != null)
                            {
                                a.Head = null;
                                var abort = h.Value.headers.Get("Rpc-Abort");
                                if (abort != null) throw new StreamAbortedException(abort);
                                if (ev.Ansno == 0) SetHead(h.Value.headers);
                                if (b.Length > h.Value.offset) a.Data.Enqueue(b.AsSpan(h.Value.offset).ToArray());
                            }
                        }
                        else if (ev.Data.Length > 0) a.Data.Enqueue(ev.Data);
                        if (ev.Last) a.Done = true;
                        Flush(false);
                        break;
                    }
                case "NUL":
                    Flush(true);
                    _finished = true;
                    if (!_haveHead) throw new BeepException("ANS stream without a head (ansno 0)");
                    break;
            }
        }

        private void Flush(bool all)
        {
            while (_ans.Count > 0)
            {
                int k = all ? _ans.Keys.First() : _nextAns;
                if (!_ans.TryGetValue(k, out var a) || (k == 0 && !_haveHead && !all)) return;
                while (a.Data.Count > 0) _ready.Enqueue(a.Data.Dequeue());
                if (a.Done || all) { _ans.Remove(k); _nextAns = k + 1; }
                else return;
            }
        }
    }

    /// <summary>BEEP listener serving the RPC profile.</summary>
    public sealed class RpcServer : IAsyncDisposable
    {
        private readonly BeepListener _listener;

        public RpcServer(Func<RpcRequest, Task<RpcReply>> handler, BeepConfig? config = null, TlsServerOptions? tls = null)
        {
            _listener = new BeepListener(new Dictionary<string, IProfileHandler> { [Rpc.Profile] = new RpcProfile(handler) }, config, tls);
        }

        public RpcServer Start(string host, int port) { _listener.Start(host, port); return this; }
        public int Port => _listener.Port;
        public ValueTask DisposeAsync() => _listener.DisposeAsync();
    }

    /// <summary>Decodes RPC envelopes and dispatches them to a handler.</summary>
    public sealed class RpcProfile : IProfileHandler
    {
        private readonly Func<RpcRequest, Task<RpcReply>> _handler;
        public RpcProfile(Func<RpcRequest, Task<RpcReply>> handler) { _handler = handler; }

        public async Task OnMessageAsync(Session session, IncomingMessage msg, Responder reply)
        {
            byte[] payload;
            try { payload = await msg.CollectAsync(session.Config.MaxMessageSize).ConfigureAwait(false); }
            catch (BeepException e)
            {
                if (!session.IsClosed) await reply.ErrorAsync(554, e.Message).ConfigureAwait(false);
                return;
            }
            var (headers, off) = Wire.SplitHead(payload, payload.Length, true)!.Value;
            var method = headers.Get("Rpc-Method");
            var target = headers.Get("Rpc-Target");
            if (string.IsNullOrEmpty(method) || target == null)
            {
                await reply.ErrorAsync(501, "request lacks Rpc-Method or Rpc-Target").ConfigureAwait(false);
                return;
            }
            var req = new RpcRequest(method, target, Rpc.AppHeaders(headers), payload.AsSpan(off).ToArray(), msg.Channel);
            RpcReply r;
            try
            {
                var task = _handler(req);
                if (req.Timeout is TimeSpan t && await Task.WhenAny(task, Task.Delay(t)).ConfigureAwait(false) != task)
                {
                    await reply.ErrorAsync(451, "deadline exceeded").ConfigureAwait(false);
                    return;
                }
                r = await task.ConfigureAwait(false);
            }
            catch (RpcFailure f)
            {
                await reply.ErrorAsync(f.Code, f.Message).ConfigureAwait(false);
                return;
            }
            catch (Exception e)
            {
                await reply.ErrorAsync(554, "handler failed: " + e.Message).ConfigureAwait(false);
                return;
            }
            var rh = new Headers { { "Rpc-Status", r.Status.ToString() } };
            foreach (var kv in Rpc.AppHeaders(r.Headers)) rh.Add(kv.Key, kv.Value);
            if (req.Id != null && rh.Get("Rpc-Id") == null) rh.Add("Rpc-Id", req.Id);
            if (r.Stream == null)
            {
                await reply.RpyAsync(Wire.EncodeEntity(rh, r.Body)).ConfigureAwait(false);
                return;
            }
            await reply.AnsAsync(Wire.EncodeEntity(rh, null)).ConfigureAwait(false);
            try
            {
                await foreach (var c in r.Stream.ConfigureAwait(false))
                    if (c != null && c.Length > 0) await reply.AnsAsync(Wire.EncodeEntity(null, c)).ConfigureAwait(false);
            }
            catch (BeepException) when (session.IsClosed) { return; }
            catch (Exception e) when (e is not BeepException)
            {
                await reply.AnsAsync(Wire.EncodeEntity(new Headers { { "Rpc-Abort", e.Message } }, null)).ConfigureAwait(false);
            }
            await reply.NulAsync().ConfigureAwait(false);
        }
    }
}
