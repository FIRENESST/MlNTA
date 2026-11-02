using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Minta.Beep
{
    /// <summary>Client side of the TLS tuning profile. CaFile (PEM) replaces the system trust store when set.</summary>
    public sealed class TlsClientOptions
    {
        public string ServerName { get; set; } = "";
        public string? CaFile { get; set; }
        public RemoteCertificateValidationCallback? Validation { get; set; }
    }

    /// <summary>Server side: PEM certificate and private key, or a ready X509Certificate2.</summary>
    public sealed class TlsServerOptions
    {
        public string? CertFile { get; set; }
        public string? KeyFile { get; set; }
        public X509Certificate2? Certificate { get; set; }
    }

    /// <summary>TLS tuning profile (RFC 3080 §3.1, http://iana.org/beep/TLS).</summary>
    public static class BeepTls
    {
        public const string Profile = "http://iana.org/beep/TLS";

        private sealed class Boot
        {
            private readonly Stream _s;
            private readonly Dictionary<int, long> _seq = new();
            public Boot(Stream s) { _s = s; }

            public async Task SendAsync(string type, int channel, int msgno, string xml, CancellationToken ct)
            {
                var p = Wire.XmlEntity(xml);
                _seq.TryGetValue(channel, out long s);
                await _s.WriteAsync(Wire.EncodeFrame(type, channel, msgno, false, s, p, 0, p.Length, 0), ct).ConfigureAwait(false);
                await _s.FlushAsync(ct).ConfigureAwait(false);
                _seq[channel] = (s + p.Length) & Session.Mask;
            }

            public async Task<(string type, int channel, int msgno, byte[] body)> RecvAsync(CancellationToken ct)
            {
                var acc = new MemoryStream();
                while (true)
                {
                    var f = await Wire.ReadFrameUnbufferedAsync(_s, 65536, ct).ConfigureAwait(false)
                        ?? throw new SessionClosedException("connection closed during TLS negotiation");
                    if (f.Type == "SEQ") continue;
                    acc.Write(f.Payload);
                    if (!f.More) return (f.Type, f.Channel, f.Msgno, Wire.ParseEntity(acc.ToArray()).body);
                }
            }
        }

        internal static async Task<Stream> ClientBootstrapAsync(Stream raw, TlsClientOptions opts, CancellationToken ct)
        {
            var b = new Boot(raw);
            await b.SendAsync("RPY", 0, 0, "<greeting />", ct).ConfigureAwait(false);
            var r = await b.RecvAsync(ct).ConfigureAwait(false);
            var g = Wire.Mgmt.Parse(r.body);
            if (r.type != "RPY" || g.Kind != "greeting") throw new BeepProtocolException("expected greeting");
            if (!g.Profiles.Exists(p => p.Uri == Profile)) throw new BeepException("peer does not offer the TLS profile");
            var start = Wire.Mgmt.Of("start");
            start.Number = 1;
            start.ServerName = opts.ServerName;
            start.Profiles.Add(new Wire.ProfileEl(Profile, "<ready />"));
            await b.SendAsync("MSG", 0, 1, start.Xml(), ct).ConfigureAwait(false);
            r = await b.RecvAsync(ct).ConfigureAwait(false);
            var m = Wire.Mgmt.Parse(r.body);
            if (m.Kind == "error") throw new RefusedException(m.Code, m.Message);
            if (r.type != "RPY" || m.Kind != "profile" || !(m.Profiles[0].Data ?? "").Contains("<proceed")) throw new BeepException("peer did not proceed with TLS");

            RemoteCertificateValidationCallback? cb = opts.Validation;
            if (cb == null && opts.CaFile != null)
            {
                var ca = X509Certificate2.CreateFromPem(File.ReadAllText(opts.CaFile));
                cb = (_, cert, _, errors) =>
                {
                    if (cert == null) return false;
                    if ((errors & SslPolicyErrors.RemoteCertificateNameMismatch) != 0) return false;
                    using var chain = new X509Chain();
                    chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
                    chain.ChainPolicy.CustomTrustStore.Add(ca);
                    chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
                    return chain.Build(new X509Certificate2(cert));
                };
            }
            var ssl = new SslStream(raw, false, cb);
            await ssl.AuthenticateAsClientAsync(new SslClientAuthenticationOptions { TargetHost = opts.ServerName }, ct).ConfigureAwait(false);
            return ssl;
        }

        internal static async Task<Stream> ServerBootstrapAsync(Stream raw, TlsServerOptions opts, CancellationToken ct)
        {
            var b = new Boot(raw);
            var greet = Wire.Mgmt.Of("greeting");
            greet.Profiles.Add(new Wire.ProfileEl(Profile));
            await b.SendAsync("RPY", 0, 0, greet.Xml(), ct).ConfigureAwait(false);
            var r = await b.RecvAsync(ct).ConfigureAwait(false);
            if (r.type != "RPY" || r.channel != 0) throw new BeepProtocolException("expected greeting");
            int? tlsChannel = null;
            bool proceeded = false;
            for (int i = 0; i < 16 && !proceeded; i++)
            {
                r = await b.RecvAsync(ct).ConfigureAwait(false);
                if (r.type != "MSG") throw new BeepProtocolException("expected MSG during TLS negotiation");
                if (r.channel == tlsChannel)
                {
                    if (Encoding.UTF8.GetString(r.body).Contains("<ready")) { await b.SendAsync("RPY", r.channel, r.msgno, "<proceed />", ct).ConfigureAwait(false); proceeded = true; }
                    else await b.SendAsync("ERR", r.channel, r.msgno, Wire.Mgmt.Error(500, "expected <ready />").Xml(), ct).ConfigureAwait(false);
                    continue;
                }
                var m = Wire.Mgmt.Parse(r.body);
                var p = m.Kind == "start" && m.Number % 2 == 1 ? m.Profiles.Find(x => x.Uri == Profile) : null;
                if (p != null)
                {
                    var reply = Wire.Mgmt.Of("profile");
                    if (p.Data != null && p.Data.Contains("<ready")) { reply.Profiles.Add(new Wire.ProfileEl(Profile, "<proceed />")); proceeded = true; }
                    else { reply.Profiles.Add(new Wire.ProfileEl(Profile)); tlsChannel = m.Number; }
                    await b.SendAsync("RPY", 0, r.msgno, reply.Xml(), ct).ConfigureAwait(false);
                }
                else if (m.Kind == "close" && m.Number == 0)
                {
                    await b.SendAsync("RPY", 0, r.msgno, "<ok />", ct).ConfigureAwait(false);
                    throw new SessionClosedException("peer released session before TLS");
                }
                else await b.SendAsync("ERR", 0, r.msgno, Wire.Mgmt.Error(538, "TLS required").Xml(), ct).ConfigureAwait(false);
            }
            if (!proceeded) throw new BeepException("peer never requested TLS");
            var ssl = new SslStream(raw, false);
            await ssl.AuthenticateAsServerAsync(new SslServerAuthenticationOptions { ServerCertificate = LoadCert(opts) }, ct).ConfigureAwait(false);
            return ssl;
        }

        private static X509Certificate2 LoadCert(TlsServerOptions opts)
        {
            if (opts.Certificate != null) return opts.Certificate;
            if (opts.CertFile == null || opts.KeyFile == null) throw new ArgumentException("CertFile and KeyFile are required");
            using var pem = X509Certificate2.CreateFromPemFile(opts.CertFile, opts.KeyFile);
            // SChannel (Windows) needs a persisted key: round-trip through PKCS#12.
#pragma warning disable SYSLIB0057
            return new X509Certificate2(pem.Export(X509ContentType.Pkcs12));
#pragma warning restore SYSLIB0057
        }
    }

    /// <summary>Accepts BEEP sessions over TCP.</summary>
    public sealed class BeepListener : IAsyncDisposable
    {
        private readonly IDictionary<string, IProfileHandler> _profiles;
        private readonly BeepConfig? _config;
        private readonly TlsServerOptions? _tls;
        private readonly HashSet<Session> _sessions = new();
        private TcpListener? _tcp;
        private readonly CancellationTokenSource _cts = new();

        public event Action<Session>? SessionStarted;

        public BeepListener(IDictionary<string, IProfileHandler> profiles, BeepConfig? config = null, TlsServerOptions? tls = null)
        {
            _profiles = profiles;
            _config = config;
            _tls = tls;
        }

        public BeepListener Start(string host, int port)
        {
            _tcp = new TcpListener(IPAddress.Parse(host == "localhost" ? "127.0.0.1" : host), port);
            _tcp.Start(128);
            _ = Task.Run(AcceptLoop);
            return this;
        }

        public int Port => ((IPEndPoint)_tcp!.LocalEndpoint).Port;

        private async Task AcceptLoop()
        {
            while (!_cts.IsCancellationRequested)
            {
                TcpClient c;
                try { c = await _tcp!.AcceptTcpClientAsync(_cts.Token).ConfigureAwait(false); }
                catch (OperationCanceledException) { return; }
                catch (ObjectDisposedException) { return; }
                catch (SocketException) { await Task.Delay(50).ConfigureAwait(false); continue; }
                _ = Task.Run(() => HandleAsync(c));
            }
        }

        private async Task HandleAsync(TcpClient c)
        {
            try
            {
                c.NoDelay = true;
                Stream s = c.GetStream();
                if (_tls != null) s = await BeepTls.ServerBootstrapAsync(s, _tls, _cts.Token).ConfigureAwait(false);
                var session = await Session.EstablishAsync(s, c, false, _config, _profiles, _cts.Token).ConfigureAwait(false);
                lock (_sessions) _sessions.Add(session);
                SessionStarted?.Invoke(session);
                await session.Completion.ConfigureAwait(false);
                lock (_sessions) _sessions.Remove(session);
            }
            catch
            {
                c.Dispose();
            }
        }

        public ValueTask DisposeAsync()
        {
            _cts.Cancel();
            _tcp?.Stop();
            List<Session> ss;
            lock (_sessions) ss = new List<Session>(_sessions);
            foreach (var s in ss) s.Abort();
            return ValueTask.CompletedTask;
        }
    }
}
