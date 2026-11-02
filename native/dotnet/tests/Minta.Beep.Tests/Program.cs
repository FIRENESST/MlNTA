// .NET binding tests: dotnet run -- ECHO_ADDR [TLS_ECHO_ADDR CA CERT KEY]   |   dotnet run -- --serve PORT
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Minta.Beep;

static class Program
{
    static int failures;

    static void Check(bool cond, string msg)
    {
        if (!cond) { failures++; Console.WriteLine("FAIL: " + msg); }
    }

    static byte[] U(string s) => Encoding.UTF8.GetBytes(s);
    static string Lines(int n) => string.Concat(Enumerable.Range(0, n).Select(i => $"line {i}\n"));
    static (string, int) Addr(string s) { int i = s.LastIndexOf(':'); return (s[..i], int.Parse(s[(i + 1)..])); }

    static async Task AgainstRustEcho(string host, int port)
    {
        await using var c = new RpcClient(host, port, new RpcClientOptions { MaxChannels = 8, Timeout = TimeSpan.FromSeconds(5) });
        var r = await c.CallAsync("POST", "/echo?lang=cs", new Headers { { "Content-Type", "text/plain" }, { "X-Test", "cs" } }, U("hello from dotnet"));
        Check(r.Status == 200 && r.Text() == "hello from dotnet", $"echo {r.Status} {r.Text()}");
        Check(r.Header("x-test") == "cs" && r.Header("X-Echo-Method") == "POST", "echo headers");
        r = await c.GetAsync("/stream/50");
        Check(r.Text() == Lines(50), "stream body");
        await using (var sr = await c.StreamAsync("GET", "/stream/1000"))
        {
            await foreach (var chunk in sr.ReadChunksAsync()) { Check(Encoding.UTF8.GetString(chunk).StartsWith("line 0"), "first chunk"); break; }
        }
        r = await c.GetAsync("/status/418");
        Check(r.Status == 418 && r.Text() == "status 418", $"status {r.Status}");
        try { await c.GetAsync("/err/553"); Check(false, "expected RemoteException"); }
        catch (RemoteException e) { Check(e.Code == 553 && e.HttpStatus == 400, $"err {e.Message}"); }
        r = await c.GetAsync("/bytes/3000000");
        Check(r.Body.Length == 3000000 && r.Body[0] == 'a', "3 MB body");
        var big = new byte[2_500_000];
        new Random(1).NextBytes(big);
        r = await c.CallAsync("POST", "/echo", null, big);
        Check(r.Body.SequenceEqual(big), "2.5 MB upload");
        var sw = Stopwatch.StartNew();
        var rs = await Task.WhenAll(Enumerable.Range(0, 40).Select(i => c.CallAsync("POST", "/sleep/50", null, U(i.ToString()))));
        Check(rs.All(x => x.Status == 200), "concurrent");
        Check(sw.ElapsedMilliseconds < 2000, $"concurrency too slow {sw.ElapsedMilliseconds}ms");
        Check(c.Status().Count == 1 && c.Status()[0] <= 8, $"pool {string.Join(",", c.Status())}");
        try { await c.CallAsync("GET", "/sleep/2000", timeout: TimeSpan.FromMilliseconds(200)); Check(false, "expected timeout"); }
        catch (BeepTimeoutException) { }
        catch (RemoteException e) { Check(e.Code == 451, $"timeout {e.Message}"); }
        r = await c.CallAsync("POST", "/echo", null, U("after-timeout"));
        Check(r.Text() == "after-timeout", "pool usable after timeout");
    }

    static async IAsyncEnumerable<byte[]> Gen(int n)
    {
        for (int i = 0; i < n; i++) { await Task.Yield(); yield return U($"line {i}\n"); }
    }

    static async IAsyncEnumerable<byte[]> Bad()
    {
        yield return U("partial");
        await Task.Yield();
        throw new InvalidOperationException("enumerator died");
    }

    static Task<RpcReply> Handler(RpcRequest req)
    {
        if (req.Path.StartsWith("/stream/")) return Task.FromResult(RpcReply.Streamed(200, new Headers { { "Content-Type", "text/plain" } }, Gen(int.Parse(req.Path[8..]))));
        if (req.Path == "/abort") return Task.FromResult(RpcReply.Streamed(200, null, Bad()));
        if (req.Path.StartsWith("/err/")) throw new RpcFailure(int.Parse(req.Path[5..]), "dotnet says no");
        return Task.FromResult(RpcReply.Of(200, new Headers { { "Content-Type", req.Header("Content-Type") ?? "application/octet-stream" }, { "X-Lang", "dotnet" } }, req.Body));
    }

    static async Task DotnetServerDotnetClient()
    {
        var cfg = new BeepConfig { RecvWindow = 6000, MaxFrameSize = 1999 };
        await using var server = new RpcServer(Handler, cfg).Start("127.0.0.1", 0);
        await using (var c = new RpcClient("127.0.0.1", server.Port, new RpcClientOptions { Config = cfg }))
        {
            var data = new byte[1_000_000];
            new Random(2).NextBytes(data);
            var r = await c.CallAsync("POST", "/echo", null, data);
            Check(r.Body.SequenceEqual(data) && r.Header("X-Lang") == "dotnet", "dotnet<->dotnet big echo with tiny windows");
            r = await c.GetAsync("/stream/20");
            Check(r.Text() == Lines(20), "dotnet stream");
            try { await c.GetAsync("/abort"); Check(false, "expected abort"); }
            catch (StreamAbortedException e) { Check(e.Message.Contains("enumerator died"), $"abort {e.Message}"); }
            try { await c.GetAsync("/err/537"); Check(false, "expected 537"); }
            catch (RemoteException e) { Check(e.Code == 537, $"err {e.Message}"); }
        }
        var s = await Session.ConnectAsync("127.0.0.1", server.Port);
        Check(s.PeerProfiles.SequenceEqual(new[] { Rpc.Profile }), $"greeting {string.Join(",", s.PeerProfiles)}");
        try { await s.StartChannelAsync("urn:none"); Check(false, "expected refusal"); }
        catch (RefusedException e) { Check(e.Code == 550, $"refused {e.Message}"); }
        var ch = await s.StartChannelAsync(Rpc.Profile);
        var streams = new List<ReplyStream>();
        for (int i = 0; i < 10; i++) streams.Add(await ch.SendAsync(Wire.EncodeEntity(new Headers { { "Rpc-Method", "GET" }, { "Rpc-Target", "/echo" } }, U(i.ToString()))));
        for (int i = 0; i < 10; i++)
        {
            var body = Encoding.UTF8.GetString((await streams[i].CollectAsync()).Payload);
            Check(body.EndsWith(i.ToString()), "pipelined replies in order");
        }
        await ch.CloseAsync();
        await s.CloseAsync();
        Check(s.IsClosed, "session closed");
    }

    static async Task Tls(string tlsAddr, string ca, string cert, string key)
    {
        var (h, p) = Addr(tlsAddr);
        await using (var c = new RpcClient(h, p, new RpcClientOptions { Tls = new TlsClientOptions { ServerName = "localhost", CaFile = ca } }))
        {
            var r = await c.CallAsync("POST", "/echo", null, U("dotnet over tls to rust"));
            Check(r.Text() == "dotnet over tls to rust", "dotnet tls -> rust");
        }
        await using var server = new RpcServer(Handler, null, new TlsServerOptions { CertFile = cert, KeyFile = key }).Start("127.0.0.1", 0);
        await using var c2 = new RpcClient("127.0.0.1", server.Port, new RpcClientOptions { Tls = new TlsClientOptions { ServerName = "localhost", CaFile = ca } });
        var r2 = await c2.CallAsync("POST", "/echo", null, U("dotnet tls both ends"));
        Check(r2.Text() == "dotnet tls both ends", "dotnet tls -> dotnet");
    }

    static async Task<int> Main(string[] args)
    {
        if (args[0] == "--serve")
        {
            var s = new RpcServer(Handler).Start("127.0.0.1", int.Parse(args[1]));
            Console.WriteLine("READY " + s.Port);
            await Task.Delay(-1);
            return 0;
        }
        var (host, port) = Addr(args[0]);
        await AgainstRustEcho(host, port);
        await DotnetServerDotnetClient();
        if (args.Length >= 5) await Tls(args[1], args[2], args[3], args[4]);
        Console.WriteLine(failures == 0 ? ".NET: all tests passed" : $".NET: {failures} FAILURE(S)");
        return failures == 0 ? 0 : 1;
    }
}
