using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;

namespace Minta.Beep
{
    /// <summary>BEEP wire format: frames, MIME entities, channel-0 XML.</summary>
    public static class Wire
    {
        public const int MaxHeaderLine = 128;
        public const string BeepXml = "application/beep+xml";
        internal const long MaxI31 = 2147483647L;
        internal const long MaxU32 = 4294967295L;
        private static readonly byte[] Trailer = Encoding.ASCII.GetBytes("END\r\n");

        internal sealed class Frame
        {
            public string Type = "";
            public int Channel;
            public int Msgno;
            public bool More;
            public long Seqno;
            public int Ansno;
            public byte[] Payload = Array.Empty<byte>();
            public long Ackno;
            public long Window;
        }

        private static long Num(string s, long max)
        {
            if (s.Length == 0 || s.Length > 10) throw new BeepProtocolException($"bad number '{s}' in frame header");
            foreach (char c in s) if (c < '0' || c > '9') throw new BeepProtocolException($"bad number '{s}' in frame header");
            long v = long.Parse(s, System.Globalization.CultureInfo.InvariantCulture);
            if (v > max) throw new BeepProtocolException($"number {v} out of range");
            return v;
        }

        /// <summary>Incremental decoder over a growable buffer.</summary>
        internal sealed class Decoder
        {
            private byte[] _buf = new byte[65536];
            private int _start, _end;
            private readonly int _maxPayload;
            public Decoder(int maxPayload) { _maxPayload = maxPayload; }
            public int Pending => _end - _start;

            public Memory<byte> WriteSpace(int min)
            {
                if (_start > 0 && _start == _end) { _start = _end = 0; }
                if (_buf.Length - _end < min)
                {
                    int live = _end - _start;
                    var nb = _buf.Length - live >= min && _start > 0 ? _buf : new byte[Math.Max(_buf.Length * 2, live + min)];
                    Buffer.BlockCopy(_buf, _start, nb, 0, live);
                    _buf = nb;
                    _start = 0;
                    _end = live;
                }
                return _buf.AsMemory(_end);
            }

            public void Advance(int n) => _end += n;

            public Frame? Next()
            {
                int len = _end - _start;
                int scan = Math.Min(len, MaxHeaderLine);
                int lineEnd = -1;
                for (int i = 0; i + 1 < scan; i++)
                {
                    if (_buf[_start + i] == '\r' && _buf[_start + i + 1] == '\n') { lineEnd = i; break; }
                }
                if (lineEnd < 0)
                {
                    if (len >= MaxHeaderLine) throw new BeepProtocolException("frame header line too long");
                    return null;
                }
                for (int i = 0; i < lineEnd; i++)
                {
                    byte b = _buf[_start + i];
                    if (b < 0x20 || b > 0x7e) throw new BeepProtocolException("frame header is not ASCII");
                }
                string line = Encoding.ASCII.GetString(_buf, _start, lineEnd);
                string[] p = line.Split(' ');
                int headLen = lineEnd + 2;
                var f = new Frame { Type = p[0] };
                if (f.Type == "SEQ")
                {
                    if (p.Length != 4) throw new BeepProtocolException($"malformed SEQ header '{line}'");
                    f.Channel = (int)Num(p[1], MaxI31);
                    f.Ackno = Num(p[2], MaxU32);
                    f.Window = Num(p[3], MaxI31);
                    _start += headLen;
                    return f;
                }
                bool ans = f.Type == "ANS";
                if (f.Type != "MSG" && f.Type != "RPY" && f.Type != "ERR" && !ans && f.Type != "NUL")
                    throw new BeepProtocolException($"unknown frame keyword '{f.Type}'");
                if (p.Length != (ans ? 7 : 6)) throw new BeepProtocolException($"malformed frame header '{line}'");
                if (p[3] != "." && p[3] != "*") throw new BeepProtocolException("bad continuation indicator");
                f.Channel = (int)Num(p[1], MaxI31);
                f.Msgno = (int)Num(p[2], MaxI31);
                f.More = p[3] == "*";
                f.Seqno = Num(p[4], MaxU32);
                int size = (int)Num(p[5], MaxI31);
                if (ans) f.Ansno = (int)Num(p[6], MaxI31);
                if (f.Type == "NUL" && (size != 0 || f.More)) throw new BeepProtocolException("NUL frame must have size 0");
                if (size > _maxPayload) throw new BeepProtocolException($"frame payload {size} exceeds limit {_maxPayload}");
                int total = headLen + size + 5;
                if (len < total) return null;
                for (int i = 0; i < 5; i++)
                    if (_buf[_start + headLen + size + i] != Trailer[i]) throw new BeepProtocolException("frame trailer is not END CRLF");
                f.Payload = new byte[size];
                Buffer.BlockCopy(_buf, _start + headLen, f.Payload, 0, size);
                _start += total;
                return f;
            }
        }

        internal static byte[] EncodeFrame(string type, int channel, int msgno, bool more, long seqno, byte[] buf, int off, int len, int ansno)
        {
            string m = more ? "*" : ".";
            string head = type == "ANS"
                ? $"ANS {channel} {msgno} {m} {seqno} {len} {ansno}\r\n"
                : $"{type} {channel} {msgno} {m} {seqno} {len}\r\n";
            var h = Encoding.ASCII.GetBytes(head);
            var outb = new byte[h.Length + len + 5];
            Buffer.BlockCopy(h, 0, outb, 0, h.Length);
            Buffer.BlockCopy(buf, off, outb, h.Length, len);
            Buffer.BlockCopy(Trailer, 0, outb, h.Length + len, 5);
            return outb;
        }

        internal static byte[] EncodeSeq(int channel, long ackno, long window) =>
            Encoding.ASCII.GetBytes($"SEQ {channel} {ackno} {window}\r\n");

        // ------------------------------------------------------------- MIME

        private static string Clean(string s) => s.Replace("\r", "").Replace("\n", "");

        public static byte[] EncodeEntity(Headers? headers, byte[]? body)
        {
            var sb = new StringBuilder();
            if (headers != null) foreach (var kv in headers) sb.Append(Clean(kv.Key)).Append(": ").Append(Clean(kv.Value)).Append("\r\n");
            sb.Append("\r\n");
            var h = Encoding.UTF8.GetBytes(sb.ToString());
            body ??= Array.Empty<byte>();
            var outb = new byte[h.Length + body.Length];
            Buffer.BlockCopy(h, 0, outb, 0, h.Length);
            Buffer.BlockCopy(body, 0, outb, h.Length, body.Length);
            return outb;
        }

        internal static int IndexOf(byte[] buf, int len, byte[] pat)
        {
            for (int i = 0; i + pat.Length <= len; i++)
            {
                int j = 0;
                while (j < pat.Length && buf[i + j] == pat[j]) j++;
                if (j == pat.Length) return i;
            }
            return -1;
        }

        private static readonly byte[] Crlf = { 13, 10 };
        private static readonly byte[] Crlf2 = { 13, 10, 13, 10 };

        /// <summary>docs/PROTOCOL.md §1.2; null when more bytes are needed to decide.</summary>
        internal static (Headers headers, int offset)? SplitHead(byte[] buf, int len, bool complete)
        {
            if (len >= 2 && buf[0] == 13 && buf[1] == 10) return (new Headers(), 2);
            int p = IndexOf(buf, len, Crlf);
            if (p < 0) return complete ? (new Headers(), 0) : null;
            if (Array.IndexOf(buf, (byte)':', 0, p) < 0) return (new Headers(), 0);
            int q = IndexOf(buf, len, Crlf2);
            if (q < 0) return complete ? (new Headers(), 0) : null;
            var list = new List<string[]>();
            foreach (var l in Encoding.UTF8.GetString(buf, 0, q + 2).Split("\r\n"))
            {
                if (l.Length == 0) continue;
                if ((l[0] == ' ' || l[0] == '\t') && list.Count > 0) { list[^1][1] += " " + l.Trim(); continue; }
                int c = l.IndexOf(':');
                if (c > 0) list.Add(new[] { l.Substring(0, c).Trim(), l.Substring(c + 1).Trim() });
            }
            var h = new Headers();
            foreach (var e in list) h.Add(e[0], e[1]);
            return (h, q + 4);
        }

        public static (Headers headers, byte[] body) ParseEntity(byte[] payload)
        {
            var (h, off) = SplitHead(payload, payload.Length, true)!.Value;
            return (h, payload.AsSpan(off).ToArray());
        }

        // ------------------------------------------------------------- channel 0

        internal static string Esc(string s) => s.Replace("&", "&amp;").Replace("<", "&lt;").Replace(">", "&gt;").Replace("'", "&apos;").Replace("\"", "&quot;");

        internal sealed class ProfileEl
        {
            public string Uri;
            public string? Data;
            public ProfileEl(string uri, string? data = null) { Uri = uri; Data = data; }
            public string Xml() =>
                Data == null ? $"<profile uri='{Esc(Uri)}' />"
                : !Data.Contains("]]>") ? $"<profile uri='{Esc(Uri)}'><![CDATA[{Data}]]></profile>"
                : $"<profile uri='{Esc(Uri)}'>{Esc(Data)}</profile>";
        }

        /// <summary>Kind: greeting, start, close, profile, ok, error.</summary>
        internal sealed class Mgmt
        {
            public string Kind = "";
            public List<ProfileEl> Profiles = new();
            public int Number;
            public int Code = 200;
            public string Message = "";
            public string? ServerName;

            public static Mgmt Of(string kind) => new Mgmt { Kind = kind };
            public static Mgmt Error(int code, string message) => new Mgmt { Kind = "error", Code = code, Message = message };

            public string Xml()
            {
                var sb = new StringBuilder();
                switch (Kind)
                {
                    case "greeting":
                        if (Profiles.Count == 0) return "<greeting />";
                        sb.Append("<greeting>");
                        foreach (var p in Profiles) sb.Append($"<profile uri='{Esc(p.Uri)}' />");
                        return sb.Append("</greeting>").ToString();
                    case "start":
                        sb.Append($"<start number='{Number}'");
                        if (ServerName != null) sb.Append($" serverName='{Esc(ServerName)}'");
                        sb.Append('>');
                        foreach (var p in Profiles) sb.Append(p.Xml());
                        return sb.Append("</start>").ToString();
                    case "close":
                        return string.IsNullOrEmpty(Message) ? $"<close number='{Number}' code='{Code}' />" : $"<close number='{Number}' code='{Code}'>{Esc(Message)}</close>";
                    case "profile": return Profiles[0].Xml();
                    case "ok": return "<ok />";
                    case "error": return $"<error code='{Code}'>{Esc(Message)}</error>";
                    default: throw new InvalidOperationException(Kind);
                }
            }

            public static Mgmt Parse(byte[] body)
            {
                var settings = new XmlReaderSettings { DtdProcessing = DtdProcessing.Prohibit, XmlResolver = null, MaxCharactersInDocument = 1 << 20 };
                var doc = new XmlDocument { XmlResolver = null };
                try
                {
                    using var r = XmlReader.Create(new MemoryStream(body), settings);
                    doc.Load(r);
                }
                catch (XmlException e)
                {
                    throw new BeepProtocolException("channel 0: bad XML: " + e.Message);
                }
                var root = doc.DocumentElement ?? throw new BeepProtocolException("channel 0: empty XML");
                var m = Of(root.Name);
                int NumAttr(string name, int? dflt)
                {
                    var v = root.GetAttribute(name);
                    if (string.IsNullOrEmpty(v))
                    {
                        if (dflt == null) throw new BeepProtocolException($"channel 0: <{root.Name}> lacks {name}");
                        return dflt.Value;
                    }
                    if (!long.TryParse(v.Trim(), out long n) || n < 0 || n > MaxI31) throw new BeepProtocolException($"channel 0: bad {name} attribute");
                    return (int)n;
                }
                ProfileEl Prof(XmlElement e)
                {
                    var t = e.InnerText.Trim();
                    return new ProfileEl(e.GetAttribute("uri"), t.Length == 0 ? null : t);
                }
                switch (root.Name)
                {
                    case "greeting":
                        foreach (XmlNode n in root.ChildNodes) if (n is XmlElement e && e.Name == "profile") m.Profiles.Add(new ProfileEl(e.GetAttribute("uri")));
                        return m;
                    case "start":
                        m.Number = NumAttr("number", null);
                        m.ServerName = root.HasAttribute("serverName") ? root.GetAttribute("serverName") : null;
                        foreach (XmlNode n in root.ChildNodes) if (n is XmlElement e && e.Name == "profile") m.Profiles.Add(Prof(e));
                        return m;
                    case "close":
                        m.Number = NumAttr("number", null);
                        m.Code = NumAttr("code", 200);
                        m.Message = root.InnerText.Trim();
                        return m;
                    case "profile":
                        m.Profiles.Add(Prof(root));
                        return m;
                    case "ok":
                        return m;
                    case "error":
                        m.Code = NumAttr("code", 554);
                        m.Message = root.InnerText.Trim();
                        return m;
                    default:
                        throw new BeepProtocolException($"channel 0: unknown element <{root.Name}>");
                }
            }
        }

        internal static byte[] XmlEntity(string xml) => EncodeEntity(new Headers { { "Content-Type", BeepXml } }, Encoding.UTF8.GetBytes(xml));
        internal static byte[] ErrorPayload(int code, string message) => XmlEntity(Mgmt.Error(code, message).Xml());

        internal static (int code, string message) ParseErrorPayload(byte[] payload)
        {
            var (_, body) = ParseEntity(payload);
            try
            {
                var m = Mgmt.Parse(body);
                if (m.Kind == "error") return (m.Code, m.Message);
            }
            catch (BeepException) { }
            return (554, Encoding.UTF8.GetString(body).Trim());
        }

        // ------------------------------------------------------------- unbuffered frame I/O (TLS bootstrap)

        internal static async Task<Frame?> ReadFrameUnbufferedAsync(Stream s, int maxPayload, CancellationToken ct)
        {
            var line = new byte[MaxHeaderLine];
            int n = 0;
            var one = new byte[1];
            while (true)
            {
                int r = await s.ReadAsync(one.AsMemory(0, 1), ct).ConfigureAwait(false);
                if (r == 0)
                {
                    if (n == 0) return null;
                    throw new BeepProtocolException("connection closed mid-frame");
                }
                if (n >= MaxHeaderLine) throw new BeepProtocolException("frame header line too long");
                line[n++] = one[0];
                if (n >= 2 && line[n - 2] == 13 && line[n - 1] == 10) break;
            }
            var d = new Decoder(maxPayload);
            line.AsSpan(0, n).CopyTo(d.WriteSpace(n).Span);
            d.Advance(n);
            var f = d.Next();
            if (f != null) return f; // SEQ
            // Data frame: read payload + trailer exactly.
            string[] p = Encoding.ASCII.GetString(line, 0, n - 2).Split(' ');
            int size = int.Parse(p[5], System.Globalization.CultureInfo.InvariantCulture);
            if (size > maxPayload) throw new BeepProtocolException("frame too large");
            var rest = new byte[size + 5];
            int off = 0;
            while (off < rest.Length)
            {
                int r = await s.ReadAsync(rest.AsMemory(off), ct).ConfigureAwait(false);
                if (r == 0) throw new BeepProtocolException("connection closed mid-frame");
                off += r;
            }
            rest.CopyTo(d.WriteSpace(rest.Length).Span);
            d.Advance(rest.Length);
            return d.Next() ?? throw new BeepProtocolException("malformed frame");
        }
    }
}
