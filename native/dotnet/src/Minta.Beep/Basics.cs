using System;
using System.Collections;
using System.Collections.Generic;

namespace Minta.Beep
{
    /// <summary>Ordered, case-insensitive multimap of header fields. Supports collection initializers.</summary>
    public sealed class Headers : IEnumerable<KeyValuePair<string, string>>
    {
        private readonly List<KeyValuePair<string, string>> _items = new();

        public Headers() { }

        public Headers(IEnumerable<KeyValuePair<string, string>>? items)
        {
            if (items != null) foreach (var kv in items) _items.Add(kv);
        }

        public Headers Add(string name, string value)
        {
            _items.Add(new KeyValuePair<string, string>(name, value));
            return this;
        }

        public Headers Set(string name, string value)
        {
            Remove(name);
            return Add(name, value);
        }

        public Headers Remove(string name)
        {
            _items.RemoveAll(kv => string.Equals(kv.Key, name, StringComparison.OrdinalIgnoreCase));
            return this;
        }

        public string? Get(string name)
        {
            foreach (var kv in _items)
                if (string.Equals(kv.Key, name, StringComparison.OrdinalIgnoreCase)) return kv.Value;
            return null;
        }

        public string? this[string name] => Get(name);
        public int Count => _items.Count;

        public IEnumerator<KeyValuePair<string, string>> GetEnumerator() => _items.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public override string ToString()
        {
            var sb = new System.Text.StringBuilder();
            foreach (var kv in _items) sb.Append(kv.Key).Append(": ").Append(kv.Value).Append("\r\n");
            return sb.ToString();
        }
    }

    /// <summary>Session tuning (docs/PROTOCOL.md §1.8).</summary>
    public sealed class BeepConfig
    {
        public int RecvWindow { get; set; } = 262_144;
        public int MaxFrameSize { get; set; } = 65_536;
        public int MaxMessageSize { get; set; } = 64 << 20;
        public TimeSpan ConnectTimeout { get; set; } = TimeSpan.FromSeconds(5);
        public TimeSpan GreetingTimeout { get; set; } = TimeSpan.FromSeconds(10);
        public TimeSpan CloseTimeout { get; set; } = TimeSpan.FromSeconds(10);
    }

    public class BeepException : Exception
    {
        public BeepException(string message) : base(message) { }
        public BeepException(string message, Exception inner) : base(message, inner) { }
    }

    /// <summary>The peer violated RFC 3080/3081; the session is dropped.</summary>
    public sealed class BeepProtocolException : BeepException
    {
        public BeepProtocolException(string message) : base(message) { }
    }

    public sealed class SessionClosedException : BeepException
    {
        public SessionClosedException(string message) : base(message) { }
        public SessionClosedException(string message, Exception inner) : base(message, inner) { }
    }

    public sealed class BeepTimeoutException : BeepException
    {
        public BeepTimeoutException(string message) : base(message) { }
    }

    /// <summary>The peer refused a channel-management request.</summary>
    public sealed class RefusedException : BeepException
    {
        public int Code { get; }
        public string BeepMessage { get; }
        public RefusedException(int code, string message) : base($"refused by peer ({code}): {message}") { Code = code; BeepMessage = message; }
    }

    /// <summary>The peer answered with BEEP ERR: no application-level response.</summary>
    public sealed class RemoteException : BeepException
    {
        public int Code { get; }
        public string BeepMessage { get; }
        public RemoteException(int code, string message) : base($"remote error {code}: {message}") { Code = code; BeepMessage = message; }
        public int HttpStatus => Rpc.BeepCodeToHttp(Code);
    }

    /// <summary>A streamed response was aborted by the responder (Rpc-Abort).</summary>
    public sealed class StreamAbortedException : BeepException
    {
        public StreamAbortedException(string message) : base(message) { }
    }

    /// <summary>Throw from an RPC handler to answer with BEEP ERR.</summary>
    public sealed class RpcFailure : Exception
    {
        public int Code { get; }
        public RpcFailure(int code, string message) : base(message) { Code = code; }
    }
}
