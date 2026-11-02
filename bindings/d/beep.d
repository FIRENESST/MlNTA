/// beep.d — D binding to libbeep. Test: test_beep.d.
///   auto c = BeepClient("127.0.0.1:10288");
///   auto r = c.call("POST", "/echo", "Content-Type: text/plain", cast(const(ubyte)[]) "hi");
module beep;

import std.string : fromStringz, toStringz;

extern (C) nothrow @nogc
{
    struct beep_client;
    struct beep_response;
    beep_client* beep_client_new(const(char)* address, int sessions, int maxChannels, int timeoutMs);
    void beep_client_free(beep_client* c);
    int beep_call(beep_client* c, const(char)* method, const(char)* target, const(char)* headers,
                  const(ubyte)* body, size_t bodyLen, beep_response** outp);
    int beep_response_status(const(beep_response)* r);
    const(ubyte)* beep_response_body(const(beep_response)* r, size_t* len);
    const(char)* beep_response_headers(const(beep_response)* r);
    void beep_response_free(beep_response* r);
    const(char)* beep_last_error();
    int beep_last_error_code();
}

class BeepException : Exception
{
    int rc, code;
    this(int rc)
    {
        this.rc = rc;
        this.code = beep_last_error_code();
        super(beep_last_error().fromStringz.idup);
    }
}

struct BeepResponse
{
    int status;
    string headers;
    immutable(ubyte)[] body;
}

struct BeepClient
{
    private beep_client* h;

    this(string address, int timeoutMs = 30_000)
    {
        h = beep_client_new(address.toStringz, 1, 16, timeoutMs);
        if (h is null)
            throw new BeepException(-1);
    }

    ~this()
    {
        if (h !is null)
            beep_client_free(h);
    }

    @disable this(this);

    BeepResponse call(string method, string target, string headers = "", const(ubyte)[] body = null)
    {
        beep_response* r;
        const rc = beep_call(h, method.toStringz, target.toStringz, headers.toStringz, body.ptr, body.length, &r);
        if (rc != 0)
            throw new BeepException(rc);
        scope (exit) beep_response_free(r);
        size_t n;
        auto p = beep_response_body(r, &n);
        return BeepResponse(beep_response_status(r), beep_response_headers(r).fromStringz.idup, p[0 .. n].idup);
    }
}
