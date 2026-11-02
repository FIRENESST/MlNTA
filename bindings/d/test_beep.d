// ldc2 test_beep.d beep.d -L-L/path/to/release -L-lbeep && ./test_beep 127.0.0.1:10288
import beep;
import std.algorithm : canFind;
import std.stdio : writeln;
import std.uni : toLower;

int failures;

void check(bool cond, string msg)
{
    if (cond)
        return;
    failures++;
    writeln("FAIL: ", msg);
}

int main(string[] args)
{
    auto c = BeepClient(args.length > 1 ? args[1] : "127.0.0.1:10288", 5000);
    auto r = c.call("POST", "/echo", "Content-Type: text/plain\nX-Test: d", cast(const(ubyte)[]) "hello from d");
    check(r.status == 200 && cast(string) r.body == "hello from d" && r.headers.toLower.canFind("x-test: d"), "echo");
    r = c.call("GET", "/stream/3");
    check(cast(string) r.body == "line 0\nline 1\nline 2\n", "stream");
    try
    {
        c.call("GET", "/err/537");
        check(false, "expected BeepException");
    }
    catch (BeepException e)
        check(e.rc == -3 && e.code == 537, "err");
    ubyte[] bin;
    foreach (_; 0 .. 10)
        foreach (i; 0 .. 256)
            bin ~= cast(ubyte) i;
    r = c.call("PUT", "/echo", "", bin);
    check(r.body == bin, "binary-safe body");
    writeln(failures == 0 ? "D (FFI): all tests passed" : "D (FFI): FAILURE(S)");
    return failures == 0 ? 0 : 1;
}
