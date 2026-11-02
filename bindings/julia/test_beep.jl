# julia test_beep.jl 127.0.0.1:10288   (BEEP_LIB=/path/to/libbeep.so)
include(joinpath(@__DIR__, "Beep.jl"))

failures = 0
function check(cond, msg)
    cond && return
    global failures += 1
    println("FAIL: ", msg)
end

c = Beep.Client(isempty(ARGS) ? "127.0.0.1:10288" : ARGS[1]; timeout_ms = 5000)
r = Beep.call(c, "POST", "/echo", "Content-Type: text/plain\nX-Test: julia", "hello from julia")
check(r.status == 200 && String(copy(r.body)) == "hello from julia" && occursin("x-test: julia", lowercase(r.headers)), "echo $r")
r = Beep.call(c, "GET", "/stream/3")
check(String(copy(r.body)) == "line 0\nline 1\nline 2\n", "stream")
try
    Beep.call(c, "GET", "/err/537")
    check(false, "expected BeepError")
catch e
    check(e isa Beep.BeepError && e.rc == -3 && e.code == 537, "err $e")
end
bin = repeat(UInt8.(0:255), 10)
r = Beep.call(c, "PUT", "/echo", "", bin)
check(r.body == bin, "binary-safe body")
println(failures == 0 ? "Julia (FFI): all tests passed" : "Julia (FFI): $failures FAILURE(S)")
exit(failures == 0 ? 0 : 1)
