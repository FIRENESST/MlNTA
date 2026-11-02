# Beep.jl — Julia binding to libbeep via ccall. Test: test_beep.jl.
#   include("Beep.jl"); c = Beep.Client("127.0.0.1:10288")
#   r = Beep.call(c, "POST", "/echo", "Content-Type: text/plain", "hi"); String(r.body)
module Beep
const lib = get(ENV, "BEEP_LIB", Sys.iswindows() ? "beep.dll" : Sys.isapple() ? "libbeep.dylib" : "libbeep.so")

struct BeepError <: Exception
    rc::Int32
    code::Int32
    msg::String
end

lasterror() = unsafe_string(ccall((:beep_last_error, lib), Cstring, ()))
lastcode() = ccall((:beep_last_error_code, lib), Cint, ())

mutable struct Client
    h::Ptr{Cvoid}
    function Client(address::AbstractString; timeout_ms::Integer = 30000)
        h = ccall((:beep_client_new, lib), Ptr{Cvoid}, (Cstring, Cint, Cint, Cint), address, 1, 16, timeout_ms)
        h == C_NULL && throw(BeepError(-1, 0, lasterror()))
        c = new(h)
        finalizer(x -> ccall((:beep_client_free, lib), Cvoid, (Ptr{Cvoid},), x.h), c)
    end
end

function call(c::Client, method, target, headers::AbstractString = "", body::Vector{UInt8} = UInt8[])
    out = Ref{Ptr{Cvoid}}(C_NULL)
    rc = ccall((:beep_call, lib), Cint, (Ptr{Cvoid}, Cstring, Cstring, Cstring, Ptr{UInt8}, Csize_t, Ref{Ptr{Cvoid}}),
               c.h, method, target, headers, body, length(body), out)
    rc != 0 && throw(BeepError(rc, lastcode(), lasterror()))
    n = Ref{Csize_t}(0)
    p = ccall((:beep_response_body, lib), Ptr{UInt8}, (Ptr{Cvoid}, Ref{Csize_t}), out[], n)
    data = copy(unsafe_wrap(Array, p, n[]))
    status = ccall((:beep_response_status, lib), Cint, (Ptr{Cvoid},), out[])
    hdrs = unsafe_string(ccall((:beep_response_headers, lib), Cstring, (Ptr{Cvoid},), out[]))
    ccall((:beep_response_free, lib), Cvoid, (Ptr{Cvoid},), out[])
    (status = Int(status), headers = hdrs, body = data)
end

call(c::Client, m, t, h::AbstractString, body::AbstractString) = call(c, m, t, h, Vector{UInt8}(body))
end
