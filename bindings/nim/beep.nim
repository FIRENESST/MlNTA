## beep.nim — Nim binding to libbeep. Test: test_beep.nim.
##   let c = newBeepClient("127.0.0.1:10288")
##   let r = c.call("POST", "/echo", "Content-Type: text/plain", "hi")
const libName = when defined(windows): "beep.dll" elif defined(macosx): "libbeep.dylib" else: "libbeep.so"

type
  BeepClientHandle = distinct pointer
  BeepError* = object of CatchableError
    code*: int
  BeepResponse* = object
    status*: int
    headers*: string
    body*: string
  BeepClient* = ref object
    h: BeepClientHandle

proc beep_client_new(address: cstring, sessions, maxChannels, timeoutMs: cint): BeepClientHandle {.importc, dynlib: libName.}
proc beep_client_free(c: BeepClientHandle) {.importc, dynlib: libName.}
proc beep_call(c: BeepClientHandle, meth, target, headers: cstring, body: pointer, bodyLen: csize_t, outp: ptr pointer): cint {.importc, dynlib: libName.}
proc beep_response_status(r: pointer): cint {.importc, dynlib: libName.}
proc beep_response_body(r: pointer, len: ptr csize_t): ptr UncheckedArray[char] {.importc, dynlib: libName.}
proc beep_response_headers(r: pointer): cstring {.importc, dynlib: libName.}
proc beep_response_free(r: pointer) {.importc, dynlib: libName.}
proc beep_last_error(): cstring {.importc, dynlib: libName.}
proc beep_last_error_code(): cint {.importc, dynlib: libName.}

proc newBeepClient*(address: string, timeoutMs = 30000): BeepClient =
  result = BeepClient(h: beep_client_new(address, 1, 16, timeoutMs.cint))
  if pointer(result.h) == nil: raise newException(BeepError, $beep_last_error())

proc close*(c: BeepClient) = beep_client_free(c.h)

proc call*(c: BeepClient, meth, target: string, headers = "", body = ""): BeepResponse =
  var outp: pointer
  let bodyPtr = if body.len > 0: unsafeAddr body[0] else: nil
  let rc = beep_call(c.h, meth, target, headers, bodyPtr, body.len.csize_t, addr outp)
  if rc != 0:
    var e = newException(BeepError, $beep_last_error())
    e.code = beep_last_error_code().int
    raise e
  defer: beep_response_free(outp)
  var n: csize_t
  let p = beep_response_body(outp, addr n)
  result.status = beep_response_status(outp).int
  result.headers = $beep_response_headers(outp)
  result.body = newString(n.int)
  if n > 0: copyMem(addr result.body[0], p, n.int)
