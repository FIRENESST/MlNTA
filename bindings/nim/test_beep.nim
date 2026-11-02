# nim c -r --hints:off test_beep.nim 127.0.0.1:10288   (libbeep on the loader path)
import std/[os, strutils]
import beep

var failures = 0
proc check(cond: bool, msg: string) =
  if not cond:
    inc failures
    echo "FAIL: ", msg

let c = newBeepClient(if paramCount() > 0: paramStr(1) else: "127.0.0.1:10288", 5000)
var r = c.call("POST", "/echo", "Content-Type: text/plain\nX-Test: nim", "hello from nim")
check(r.status == 200 and r.body == "hello from nim" and "x-test: nim" in r.headers.toLowerAscii, "echo " & $r)
r = c.call("GET", "/stream/3")
check(r.body == "line 0\nline 1\nline 2\n", "stream " & r.body)
try:
  discard c.call("GET", "/err/537")
  check(false, "expected BeepError")
except BeepError as e:
  check(e.code == 537, "err code " & $e.code)
var bin = ""
for _ in 0 ..< 10:
  for i in 0 .. 255: bin.add chr(i)
r = c.call("PUT", "/echo", "", bin)
check(r.body == bin, "binary-safe body")
c.close()
echo(if failures == 0: "Nim (FFI): all tests passed" else: "Nim (FFI): " & $failures & " FAILURE(S)")
quit(if failures == 0: 0 else: 1)
