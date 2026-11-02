// .build/debug/beep-test 127.0.0.1:10288   (libbeep on the loader path)
import Beep
import Foundation

var failures = 0
func check(_ cond: Bool, _ msg: String) {
    if cond { return }
    failures += 1
    print("FAIL: \(msg)")
}

let addr = CommandLine.arguments.count > 1 ? CommandLine.arguments[1] : "127.0.0.1:10288"
do {
    let c = try BeepClient(addr, timeoutMs: 5000)
    var r = try c.call("POST", "/echo", headers: ["Content-Type": "text/plain", "X-Test": "swift"], body: Data("hello from swift".utf8))
    check(r.status == 200 && String(decoding: r.body, as: UTF8.self) == "hello from swift"
        && r.headers.contains { $0.0.lowercased() == "x-test" && $0.1 == "swift" }, "echo \(r)")
    r = try c.call("GET", "/stream/3")
    check(String(decoding: r.body, as: UTF8.self) == "line 0\nline 1\nline 2\n", "stream")
    do {
        _ = try c.call("GET", "/err/537")
        check(false, "expected BeepError")
    } catch let e as BeepError {
        check(e.rc == -3 && e.code == 537, "err \(e)")
    }
    let bin = Data((0..<2560).map { UInt8($0 % 256) })
    r = try c.call("PUT", "/echo", body: bin)
    check(r.body == bin, "binary-safe body")
} catch {
    check(false, "unexpected \(error)")
}
print(failures == 0 ? "Swift (FFI): all tests passed" : "Swift (FFI): \(failures) FAILURE(S)")
exit(failures == 0 ? 0 : 1)
