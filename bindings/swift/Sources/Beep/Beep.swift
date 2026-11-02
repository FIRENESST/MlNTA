// Swift wrapper over libbeep. Test: Sources/beep-test.
//   let c = try BeepClient("127.0.0.1:10288")
//   let r = try c.call("POST", "/echo", headers: ["Content-Type": "text/plain"], body: Data("hi".utf8))
import CBeep
import Foundation

public struct BeepError: Error {
    public let rc: Int32
    public let code: Int32
    public let message: String
}

public struct BeepResponse {
    public let status: Int
    public let headers: [(String, String)]
    public let body: Data
}

public final class BeepClient {
    private let handle: OpaquePointer

    public init(_ address: String, sessions: Int32 = 1, maxChannels: Int32 = 64, timeoutMs: Int32 = 30000) throws {
        guard let h = beep_client_new(address, sessions, maxChannels, timeoutMs) else {
            throw BeepError(rc: -1, code: beep_last_error_code(), message: String(cString: beep_last_error()))
        }
        handle = h
    }

    deinit { beep_client_free(handle) }

    public func call(_ method: String, _ target: String, headers: [String: String] = [:], body: Data = Data()) throws -> BeepResponse {
        let hs = headers.map { "\($0.key): \($0.value)" }.joined(separator: "\n")
        var out: OpaquePointer?
        let rc = body.withUnsafeBytes { buf in
            beep_call(handle, method, target, hs, buf.bindMemory(to: UInt8.self).baseAddress, body.count, &out)
        }
        guard rc == BEEP_OK, let r = out else {
            throw BeepError(rc: rc, code: beep_last_error_code(), message: String(cString: beep_last_error()))
        }
        defer { beep_response_free(r) }
        var n = 0
        let ptr = beep_response_body(r, &n)
        let data = n > 0 && ptr != nil ? Data(bytes: ptr!, count: n) : Data()
        let pairs = String(cString: beep_response_headers(r)).components(separatedBy: "\r\n").compactMap { line -> (String, String)? in
            guard let i = line.firstIndex(of: ":") else { return nil }
            return (String(line[..<i]), line[line.index(after: i)...].trimmingCharacters(in: .whitespaces))
        }
        return BeepResponse(status: Int(beep_response_status(r)), headers: pairs, body: data)
    }
}
