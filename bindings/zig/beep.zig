//! beep.zig — Zig wrapper over libbeep (C ABI). Plain `extern fn` declarations (no @cImport),
//! so it works across Zig versions; link with `-lc` and the beep library.
//!
//!   const beep = @import("beep.zig");
//!   var c = try beep.Client.init("127.0.0.1:10288", 5000);
//!   defer c.deinit();
//!   var r = try c.call(allocator, "POST", "/echo", "Content-Type: text/plain", "hi");
//!   defer r.deinit(allocator);
const std = @import("std");

pub const OK: c_int = 0;
pub const E_ARG: c_int = -1;
pub const E_CONNECT: c_int = -2;
pub const E_REMOTE: c_int = -3;
pub const E_TIMEOUT: c_int = -4;
pub const E_TRANSPORT: c_int = -5;

pub const beep_client = opaque {};
pub const beep_response = opaque {};

pub extern fn beep_client_new(address: [*:0]const u8, sessions: c_int, max_channels: c_int, timeout_ms: c_int) ?*beep_client;
pub extern fn beep_client_new_tls(address: [*:0]const u8, sessions: c_int, max_channels: c_int, timeout_ms: c_int, server_name: [*:0]const u8, ca_file: ?[*:0]const u8) ?*beep_client;
pub extern fn beep_client_free(client: ?*beep_client) void;
pub extern fn beep_call(client: ?*beep_client, method: [*:0]const u8, target: [*:0]const u8, headers: ?[*:0]const u8, body: ?[*]const u8, body_len: usize, out: *?*beep_response) c_int;
pub extern fn beep_response_status(resp: ?*const beep_response) c_int;
pub extern fn beep_response_body(resp: ?*const beep_response, len: ?*usize) [*]const u8;
pub extern fn beep_response_headers(resp: ?*const beep_response) [*:0]const u8;
pub extern fn beep_response_free(resp: ?*beep_response) void;
pub extern fn beep_last_error() [*:0]const u8;
pub extern fn beep_last_error_code() c_int;

pub const Error = error{ InvalidArgument, Connect, Remote, Timeout, Transport, Internal, OutOfMemory };

fn mapRc(rc: c_int) Error {
    return switch (rc) {
        E_ARG => error.InvalidArgument,
        E_CONNECT => error.Connect,
        E_REMOTE => error.Remote,
        E_TIMEOUT => error.Timeout,
        E_TRANSPORT => error.Transport,
        else => error.Internal,
    };
}

/// BEEP reply code of the last failure on this thread (e.g. 550).
pub fn lastErrorCode() i32 {
    return beep_last_error_code();
}

pub fn lastError() []const u8 {
    return std.mem.span(beep_last_error());
}

pub const Response = struct {
    status: i32,
    headers: []u8,
    body: []u8,

    pub fn deinit(self: *Response, allocator: std.mem.Allocator) void {
        allocator.free(self.headers);
        allocator.free(self.body);
    }
};

pub const Client = struct {
    handle: *beep_client,

    pub fn init(address: [:0]const u8, timeout_ms: c_int) Error!Client {
        const h = beep_client_new(address.ptr, 1, 16, timeout_ms) orelse return error.InvalidArgument;
        return .{ .handle = h };
    }

    pub fn deinit(self: *Client) void {
        beep_client_free(self.handle);
    }

    /// headers: "Name: value" lines separated by '\n'.
    pub fn call(self: *Client, allocator: std.mem.Allocator, method: [:0]const u8, target: [:0]const u8, headers: [:0]const u8, body: []const u8) Error!Response {
        var out: ?*beep_response = null;
        const rc = beep_call(self.handle, method.ptr, target.ptr, headers.ptr, body.ptr, body.len, &out);
        if (rc != OK) return mapRc(rc);
        defer beep_response_free(out);
        var n: usize = 0;
        const b = beep_response_body(out, &n);
        const hs = std.mem.span(beep_response_headers(out));
        const headers_copy = try allocator.dupe(u8, hs);
        errdefer allocator.free(headers_copy);
        return .{
            .status = beep_response_status(out),
            .headers = headers_copy,
            .body = try allocator.dupe(u8, b[0..n]),
        };
    }
};
