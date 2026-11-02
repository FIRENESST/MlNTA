// BEEP_ECHO=127.0.0.1:10288 zig test test_beep.zig -lc <beep import lib or -lbeep>
const std = @import("std");
const beep = @import("beep.zig");

extern "c" fn getenv(name: [*:0]const u8) ?[*:0]const u8;

test "libbeep from zig" {
    const alloc = std.testing.allocator;
    const addr: [:0]const u8 = if (getenv("BEEP_ECHO")) |p| std.mem.span(p) else "127.0.0.1:10288";

    var client = try beep.Client.init(addr, 5000);
    defer client.deinit();

    var r = try client.call(alloc, "POST", "/echo", "Content-Type: text/plain\nX-Test: zig", "hello from zig");
    defer r.deinit(alloc);
    try std.testing.expectEqual(@as(i32, 200), r.status);
    try std.testing.expectEqualStrings("hello from zig", r.body);
    try std.testing.expect(std.mem.indexOf(u8, r.headers, "X-Test: zig") != null);

    var s = try client.call(alloc, "GET", "/stream/2", "", "");
    defer s.deinit(alloc);
    try std.testing.expectEqualStrings("line 0\nline 1\n", s.body);

    try std.testing.expectError(error.Remote, client.call(alloc, "GET", "/err/550", "", ""));
    try std.testing.expectEqual(@as(i32, 550), beep.lastErrorCode());
}
