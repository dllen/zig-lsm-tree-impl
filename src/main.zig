const std = @import("std");
const LSMTree = @import("zig_lsm_tree_lib").LSMTree;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var lsm = try LSMTree.init(allocator);
    defer lsm.deinit();

    std.debug.print("LSM Tree teaching example\n", .{});
    std.debug.print("------------------------\n\n", .{});

    // Insert key-value pairs that are small enough to fit in one MemTable flush.
    const pairs = [_]struct { []const u8, []const u8 }{
        .{ "apple", "red" },
        .{ "banana", "yellow" },
        .{ "cherry", "red" },
        .{ "date", "brown" },
        .{ "elderberry", "purple" },
    };

    std.debug.print("Inserting {d} key-value pairs...\n", .{pairs.len});
    for (pairs) |pair| {
        try lsm.put(pair[0], pair[1]);
        std.debug.print("  put({s}, {s})\n", .{ pair[0], pair[1] });
    }

    std.debug.print("\nReading values back...\n", .{});
    for (pairs) |pair| {
        if (try lsm.get(pair[0])) |value| {
            defer allocator.free(value);
            std.debug.print("  get({s}) -> {s}\n", .{ pair[0], value });
        } else {
            std.debug.print("  get({s}) -> <missing>\n", .{pair[0]});
        }
    }

    std.debug.print("\nRun `zig build test` to verify the implementation.\n", .{});
}
