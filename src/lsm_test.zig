const std = @import("std");
const LSMTree = @import("lsm.zig").LSMTree;

test "LSMTree multi-level operations" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer std.testing.expect(gpa.deinit() == .ok) catch @panic("memory leak detected");
    const allocator = gpa.allocator();

    var lsm = try LSMTree.init(allocator);
    defer lsm.deinit();

    const test_pairs = [_]struct { []const u8, []const u8 }{
        .{ "key1", "value1" },
        .{ "key2", "value2" },
        .{ "key3", "value3" },
        .{ "key4", "value4" },
        .{ "key5", "value5" },
    };

    var i: usize = 0;
    while (i < 200) : (i += 1) {
        for (test_pairs) |pair| {
            const key = try std.fmt.allocPrint(allocator, "{s}_{}", .{ pair[0], i });
            defer allocator.free(key);
            try lsm.put(key, pair[1]);
        }

        if (i % 10 == 0) {
            for (test_pairs) |pair| {
                const key = try std.fmt.allocPrint(allocator, "{s}_{}", .{ pair[0], i });
                defer allocator.free(key);
                if (try lsm.get(key)) |value| {
                    defer allocator.free(value);
                    try std.testing.expect(std.mem.eql(u8, value, pair[1]));
                } else {
                    try std.testing.expect(false);
                }
            }
        }
    }

    i = 0;
    while (i < 200) : (i += 1) {
        for (test_pairs) |pair| {
            const key = try std.fmt.allocPrint(allocator, "{s}_{}", .{ pair[0], i });
            defer allocator.free(key);
            if (try lsm.get(key)) |value| {
                defer allocator.free(value);
                try std.testing.expect(std.mem.eql(u8, value, pair[1]));
            } else {
                try std.testing.expect(false);
            }
        }
    }

    try std.testing.expect(lsm.level_sizes[0] < 4096);
    try std.testing.expect(lsm.level_sizes[1] > 0);
}

test "LSMTree compaction behavior" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer std.testing.expect(gpa.deinit() == .ok) catch @panic("memory leak detected");
    const allocator = gpa.allocator();

    var lsm = try LSMTree.init(allocator);
    defer lsm.deinit();

    var i: usize = 0;
    while (i < 800) : (i += 1) {
        const key = try std.fmt.allocPrint(allocator, "key_{}", .{i});
        defer allocator.free(key);
        const value = try std.fmt.allocPrint(allocator, "value_{}", .{i});
        defer allocator.free(value);
        try lsm.put(key, value);
    }

    var level: usize = 1;
    while (level < LSMTree.MAX_LEVEL) : (level += 1) {
        if (lsm.level_sizes[level] > 0 and lsm.level_sizes[level - 1] > 0) {
            const size_ratio = lsm.level_sizes[level] / lsm.level_sizes[level - 1];
            try std.testing.expect(size_ratio <= LSMTree.LEVEL_SIZE_MULTIPLIER);
        }
    }

    i = 0;
    while (i < 800) : (i += 1) {
        const key = try std.fmt.allocPrint(allocator, "key_{}", .{i});
        defer allocator.free(key);
        const expected_value = try std.fmt.allocPrint(allocator, "value_{}", .{i});
        defer allocator.free(expected_value);
        if (try lsm.get(key)) |value| {
            defer allocator.free(value);
            try std.testing.expect(std.mem.eql(u8, value, expected_value));
        } else {
            try std.testing.expect(false);
        }
    }

    try std.testing.expect(lsm.level_sizes[0] < 4);

    if (lsm.level_sizes[1] == 0) {
        try lsm.forceCompaction(0);
    }

    try std.testing.expect(lsm.level_sizes[1] > 0);
}
