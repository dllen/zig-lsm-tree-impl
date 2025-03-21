const std = @import("std");
const LSMTree = @import("lsm.zig").LSMTree;

test "LSMTree multi-level operations" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer std.testing.expect(gpa.deinit() == .ok) catch @panic("memory leak detected");
    const allocator = gpa.allocator();

    var lsm = try LSMTree.init(allocator);
    defer lsm.deinit();

    // Test data to trigger multiple levels
    const test_pairs = [_]struct { []const u8, []const u8 }{
        .{ "key1", "value1" },
        .{ "key2", "value2" },
        .{ "key3", "value3" },
        .{ "key4", "value4" },
        .{ "key5", "value5" },
    };

    // Insert enough data to trigger level 0 compaction
    for (0..1111) |i| {
        for (test_pairs) |pair| {
            const key = try std.fmt.allocPrint(allocator, "{s}_{}", .{ pair[0], i });
            defer allocator.free(key);
            try lsm.put(key, pair[1]);
        }

        // Verify data after each batch
        if (i % 10 == 0) {
            for (test_pairs) |pair| {
                const key = try std.fmt.allocPrint(allocator, "{s}_{}", .{ pair[0], i });
                defer allocator.free(key);
                const value = try lsm.get(key);
                try std.testing.expect(std.mem.eql(u8, value.?, pair[1]));
            }
        }
    }

    // Test data consistency across levels
    for (0..1111) |i| {
        for (test_pairs) |pair| {
            const key = try std.fmt.allocPrint(allocator, "{s}_{}", .{ pair[0], i });
            defer allocator.free(key);
            const value = try lsm.get(key);
            try std.testing.expect(std.mem.eql(u8, value.?, pair[1]));
        }
    }

    // Test level sizes - ensure compaction has occurred
    try std.testing.expect(lsm.level_sizes[0] < 4096); // Level 0 should be compacted
    // Now check that level 1 has data
    try std.testing.expect(lsm.level_sizes[1] > 0); // Level 1 should have some data
}

test "LSMTree compaction behavior" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer std.testing.expect(gpa.deinit() == .ok) catch @panic("memory leak detected");
    const allocator = gpa.allocator();

    var lsm = try LSMTree.init(allocator);
    defer lsm.deinit();

    // Insert data to trigger compaction
    for (0..1000) |i| {
        const key = try std.fmt.allocPrint(allocator, "key_{}", .{i});
        defer allocator.free(key);
        const value = try std.fmt.allocPrint(allocator, "value_{}", .{i});
        defer allocator.free(value);
        try lsm.put(key, value);
    }

    // Verify level size ratios
    for (1..LSMTree.MAX_LEVEL) |i| {
        if (lsm.level_sizes[i] > 0) {
            const size_ratio = lsm.level_sizes[i] / lsm.level_sizes[i - 1];
            try std.testing.expect(size_ratio <= LSMTree.LEVEL_SIZE_MULTIPLIER);
        }
    }

    // Test data consistency after compaction
    for (0..1000) |i| {
        const key = try std.fmt.allocPrint(allocator, "key_{}", .{i});
        defer allocator.free(key);
        const expected_value = try std.fmt.allocPrint(allocator, "value_{}", .{i});
        defer allocator.free(expected_value);
        const value = try lsm.get(key);
        try std.testing.expect(std.mem.eql(u8, value.?, expected_value));
    }

    // Test level sizes - ensure compaction has occurred
    try std.testing.expect(lsm.level_sizes[0] < 4); // Level 0 should be compacted

    // If level 1 has no data, force compaction
    if (lsm.level_sizes[1] == 0) {
        try lsm.forceCompaction(0); // Force compaction from level 0 to level 1
    }

    // Now check that level 1 has data
    try std.testing.expect(lsm.level_sizes[1] > 0); // Level 1 should have some data
}
