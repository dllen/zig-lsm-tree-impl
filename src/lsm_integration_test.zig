const std = @import("std");
const LSMTree = @import("lsm.zig").LSMTree;

test "LSMTree put and get cover basic lifecycle" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer std.testing.expect(gpa.deinit() == .ok) catch @panic("memory leak detected");
    const allocator = gpa.allocator();

    var lsm = try LSMTree.init(allocator);
    defer lsm.deinit();

    try lsm.put("version", "first");
    if (try lsm.get("version")) |value| {
        defer allocator.free(value);
        try std.testing.expect(std.mem.eql(u8, value, "first"));
    } else {
        try std.testing.expect(false);
    }
}

test "LSMTree flush writes memtable contents to disk" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer std.testing.expect(gpa.deinit() == .ok) catch @panic("memory leak detected");
    const allocator = gpa.allocator();

    var lsm = try LSMTree.init(allocator);
    defer lsm.deinit();

    var i: usize = 0;
    while (i < 300) : (i += 1) {
        const key = try std.fmt.allocPrint(allocator, "mem_{}", .{i});
        defer allocator.free(key);
        const value = try std.fmt.allocPrint(allocator, "data_{}", .{i});
        defer allocator.free(value);
        try lsm.put(key, value);
    }

    try std.testing.expect(lsm.level_sizes[0] > 0);
}

test "LSMTree compaction produces readable data" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer std.testing.expect(gpa.deinit() == .ok) catch @panic("memory leak detected");
    const allocator = gpa.allocator();

    var lsm = try LSMTree.init(allocator);
    defer lsm.deinit();

    try lsm.put("alpha", "1");
    try lsm.put("beta", "2");

    try lsm.forceCompaction(0);
    try std.testing.expect(lsm.level_sizes[1] > 0);

    if (try lsm.get("alpha")) |value| {
        defer allocator.free(value);
        try std.testing.expect(std.mem.eql(u8, value, "1"));
    } else {
        try std.testing.expect(false);
    }
}

test "LSMTree later writes shadow older values after compaction" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer std.testing.expect(gpa.deinit() == .ok) catch @panic("memory leak detected");
    const allocator = gpa.allocator();

    var lsm = try LSMTree.init(allocator);
    defer lsm.deinit();

    try lsm.put("fruit", "apple");
    try lsm.put("fruit", "banana");
    try lsm.forceCompaction(0);

    if (try lsm.get("fruit")) |value| {
        defer allocator.free(value);
        try std.testing.expect(std.mem.eql(u8, value, "banana"));
    } else {
        try std.testing.expect(false);
    }
}
