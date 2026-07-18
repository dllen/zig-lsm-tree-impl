const std = @import("std");

const SSTable = @import("sstable.zig").SSTable;

test "SSTable returns null for missing keys" {
    const allocator = std.testing.allocator;
    const test_file = "test_sstable_missing";
    defer std.fs.cwd().deleteFile(test_file) catch {};

    var sstable = try SSTable.create(allocator, test_file);
    defer sstable.deinit();

    const entries = [_]SSTable.Entry{
        .{ .key = "alpha", .value = "1", .timestamp = 1 },
    };
    try sstable.write(&entries);

    try std.testing.expect((try sstable.get("zeta")) == null);
}

test "SSTable preserves timestamps during writes" {
    const allocator = std.testing.allocator;
    const test_file = "test_sstable_timestamps";
    defer std.fs.cwd().deleteFile(test_file) catch {};

    var sstable = try SSTable.create(allocator, test_file);
    defer sstable.deinit();

    const entries = [_]SSTable.Entry{
        .{ .key = "x", .value = "old", .timestamp = 1 },
        .{ .key = "x", .value = "new", .timestamp = 5 },
    };
    try sstable.write(&entries);

    const value = try sstable.get("x");
    defer if (value) |v| allocator.free(v);
    try std.testing.expect(std.mem.eql(u8, value.?, "new"));
}

test "SSTable readAllEntries returns written entries" {
    const allocator = std.testing.allocator;
    const test_file = "test_sstable_read_all";
    defer std.fs.cwd().deleteFile(test_file) catch {};

    var sstable = try SSTable.create(allocator, test_file);
    defer sstable.deinit();

    const entries = [_]SSTable.Entry{
        .{ .key = "aa", .value = "11", .timestamp = 1 },
        .{ .key = "bb", .value = "22", .timestamp = 2 },
    };
    try sstable.write(&entries);

    var all = try sstable.readAllEntries();
    defer all.deinit();

    try std.testing.expect(all.items.len == 2);
    try std.testing.expect(std.mem.eql(u8, all.items[0].key, "aa"));
    try std.testing.expect(std.mem.eql(u8, all.items[0].value, "11"));
    try std.testing.expect(std.mem.eql(u8, all.items[1].key, "bb"));
}
