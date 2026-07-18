const std = @import("std");

const MemTable = @import("memtable.zig").MemTable;

test "MemTable empty lookup returns null" {
    const allocator = std.testing.allocator;
    var memtable = try MemTable.init(allocator);
    defer memtable.deinit();

    try std.testing.expect(memtable.get("missing") == null);
}

test "MemTable stores and retrieves values" {
    const allocator = std.testing.allocator;
    var memtable = try MemTable.init(allocator);
    defer memtable.deinit();

    try memtable.put("color", "red");
    try std.testing.expect(std.mem.eql(u8, memtable.get("color").?, "red"));
}

test "MemTable updates existing keys" {
    const allocator = std.testing.allocator;
    var memtable = try MemTable.init(allocator);
    defer memtable.deinit();

    try memtable.put("size", "small");
    try memtable.put("size", "large");
    try std.testing.expect(std.mem.eql(u8, memtable.get("size").?, "large"));
}

test "MemTable maintains sorted key order" {
    const allocator = std.testing.allocator;
    var memtable = try MemTable.init(allocator);
    defer memtable.deinit();

    try memtable.put("banana", "1");
    try memtable.put("apple", "2");
    try memtable.put("cherry", "3");

    try std.testing.expect(std.mem.order(u8, "apple", "banana") == .lt);
    try std.testing.expect(std.mem.order(u8, "banana", "cherry") == .lt);

    const apple = memtable.get("apple");
    const banana = memtable.get("banana");
    const cherry = memtable.get("cherry");
    try std.testing.expect(std.mem.eql(u8, apple.?, "2"));
    try std.testing.expect(std.mem.eql(u8, banana.?, "1"));
    try std.testing.expect(std.mem.eql(u8, cherry.?, "3"));
}

test "MemTable size grows with inserts" {
    const allocator = std.testing.allocator;
    var memtable = try MemTable.init(allocator);
    defer memtable.deinit();

    try std.testing.expect(memtable.size == 0);
    try memtable.put("a", "1");
    try memtable.put("b", "2");
    try std.testing.expect(memtable.size == 2);
}
