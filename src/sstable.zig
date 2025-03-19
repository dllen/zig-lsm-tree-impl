const std = @import("std");
const Allocator = std.mem.Allocator;

/// SSTable represents a Sorted String Table on disk
pub const SSTable = struct {
    const Self = @This();

    /// Entry represents a key-value pair in the SSTable
    pub const Entry = struct {
        key: []const u8,
        value: []const u8,
        timestamp: i64,
    };

    allocator: Allocator,
    file_path: []const u8,
    index: std.StringHashMap(u64), // key -> offset mapping
    file: std.fs.File,

    pub fn create(allocator: Allocator, file_path: []const u8) !*Self {
        const file = try std.fs.cwd().createFile(file_path, .{ .read = true });
        const self = try allocator.create(Self);
        self.* = Self{
            .allocator = allocator,
            .file_path = try allocator.dupe(u8, file_path),
            .index = std.StringHashMap(u64).init(allocator),
            .file = file,
        };
        return self;
    }

    pub fn deinit(self: *Self) void {
        self.file.close();
        self.index.deinit();
        self.allocator.free(self.file_path);
        self.allocator.destroy(self);
    }

    pub fn write(self: *Self, entries: []const Entry) !void {
        var writer = self.file.writer();

        // Write entries sorted by key
        for (entries) |entry| {
            const offset = try self.file.getPos();
            try self.index.put(entry.key, offset);

            // Write key length and key
            try writer.writeInt(u32, @intCast(entry.key.len), .little);
            try writer.writeAll(entry.key);

            // Write value length and value
            try writer.writeInt(u32, @intCast(entry.value.len), .little);
            try writer.writeAll(entry.value);

            // Write timestamp
            try writer.writeInt(i64, entry.timestamp, .little);
        }
    }

    pub fn get(self: *Self, key: []const u8) !?[]const u8 {
        const offset = self.index.get(key) orelse return null;
        try self.file.seekTo(offset);

        var reader = self.file.reader();

        // Read key length and key
        const key_len = try reader.readInt(u32, .little);
        const key_buf = try self.allocator.alloc(u8, key_len);
        defer self.allocator.free(key_buf);
        try reader.readNoEof(key_buf);

        // Verify key matches
        if (!std.mem.eql(u8, key, key_buf)) return null;

        // Read value length and value
        const value_len = try reader.readInt(u32, .little);
        const value = try self.allocator.alloc(u8, value_len);
        try reader.readNoEof(value);

        return value;
    }

    pub fn readAllEntries(self: *Self) !std.ArrayList(Entry) {
        var entries = std.ArrayList(Entry).init(self.allocator);
        errdefer entries.deinit();

        try self.file.seekTo(0);
        var reader = self.file.reader();

        while (true) {
            _ = try self.file.getPos();
            const key_len = reader.readInt(u32, .little) catch |err| switch (err) {
                error.EndOfStream => break,
                else => return err,
            };

            const key = try self.allocator.alloc(u8, key_len);
            errdefer self.allocator.free(key);
            try reader.readNoEof(key);

            const value_len = try reader.readInt(u32, .little);
            const value = try self.allocator.alloc(u8, value_len);
            errdefer self.allocator.free(value);
            try reader.readNoEof(value);

            const timestamp = try reader.readInt(i64, .little);

            try entries.append(Entry{
                .key = key,
                .value = value,
                .timestamp = timestamp,
            });
        }

        return entries;
    }
};

test "SSTable basic operations" {
    const allocator = std.testing.allocator;

    // Create a temporary file for testing
    const test_file = "test_sstable";
    defer std.fs.cwd().deleteFile(test_file) catch {};

    var sstable = try SSTable.create(allocator, test_file);
    defer sstable.deinit();

    // Test writing entries
    const entries = [_]SSTable.Entry{
        .{
            .key = "key1",
            .value = "value1",
            .timestamp = 1,
        },
        .{
            .key = "key2",
            .value = "value2",
            .timestamp = 2,
        },
    };

    try sstable.write(&entries);

    // Test reading entries
    const value1 = try sstable.get("key1");
    try std.testing.expect(std.mem.eql(u8, value1.?, "value1"));

    const value2 = try sstable.get("key2");
    try std.testing.expect(std.mem.eql(u8, value2.?, "value2"));

    const non_existent = try sstable.get("key3");
    try std.testing.expect(non_existent == null);
}
