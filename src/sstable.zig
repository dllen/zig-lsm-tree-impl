const std = @import("std");
const Allocator = std.mem.Allocator;

/// SSTable represents a Sorted String Table on disk.
pub const SSTable = struct {
    const Self = @This();

    /// Entry represents a key-value pair in the SSTable.
    pub const Entry = struct {
        key: []const u8,
        value: []const u8,
        timestamp: i64,
    };

    allocator: Allocator,
    file_path: []const u8,
    index: std.StringHashMap(u64),
    file: std.fs.File,

    /// Create or open an SSTable file for writing.
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

    /// Release resources owned by the SSTable.
    pub fn deinit(self: *Self) void {
        self.file.close();
        self.index.deinit();
        self.allocator.free(self.file_path);
        self.allocator.destroy(self);
    }

    /// Write entries to the SSTable file in the current file position.
    pub fn write(self: *Self, entries: []const Entry) !void {
        var writer = self.file.writer();

        for (entries) |entry| {
            const offset = try self.file.getPos();
            try self.index.put(try self.allocator.dupe(u8, entry.key), offset);

            try writer.writeInt(u32, @intCast(entry.key.len), .little);
            try writer.writeAll(entry.key);

            try writer.writeInt(u32, @intCast(entry.value.len), .little);
            try writer.writeAll(entry.value);

            try writer.writeInt(i64, entry.timestamp, .little);
        }
    }

    /// Read the value for a key if it exists.
    ///
    /// The returned memory is owned by the caller and must be released with
    /// the allocator passed to `create`.
    pub fn get(self: *Self, key: []const u8) !?[]const u8 {
        const offset = self.index.get(key) orelse return null;
        try self.file.seekTo(offset);

        var reader = self.file.reader();

        const key_len = try reader.readInt(u32, .little);
        const key_buf = try self.allocator.alloc(u8, key_len);
        defer self.allocator.free(key_buf);
        try reader.readNoEof(key_buf);

        if (!std.mem.eql(u8, key, key_buf)) return null;

        const value_len = try reader.readInt(u32, .little);
        const value = try self.allocator.alloc(u8, value_len);
        errdefer self.allocator.free(value);
        try reader.readNoEof(value);

        return value;
    }

    /// Read all entries from the SSTable sequentially.
    pub fn readAllEntries(self: *Self) !std.ArrayList(Entry) {
        var entries = std.ArrayList(Entry).init(self.allocator);
        errdefer entries.deinit();

        try self.file.seekTo(0);
        var reader = self.file.reader();

        while (true) {
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
    const test_file = "test_sstable";
    defer std.fs.cwd().deleteFile(test_file) catch {};

    var sstable = try SSTable.create(allocator, test_file);
    defer sstable.deinit();

    const entries = [_]SSTable.Entry{
        .{ .key = "key1", .value = "value1", .timestamp = 1 },
        .{ .key = "key2", .value = "value2", .timestamp = 2 },
    };

    try sstable.write(&entries);

    const value1 = try sstable.get("key1");
    defer if (value1) |v| allocator.free(v);
    try std.testing.expect(std.mem.eql(u8, value1.?, "value1"));

    const value2 = try sstable.get("key2");
    defer if (value2) |v| allocator.free(v);
    try std.testing.expect(std.mem.eql(u8, value2.?, "value2"));

    const non_existent = try sstable.get("key3");
    try std.testing.expect(non_existent == null);
}
