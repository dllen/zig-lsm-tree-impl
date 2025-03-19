const std = @import("std");
const Allocator = std.mem.Allocator;
const MemTable = @import("memtable.zig").MemTable;
const SSTable = @import("sstable.zig").SSTable;

pub const LSMTree = struct {
    const Self = @This();
    pub const MAX_MEMTABLE_SIZE = 1024 * 1024; // 1MB
    pub const MAX_LEVEL = 7; // Maximum number of levels
    pub const LEVEL_SIZE_MULTIPLIER = 10; // Size ratio between levels

    allocator: Allocator,
    memtable: *MemTable,
    levels: [MAX_LEVEL]std.ArrayList(*SSTable),
    level_sizes: [MAX_LEVEL]usize,
    sstable_counter: usize,

    pub fn init(allocator: Allocator) !*Self {
        const self = try allocator.create(Self);
        self.allocator = allocator;
        self.memtable = try MemTable.init(allocator);
        self.level_sizes = [_]usize{0} ** MAX_LEVEL;
        self.sstable_counter = 0;

        // Initialize level arrays
        for (0..MAX_LEVEL) |i| {
            self.levels[i] = std.ArrayList(*SSTable).init(allocator);
        }
        return self;
    }

    pub fn deinit(self: *Self) void {
        self.memtable.deinit();
        for (self.levels) |level| {
            for (level.items) |sstable| {
                sstable.deinit();
            }
            level.deinit();
        }
        self.allocator.destroy(self);
    }

    pub fn get(self: *Self, key: []const u8) !?[]const u8 {
        // First check memtable
        if (self.memtable.get(key)) |value| {
            return value;
        }

        // Check each level from top to bottom
        for (self.levels) |level| {
            // Check sstables in the level from newest to oldest
            var i: usize = level.items.len;
            while (i > 0) {
                i -= 1;
                if (try level.items[i].get(key)) |value| {
                    return value;
                }
            }
        }

        return null;
    }

    pub fn put(self: *Self, key: []const u8, value: []const u8) !void {
        try self.memtable.put(key, value);

        // Check if memtable size exceeds threshold
        if (self.memtable.size >= MAX_MEMTABLE_SIZE) {
            try self.flushMemTable();
        }
    }

    fn flushMemTable(self: *Self) !void {
        const sstable_path = try std.fmt.allocPrint(
            self.allocator,
            "L0_sstable_{}.db",
            .{self.sstable_counter},
        );
        defer self.allocator.free(sstable_path);

        const sstable = try SSTable.create(self.allocator, sstable_path);
        // TODO: Implement conversion from MemTable to SSTable entries
        try self.levels[0].append(sstable);
        self.level_sizes[0] += 1;

        // Create new memtable
        self.memtable.deinit();
        self.memtable = try MemTable.init(self.allocator);
        self.sstable_counter += 1;

        // Check if level 0 needs compaction
        if (self.level_sizes[0] >= 4) { // Level 0 size threshold
            try self.compact();
        }
    }

    pub fn compact(self: *Self) !void {
        var level: usize = 0;
        while (level < MAX_LEVEL - 1) : (level += 1) {
            const level_threshold = std.math.pow(usize, LEVEL_SIZE_MULTIPLIER, level + 1);
            if (self.level_sizes[level] >= level_threshold) {
                // Merge current level into next level
                try self.mergeLevel(level);
            }
        }
    }

    fn mergeLevel(self: *Self, level: usize) !void {
        const next_level = level + 1;
        if (next_level >= MAX_LEVEL) return;

        // Create a new SSTable for the merged result
        const merged_path = try std.fmt.allocPrint(
            self.allocator,
            "L{}_merged_{}.db",
            .{ next_level, self.sstable_counter },
        );
        defer self.allocator.free(merged_path);

        var merged_table = try SSTable.create(self.allocator, merged_path);
        errdefer merged_table.deinit();

        // Collect all entries from current level and next level
        var all_entries = std.ArrayList(SSTable.Entry).init(self.allocator);
        defer all_entries.deinit();

        // Read entries from current level
        for (self.levels[level].items) |table| {
            var entries = try table.readAllEntries();
            defer entries.deinit();
            for (entries.items) |entry| {
                try all_entries.append(.{ .key = try self.allocator.dupe(u8, entry.key), .value = try self.allocator.dupe(u8, entry.value), .timestamp = entry.timestamp });
            }
        }

        // Read entries from next level
        for (self.levels[next_level].items) |table| {
            var entries = try table.readAllEntries();
            defer entries.deinit();
            for (entries.items) |entry| {
                try all_entries.append(.{ .key = try self.allocator.dupe(u8, entry.key), .value = try self.allocator.dupe(u8, entry.value), .timestamp = entry.timestamp });
            }
        }

        // Sort entries by key and timestamp
        std.sort.block(SSTable.Entry, all_entries.items, {}, struct {
            pub fn lessThan(_: void, a: SSTable.Entry, b: SSTable.Entry) bool {
                const key_cmp = std.mem.order(u8, a.key, b.key);
                if (key_cmp == .eq) {
                    return a.timestamp > b.timestamp; // More recent entries come first
                }
                return key_cmp == .lt;
            }
        }.lessThan);

        // Write merged entries to new SSTable
        try merged_table.write(all_entries.items);

        // Update level information
        try self.levels[next_level].append(merged_table);
        self.level_sizes[next_level] += 1;

        // Clean up old tables from current level
        for (self.levels[level].items) |table| {
            table.deinit();
        }
        try self.levels[level].resize(0);
        self.level_sizes[level] = 0;

        self.sstable_counter += 1;
    }
};

test "LSMTree basic operations" {
    const allocator = std.testing.allocator;
    var lsm = try LSMTree.init(allocator.*);
    defer lsm.deinit();

    try lsm.put("key1", "value1");
    try lsm.put("key2", "value2");

    const value1 = try lsm.get("key1");
    try std.testing.expect(std.mem.eql(u8, value1.?, "value1"));

    const value2 = try lsm.get("key2");
    try std.testing.expect(std.mem.eql(u8, value2.?, "value2"));

    const non_existent = try lsm.get("key3");
    try std.testing.expect(non_existent == null);
}
