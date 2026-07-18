const std = @import("std");
const Allocator = std.mem.Allocator;
const MemTable = @import("memtable.zig").MemTable;
const SSTable = @import("sstable.zig").SSTable;

/// A minimal LSM-tree teaching implementation.
///
/// Writes always go to the in-memory MemTable first. When the MemTable
/// exceeds the configured size threshold, it is flushed to an immutable
/// SSTable on disk. If the number of SSTables in a level exceeds the
/// compaction threshold, multiple levels are merged into the next level.
pub const LSMTree = struct {
    const Self = @This();

    /// Maximum number of entries stored in a single MemTable before flush.
    pub const MAX_MEMTABLE_SIZE = 256;

    /// Maximum number of levels in the LSM tree.
    pub const MAX_LEVEL = 7;

    /// Size multiplier between consecutive levels for compaction thresholds.
    pub const LEVEL_SIZE_MULTIPLIER = 10;

    allocator: Allocator,
    memtable: *MemTable,
    levels: [MAX_LEVEL]std.ArrayList(*SSTable),
    level_sizes: [MAX_LEVEL]usize,
    sstable_counter: usize,

    /// Initialize a new LSM tree instance.
    pub fn init(allocator: Allocator) !*Self {
        const self = try allocator.create(Self);
        self.allocator = allocator;
        self.memtable = try MemTable.init(allocator);
        self.level_sizes = [_]usize{0} ** MAX_LEVEL;
        self.sstable_counter = 0;

        for (0..MAX_LEVEL) |i| {
            self.levels[i] = std.ArrayList(*SSTable).init(allocator);
        }
        return self;
    }

    /// Release all resources owned by the LSM tree.
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

    /// Look up a key.
    ///
    /// The returned memory is owned by the caller and must be released with
    /// the same allocator passed to `init`.
    pub fn get(self: *Self, key: []const u8) !?[]const u8 {
        // First check memtable.
        if (self.memtable.get(key)) |value| {
            return try self.allocator.dupe(u8, value);
        }

        // Then check SSTables from newest to oldest within each level.
        for (self.levels) |level| {
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

    /// Insert or update a key-value pair.
    ///
    /// The LSM tree copies the provided key/value internally, so the caller
    /// does not need to keep them alive after this call returns.
    pub fn put(self: *Self, key: []const u8, value: []const u8) !void {
        try self.memtable.put(key, value);

        if (self.memtable.size >= MAX_MEMTABLE_SIZE) {
            try self.flushMemTable();
        }
    }

    /// Flush the current MemTable to a new SSTable in level 0.
    fn flushMemTable(self: *Self) !void {
        const sstable_path = try std.fmt.allocPrint(
            self.allocator,
            "L0_sstable_{}.db",
            .{self.sstable_counter},
        );
        defer self.allocator.free(sstable_path);

        const sstable = try SSTable.create(self.allocator, sstable_path);
        errdefer sstable.deinit();

        var entries = std.ArrayList(SSTable.Entry).init(self.allocator);
        defer entries.deinit();

        var current = self.memtable.head;
        while (current.next[0]) |next| {
            const timestamp = std.time.timestamp();
            try entries.append(.{
                .key = try self.allocator.dupe(u8, next.key),
                .value = try self.allocator.dupe(u8, next.value),
                .timestamp = timestamp,
            });
            current = next;
        }

        try sstable.write(entries.items);
        try self.levels[0].append(sstable);

        self.level_sizes[0] += entries.items.len;
        self.memtable.deinit();
        self.memtable = try MemTable.init(self.allocator);
        self.sstable_counter += 1;

        // Trigger compaction when level 0 exceeds the threshold.
        if (self.level_sizes[0] >= self.levelThreshold(0)) {
            try self.compact();
        }
    }

    /// Walk the LSM tree and compact levels that exceed their thresholds.
    pub fn compact(self: *Self) !void {
        var level: usize = 0;
        while (level < MAX_LEVEL - 1) {
            const threshold = Self.levelThreshold(level);
            if (self.level_sizes[level] < threshold) {
                break;
            }
            try self.mergeLevel(level);
            level += 1;
        }
    }

    /// Merge a level into the next level when its size exceeds the threshold.
    fn mergeLevel(self: *Self, level: usize) !void {
        const next_level = level + 1;
        if (next_level >= MAX_LEVEL) return;

        const merged_path = try std.fmt.allocPrint(
            self.allocator,
            "L{}_merged_{}.db",
            .{ next_level, self.sstable_counter },
        );
        defer self.allocator.free(merged_path);

        var merged_table = try SSTable.create(self.allocator, merged_path);
        errdefer merged_table.deinit();

        var all_entries = std.ArrayList(SSTable.Entry).init(self.allocator);
        errdefer all_entries.deinit();

        for (self.levels[level].items) |table| {
            var entries = try table.readAllEntries();
            defer entries.deinit();
            for (entries.items) |entry| {
                try all_entries.append(.{
                    .key = try self.allocator.dupe(u8, entry.key),
                    .value = try self.allocator.dupe(u8, entry.value),
                    .timestamp = entry.timestamp,
                });
            }
        }

        for (self.levels[next_level].items) |table| {
            var entries = try table.readAllEntries();
            defer entries.deinit();
            for (entries.items) |entry| {
                try all_entries.append(.{
                    .key = try self.allocator.dupe(u8, entry.key),
                    .value = try self.allocator.dupe(u8, entry.value),
                    .timestamp = entry.timestamp,
                });
            }
        }

        std.sort.block(SSTable.Entry, all_entries.items, {}, struct {
            pub fn lessThan(_: void, a: SSTable.Entry, b: SSTable.Entry) bool {
                const key_cmp = std.mem.order(u8, a.key, b.key);
                if (key_cmp == .eq) {
                    return a.timestamp > b.timestamp;
                }
                return key_cmp == .lt;
            }
        }.lessThan);

        // Deduplicate while keeping the newest version.
        var deduped = std.ArrayList(SSTable.Entry).init(self.allocator);
        defer deduped.deinit();
        if (all_entries.items.len > 0) {
            var prev_key = all_entries.items[0].key;
            try deduped.append(.{
                .key = try self.allocator.dupe(u8, all_entries.items[0].key),
                .value = try self.allocator.dupe(u8, all_entries.items[0].value),
                .timestamp = all_entries.items[0].timestamp,
            });
            for (all_entries.items[1..]) |entry| {
                if (!std.mem.eql(u8, entry.key, prev_key)) {
                    try deduped.append(.{
                        .key = try self.allocator.dupe(u8, entry.key),
                        .value = try self.allocator.dupe(u8, entry.value),
                        .timestamp = entry.timestamp,
                    });
                    prev_key = entry.key;
                }
            }
        }

        try merged_table.write(deduped.items);
        try self.levels[next_level].append(merged_table);

        // Release duplicate entries and merged SSTable copies.
        for (all_entries.items) |entry| {
            self.allocator.free(entry.key);
            self.allocator.free(entry.value);
        }

        for (self.levels[level].items) |table| {
            table.deinit();
        }
        self.levels[level].clearRetainingCapacity();
        self.level_sizes[level] = 0;
        self.level_sizes[next_level] = deduped.items.len;
        self.sstable_counter += 1;
    }

    /// Force compaction starting from a specific level.
    pub fn forceCompaction(self: *Self, level: usize) !void {
        if (level >= MAX_LEVEL - 1) {
            return;
        }

        if (self.memtable.size > 0) {
            try self.flushMemTable();
        }

        if (self.levels[level].items.len == 0) {
            return;
        }

        try self.mergeLevel(level);
    }

    /// Compute the compaction threshold for a given level.
    fn levelThreshold(level: usize) usize {
        return std.math.pow(usize, LEVEL_SIZE_MULTIPLIER, level + 1);
    }
};

test "LSMTree basic operations" {
    const allocator = std.testing.allocator;
    var lsm = try LSMTree.init(allocator);
    defer lsm.deinit();

    try lsm.put("key1", "value1");
    try lsm.put("key2", "value2");

    const value1 = try lsm.get("key1");
    defer if (value1) |v| allocator.free(v);
    try std.testing.expect(std.mem.eql(u8, value1.?, "value1"));

    const value2 = try lsm.get("key2");
    defer if (value2) |v| allocator.free(v);
    try std.testing.expect(std.mem.eql(u8, value2.?, "value2"));

    const non_existent = try lsm.get("key3");
    try std.testing.expect(non_existent == null);
}
