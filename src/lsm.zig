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
                    // Make a copy that we own and can return safely
                    const result = try self.allocator.dupe(u8, value);
                    // Free the original value from the SSTable
                    self.allocator.free(value);
                    return result;
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
        errdefer sstable.deinit();

        // Convert MemTable entries to SSTable entries
        var entries = std.ArrayList(SSTable.Entry).init(self.allocator);
        defer entries.deinit();

        // Traverse the MemTable's skip list to extract all key-value pairs
        var current = self.memtable.head;
        while (current.next[0]) |next| {
            const timestamp = std.time.timestamp();
            try entries.append(.{
                .key = next.key,
                .value = next.value,
                .timestamp = timestamp,
            });
            current = next;
        }

        // Write entries to SSTable
        try sstable.write(entries.items);

        try self.levels[0].append(sstable);
        // 确保 level_sizes 正确更新
        self.level_sizes[0] += entries.items.len; // 使用实际条目数

        // Create new memtable
        self.memtable.deinit();
        self.memtable = try MemTable.init(self.allocator);
        self.sstable_counter += 1;

        // Check if level 0 needs compaction
        // 检查是否需要压缩
        if (self.level_sizes[0] >= 4096) { // 这个阈值可能需要调整
            try self.compact();
        }
    }

    pub fn compact(self: *Self) !void {
        var level: usize = 0;
        while (level < MAX_LEVEL - 1) : (level += 1) {
            const level_threshold = std.math.pow(usize, LEVEL_SIZE_MULTIPLIER, level + 1);
            if (self.level_sizes[level] >= level_threshold) {
                // 合并当前级别到下一级别
                try self.mergeLevel(level);
                // 检查是否需要继续压缩下一级别
                continue;
            }
            // 如果当前级别不需要压缩，则停止
            break;
        }
    }

    fn mergeLevel(self: *Self, level: usize) !void {
        const next_level = level + 1;
        if (next_level >= MAX_LEVEL) return;

        // 创建新的SSTable用于合并结果
        const merged_path = try std.fmt.allocPrint(
            self.allocator,
            "L{}_merged_{}.db",
            .{ next_level, self.sstable_counter },
        );
        defer self.allocator.free(merged_path);

        var merged_table = try SSTable.create(self.allocator, merged_path);
        errdefer merged_table.deinit();

        // 收集当前级别和下一级别的所有条目
        var all_entries = std.ArrayList(SSTable.Entry).init(self.allocator);
        defer {
            // 清理所有条目的内存
            for (all_entries.items) |entry| {
                self.allocator.free(entry.key);
                self.allocator.free(entry.value);
            }
            all_entries.deinit();
        }

        // 读取当前级别的条目
        var current_level_entries: usize = 0;
        for (self.levels[level].items) |table| {
            var entries = try table.readAllEntries();
            defer {
                for (entries.items) |entry| {
                    self.allocator.free(entry.key);
                    self.allocator.free(entry.value);
                }
                entries.deinit();
            }
            current_level_entries += entries.items.len;
            for (entries.items) |entry| {
                try all_entries.append(.{ .key = try self.allocator.dupe(u8, entry.key), .value = try self.allocator.dupe(u8, entry.value), .timestamp = entry.timestamp });
            }
        }

        // 读取下一级别的条目
        var next_level_entries: usize = 0;
        for (self.levels[next_level].items) |table| {
            var entries = try table.readAllEntries();
            defer {
                for (entries.items) |entry| {
                    self.allocator.free(entry.key);
                    self.allocator.free(entry.value);
                }
                entries.deinit();
            }
            next_level_entries += entries.items.len;
            for (entries.items) |entry| {
                try all_entries.append(.{ .key = try self.allocator.dupe(u8, entry.key), .value = try self.allocator.dupe(u8, entry.value), .timestamp = entry.timestamp });
            }
        }

        // 按键和时间戳排序条目
        std.sort.block(SSTable.Entry, all_entries.items, {}, struct {
            pub fn lessThan(_: void, a: SSTable.Entry, b: SSTable.Entry) bool {
                const key_cmp = std.mem.order(u8, a.key, b.key);
                if (key_cmp == .eq) {
                    return a.timestamp > b.timestamp; // 更新的条目优先
                }
                return key_cmp == .lt;
            }
        }.lessThan);

        // 写入合并后的条目到新的SSTable
        try merged_table.write(all_entries.items);

        // 更新级别信息
        try self.levels[next_level].append(merged_table);

        // 更新level_sizes - 使用实际条目数
        self.level_sizes[next_level] = all_entries.items.len;

        // 清理当前级别的旧表
        for (self.levels[level].items) |table| {
            table.deinit();
        }
        try self.levels[level].resize(0);
        self.level_sizes[level] = 0;

        self.sstable_counter += 1;

        // 打印调试信息
        std.debug.print("合并完成: level_sizes[{}] = {}, level_sizes[{}] = {}\n", .{ level, self.level_sizes[level], next_level, self.level_sizes[next_level] });
    }

    pub fn forceCompaction(self: *Self, level: usize) !void {
        if (level >= MAX_LEVEL - 1) {
            std.debug.print("无法压缩最后一级\n", .{});
            return;
        }

        // 如果内存表有数据，先刷新到level 0
        if (self.memtable.size > 0) {
            std.debug.print("刷新内存表到level 0\n", .{});
            try self.flushMemTable();
        }

        // 如果当前级别没有数据，无法压缩
        if (self.levels[level].items.len == 0) {
            std.debug.print("level {} 没有数据，无法压缩\n", .{level});
            return;
        }

        std.debug.print("强制压缩 level {} -> level {}\n", .{ level, level + 1 });

        // 直接调用mergeLevel强制压缩
        try self.mergeLevel(level);

        // 确保压缩后level_sizes正确更新
        std.debug.print("强制压缩完成: level_sizes[{}] = {}, level_sizes[{}] = {}\n", .{ level, self.level_sizes[level], level + 1, self.level_sizes[level + 1] });
    }
};

test "LSMTree basic operations" {
    const allocator = std.testing.allocator;
    var lsm = try LSMTree.init(allocator);
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
