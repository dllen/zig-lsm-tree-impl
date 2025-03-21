const std = @import("std");
const Allocator = std.mem.Allocator;

/// SkipNode represents a node in the skip list
pub const SkipNode = struct {
    key: []const u8,
    value: []const u8,
    next: []?*SkipNode,
    level: usize,

    pub fn init(allocator: Allocator, key: []const u8, value: []const u8, level: usize) !*SkipNode {
        const node = try allocator.create(SkipNode);
        node.* = SkipNode{
            .key = try allocator.dupe(u8, key),
            .value = try allocator.dupe(u8, value),
            .next = blk: {
                const next = try allocator.alloc(?*SkipNode, level + 1);
                for (next) |*n| n.* = null;
                break :blk next;
            },
            .level = level,
        };
        return node;
    }

    pub fn deinit(self: *SkipNode, allocator: Allocator) void {
        allocator.free(self.key);
        allocator.free(self.value);
        allocator.free(self.next);
        allocator.destroy(self);
    }
};

/// MemTable implements an in-memory sorted key-value store using a skip list
pub const MemTable = struct {
    const MAX_LEVEL = 16;
    const P = 0.5; // Probability for level generation

    allocator: Allocator,
    head: *SkipNode,
    level: usize,
    size: usize,
    prng: std.Random.Xoshiro256,

    pub fn init(allocator: Allocator) !*MemTable {
        const self = try allocator.create(MemTable);
        const head = try SkipNode.init(allocator, "", "", MAX_LEVEL);
        const prng = std.Random.Xoshiro256.init(0); // Using static seed for initialization
        self.* = MemTable{
            .allocator = allocator,
            .head = head,
            .level = 0,
            .size = 0,
            .prng = prng,
        };
        return self;
    }

    pub fn deinit(self: *MemTable) void {
        // Free nodes one by one, starting from head
        var current = self.head;

        // Store the next node before freeing the current one
        while (true) {
            var next_node: ?*SkipNode = null;

            // Get the next node if it exists
            if (current.next.len > 0) {
                next_node = current.next[0];
            }

            // Free the current node
            current.deinit(self.allocator);

            // Move to the next node or exit if we've reached the end
            if (next_node) |next| {
                current = next;
            } else {
                break;
            }
        }

        self.allocator.destroy(self);
    }

    fn randomLevel(self: *MemTable) usize {
        var level: usize = 0;
        while (self.prng.random().float(f64) < P and level < MAX_LEVEL) {
            level += 1;
        }
        return level;
    }

    pub fn get(self: *MemTable, key: []const u8) ?[]const u8 {
        var current = self.head;
        var i: usize = self.level;
        
        // Use a different loop structure to avoid integer underflow
        while (true) {
            while (current.next[i]) |next| {
                const cmp = std.mem.order(u8, next.key, key);
                if (cmp == .eq) return next.value;
                if (cmp == .gt) break;
                current = next;
            }
            
            if (i == 0) break;
            i -= 1;
        }
        
        return null;
    }

    pub fn put(self: *MemTable, key: []const u8, value: []const u8) !void {
        // Allocate update array for all possible levels
        var update = try self.allocator.alloc(*SkipNode, MAX_LEVEL + 1);
        defer self.allocator.free(update);

        // Initialize all update entries to head
        for (0..MAX_LEVEL + 1) |j| {
            update[j] = self.head;
        }

        // Find position for insertion
        var current = self.head;
        var i: usize = self.level;
        while (true) {
            // Check if current node's next at level i has a key > our key
            if (i < current.next.len) {
                if (current.next[i]) |next| {
                    const cmp = std.mem.order(u8, next.key, key);
                    if (cmp == .eq) {
                        // Key already exists, update value
                        const old_node = next;
                        const new_node = try SkipNode.init(self.allocator, key, value, old_node.level);

                        // Copy all next pointers
                        for (0..old_node.next.len) |l| {
                            if (l < new_node.next.len) {
                                new_node.next[l] = old_node.next[l];
                            }
                        }

                        // Update all references to the old node
                        for (0..old_node.level + 1) |l| {
                            if (l < update[l].next.len and update[l].next[l] == old_node) {
                                update[l].next[l] = new_node;
                            }
                        }

                        // Free old node
                        old_node.deinit(self.allocator);
                        return;
                    }
                    if (cmp == .lt) {
                        current = next;
                        continue;
                    }
                }
            }

            // Save current node as update for level i
            update[i] = current;

            // Move to next level down
            if (i == 0) break;
            i -= 1;
        }

        // Generate random level for new node
        const newLevel = self.randomLevel();

        // Create new node
        const node = try SkipNode.init(self.allocator, key, value, newLevel);
        errdefer node.deinit(self.allocator);

        // Update skip list level if necessary
        if (newLevel > self.level) {
            self.level = newLevel;
        }

        // Insert node at each level
        for (0..newLevel + 1) |level| {
            if (level < node.next.len and level < update[level].next.len) {
                node.next[level] = update[level].next[level];
                update[level].next[level] = node;
            }
        }

        self.size += 1;
    }
};

test "MemTable basic operations" {
    const allocator = std.testing.allocator;
    var memtable = try MemTable.init(allocator);
    defer memtable.deinit();

    try memtable.put("key1", "value1");
    try memtable.put("key2", "value2");

    const value1 = memtable.get("key1");
    try std.testing.expect(std.mem.eql(u8, value1.?, "value1"));

    const value2 = memtable.get("key2");
    try std.testing.expect(std.mem.eql(u8, value2.?, "value2"));

    const non_existent = memtable.get("key3");
    try std.testing.expect(non_existent == null);
}
