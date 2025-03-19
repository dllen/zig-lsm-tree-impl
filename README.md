# Zig LSM Tree

A Log-Structured Merge (LSM) Tree implementation in Zig, providing efficient storage and retrieval of key-value pairs with optimized write performance.

## Features

- **MemTable**: In-memory storage using Skip List for fast operations
- **SSTable**: On-disk storage format for persistent data
- **Basic Operations**: Support for get/put operations
- **Compaction**: Background process to merge and optimize storage (planned)
- **Bloom Filters**: Efficient membership testing to improve read performance (planned)

## Project Structure

```
src/
  ├── lsm.zig       # Main LSM tree implementation
  ├── memtable.zig  # In-memory table implementation
  ├── sstable.zig   # Sorted string table implementation
  ├── main.zig      # Example usage and benchmarks
  └── root.zig      # Library root
```

## Building

To build the project:

```bash
zig build
```

To run tests:

```bash
zig build test
```

To run the example:

```bash
zig build run
```

## Usage

```zig
const std = @import("std");
const LSMTree = @import("zig_lsm_tree_lib");

pub fn main() !void {
    // Initialize LSM tree
    var lsm = try LSMTree.init(allocator);
    defer lsm.deinit();

    // Basic operations
    try lsm.put("key", "value");
    if (try lsm.get("key")) |value| {
        std.debug.print("Value: {s}\n", .{value});
    }
}
```

## Implementation Details

### MemTable

- Uses Skip List for efficient in-memory storage
- Provides O(log n) complexity for operations
- Automatically flushes to SSTable when size threshold is reached

### SSTable

- Immutable sorted files on disk
- Includes index blocks for quick lookups
- Supports efficient range queries

### Compaction Strategy

- Size-tiered compaction (planned)
- Leveled compaction (planned)
- Background compaction process

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.