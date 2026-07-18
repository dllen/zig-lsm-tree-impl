# 代码导读

这份导读按**执行顺序**而不是文件顺序来写：先看一个 `put` 如何一路走到磁盘，再看一个 `get` 如何从内存查到磁盘。读完这份导读后，再回头看各个源文件会更容易。

## 1. 请求入口：`src/main.zig`

`main.zig` 是本项目唯一的可执行入口，用来演示 LSM-Tree 的基本用法。

建议重点看 3 件事：

- 如何创建 `GeneralPurposeAllocator`
- 如何 `init` / `deinit` 一个 `LSMTree`
- 如何在收到 `get` 返回值后 `defer allocator.free(value)`

```zig
var lsm = try LSMTree.init(allocator);
defer lsm.deinit();

try lsm.put("hello", "world");

if (try lsm.get("hello")) |value| {
    defer allocator.free(value);
    std.debug.print("hello = {s}\n", .{value});
}
```

这其实就是教学项目的“最小正确用法模板”。

## 2. 库入口：`src/root.zig`

`root.zig` 是库的根文件。它自己不实现业务逻辑，只做三件事：

1. 导入 `memtable.zig`、`sstable.zig`、`lsm.zig`
2. 把三个模块里的公共类型重新导出
3. 让外部代码可以用 `@import("zig_lsm_tree_lib").LSMTree` 使用本库

对应代码只有 4 行导出：

```zig
pub const MemTable = memtable.MemTable;
pub const SSTable = sstable.SSTable;
pub const LSMTree = lsm.LSMTree;
```

记住这个结构：以后加新模块，通常也在这里补一个导出。

## 3. 内存表：`src/memtable.zig`

`MemTable` 是 LSM-Tree 的**第一站**。本项目用一个简化跳表实现它，核心目标是：把 key 保持在内存里有序。

### 3.1 节点：`SkipNode`

每个节点存：

- `key`
- `value`
- `next`：长度为 `level + 1` 的指针数组，这就是跳表多层的来源
- `level`：当前节点最高参与到的层数

关键细节：

- `init` 会复制 `key/value`，避免外部缓冲区失效
- `deinit` 负责释放 `key`、`value`、`next`，最后销毁节点本身

### 3.2 跳表结构：`MemTable`

字段含义：

- `head`：一个虚拟头节点，简化边界判断
- `level`：当前跳表实际使用的最高层数
- `size`：当前节点个数
- `prng`：伪随机数生成器，用于决定新节点升到哪一层

初始化时创建了一个空头节点，因此插入和查找逻辑里不用反复判断“是否为第一个节点”。

### 3.3 查询：`get`

`get` 的逻辑可以这样理解：

1. 从最高层开始往右走
2. 如果右边 key 更小，继续往右
3. 如果右边 key 更大，就降一层
4. 如果相等，就返回 value

```zig
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
```

这里返回的是节点内部保存的 `value`，**不是 caller-owned 内存**；只有 `LSMTree.get` 和 `SSTable.get` 才需要调用方 `free`。

### 3.4 插入/更新：`put`

`put` 做了两类事：

- 如果 key 已存在，就替换 value，并把旧节点断开
- 如果 key 不存在，就随机生成一个层数，把新节点挂到对应层上

教学上最值得记住的是：

- 先记录“每个层上最后一个小于目标 key 的节点”
- 再修改指针，让新节点“插进”多层链表里
- 最后 `size += 1`

这也是跳表比普通有序数组更适合学习的地方：插入仍然是“局部修改”，不是整体搬运。

## 4. 磁盘表：`src/sstable.zig`

`SSTable` 表示一个**不可变**的磁盘文件。文件本身不能原地修改，只能顺序写、按偏移读。

### 4.1 文件格式

当前实现采用非常简单的格式：

| 部分 | 说明 |
|------|------|
| 每条记录 | `key_len (u32) + key + value_len (u32) + value + timestamp (i64)` |
| 索引 | 内存中的 `StringHashMap<key, offset>` |

所以一个 `get(key)` 的真实流程是：

1. 在内存索引里找 `offset`
2. `seekTo(offset)`
3. 读 key/value
4. 再检查一次读出来的 key 是否和查询 key 一致

这里做 key 一致性检查是为了防止索引和内容出现静默错位。

### 4.2 写：`write`

`write` 从当前文件末尾顺序追加记录，并在索引中记录每个 key 的偏移量。教学重点：

- SSTable 是**不可变文件**
- 写入是**顺序写**，因此性能好
- 索引让点查变成 O(1) 定位 + O(1) 条数读取

### 4.3 读：`get`

```zig
const offset = self.index.get(key) orelse return null;
...
const value = try self.allocator.alloc(u8, value_len);
errdefer self.allocator.free(value);
...
return value;
```

注意两点：

- 没有命中索引就 `return null`
- 一旦分配了 `value`，就由**调用方负责 free**

### 4.4 全量读：`readAllEntries`

这是给 Compaction 用的辅助函数。它会从文件头顺序读所有条目，返回一个 `ArrayList(Entry)`。教学意义在于：你可以把它理解为“把磁盘文件恢复成内存中的有序列表”。

## 5. 核心引擎：`src/lsm.zig`

`LSMTree` 把 `MemTable` 和 `SSTable` 串起来，形成完整的主流程。

### 5.1 结构总览

核心字段：

- `memtable`：当前活跃内存表
- `levels`：固定长度数组，每层是一组 `SSTable` 指针
- `level_sizes`：每层当前记录数
- `sstable_counter`：生成文件名的自增计数器

### 5.2 `put` 主流程

```zig
pub fn put(self: *Self, key: []const u8, value: []const u8) !void {
    try self.memtable.put(key, value);

    if (self.memtable.size >= MAX_MEMTABLE_SIZE) {
        try self.flushMemTable();
    }
}
```

重点：

- 写操作**永远先写 MemTable**
- 只有 MemTable 超过 `MAX_MEMTABLE_SIZE` 时才 flush

这意味着教学项目里的写入路径非常短，适合先理解“为什么 LSM-Tree 写快”。

### 5.3 `flushMemTable`

这是第一个关键流程。它做 5 件事：

1. 生成一个 L0 文件名，例如 `L0_sstable_0.db`
2. 遍历 MemTable 的跳表，提取所有 key/value
3. 写入一个新的 SSTable
4. 把这个 SSTable 挂到 `levels[0]`
5. 重建一个新的 MemTable

这里最容易困惑的是第 2 步：代码里没有暴露“遍历所有节点”的公开 API，而是在 `flushMemTable` 内部直接遍历 `self.memtable.head`。这是当前实现的教学简化版；真实工程里一般会把“导出全部条目”做成 `MemTable` 的公开方法。

### 5.4 `get` 主流程

```zig
if (self.memtable.get(key)) |value| {
    return try self.allocator.dupe(u8, value);
}
```

先查 MemTable。如果命中，直接复制一份返回。

```zig
for (self.levels) |level| {
    var i: usize = level.items.len;
    while (i > 0) {
        i -= 1;
        if (try level.items[i].get(key)) |value| {
            return value;
        }
    }
}
```

再按层级查找。顺序是：

- 先查 level 0
- 再查 level 1
- ...

每一层里都从**最新文件往旧文件查**，保证返回最新写入结果。

### 5.5 `compact`

`compact` 是一个简化版自动压缩入口。它会：

- 从 level 0 开始向上检查
- 一旦某层超过阈值，就执行 `mergeLevel`
- 再继续检查下一层

阈值公式是：

```zig
std.math.pow(usize, LEVEL_SIZE_MULTIPLIER, level + 1)
```

也就是说：

- level 0 阈值是 10
- level 1 阈值是 100
- level 2 阈值是 1000

### 5.6 `mergeLevel`

这是第二个关键流程，也是本项目最复杂的部分。它做 6 件事：

1. 收集当前层和下一层所有 SSTable 的记录
2. 按 key 排序；相同 key 保留时间戳更大的（也就是更新的）
3. 去重，只保留最新版本
4. 写入一个新的合并 SSTable 到下一层
5. 删除当前层旧文件
6. 更新 `level_sizes`

教学上最值得注意的点：

- 这里使用了“先合并不去重，再单独做 dedup”的两步策略
- 旧文件释放顺序要小心：先准备好新文件，再删旧文件
- 当前实现把所有记录读进内存，所以只适合小数据集

## 6. 测试策略：`src/lsm_test.zig`、`src/memtable_test.zig`、`src/sstable_test.zig`、`src/lsm_integration_test.zig`

本项目现在有四组测试文件，建议按这个顺序阅读：

1. `memtable_test.zig`：理解跳表插入/查询/更新
2. `sstable_test.zig`：理解磁盘文件格式和索引
3. `lsm_test.zig`：理解 MemTable flush、Compaction、`level_sizes` 含义
4. `lsm_integration_test.zig`：把多阶段流程串起来验证

### 6.1 为什么拆成多文件

Zig 允许一个模块里写多个 `test`，也允许单独文件里写 `test`。拆文件的好处是：

- 每个文件只关注一个模块
- 看到文件名就能知道它在测哪一层
- 后续继续扩展时不容易变成一锅粥

### 6.2 测试里的内存习惯

本项目所有测试都尽量满足：

- `get` 的返回值后面跟着 `defer allocator.free(value)`
- 动态创建的 `key` / `expected_value` 都立即 `defer free`
- 使用 `GeneralPurposeAllocator` 并在末尾检查 `gpa.deinit() == .ok`

这样测试不仅验证正确性，也在示范“如何写 Zig 风格的内存安全代码”。

## 7. 建议的阅读顺序

如果你刚打开项目，推荐这样读：

1. `README.md`
2. `LSM-TREE-PRINCIPLES.md`
3. `src/main.zig`
4. `src/memtable.zig`
5. `src/sstable.zig`
6. `src/lsm.zig`
7. `src/memtable_test.zig`
8. `src/sstable_test.zig`
9. `src/lsm_test.zig`
10. `src/lsm_integration_test.zig`
11. `CODE_WALKTHROUGH.md`（就是你现在看的这份）

## 8. 几个容易卡住的地方

### 8.1 `get` 的返回值所有权

- `MemTable.get` 返回的是内部指针，不要 `free`
- `SSTable.get` 和 `LSMTree.get` 返回的是新分配内存，**必须 free**

### 8.2 flush 时节点归属变化

`flushMemTable` 把跳表节点里的 `key/value` 复制到 SSTable 后，原来的 MemTable 被整体销毁，新建一个空的。所以 flush 前后不需要手动释放旧节点，`MemTable.deinit()` 会处理。

### 8.3 compaction 不是“原地修改”

每次 `mergeLevel` 都会：

- 生成新文件
- 挂到下一层
- 删掉当前层旧文件

所以 compaction 的重点不是“修改现有文件”，而是**生成新版本、废弃旧版本**。

### 8.4 为什么教学项目不追求生产级性能

因为教学级实现要先保证“人能读懂”。生产系统通常会做：

- 分块索引
- Bloom Filter
- 并行合并
- 可恢复 WAL
- 更复杂的大小分层策略

本项目有意避免这些，先把主链路讲清楚。
