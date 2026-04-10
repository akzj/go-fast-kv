# go-fast-kv Checkpoint 优化设计

## 1. 概述

**目标**：消除当前 checkpoint 的 P0 瓶颈，实现非阻塞 checkpoint。

**当前瓶颈**：
| 瓶颈 | 影响 |
|------|------|
| WAL Truncate 全量重写 | checkpoint 时间 ∝ WAL 大小 |
| s.mu.Lock() 排他锁持有 | 期间阻塞所有读写操作 |
| 两次 Segment Sync | 额外 2 次 fsync |

---

## 2. 新架构

### 2.1 去中心化 LSN 追踪

```
┌─────────────┐    checkpoint_lsn = 1000
│   LSM       │──────────────────────────→ min_lsm = 1000
└─────────────┘

┌─────────────┐    checkpoint_lsn = 800
│ B-link tree │──────────────────────────→ min_tree = 800
└─────────────┘

┌─────────────┐    checkpoint_lsn = 1200
│ BlobStore   │──────────────────────────→ min_blob = 1200
└─────────────┘

WAL 删除：min(1000, 800, 1200) = 800
```

**核心原则**：各模块独立追踪自己的 checkpoint_lsn，WAL 删除用全局最小值。

### 2.2 WAL 分段设计

```
wal/
  wal.0000000000000001.0000000100.log   ← end_lsn < checkpoint → 可删除
  wal.0000000000100001.0000000200.log   ← 可删除
  wal.0000000000200001.active.log        ← 活跃 segment
```

**WAL Record 格式**（新增 ModuleType）：
```
[0:8]   uint64  LSN
[8]     uint8   ModuleType  ← 新增字段
[9]     uint8   RecordType
[10:18] uint64  ID (pageID/blobID/xid)
[18:26] uint64  VAddr (packed: segID<<32 | offset)
[26:30] uint32  Size (blob 专用)
[30:34] uint32  CRC32-C

ModuleType:
  0x01 = Tree    (B-link tree page)
  0x02 = Blob    (BlobStore data)
  0x03 = LSM     (MappingStore, 新增)
```

**WAL Segment 删除**：
- 根据文件名判断 `end_lsn`
- 删除所有 `end_lsn < min_checkpoint_lsn` 的 segment 文件
- 不需要读 WAL 内容，O(k) 操作

### 2.3 LSM Store 自管理

**职责**：管理 PageID→VAddr 和 BlobID→VAddr 映射，替代当前的 Dense Array。

**内部结构**：
```
LSM Store
├── memtable-0 (活跃，接收写入)
├── memtable-1 (frozen，等待合并)
├── segments/ (SSTable 文件)
│   ├── memtable-0.sst
│   └── memtable-1.sst
└── manifest.json
```

**自动 Compaction 流程**：
```
memtable-0 (活跃，接收写入)
    │
    │ 达到 N keys 或大小上限
    ▼
memtable-0 freeze → 变为 immutable
    │
    │ 后台 goroutine
    ▼
memtable-0 与其他 sst 合并 → segment-0.sst
    │
    │ merge 完成
    ▼
释放 memtable → 更新 manifest.json
```

### 2.4 Checkpoint 流程（两阶段，非阻塞）

```
┌──────────────────────────────────────────────────────────────┐
│  Phase 1 (持锁，毫秒级)                                       │
│                                                              │
│  1. 获取当前 WAL LSN                                         │
│  2. 各模块记录自己的 checkpoint_lsn:                           │
│       lsm.Checkpoint(lsn)     → lsm.checkpoint_lsn = lsn    │
│       tree.Checkpoint(lsn)   → tree.checkpoint_lsn = lsn   │
│       blob.Checkpoint(lsn)   → blob.checkpoint_lsn = lsn   │
│  3. 写 metadata.json                                          │
│  4. 释放锁                                                   │
├──────────────────────────────────────────────────────────────┤
│  Phase 2 (后台 goroutine)                                     │
│                                                              │
│  1. min_lsn = min(lsm.lsn, tree.lsn, blob.lsn)              │
│  2. 删除 WAL segments (end_lsn < min_lsn)                    │
│  3. LSM 内部 flush (已经在后台运行)                           │
│  4. 完成                                                     │
└──────────────────────────────────────────────────────────────┘
```

### 2.5 Metadata.json 格式

```json
{
  "version": 1,
  "timestamp": "2024-01-01T00:00:00Z",
  "modules": {
    "lsm": {
      "checkpoint_lsn": 1000,
      "manifest_version": 5
    },
    "tree": {
      "checkpoint_lsn": 800,
      "root_page_id": 42
    },
    "blob": {
      "checkpoint_lsn": 1200,
      "next_blob_id": 5000
    }
  }
}
```

### 2.6 Recovery 流程

```
Open():
  1. 读取 metadata.json → 各模块 checkpoint_lsn
  2. 加载各模块状态:
       - LSM: 加载 manifest.json，恢复 memtable 状态
       - Tree: 设置 root_page_id
       - Blob: 设置 next_blob_id
  3. Replay WAL:
       for each WAL record:
         switch record.ModuleType:
           case 0x01 (Tree):
             if record.LSN > tree.checkpoint_lsn → tree.Apply(record)
           case 0x02 (Blob):
             if record.LSN > blob.checkpoint_lsn → blob.Apply(record)
           case 0x03 (LSM):
             if record.LSN > lsm.checkpoint_lsn → lsm.Apply(record)
```

**模块间完全独立**：
- BlobStore 不需要知道 PageID→VAddr 映射
- 每个模块只处理自己的 ModuleType
- Recovery 时各模块 skip 到自己的 checkpoint_lsn

---

## 3. 接口设计

### 3.1 Checkpointable 接口

所有需要参与 checkpoint 的模块实现此接口：

```go
// Checkpointable 模块的 checkpoint 接口
type Checkpointable interface {
    // Checkpoint 记录当前检查点 LSN
    Checkpoint(lsn uint64) error
    
    // CheckpointLSN 返回最后一次 checkpoint 的 LSN
    CheckpointLSN() uint64
    
    // Apply 应用一条 WAL record（recovery 时使用）
    Apply(record WALRecord) error
}
```

### 3.2 LSM Store 接口

```go
type LSMStore interface {
    // 读写接口
    SetPage(pageID uint64, vaddr uint64) error
    GetPage(pageID uint64) (vaddr uint64, ok bool)
    
    SetBlob(blobID uint64, vaddr uint64, size uint32) error
    GetBlob(blobID uint64) (vaddr uint64, size uint32, ok bool)
    
    DeleteBlob(blobID uint64) error
    
    // Checkpointable 实现
    Checkpoint(lsn uint64) error
    CheckpointLSN() uint64
    Apply(record WALRecord) error
    
    // 内部 compaction
    MaybeCompact() error
    
    Close() error
}
```

### 3.3 CheckpointManager 接口

```go
type CheckpointManager interface {
    // DoCheckpoint 执行两阶段 checkpoint
    DoCheckpoint() error
    
    // GetCheckpointLSN 返回全局最小 checkpoint LSN
    GetCheckpointLSN() uint64
    
    // GetMetadata 返回当前 metadata
    GetMetadata() CheckpointMetadata
}
```

---

## 4. 实现计划

### 4.1 阶段 1：WAL 分段（基础层）

- 修改 WAL 支持分段写入
- 支持按 LSN 范围删除 segment 文件
- 保持 Record 格式兼容（新增 ModuleType）

### 4.2 阶段 2：LSM Store（新模块）

- 创建 `internal/lsm/` 目录
- 实现简化的 LSM（memtable → sst，无多 level compaction）
- 实现 manifest.json 管理
- 替换现有的 Dense Array 映射表

### 4.3 阶段 3：CheckpointManager（集成）

- 实现两阶段 checkpoint
- 实现 metadata.json 读写
- 实现 WAL segment 清理

### 4.4 阶段 4：集成测试

- 验证 checkpoint/recovery 流程
- 验证 WAL 清理正确性
- 性能测试

---

## 5. 设计决策

### 5.1 LSM 简化为 Memtable + SST（无多 level）

**决策**：不实现 RocksDB 风格的多层 LSM，只做 memtable → sst 的单层合并。

**理由**：
- 当前映射表是扁平的（PageID → VAddr），不需要多层组织
- 单层 sst 已经满足需求（checkpoint 时整个 memtable 刷到 sst）
- 简化实现，降低复杂度

### 5.2 ModuleType 字段

**决策**：WAL Record 新增 1 byte ModuleType 字段。

**理由**：
- 各模块独立追踪自己的 checkpoint_lsn
- Recovery 时各模块只处理自己的 record
- 34 bytes/record vs 33 bytes/record，1 byte 开效可接受

### 5.3 Checkpoint 不阻塞读写

**决策**：Phase 1 只记录 LSN + 写 metadata.json，毫秒级完成。

**理由**：
- 现有的 `s.mu.Lock()` 持有多秒的问题解决
- WAL 清理在 Phase 2 后台执行
- LSM 内部 compaction 也在后台执行

---

## 6. 文件结构

```
internal/
├── lsm/                    ← 新增 LSM Store
│   ├── api/api.go         # 接口定义
│   ├── lsm.go             # 主实现
│   ├── memtable.go        # memtable (skip list)
│   ├── sstable.go          # SSTable 格式
│   ├── manifest.go         # manifest.json 管理
│   └── recovery.go         # Apply 方法
├── wal/                   ← 修改：支持分段
│   └── wal.go             # 分段写入 + 删除
├── checkpoint/            ← 新增 CheckpointManager
│   ├── api/api.go
│   ├── manager.go
│   └── metadata.go
└── kvstore/               ← 修改：集成新组件
    └── kvstore.go
```

---

## 7. 与现有设计的兼容性

### 7.1 向后兼容

- 现有的 RecordType 保持不变
- ModuleType 默认值为 0x01 (Tree)，兼容旧记录
- Recovery 时忽略未知 ModuleType

### 7.2 迁移策略

1. Phase 1-3 实现新组件（LSM Store, WAL 分段, CheckpointManager）
2. 保持旧的 Dense Array 映射表
3. 新旧组件并行运行
4. 验证通过后，删除旧组件

---

## 8. 风险与缓解

| 风险 | 缓解措施 |
|------|----------|
| LSM memtable 内存膨胀 | 设置大小上限，强制 flush |
| WAL segment 清理丢失数据 | min(checkpoint_lsn) 保证安全 |
| metadata.json 写入失败 | 使用 temp 文件 + rename |
| Recovery 时 record 顺序 | WAL 按 LSN 有序，只需顺序回放 |