# go-fast-kv 架构设计文档

## 1. 概述

go-fast-kv 是一个高性能的键值存储数据库,采用以下核心设计:

- **B-link-tree**: 读优化的B+树变体,支持页面级锁和并发读取
- **追加写入**: 所有数据追加到活动段,无随机写入
- **异步WAL**: 3层通道架构,无锁写入
- **分层GC**: 根据段类型采用不同垃圾回收策略

### 性能指标

| Benchmark | 结果 |
|-----------|------|
| Sequential Put | ~2-3.5M ops/sec |
| Sequential Get | ~6-8M ops/sec |
| Concurrent Writes | >500K ops/sec |
| Concurrent Reads | >3M ops/sec |

## 2. 模块隔离结构

每个模块遵循严格隔离原则:

```
pkg/{mod}/
├── api/api.go          # 公共接口定义
├── internal/           # 私有实现
│   ├── {mod}.go      # 核心实现
│   ├── *_test.go     # 单元测试
│   └── bench_test.go # 性能测试
└── {mod}.go          # 从 api 包导出
```

**规则**:
- `api/api.go` 仅包含接口和公共类型,无实现
- `internal/` 可导入自己的 `api` 包,不可导入其他模块的 `internal`
- 模块间通信仅通过 `api` 接口

## 3. 核心组件

### 3.1 B+ Tree 层 (pkg/btree/)

B-link-tree 实现,具有以下特点:

- **页面级锁**: 每页独立的读写锁,支持并发读取
- **Lock Coupling**: 先锁子节点再解锁父节点
- **虚拟PageID**: B+树只操作虚拟PageID,不知道物理位置
- **内联存储**: 小值(<512B)直接存储在叶子节点
- **引用存储**: 大值(>=512B)存储在ObjectStore

**B+Tree值格式**:
```
内联值:   [flag:1][data:1~511]
引用值:   [flag:1][blob_id:8]
```

### 3.2 Object Store 层 (pkg/objectstore/)

统一存储层,管理所有页面和Blob对象。

**核心设计**:
- 所有写入追加到活动段
- 维护Mapping Index(内存+WAL持久化)
- ObjectID到物理位置的映射

### 3.3 Global WAL 层 (pkg/wal/)

3层异步通道架构,实现无锁写入:

```
┌─────────────┐     chan     ┌───────────────┐     chan     ┌─────────────┐
│   Put()     │ ──────────→ │  Batch Loop   │ ──────────→ │  File I/O   │
│  (Producer) │  non-block  │  (Consumer)   │  non-block  │  (Writer)   │
└─────────────┘             │  drain+batch  │             │  write+sync │
      ↑                      └───────────────┘             └─────────────┘
      │ callback                  ↑                             │
      └──────────────────────────┘                             ↓
```

**Layer 1 - Entry Queue**: `entryChan` 接收生产者条目(非阻塞发送)
**Layer 2 - Memory Buffer**: Consumer 累积条目到内存缓冲区
**Layer 3 - File Writer**: 独立goroutine处理所有文件I/O

**优势**:
- 写入操作无锁,直接发送到channel
- 批量写入减少I/O次数
- Consumer不会因文件I/O阻塞

### 3.4 GC Controller 层 (pkg/gc/)

垃圾回收控制器,采用阈值+稳定性检查策略。

## 4. 段类型

| 段类型 | 大小 | GC策略 | 删除处理 |
|--------|------|--------|----------|
| Page Segment | 64MB固定 | 高频(40%阈值) | 标记+压缩 |
| Blob Segment | 256MB固定 | 低频(50%阈值+稳定性) | 标记+压缩 |
| Large Blob Segment | 弹性大小 | 无GC | 删除文件 |

**Large Blob判定**: size >= 256MB

## 5. 并发架构

### 5.1 锁策略

| 层级 | 锁策略 | 并发读 | 并发写 |
|------|--------|--------|--------|
| KVStore | 无锁 (atomic.Bool) | ✅ 完全并发 | ✅ 完全并发 |
| BTree | Page-level latches + lock coupling | ✅ | ✅ |
| ObjectStore | Per-segment mutexes | ✅ | ✅ |
| WAL | 异步3层 (entryChan → buffer → writer) | N/A | ✅ 并发入队 |

### 5.2 B-link-tree Lock Coupling

```
传统B+Tree (全局锁):
  Lock → traverse → modify → unlock

B-link-tree (页面级锁):
  Lock parent → Lock child → Unlock parent → Lock grandchild → ...
```

**优势**:
- 多个读操作可同时遍历
- 写操作仅在特定页面相互阻塞
- Lock coupling确保一致性

## 6. 数据流

### 6.1 写入流程

```
1. KVStore.Put(key, value)
   ├── value < 512B: 直接内联
   └── value >= 512B: 调用ObjectStore.WriteBlob()

2. ObjectStore.WriteBlob()
   ├── 计算Segment类型
   ├── 追加到活动段
   └── 更新Mapping Index

3. WAL.Write() (异步)
   ├── 发送到entryChan (非阻塞)
   └── Consumer处理批量写入

4. Consumer (Batch Loop)
   ├── drain entryChan
   ├── batch到writeBuffer
   └── 发送到writerChan
```

### 6.2 读取流程

```
1. BTree.Get(key)
   ├── 读取叶子节点
   ├── 检查flag
   ├── flag=内联: 直接返回data
   └── flag=引用: 调用ObjectStore.ReadBlob()

2. ObjectStore.ReadBlob(blob_id)
   ├── 查询Mapping Index获取位置
   ├── 从指定Segment读取
   └── 验证Checksum
```

### 6.3 删除流程

```
BTree.Delete(key)
├── 从BTree移除键
├── 如果value是blob,调用ObjectStore.Delete()
└── ObjectStore标记对象为已删除(GC回收)
```

## 7. GC策略

### 7.1 Page Segment GC

**触发条件**: 垃圾比率 > 40%

**执行流程**:
1. 扫描段,识别存活对象
2. 将存活对象复制到活动段
3. 更新Mapping Index
4. 删除旧段

### 7.2 Blob Segment GC

**触发条件**: 垃圾比率 > 50% **且** 修改率 < 阈值

**为什么需要稳定性检查?**
Blob Segment容量大(256MB),频繁压缩成本高。只有当修改率稳定时才触发GC。

### 7.3 Large Blob Segment GC

**无需GC**,每个Large Blob独占一个Segment,删除即回收文件。

## 8. WAL设计

### 8.1 3层异步架构

```
Layer 1: entryChan (无锁入队)
  - Producer: select non-blocking send
  - Fallback: sync write if channel full

Layer 2: Memory Buffer (批量累积)
  - Consumer drain with timeout
  - batchEntry() marshals and buffers
  - Send to writer when batch full

Layer 3: writerRun() (文件I/O)
  - Separate goroutine
  - writeChan receives batches
  - Signal callbacks when written
```

### 8.2 条目类型

| Type | 模块 | 用途 |
|------|------|------|
| 0x01 | ObjectStore | 对象分配/写入/删除 |
| 0x02 | B+Tree | 键值插入/删除 |
| 0xFF | Checkpoint | 检查点标记 |

### 8.3 WAL条目格式

```
[Type:1][Length:4][Payload:n][Checksum:4]
Total: 9 + len(Payload) bytes per entry
```

## 9. 崩溃恢复

### 9.1 恢复流程

```
1. 加载最后Checkpoint
   └── 恢复Mapping Index快照

2. 重放WAL条目
   └── 从Checkpoint LSN之后开始

3. 验证Segment Checksum
   └── 检测数据损坏
```

### 9.2 Checkpoint机制

1. 写入Checkpoint标记到WAL
2. 将Mapping Index快照序列化到WAL
3. Fsync确保所有之前的写入已持久化

## 10. 关键参数

| 参数 | 值 | 说明 |
|------|-----|------|
| PageSize | 4KB | B+Tree页面大小 |
| InlineThreshold | 512B | 内联存储阈值 |
| PageSegmentSize | 64MB | Page Segment固定大小 |
| BlobSegmentSize | 256MB | Blob Segment固定大小 |
| LargeBlobThreshold | 256MB | Large Blob判定阈值 |
| PageSegmentGCThreshold | 40% | Page Segment GC阈值 |
| BlobSegmentGCThreshold | 50% | Blob Segment GC阈值 |
| ObjectHeaderSize | 32B | 对象头部固定大小 |
| MappingIndexEntrySize | 16B | 映射条目大小 |
| WALBatchSize | 4MB | WAL缓冲区大小 |

## 11. Object ID格式

```
63            56 55                                          0
┌───────────────┬─────────────────────────────────────────────┐
│  ObjectType   │              Sequence (56 bits)            │
│   (8 bits)    │                                            │
└───────────────┴─────────────────────────────────────────────┘
```

**ObjectType**:
- 0x00: Page (B+Tree页面)
- 0x01: Blob (普通Blob)
- 0x02: Large (大型Blob)

## 12. Object Header格式

固定32字节:

```
Offset  Size  Field
0       2     Magic (0xF0 0xKB)
2       1     Version
3       1     Type (ObjectType)
4       4     Checksum (CRC32)
8       4     Size (数据大小)
12      20    Reserved
```

## 13. 项目结构

```
go-fast-kv/
├── docs/
│   └── architecture.md        # 本文档
├── pkg/
│   ├── objectstore/           # 统一对象存储
│   │   ├── api/api.go        # 接口定义
│   │   ├── internal/          # 私有实现
│   │   │   ├── store.go
│   │   │   ├── segment_manager.go
│   │   │   └── mapping_index.go
│   │   └── objectstore.go     # 导出
│   ├── wal/                   # 异步WAL
│   │   ├── api/api.go
│   │   ├── internal/wal.go   # 3层异步实现
│   │   └── wal.go
│   ├── btree/                # B-link-tree
│   │   ├── api/api.go
│   │   ├── internal/
│   │   │   ├── btree_impl.go
│   │   │   ├── page.go
│   │   │   └── latch.go      # 页面级锁
│   │   └── btree.go
│   ├── gc/                   # GC控制器
│   │   ├── api/api.go
│   │   ├── internal/gc_impl.go
│   │   └── gc.go
│   └── kvstore/              # KV数据库
│       ├── api/api.go
│       ├── internal/kvstore.go
│       └── kvstore.go
├── cmd/                       # CLI工具
├── go.mod
└── go.sum
```

## 14. 设计权衡

### 14.1 为什么追加写入?

- **顺序写入友好**: 机械硬盘和SSD都有顺序写入优势
- **简化GC**: 只需移动存活对象
- **避免碎片化**: 新数据写入新位置

### 14.2 为什么B+Tree不知道物理位置?

- **隔离变化**: GC压缩后物理位置变化,B+Tree无需更新
- **简化接口**: B+Tree只需ObjectID操作
- **一致性**: 所有读写都通过ObjectStore,保证数据一致性

### 14.3 为什么Large Blob不需要GC?

- 1:1映射(每个Large Blob独占一个Segment)
- 删除即回收文件
- 压缩成本太高,不值得

### 14.4 WAL为什么用3层架构?

- Layer 1 (channel): 实现无锁入队
- Layer 2 (buffer): 批量累积减少I/O
- Layer 3 (writer): 文件I/O不阻塞Consumer

## 15. 测试覆盖

| 模块 | 测试数 | 状态 |
|------|--------|------|
| objectstore | 15 | ✅ |
| wal | 12 | ✅ |
| btree | 10 | ✅ |
| gc | 11 | ✅ |
| kvstore | 9 | ✅ |
| **总计** | **57** | ✅ |

## 16. 未来扩展

- [ ] 支持多盘并发写入
- [ ] 支持压缩(热数据LZ4,冷数据ZSTD)
- [ ] 支持加密(AES-256-GCM)
- [ ] 支持分布式复制(Raft共识)
- [ ] 支持事务(Batch operations)
