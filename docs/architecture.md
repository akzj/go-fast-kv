# go-fast-kv 架构设计文档

## 1. 概述

go-fast-kv 是一个高性能的键值存储数据库,采用以下核心设计:

- **B-link-tree**: 读优化的B+树变体
- **追加写入**: 所有数据追加到活动段,无随机写入
- **全局WAL**: 所有模块共享预写日志
- **分层GC**: 根据段类型采用不同垃圾回收策略

## 2. 核心组件

### 2.1 B+ Tree 层

B+树负责键值索引,具有以下特点:

- **虚拟PageID**: B+树只操作虚拟PageID,不知道物理位置
- **内联存储**: 小值(<512B)直接存储在叶子节点
- **引用存储**: 大值(>=512B)存储在ObjectStore,节点中只存blob_id

**B+Tree值格式**:
```
内联值:   [flag:1][data:1~511]
引用值:   [flag:1][blob_id:8]
```

### 2.2 Object Store 层

统一存储层,管理所有页面和Blob对象。

**核心设计**:
- 所有写入追加到活动段
- 维护Mapping Index(内存+WAL持久化)
- ObjectID到物理位置的映射

**写入流程**:
```
B+Tree → ObjectStore → 追加到活动段 → 更新Mapping Index → 写WAL → Batch Fsync
```

### 2.3 Global WAL 层

全局预写日志,所有模块共享。

**设计原则**:
- 条目类型区分不同模块(ObjectStore/B+Tree)
- 批量fsync优化性能
- Checkpoint持久化Mapping Index快照

**WAL条目格式**:
```
[Type:1][Length:4][Payload:n]
```

### 2.4 GC Controller 层

垃圾回收控制器,采用阈值+稳定性检查策略。

## 3. 段类型

| 段类型 | 大小 | GC策略 | 删除处理 |
|--------|------|--------|----------|
| Page Segment | 64MB固定 | 高频(40%阈值) | 标记+压缩 |
| Blob Segment | 256MB固定 | 低频(50%阈值+稳定性) | 标记+压缩 |
| Large Blob Segment | 弹性大小 | 无GC | 删除文件 |

**Large Blob判定**: size >= 256MB

## 4. 数据流

### 4.1 写入流程

```
1. B+Tree.Put(key, value)
   ├── value < 512B: 直接内联
   └── value >= 512B: 调用ObjectStore.WriteBlob()

2. ObjectStore.WriteBlob()
   ├── 计算Segment类型
   ├── 追加到活动段
   ├── 更新Mapping Index
   └── 写入WAL

3. WAL.Sync()
   └── Batch fsync所有待写入条目
```

### 4.2 读取流程

```
1. B+Tree.Get(key)
   ├── 读取叶子节点
   ├── 检查flag
   ├── flag=内联: 直接返回data
   └── flag=引用: 调用ObjectStore.ReadBlob()

2. ObjectStore.ReadBlob(blob_id)
   ├── 查询Mapping Index获取位置
   ├── 从指定Segment读取
   └── 验证Checksum
```

### 4.3 删除流程

```
B+Tree.Delete(key)
├── 从B+Tree移除键
├── 如果value是blob,调用ObjectStore.Delete()
└── ObjectStore标记对象为已删除(GC回收)
```

## 5. GC策略

### 5.1 Page Segment GC

**触发条件**: 垃圾比率 > 40%

**执行流程**:
1. 扫描段,识别存活对象
2. 将存活对象复制到活动段
3. 更新Mapping Index
4. 删除旧段

### 5.2 Blob Segment GC

**触发条件**: 垃圾比率 > 50% **且** 修改率 < 阈值

**为什么需要稳定性检查?**
Blob Segment容量大(256MB),频繁压缩成本高。只有当修改率稳定时才触发GC。

**修改率稳定性检查**:
- 使用滑动窗口跟踪历史修改率
- 方差小于阈值时判定为稳定

### 5.3 Large Blob Segment GC

**无需GC**,每个Large Blob独占一个Segment,删除即回收文件。

## 6. WAL设计

### 6.1 条目类型

| Type | 模块 | 用途 |
|------|------|------|
| 0x01 | ObjectStore | 对象分配/写入/删除 |
| 0x02 | B+Tree | 键值插入/删除 |
| 0xFF | Checkpoint | 检查点标记 |

### 6.2 Checkpoint机制

1. 写入Checkpoint标记到WAL
2. 将Mapping Index快照序列化到WAL
3. Fsync确保所有之前的写入已持久化

### 6.3 批量Sync

多个写入累积后一次性fsync,显著提升吞吐量。

## 7. 崩溃恢复

### 7.1 恢复流程

```
1. 加载最后Checkpoint
   └── 恢复Mapping Index快照

2. 重放WAL条目
   └── 从Checkpoint LSN之后开始

3. 验证Segment Checksum
   └── 检测数据损坏
```

### 7.2 Checkpoint频率

建议每10000次操作或30秒执行一次Checkpoint。

## 8. 关键参数

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

## 9. Object ID格式

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

## 10. Object Header格式

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

## 11. 目录结构

```
go-fast-kv/
├── docs/
│   └── architecture.md        # 本文档
├── pkg/
│   ├── objectstore/           # 对象存储层
│   │   ├── types.go           # 类型定义
│   │   └── objectstore.go     # 接口定义
│   ├── wal/                   # 预写日志
│   │   ├── types.go           # WAL类型和接口
│   │   └── wal.go             # 辅助函数
│   ├── btree/                 # B+树
│   │   └── btree.go           # B+树接口和工具
│   └── gc/                    # 垃圾回收
│       └── gc.go              # GC控制器
├── internal/                  # 内部实现包
├── cmd/                       # 命令行工具
├── go.mod
└── go.sum
```

## 12. 设计权衡

### 12.1 为什么追加写入?

- **顺序写入友好**: 机械硬盘和SSD都有顺序写入优势
- **简化GC**: 只需移动存活对象
- **避免碎片化**: 新数据写入新位置

### 12.2 为什么B+Tree不知道物理位置?

- **隔离变化**: GC压缩后物理位置变化,B+Tree无需更新
- **简化接口**: B+Tree只需ObjectID操作
- **一致性**: 所有读写都通过ObjectStore,保证数据一致性

### 12.3 为什么Large Blob不需要GC?

- 1:1映射(每个Large Blob独占一个Segment)
- 删除即回收文件
- 压缩成本太高,不值得

## 13. 未来扩展

- 支持多盘并发写入
- 支持压缩(热数据LZ4,冷数据ZSTD)
- 支持加密(AES-256-GCM)
- 支持分布式复制(Raft共识)