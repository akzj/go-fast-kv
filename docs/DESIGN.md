# go-fast-kv 设计方案 (v4)

## 1. 项目目标

一个嵌入式 KV 存储引擎，支持：
- `Put(key, value)` / `Get(key)` / `Delete(key)`
- `Scan(startKey, endKey)` 有序范围扫描
- 大 value 透明存储（Blob Storage）
- 持久化 + 崩溃恢复
- GC（回收 page 和 blob 的旧数据）

不做：分布式、网络协议、SQL。

---

## 2. 整体架构

```
┌─────────────────────────────────────────────────┐
│                   KVStore                        │  用户接口层
│           Put / Get / Delete / Scan              │
└────────────────────┬────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────┐
│                 B-link Tree                      │  索引层
│       key=[]byte   child/sibling=PageID          │
│       value: inline 或 BlobRef                   │
│       不知道 VAddr 的存在                          │
└────────┬───────────────────────────┬────────────┘
         │                           │
         │  page 读写                 │  大 value 读写
         ▼                           ▼
┌─────────────────┐      ┌────────────────────────┐
│    PageStore     │      │     BlobStore           │
│  Read/Write by  │      │  Read/Write by          │
│    PageID       │      │    BlobID               │
│                 │      │                          │
│  page_id→vaddr  │      │  blob_id→(vaddr,size)   │
│  (dense array)  │      │  (dense array)           │
└────────┬────────┘      └────────────┬────────────┘
         │                            │
         ▼                            ▼
┌─────────────────────────────────────────────────┐
│              Segment Manager                     │  物理存储层
│           Append-only segment files              │
│           VAddr = (segmentID, offset)            │
│                                                  │
│   PageStore 和 BlobStore 共享同一组 segment       │
│   （或各自独立的 segment，待讨论）                  │
└─────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────┐
│                  WAL (共享)                       │  所有模块共用
│           page 映射变更 + blob 映射变更            │
│           + root 变更 + checkpoint               │
└─────────────────────────────────────────────────┘
```

**核心原则**：
- B-tree 只看到 PageID 和 BlobID，不碰 VAddr
- 两套映射表都在内存中，O(1) 查找
- 所有持久化变更通过共享 WAL 保证原子性

---

## 3. 各层详细设计

### 3.1 Segment Manager（最底层）

**职责**：管理磁盘上的 segment 文件，提供 append-only 写入和按地址读取。

```
VAddr = { SegmentID uint32, Offset uint32 }   // 8 bytes

Segment 文件：data/{segmentID}.seg
每个 segment 最大 64MB，写满后 seal，开新 segment。
```

**接口**：
```go
type SegmentManager interface {
    Append(data []byte) (VAddr, error)
    ReadAt(addr VAddr, size uint32) ([]byte, error)
    Rotate() error
    RemoveSegment(segID uint32) error
    Close() error
}
```

**要点**：
- 纯粹的 append-only 日志
- 不关心写入的是 page 还是 blob — 对它来说都是 `[]byte`
- 不做 page 对齐 — 数据紧凑排列

**讨论点：Page 和 Blob 是否共享 segment？**

方案 A：共享 segment
- 优点：简单，一套 segment 管理
- 缺点：GC 复杂 — 一个 segment 里混着 page 和 blob，回收粒度粗

方案 B：分开 segment（page segment + blob segment）
- 优点：GC 可以独立处理，page segment 内容固定大小更好管理
- 缺点：两套 segment 管理，稍复杂

**我倾向方案 B** — page 和 blob 的生命周期和大小差异大，分开管理更干净。

---

### 3.2 PageStore（页面存储层）

**职责**：固定大小的 page 存储，提供稳定的 PageID 抽象。

```
PageID = uint64（自增分配，永不复用）
Page 大小：固定 4KB
映射表：[]VAddr（dense array，PageID 做下标）
```

**接口**：
```go
type PageStore interface {
    Alloc() PageID
    Write(pageID PageID, data []byte) error   // len(data) 必须 = PageSize
    Read(pageID PageID) ([]byte, error)
    Free(pageID PageID)
    Close() error
}
```

**Write 内部流程**：
```
Write(pageID, data):
  1. vaddr = segmentMgr.Append(data)         // append 到 page segment
  2. oldVAddr = mapping[pageID]
  3. WAL.Append(PageWrite, pageID, vaddr)     // 先写 WAL
  4. mapping[pageID] = vaddr                  // 更新内存映射
  5. 标记 oldVAddr 所在 segment 废弃 +1
```

**映射表持久化**：
- 内存中是 `[]VAddr` dense array — PageID 直接做下标，O(1) 查找
- 持久化通过 WAL（增量）+ Checkpoint（全量快照）
- 不用 LSM（见下文分析）

---

### 3.3 BlobStore（大 value 存储层）

**职责**：变长 blob 存储，提供稳定的 BlobID 抽象。

```
BlobID = uint64（自增分配，永不复用）
映射表：[]BlobMeta（dense array，BlobID 做下标）

BlobMeta = { VAddr, Size uint32 }   // 12 bytes
```

**接口**：
```go
type BlobStore interface {
    Write(data []byte) (BlobID, error)         // 分配 BlobID + 写入
    Read(blobID BlobID) ([]byte, error)
    Delete(blobID BlobID) error
    Close() error
}
```

**与 PageStore 的区别**：

| | PageStore | BlobStore |
|---|---|---|
| 大小 | 固定 4KB | 变长 |
| ID | 调用方指定 (Alloc 后 Write) | Write 时分配返回 |
| 映射 | `PageID → VAddr` | `BlobID → (VAddr, Size)` |
| 用途 | B-tree 节点 | 大 value |
| GC 单位 | page segment | blob segment |

**Blob 写入格式**：
```
每个 blob 在 segment 中的存储：
  [0:4]   uint32  size（blob 数据长度）
  [4:4+size]      blob 数据
  无 padding — 紧凑排列
```

**Read 流程**：
```
Read(blobID):
  1. meta = mapping[blobID]
  2. rawData = segmentMgr.ReadAt(meta.VAddr, meta.Size + 4)
  3. return rawData[4:]   // 跳过 size header
```

---

### 3.4 B-link Tree（索引层）

**职责**：有序索引，支持 Put/Get/Delete/Scan。

**key 直接用 `[]byte`**，不做任何映射。

**value 存储策略**：
```
if len(value) <= InlineThreshold (比如 256 bytes):
    直接存在 leaf entry 中（inline）
else:
    blobID = blobStore.Write(value)
    leaf entry 中存 BlobRef{blobID}
```

**Node 数据模型**：
```go
type Node struct {
    IsLeaf   bool
    Count    uint16
    Next     PageID       // leaf 右兄弟（Scan 用）
    Keys     [][]byte
    Children []PageID     // internal node
    Values   []Value      // leaf node
}

type Value struct {
    Inline   []byte       // len > 0 表示 inline
    BlobID   uint64       // > 0 表示外部引用
}
```

**接口**：
```go
type BTree interface {
    Put(key, value []byte) error
    Get(key []byte) ([]byte, error)
    Delete(key []byte) error
    Scan(start, end []byte) Iterator
    RootPageID() PageID
    SetRootPageID(PageID)
}
```

**Put 流程（无 split）**：
```
1. 从 root 向下搜索到 leaf
2. 在 leaf 中插入 (key, value)
   - value 大 → blobStore.Write → 存 BlobRef
   - value 小 → inline
3. pageStore.Write(leaf.pageID, serialize(leaf))
4. 结束 — parent 不需要更新
```

**Put 流程（有 split）**：
```
1. leaf 满了 → split 成 left + right
2. rightPID = pageStore.Alloc()
3. pageStore.Write(left.pageID, serialize(left))
4. pageStore.Write(rightPID, serialize(right))
5. 在 parent 中插入 (splitKey, rightPID)
6. pageStore.Write(parent.pageID, serialize(parent))
7. 递归直到不需要 split
```

**没有 repersistPath。PageID 稳定，parent 的 child 指针不变。**

**Scan**：
```
1. 搜索到 start 所在 leaf
2. 遍历 entries，遇到 BlobRef 则 blobStore.Read(blobID)
3. leaf 遍历完 → pageStore.Read(leaf.Next) 跳到右兄弟
4. 直到 key >= end
```

---

### 3.5 Node 序列化格式

```
Header (16 bytes):
  [0]     uint8   flags (bit0=isLeaf)
  [1]     uint8   reserved
  [2:4]   uint16  count
  [4:12]  uint64  next (PageID, leaf only)
  [12:16] uint32  checksum

Leaf entry (变长):
  [0:2]   uint16  keyLen
  [2:2+kl]        key
  [next]  uint8   valueType (0=inline, 1=blobRef)
  if inline:
    [next 4] uint32  valueLen
    [next vl]        value bytes
  if blobRef:
    [next 8] uint64  blobID

Internal entry (变长):
  [0:2]   uint16  keyLen
  [2:2+kl]        key
  [next 8] uint64  childPageID
```

**split 阈值**：序列化后总字节数 > 4KB 时 split。

---

### 3.6 共享 WAL

**所有模块共用一个 WAL 实例**。

**WAL Record 类型**：
```go
const (
    WALPageMap   = 1   // PageStore 映射变更: pageID → vaddr
    WALBlobMap   = 2   // BlobStore 映射变更: blobID → (vaddr, size)
    WALBlobFree  = 3   // BlobStore 删除: blobID
    WALPageFree  = 4   // PageStore 释放: pageID
    WALSetRoot   = 5   // B-tree root 变更: rootPageID
    WALCheckpoint = 6  // Checkpoint 完成标记: LSN
)
```

**WAL Record 格式**：
```
[0:8]   uint64  LSN
[8]     uint8   Type
[9:17]  uint64  ID (pageID 或 blobID)
[17:25] uint64  VAddr (packed: segID<<32 | offset)
[25:29] uint32  Size (blob 专用，page 固定 4KB 不需要)
[29:33] uint32  CRC
```

**固定 33 bytes per record** — 简单、高效、不需要变长解析。

**写入原子性**：
```
一次 Put 操作可能产生多条 WAL record（比如 split 时写多个 page）。
用 batch write：
  1. 攒好所有 record
  2. 一次 write + fsync
  3. 然后更新内存映射

崩溃恢复时，不完整的 batch（CRC 校验失败）整体丢弃。
```

**Checkpoint**：
```
1. 将 PageStore 映射表 + BlobStore 映射表全量写入 checkpoint 文件
2. 写入 WAL: WALCheckpoint(LSN)
3. 截断 checkpoint.LSN 之前的 WAL

Checkpoint 文件格式：
  Header: { LSN uint64, PageCount uint32, BlobCount uint32 }
  Page mappings: { PageID uint64, VAddr uint64 } × PageCount
  Blob mappings: { BlobID uint64, VAddr uint64, Size uint32 } × BlobCount
```

---

### 3.7 GC（垃圾回收）

**两套 GC，分别处理 page segment 和 blob segment**。

**Page GC**：
```
1. 选择废弃比例最高的 sealed page segment
2. 顺序读取该 segment 中所有 page（每个 4KB）
3. 对每个 page：
   - 从 page header 中提取 pageID
   - 检查 mapping[pageID] 是否仍指向此 VAddr
   - 是 → copy 到活跃 segment，CAS 更新 mapping
   - 否 → 跳过（已被覆盖）
4. 所有活数据 copy 完 → 删除旧 segment
```

**Blob GC**：
```
1. 选择废弃比例最高的 sealed blob segment
2. 顺序读取该 segment 中所有 blob（变长，靠 size header 定位）
3. 对每个 blob：
   - 提取 blobID
   - 检查 mapping[blobID] 是否仍指向此 VAddr
   - 是 → copy 到活跃 segment，CAS 更新 mapping
   - 否 → 跳过
4. 删除旧 segment
```

**GC 需要知道 pageID/blobID** — 所以 segment 中的数据需要带 ID header：
```
Page segment 中每个 page 的格式：
  [0:8]    uint64  pageID
  [8:4104] [4096]byte  page data

Blob segment 中每个 blob 的格式：
  [0:8]    uint64  blobID
  [8:12]   uint32  size
  [12:12+size]     blob data
```

---

### 3.8 并发模型：per-page RwLock + B-link right-link 修正

**设计来源**：借鉴 page-server-rs 的 B-link tree 并发方案。

#### 3.8.1 锁管理器

每个 PageID 拥有独立的 RwLock，按需创建，分片减少争用：

```go
type PageRWLocks struct {
    shards [16]lockShard
}

type lockShard struct {
    mu    sync.Mutex
    locks map[PageID]*sync.RWMutex
}

// 获取指定 page 的锁（不存在则创建）
func (l *PageRWLocks) GetLock(pid PageID) *sync.RWMutex

// 便捷方法
func (l *PageRWLocks) RLock(pid PageID)    // 加读锁
func (l *PageRWLocks) RUnlock(pid PageID)  // 释放读锁
func (l *PageRWLocks) WLock(pid PageID)    // 加写锁
func (l *PageRWLocks) WUnlock(pid PageID)  // 释放写锁
```

分片策略：`shardIndex = pageID % 16`，每个 shard 一把 mutex 保护 map 操作。

#### 3.8.2 Get（搜索）的锁协议

**全程只加读锁，每次只持有一把锁**：

```
Get(key):
  currentPID = root (atomic load)
  loop:
    RLock(currentPID)
    node = pageStore.Read(currentPID)

    if node.IsLeaf:
      // B-link 修正：key 可能因并发 split 移到了右兄弟
      if key > node.HighKey && node.Next != 0:
        RUnlock(currentPID)
        currentPID = node.Next
        continue                    // 沿 right-link 继续
      result = node.Search(key)
      RUnlock(currentPID)
      return result

    else: // internal node
      childPID = node.FindChild(key)
      RUnlock(currentPID)           // 释放当前锁再锁下一层
      currentPID = childPID
```

**关键点**：
- 永远不同时持有两把锁 → 不可能死锁
- 如果搜索到 leaf 时发现 key 不在范围内（被并发 split 移走了），沿 right-link 继续找

#### 3.8.3 Put（插入）的锁协议

**搜索阶段用读锁，修改阶段用写锁**：

```
Put(key, value):
  // Phase 1: 向下搜索，记录路径（只加读锁）
  path = []
  currentPID = root (atomic load)
  loop:
    RLock(currentPID)
    node = pageStore.Read(currentPID)
    path.append(currentPID)

    if node.IsLeaf:
      RUnlock(currentPID)
      break
    else:
      childPID = node.FindChild(key)
      RUnlock(currentPID)
      currentPID = childPID

  // Phase 2: 对 leaf 加写锁
  leafPID = path[last]
  WLock(leafPID)
  leaf = pageStore.Read(leafPID)

  // B-link 修正：key 可能已不属于这个 leaf
  while key > leaf.HighKey && leaf.Next != 0:
    WUnlock(leafPID)
    leafPID = leaf.Next
    WLock(leafPID)
    leaf = pageStore.Read(leafPID)

  // Phase 3: 插入
  if leaf 有空间:
    leaf.Insert(key, value)
    pageStore.Write(leafPID, serialize(leaf))
    WUnlock(leafPID)
    return

  // Phase 4: Split
  left, right, splitKey = leaf.Split()
  rightPID = pageStore.Alloc()
  right.Next = left.Next       // 继承原 leaf 的右兄弟
  left.Next = rightPID         // left 指向新的 right

  // 决定 key 插入哪边
  if key > splitKey:
    right.Insert(key, value)
  else:
    left.Insert(key, value)

  pageStore.Write(rightPID, serialize(right))
  pageStore.Write(leafPID, serialize(left))
  WUnlock(leafPID)

  // Phase 5: Split 向上传播
  propagateSplit(path, splitKey, rightPID)
```

#### 3.8.4 Split 向上传播

```
propagateSplit(path, splitKey, newChildPID):
  // 从 path 倒数第二个开始（parent of leaf）向上
  for i = len(path)-2; i >= 0; i--:
    parentPID = path[i]
    WLock(parentPID)
    parent = pageStore.Read(parentPID)

    if parent 有空间:
      parent.InsertSeparator(splitKey, newChildPID)
      pageStore.Write(parentPID, serialize(parent))
      WUnlock(parentPID)
      return                    // 传播结束

    // Parent 也满了，继续 split
    parentLeft, parentRight, newSplitKey = parent.Split()
    newParentPID = pageStore.Alloc()

    // 决定 separator 插入哪边
    if splitKey > newSplitKey:
      parentRight.InsertSeparator(splitKey, newChildPID)
    else:
      parentLeft.InsertSeparator(splitKey, newChildPID)

    pageStore.Write(newParentPID, serialize(parentRight))
    pageStore.Write(parentPID, serialize(parentLeft))
    WUnlock(parentPID)

    // 准备下一层传播
    splitKey = newSplitKey
    newChildPID = newParentPID

  // 到达 root 还需要 split → 创建新 root
  newRoot = NewInternalNode(entries: [{key:0, child:oldRootPID}, {key:splitKey, child:newChildPID}])
  newRootPID = pageStore.Alloc()
  pageStore.Write(newRootPID, serialize(newRoot))
  atomic.Store(&rootPageID, newRootPID)
```

**关键点**：
- 每层只持有一把写锁，处理完立即释放
- 不需要同时锁 parent + child → 不可能死锁
- 并发读线程如果在 split 过程中访问到旧 node，会通过 right-link 修正找到正确位置

#### 3.8.5 Delete 的锁协议

与 Put 类似：搜索用读锁，修改用写锁，B-link 修正找到正确 leaf。

MVCC 逻辑删除（标记 tombstone）不需要 merge，物理删除和 merge 在 GC 阶段做。

#### 3.8.6 Scan 的锁协议

```
Scan(start, end):
  // 搜索到 start 所在 leaf（同 Get 的读锁协议）
  leafPID = findLeaf(start)

  loop:
    RLock(leafPID)
    leaf = pageStore.Read(leafPID)
    for entry in leaf.Entries:
      if entry.Key >= end: break
      yield entry
    nextPID = leaf.Next
    RUnlock(leafPID)
    if nextPID == 0 or entry.Key >= end: break
    leafPID = nextPID
```

每次只锁一个 leaf，读完释放再锁下一个。Scan 不会阻塞写操作。

#### 3.8.7 为什么这个方案可行

**对比传统 latch crabbing**：

| | Latch Crabbing | B-link per-page RwLock |
|---|---|---|
| 同时持有多把锁 | 是（parent + child） | **否（每次只一把）** |
| 死锁可能 | 需要严格顺序防止 | **不可能** |
| 安全节点判断 | 需要（复杂） | **不需要** |
| Split 时锁范围 | parent + child | **只锁当前节点** |
| 实现复杂度 | 高 | **低** |
| 代价 | - | 读线程偶尔需要 right-link 修正（额外一次 IO） |

**为什么不会丢数据**：
1. 写线程 split 时，先写 right node（含正确数据），再更新 left node 的 Next 指针，最后更新 parent
2. 如果读线程在 step 1 和 step 3 之间访问：
   - 从 parent 下来找到 left → left 的 HighKey 告诉它 key 不在这里 → 沿 Next 找到 right → 正确
   - 从 parent 下来找到 right（parent 已更新）→ 直接正确
3. 如果读线程在 step 1 之前访问：读到旧 leaf（未 split），数据还在 → 正确

**PageStore 层不需要改动**：B-tree 的 per-page 锁已保证同一个 PageID 不会被并发写。PageStore.Write 只需要一个简单的 mutex 保护映射表更新。

#### 3.8.8 Root PageID 的原子更新

```go
type BTree struct {
    rootPageID atomic.Uint64    // 原子操作，无锁读取
    pageLocks  *PageRWLocks
    pages      PageStore
    blobs      BlobStore
}
```

- `Get/Scan` 开始时 `atomic.Load(&rootPageID)` 获取当前 root
- `split root` 时 `atomic.Store(&rootPageID, newRootPID)` 原子更新
- 读线程可能用旧 root 开始搜索 → 通过 right-link 修正仍能找到正确数据

---

## 4. 映射表为什么不用 LSM

**访问模式分析**：
- 每次 B-tree Get：读 3-4 个 node → 查映射表 3-4 次
- 每次 B-tree Put（无 split）：读 3-4 次 + 写 1 次
- **读写比大约 4:1 到 10:1**

**LSM 的问题**：
- 读放大：查一个 key 可能穿透 L0→L1→L2 多层
- 映射表的 key 是 uint64（PageID），value 是 uint64（VAddr），条目极小
- LSM 的 block/bloom filter 开销相对于 16 bytes 的条目来说太重了

**Dense Array 的优势**：
- O(1) 查找：`mapping[pageID]` 直接寻址
- 内存占用可控：100 万 page × 8 bytes = 8MB
- 持久化简单：checkpoint 全量 dump + WAL 增量

**什么时候该用 LSM？**
- 如果 pageID 空间极大且稀疏（比如 UUID 做 key）→ dense array 浪费内存
- 如果映射表不需要全量加载到内存 → LSM 可以按需读取

**当前场景**：pageID 自增、密集、总量可控 → **dense array 更合适**。

---

## 5. 模块依赖

```
kvstore       →  btree, blobstore, wal
btree         →  pagestore, blobstore
pagestore     →  segmentmgr, wal
blobstore     →  segmentmgr, wal
segmentmgr    →  (os)
wal           →  (os)
gc            →  pagestore, blobstore, segmentmgr
```

```
go-fast-kv/
  go.mod
  store.go                ← KVStore 公开接口
  docs/DESIGN.md
  internal/
    segment/              ← Segment Manager
    wal/                  ← 共享 WAL
    pagestore/            ← PageStore (page_id→vaddr)
    blobstore/            ← BlobStore (blob_id→vaddr)
    btree/                ← B-link Tree
    gc/                   ← GC (page + blob)
```

---

## 6. 实现顺序

| 阶段 | 模块 | 测试标准 |
|------|------|---------|
| 1 | segment | Append + ReadAt + Rotate |
| 2 | wal | Append + Sync + Replay + Truncate |
| 3 | pagestore | Alloc + Write + Read + checkpoint 恢复 |
| 4 | blobstore | Write + Read + Delete + checkpoint 恢复 |
| 5 | btree（mock pagestore） | Put/Get/Delete/Scan 1000 keys 正确 |
| 6 | btree + pagestore 集成 | 持久化后重启，数据完整 |
| 7 | kvstore | 公开接口 + 大 value 透明存储 + 崩溃恢复 |
| 8 | gc | 写入→删除→GC→验证空间回收 |

---

## 7. 设计决策

### 7.1 Page 和 Blob 分开 segment

**结论：分开。**

Page segment 和 Blob segment 各自独立管理。理由：
- Page 固定 4KB，Blob 变长 — 混在一起 GC 时解析复杂
- Page 的写入频率远高于 Blob（每次 B-tree 操作写多个 page，只偶尔写 blob）
- 分开后 GC 可以独立调度：page segment 废弃快可以频繁 GC，blob segment 可以懒一点
- Segment Manager 接口不变，只是实例化两个：`pageSegMgr` 和 `blobSegMgr`

### 7.2 并发模型：多读多写（per-page RwLock + B-link right-link 修正）

**结论：多读多写。**

借鉴 [page-server-rs](../../../page-server-rs) 的 B-link tree 并发方案。

**核心机制**：每个 PageID 一把 RwLock，搜索只加读锁，修改时才加写锁，利用 B-link 的 right-link 处理并发 split。

**详见 3.8 节。**

### 7.3 Inline 阈值：256 bytes

**结论：256 bytes。**

- Leaf entry 中 inline value 最大 256 bytes
- 超过 256 bytes 的 value 存入 BlobStore，leaf 中只存 BlobRef（8 bytes blobID）
- 理由：4KB page 大约能放 10-15 个 inline entry（key + 256B value），split 频率合理
- 如果阈值太大（比如 1KB），每个 page 只能放 3-4 个 entry，树太深
- 如果阈值太小（比如 64B），大量中等 value 都走 blob，多一次 IO

### 7.4 Checkpoint 频率：WAL 达到 16MB

**结论：WAL 文件大小达到 16MB 时触发 checkpoint。**

- 不按写入次数，按 WAL 体积 — 直接控制恢复时间
- 16MB WAL ÷ 33 bytes/record ≈ 50 万条 record，重放约 0.5 秒
- Checkpoint 本身是全量 dump 映射表，100 万条 × 16 bytes = 16MB，写入很快
- Checkpoint 完成后截断旧 WAL

### 7.5 Segment 大小：64MB

**结论：64MB。**

- 太小（4MB）：文件数多，GC 频繁，fd 开销大
- 太大（1GB）：GC 一次 copy 量大，延迟高
- 64MB 是 LevelDB/RocksDB SSTable 的常见大小，经过验证的平衡点

### 7.6 Dense Array 扩容：初始 1024，倍增，上限 realloc

**结论：**
- 初始容量 1024 个槽位（8KB 内存）
- 当 pageID/blobID 超出当前容量时，倍增扩容
- Go 的 slice append 本身就是倍增策略，直接用 slice 即可：
  ```go
  type DenseMap struct {
      entries []VAddr  // index = pageID, value = VAddr
  }
  ```
- 100 万 page 时占 8MB，1000 万 page 时占 80MB — 对嵌入式 KV 来说完全可接受

### 7.7 Segment 中存 pageID/blobID header

**结论：存。**

每条数据前面带 8 bytes 的 ID header：
```
Page segment:  [pageID uint64][4096 bytes page data]  = 4104 bytes per entry
Blob segment:  [blobID uint64][size uint32][blob data] = 12 + size bytes per entry
```

理由：
- GC 时顺序扫描 segment，直接读出 ID，查映射表判断是否存活 — O(1)
- 如果不存 ID，GC 需要遍历整个映射表找出哪些 VAddr 落在该 segment 内 — O(N)
- 8 bytes 的空间开销相对于 4KB page 可以忽略（0.2%）
