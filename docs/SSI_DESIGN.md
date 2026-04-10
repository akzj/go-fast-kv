# go-fast-kv v5: Serializable Snapshot Isolation

## 1. 概述

**目标**: 在 v5-serializable 分支实现 Serializable Snapshot Isolation (SSI)

**当前问题**: v4 实现的是 Snapshot Isolation (SI)，允许 Write Skew 异常

### SI vs SSI

| 隔离级别 | Write Skew | 实现复杂度 | 性能 |
|----------|------------|-----------|------|
| SI | ❌ 允许 | 低 | 高 |
| SSI | ✅ 防止 | 中 | 中 |
| Serailizable | ✅ 防止 | 高 | 低 |

---

## 2. SSI 核心概念

### 2.1 冲突类型

**RW-Conflict (读-写冲突)**:
```
T1: BEGIN; SELECT balance FROM accounts WHERE id=1;  -- reads X
T2:           UPDATE accounts SET balance=100 WHERE id=1; COMMIT;
T1: COMMIT;
-- T1 在提交时检测到 RW-conflict，必须 abort
```

**WW-Conflict (写-写冲突)**:
```
T1: BEGIN; UPDATE accounts SET balance=50 WHERE id=1;
T2: BEGIN; UPDATE accounts SET balance=75 WHERE id=1;
T1: COMMIT;
T2: COMMIT;
-- T2 在提交时检测到 WW-conflict，必须 abort
```

### 2.2 SSI Indexes

```go
// 全局 SSI 索引
type SSIIndex struct {
    mu sync.RWMutex
    
    // SIndex: Key → 上次写入并提交的事务信息
    // 用于检测 RW-conflict (我的读 vs 你的写)
    SIndex map[Key]*WriteInfo
    
    // TIndex: Key → 上次读取的事务 ID
    // 用于检测 WW-conflict (我的写 vs 你的读)
    TIndex map[Key]uint64
}

type WriteInfo struct {
    TxnID    uint64  // 写入事务 ID
    CommitTS uint64  // 提交时间戳
}
```

### 2.3 事务 SSI 状态

```go
type Transaction struct {
    // 现有 MVCC 字段
    ID      uint64
    Status  TxnStatus
    Snap    *Snapshot
    
    // SSI 新增字段
    SSI     *SSIState
}

type SSIState struct {
    RWSet       map[Key]struct{}  // 读取的 key 集合
    WWSet       map[Key]struct{}  // 写入的 key 集合
    CommitTS    uint64             // 分配的提交时间戳
    Dangerous   bool               // 是否检测到危险结构
    Conflicts   []Conflict        // 冲突详情
}

type Conflict struct {
    Type     ConflictType  // RW or WW
    Key      Key
    OtherTxn uint64
    Reason   string
}
```

---

## 3. SSI 算法

### 3.1 事务开始

```go
func (txn *TxnManager) Begin() *Transaction {
    txn.mu.Lock()
    defer txn.mu.Unlock()
    
    xid := txn.nextXID
    txn.nextXID++
    txn.active[xid] = struct{}{}
    
    snap := &Snapshot{
        XID:      xid,
        XMin:     txn.getOldestActiveXID(),
        XMax:     xid,
        ActiveXIDs: copyMap(txn.active),
    }
    
    return &Transaction{
        ID:    xid,
        Status: InProgress,
        Snap:  snap,
        SSI: &SSIState{
            RWSet: make(map[Key]struct{}),
            WWSet: make(map[Key]struct{}),
        },
    }
}
```

### 3.2 读取操作 (带 SSI 跟踪)

```go
func (txn *Transaction) Get(key []byte) ([]byte, error) {
    // 1. 标准 MVCC 读取
    value, err := txn.tree.Get(key, txn.Snap, txn.clog)
    if err != nil {
        return nil, err
    }
    
    // 2. SSI: 记录读集
    k := bytesToKey(key)
    txn.SSI.RWSet[k] = struct{}{}
    
    // 3. SSI: 检查 RW-conflict
    // 如果在我快照开始后有事务提交了对我读过的 key 的写入，我必须 abort
    sindex := txn.ssiIndex
    
    sindex.mu.RLock()
    if info := sindex.SIndex[k]; info != nil {
        // 有一个写事务在我快照之后提交了
        if info.CommitTS > txn.Snap.XMin {
            txn.SSI.Dangerous = true
            txn.SSI.Conflicts = append(txn.SSI.Conflicts, Conflict{
                Type:     RWConflict,
                Key:      k,
                OtherTxn: info.TxnID,
                Reason:   fmt.Sprintf("Read %v at snapshot [%d,%d), but txn %d committed at %d", k, txn.Snap.XMin, txn.Snap.XMax, info.TxnID, info.CommitTS),
            })
        }
    }
    sindex.mu.RUnlock()
    
    return value, nil
}
```

### 3.3 写入操作 (带 SSI 跟踪)

```go
func (txn *Transaction) Put(key, value []byte) error {
    k := bytesToKey(key)
    
    // 1. 检查 key 是否已被当前事务读取 (Read-Your-Writes)
    if _, read := txn.SSI.RWSet[k]; read {
        // 自己读了又写，可以继续（Read-Your-Writes 语义）
    }
    
    // 2. SSI: 检查 WW-conflict
    // 如果在我快照期间有事务读取了我要写的 key，我必须 abort
    sindex := txn.ssiIndex
    
    sindex.mu.RLock()
    if readerID := sindex.TIndex[k]; readerID != 0 && readerID != txn.ID {
        // 有其他事务读取了这个 key
        if txn.isActive(readerID) {
            txn.SSI.Dangerous = true
            txn.SSI.Conflicts = append(txn.SSI.Conflicts, Conflict{
                Type:     WWConflict,
                Key:      k,
                OtherTxn: readerID,
                Reason:   fmt.Sprintf("Write %v, but active txn %d read it", k, readerID),
            })
        }
    }
    sindex.mu.RUnlock()
    
    // 3. 执行写入
    txn.SSI.WWSet[k] = struct{}{}
    return txn.tree.Put(key, value, txn.ID)
}
```

### 3.4 提交验证

```go
func (txn *Transaction) Commit() error {
    // 1. SSI 冲突检测
    if txn.SSI.Dangerous {
        return txn.Abort(), ErrSerializationFailure
    }
    
    // 2. 双重检查: 在持有锁的情况下再次验证
    txn.mgr.mu.Lock()
    defer txn.mgr.mu.Unlock()
    
    // 检查在我执行期间是否有新的冲突
    if !txn.validateSSI() {
        txn.Abort()
        return ErrSerializationFailure
    }
    
    // 3. 获取提交时间戳
    commitTS := txn.mgr.nextCommitTS
    txn.mgr.nextCommitTS++
    
    // 4. 更新 SSI 索引 (update-after-validate)
    sindex := txn.mgr.ssiIndex
    
    // 更新 SIndex: 记录写入的 key
    for key := range txn.SSI.WWSet {
        sindex.SIndex[key] = &WriteInfo{
            TxnID:    txn.ID,
            CommitTS: commitTS,
        }
    }
    
    // 更新 TIndex: 记录读取的 key
    for key := range txn.SSI.RWSet {
        sindex.TIndex[key] = txn.ID
    }
    
    // 5. WAL + CLOG 提交
    batch := txn.mgr.wal.NewBatch()
    batch.Add(...)
    txn.mgr.wal.WriteBatch(batch)
    txn.mgr.clog.Commit(txn.ID)
    
    // 6. 从活跃列表移除
    delete(txn.mgr.active, txn.ID)
    
    return nil
}

func (txn *Transaction) validateSSI() bool {
    sindex := txn.mgr.ssiIndex
    
    // 检查 RW-conflict: 是否有比我快照更新的提交写了我读过的 key
    for key := range txn.SSI.RWSet {
        if info := sindex.SIndex[key]; info != nil {
            if info.CommitTS > txn.Snap.XMin && info.TxnID != txn.ID {
                return false  // 有冲突
            }
        }
    }
    
    // 检查 WW-conflict: 是否有活跃事务读了我写过的 key
    for key := range txn.SSI.WWSet {
        if readerID := sindex.TIndex[key]; readerID != 0 && readerID != txn.ID {
            if _, active := txn.mgr.active[readerID]; active {
                return false  // 有冲突
            }
        }
    }
    
    return true  // 无冲突
}
```

---

## 4. 接口设计

### 4.1 新增错误类型

```go
// internal/txn/api/errors.go
var (
    ErrSerializationFailure = errors.New("txn: serialization failure (write skew detected)")
    ErrSSIConflict          = errors.New("txn: SSI conflict detected")
)
```

### 4.2 TxnManager 扩展

```go
// internal/txn/api/api.go

type TxnManager interface {
    // 现有接口
    Begin() *Transaction
    Get(key []byte) ([]byte, error)
    Put(key, value []byte) error
    Delete(key []byte) error
    Commit() error
    Abort() error
    
    // 新增 SSI 接口
    SetIsolationLevel(level IsolationLevel) error
    GetIsolationLevel() IsolationLevel
    
    // SSI 索引访问 (内部)
    GetSSIIndex() *SSIIndex
}

type IsolationLevel int
const (
    IsolationSnapshot IsolationLevel = iota  // SI (default)
    IsolationSerializable                    // SSI
)
```

---

## 5. 实现计划

### Phase 1: SSI Core
1. 创建 `internal/ssi/` 模块
2. 实现 SSIIndex (SIndex, TIndex)
3. 实现 SSIState 跟踪

### Phase 2: Integration
1. 修改 TxnManager 支持 SSI 模式
2. 在 Read/Write 中添加 SSI 跟踪
3. 实现提交验证

### Phase 3: Testing
1. 测试 Write Skew 防止
2. 测试序列化正确性
3. 性能对比测试

---

## 6. Write Skew 测试案例

```go
func TestWriteSkew(t *testing.T) {
    // 初始化 SSI 模式
    db, err := kvstore.Open(kvstore.Options{
        IsolationLevel: kvstore.IsolationSerializable,
    })
    
    // 初始状态: 2 个医生都在值班
    db.Put([]byte("doctor:alice:on_call"), []byte("true"))
    db.Put([]byte("doctor:bob:on_call"), []byte("true"))
    
    // T1: 读取两个医生状态
    txn1 := db.Begin()
    alice1, _ := txn1.Get([]byte("doctor:alice:on_call"))
    bob1, _ := txn1.Get([]byte("doctor:bob:on_call"))
    
    // T2: 读取两个医生状态
    txn2 := db.Begin()
    alice2, _ := txn2.Get([]byte("doctor:alice:on_call"))
    bob2, _ := txn2.Get([]byte("doctor:bob:on_call"))
    
    // T1: 只要 Alice 在值班，就让 Bob 下班
    if string(alice1) == "true" {
        txn1.Put([]byte("doctor:bob:on_call"), []byte("false"))
    }
    
    // T2: 只要 Bob 在值班，就让 Alice 下班
    if string(bob2) == "true" {
        txn2.Put([]byte("doctor:alice:on_call"), []byte("false"))
    }
    
    // 提交顺序: T1 先提交
    err1 := txn1.Commit()  // 成功
    assert.NoError(t, err1)
    
    // T2 后提交 - 应该检测到 Write Skew 并 abort
    err2 := txn2.Commit()  
    assert.Error(t, err2, ErrSerializationFailure)
    
    // 最终状态: 一个医生下班，一个还在
    // 不会出现两个都下班的情况
}
```

---

## 7. 与 MVCC 的关系

### 7.1 MVCC 保持不变

MVCC 的核心机制 (TxnMin/TxnMax, Snapshot, Visibility) 保持不变：

```go
// Visibility 判断仍然基于 MVCC
func (snap *Snapshot) IsVisible(txnMin, txnMax uint64) bool {
    // ...
}
```

### 7.2 SSI 在 MVCC 之上

```
┌─────────────────────────────────────┐
│          SSI Layer                  │  ← 新增: 冲突检测
│  - RWSet / WWSet 跟踪              │
│  - validateSSI()                    │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│          MVCC Layer                  │  ← 现有: 版本管理
│  - TxnMin / TxnMax                 │
│  - Snapshot visibility              │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│          Storage Layer               │  ← 现有: B-tree + WAL
│  - PageStore / BlobStore            │
│  - WAL + Checkpoint                │
└─────────────────────────────────────┘
```

---

## 8. 性能考虑

### 8.1 SSI 开销

- **内存**: RWSet/WWSet 跟踪，每个 key 8 bytes
- **时间**: 每次 Read/Write 多一次 map 查询 O(1)
- **提交**: validateSSI() 需要遍历 RWSet/WWSet

### 8.2 优化策略

1. **延迟更新**: TIndex 不立即更新，提交时再更新
2. **索引裁剪**: 定期清理过期的 SIndex/TIndex 条目
3. **批量验证**: 多个事务一起验证，减少锁竞争

---

## 9. 限制

### 9.1 Predicate-based SSI

当前实现是基于 key 的 SSI，不是完整的 Predicate-based SSI。

```go
// 当前: 检查具体的 key
txn.SSI.RWSet["doctor:alice:on_call"]

// Predicate SSI 应该检查:
// SELECT * FROM doctors WHERE on_call=true
// 这个查询依赖哪些 key？
```

### 9.2 适用场景

- 简单的 key-value 操作
- 明确的读写 key 集合
- 需要防止 Write Skew 的业务逻辑

### 9.3 不适用场景

- 复杂的 WHERE 条件
- 聚合查询 (COUNT, SUM, etc.)
- 范围查询的 Write Skew