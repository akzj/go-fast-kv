# go-fast-kv pkg/kvstore 使用指南

本文档介绍如何使用 go-fast-kv 的公开 API 包 `pkg/kvstore`。

## 目录

1. [安装和导入](#安装和导入)
2. [快速开始](#快速开始)
3. [API 参考](#api-参考)
4. [完整示例](#完整示例)
5. [错误处理](#错误处理)
6. [最佳实践](#最佳实践)

---

## 安装和导入

```bash
go get github.com/akzj/go-fast-kv
```

```go
import "github.com/akzj/go-fast-kv/pkg/kvstore"
```

---

## 快速开始

5 行代码完成数据库创建和基本操作：

```go
// 1. 创建配置
cfg := kvstore.Config{Dir: "/path/to/db"}

// 2. 打开数据库
store, err := kvstore.Open(cfg)
if err != nil {
    log.Fatal(err)
}
defer store.Close()

// 3. 写入数据
store.Put([]byte("key"), []byte("value"))

// 4. 读取数据
value, err := store.Get([]byte("key"))
if err != nil {
    log.Fatal(err)
}
fmt.Println(string(value)) // 输出: value
```

---

## API 参考

### Config 配置

```go
type Config struct {
    Dir string  // 数据库目录路径
}
```

### Store 接口

```go
type Store interface {
    // Put 插入或更新键值对
    Put(key, value []byte) error

    // Get 获取键对应的值
    // 返回 kvstore.ErrKeyNotFound 如果键不存在
    Get(key []byte) ([]byte, error)

    // Delete 删除键
    // 返回 kvstore.ErrKeyNotFound 如果键不存在
    Delete(key []byte) error

    // Scan 遍历 [start, end) 范围内的键
    Scan(start, end []byte) Iterator

    // ScanWithParams 带参数的扫描，支持 LIMIT 和 OFFSET
    ScanWithParams(start, end []byte, params ScanParams) Iterator

    // NewWriteBatch 创建批量写入器
    NewWriteBatch() WriteBatch

    // Checkpoint 执行检查点，持久化当前状态
    Checkpoint() error

    // RunVacuum 执行垃圾回收，清理旧的 MVCC 版本
    RunVacuum() (*VacuumStats, error)

    // DeleteRange 删除范围内的所有键
    DeleteRange(start, end []byte) (int, error)

    // BulkLoad 批量导入预排序的键值对 (O(n) 复杂度)
    BulkLoad(pairs []KVPair) error

    // Close 关闭数据库
    Close() error
}
```

### Iterator 接口

```go
type Iterator interface {
    Next() bool          // 移动到下一个键，返回 false 表示结束
    Key() []byte         // 获取当前键
    Value() []byte       // 获取当前值
    Err() error          // 获取迭代过程中的错误
    Close()              // 关闭迭代器
}
```

### WriteBatch 接口

```go
type WriteBatch interface {
    // Put 暂存键值对，提交前不可见
    Put(key, value []byte) error

    // Delete 暂存删除操作，提交前不可见
    Delete(key []byte) error

    // Commit 原子提交所有暂存的操作
    Commit() error

    // Discard 丢弃所有暂存操作
    Discard()
}
```

### ScanParams 参数

```go
type ScanParams struct {
    Limit  int  // 最大返回键数量，0 表示无限制
    Offset int  // 跳过的键数量
}
```

### SyncMode 同步模式

```go
const (
    // SyncAlways 每次写入后执行 fsync，最高持久化保证
    SyncAlways SyncMode = iota
    
    // SyncNone 不执行 fsync，写入 OS 页缓存后立即返回
    // 崩溃时可能丢失最近写入，Checkpoint 时仍会持久化
    SyncNone
)
```

### KVPair 批量加载结构

```go
type KVPair struct {
    Key   []byte
    Value []byte
}
```

### 错误类型

```go
var (
    ErrKeyNotFound   = errors.New("kvstore: key not found")
    ErrKeyTooLarge   = errors.New("kvstore: key too large")
    ErrClosed        = errors.New("kvstore: store is closed")
    ErrBatchCommitted = errors.New("kvstore: batch already committed or discarded")
    ErrNotImplemented = errors.New("kvstore: not implemented")
)
```

---

## 完整示例

### 基本 CRUD 操作

```go
package main

import (
    "fmt"
    "log"
    
    "github.com/akzj/go-fast-kv/pkg/kvstore"
)

func main() {
    // 打开数据库
    store, err := kvstore.Open(kvstore.Config{Dir: "/tmp/testdb"})
    if err != nil {
        log.Fatal(err)
    }
    defer store.Close()

    // 写入
    err = store.Put([]byte("name"), []byte("Alice"))
    if err != nil {
        log.Fatal(err)
    }

    // 读取
    value, err := store.Get([]byte("name"))
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("name = %s\n", value)

    // 更新
    err = store.Put([]byte("name"), []byte("Bob"))
    if err != nil {
        log.Fatal(err)
    }

    // 删除
    err = store.Delete([]byte("name"))
    if err != nil {
        log.Fatal(err)
    }

    // 验证删除
    _, err = store.Get([]byte("name"))
    if err == kvstore.ErrKeyNotFound {
        fmt.Println("key not found (expected)")
    }
}
```

### 范围扫描

```go
// 扫描所有以 "user:" 开头的键
iter := store.Scan([]byte("user:"), []byte("user;\x00"))
for iter.Next() {
    fmt.Printf("key=%s value=%s\n", iter.Key(), iter.Value())
}
if err := iter.Err(); err != nil {
    log.Fatal(err)
}
iter.Close()

// 带分页的扫描: 获取第 11-20 条记录
params := kvstore.ScanParams{Limit: 10, Offset: 10}
iter = store.ScanWithParams([]byte("a"), []byte("z"), params)
for iter.Next() {
    fmt.Printf("key=%s\n", iter.Key())
}
iter.Close()
```

### 批量写入

```go
// 创建批量写入器
batch := store.NewWriteBatch()

// 添加操作
batch.Put([]byte("k1"), []byte("v1"))
batch.Put([]byte("k2"), []byte("v2"))
batch.Put([]byte("k3"), []byte("v3"))
batch.Delete([]byte("k-old"))

// 提交 (原子操作)
err = batch.Commit()
if err != nil {
    log.Fatal(err)
}
```

### 批量加载 (高性能导入)

```go
// 预排序的键值对
pairs := []kvstore.KVPair{
    {Key: []byte("a:001"), Value: []byte("data1")},
    {Key: []byte("a:002"), Value: []byte("data2")},
    {Key: []byte("a:003"), Value: []byte("data3")},
    // ... 必须按 Key 排序
}

// O(n) 复杂度导入，比逐个 Put 快 100x+
err = store.BulkLoad(pairs)
if err != nil {
    log.Fatal(err)
}
```

### 垃圾回收

```go
// 执行 MVCC 垃圾回收，清理已删除/覆盖的旧版本
stats, err := store.RunVacuum()
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Vacuum: removed %d versions, reclaimed %d bytes\n", 
    stats.RemovedVersions, stats.ReclaimedBytes)
```

### 检查点和恢复

```go
// 执行检查点，持久化当前状态
err = store.Checkpoint()
if err != nil {
    log.Fatal(err)
}

// 崩溃后重启，数据自动恢复
store, err = kvstore.Open(kvstore.Config{Dir: "/tmp/testdb"})
if err != nil {
    log.Fatal(err)
}
```

### 范围删除

```go
// 删除所有以 "temp:" 开头的键
count, err := store.DeleteRange([]byte("temp:"), []byte("temp;\x00"))
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Deleted %d keys\n", count)
```

---

## 错误处理

### 常见错误

```go
// 键不存在
value, err := store.Get([]byte("nonexistent"))
if err == kvstore.ErrKeyNotFound {
    fmt.Println("key not found")
} else if err != nil {
    log.Fatal(err)
}

// 键值过大
err = store.Put(largeKey, largeValue)
if err == kvstore.ErrKeyTooLarge {
    fmt.Println("key or value exceeds maximum size")
}

// 操作已关闭的存储
err = store.Put([]byte("k"), []byte("v"))
if err == kvstore.ErrClosed {
    fmt.Println("store is closed")
}
```

### 批量操作错误处理

```go
batch := store.NewWriteBatch()
batch.Put([]byte("k1"), []byte("v1"))
batch.Put([]byte("k2"), []byte("v2"))

// Commit 失败，已提交的操作会回滚
err = batch.Commit()
if err != nil {
    // 所有操作都不会生效
    log.Fatal(err)
}

// Commit 后再次 Commit 会报错
err = batch.Commit()
if err == kvstore.ErrBatchCommitted {
    fmt.Println("batch already committed")
}

// Discard 后无法使用
batch.Discard()
err = batch.Put([]byte("k3"), []byte("v3"))
if err == kvstore.ErrBatchCommitted {
    fmt.Println("batch was discarded")
}
```

---

## 最佳实践

### 1. 连接管理

```go
// ✅ 推荐: 使用 defer 确保关闭
store, err := kvstore.Open(cfg)
if err != nil {
    return err
}
defer store.Close()

// ❌ 避免: 忘记关闭
store, _ = kvstore.Open(cfg)
// ... 使用 store
// 可能忘记 store.Close()
```

### 2. 批量写入

```go
// ✅ 推荐: 使用 WriteBatch 减少 fsync 开销
batch := store.NewWriteBatch()
for _, item := range items {
    batch.Put(item.Key, item.Value)
}
batch.Commit()

// ❌ 避免: 逐个 Put，每次都有 fsync
for _, item := range items {
    store.Put(item.Key, item.Value)  // 慢!
}
```

### 3. 大数据导入

```go
// ✅ 推荐: 预排序后使用 BulkLoad (O(n))
sort.Slice(pairs, func(i, j int) bool {
    return bytes.Compare(pairs[i].Key, pairs[j].Key) < 0
})
store.BulkLoad(pairs)

// ❌ 避免: 逐个 Put (O(n log n))
for _, p := range pairs {
    store.Put(p.Key, p.Value)  // 慢 100x!
}
```

### 4. 迭代器使用

```go
// ✅ 推荐: 确保关闭迭代器
iter := store.Scan(start, end)
defer iter.Close()
for iter.Next() {
    // 处理 iter.Key(), iter.Value()
}

// ✅ 推荐: 检查迭代器错误
for iter.Next() {
    // ...
}
if err := iter.Err(); err != nil {
    log.Fatal(err)
}
```

### 5. 并发安全

```go
// ✅ Store 可以安全并发使用
// ✅ 每个 goroutine 使用独立的 WriteBatch
go func() {
    batch := store.NewWriteBatch()
    batch.Put(k1, v1)
    batch.Commit()
}()

go func() {
    batch := store.NewWriteBatch()
    batch.Put(k2, v2)
    batch.Commit()
}()

// ❌ 避免: 共享 WriteBatch
batch := store.NewWriteBatch()  // 不要这样做!
```

### 6. 扫描优化

```go
// ✅ 推荐: 使用 LIMIT 限制扫描范围
params := kvstore.ScanParams{Limit: 100}
iter := store.ScanWithParams(start, end, params)
// 存储层会在达到限制后停止扫描

// ❌ 避免: 全量扫描后再限制
iter := store.Scan(start, end)
count := 0
for iter.Next() && count < 100 {
    count++
}
// 需要扫描所有数据，效率低
```

### 7. 垃圾回收策略

```go
// 定期运行 Vacuum reclaim 空间
// 建议在批量删除/更新后执行
store.DeleteRange(start, end)  // 删除大量数据
stats, _ := store.RunVacuum()  // 回收空间

// 或定期执行
ticker := time.NewTicker(5 * time.Minute)
go func() {
    for range ticker.C {
        store.RunVacuum()
    }
}()
```

### 8. 检查点策略

```go
// 定期检查点，减少崩溃恢复时间
ticker := time.NewTicker(1 * time.Minute)
go func() {
    for range ticker.C {
        store.Checkpoint()
    }
}()

// 或在关闭前检查点
defer func() {
    store.Checkpoint()  // 确保数据持久化
    store.Close()
}()
```

---

## 性能提示

| 操作 | 性能 | 建议 |
|------|------|------|
| Put | ~10-20ms | 使用 WriteBatch 批量 |
| Get | ~1-2ms | 随机读取 |
| Scan | ~18µs/100 keys | 使用 LIMIT 限制 |
| WriteBatch | ~1ms/10 ops | 批量写入首选 |
| BulkLoad | O(n) | 大数据导入首选 |
| Vacuum | 后台运行 | 不阻塞读写 |

详见 [PERFORMANCE.md](./PERFORMANCE.md) 获取详细基准测试数据。
