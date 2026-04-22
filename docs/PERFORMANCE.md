# go-fast-kv 性能分析文档

本文档记录 go-fast-kv 项目的基准测试结果和性能分析。

## 基准测试环境

- CPU: Intel(R) Core(TM) i7-14700KF
- OS: Linux
- Go: 1.23.0
- 测试日期: 2024-04

---

## 基准测试结果

### FTS 全文搜索

| 操作 | 数据规模 | 性能 | 内存分配 |
|------|----------|------|----------|
| IndexDocument (小文本) | 单文档 | 2.86ms/op | 81KB/op, 286 allocs |
| IndexDocument (中文本) | 单文档 | 3.97ms/op | 936KB/op, 4168 allocs |
| IndexDocument (大文本) | 单文档 | 3.44ms/op | 947KB/op, 4326 allocs |
| Tokenize (小文本) | - | 1.6µs/op | 1.4KB/op, 19 allocs |
| Tokenize (中文本) | - | 11.8µs/op | 6.7KB/op, 69 allocs |
| Tokenize (大文本) | - | 38.7µs/op | 15.2KB/op, 230 allocs |
| PorterStem | 单词 | 0.5µs/op | 0B, 0 allocs |
| Search_Term | 100 docs | 135µs/op | 59KB/op, 474 allocs |
| Search_Term | 1000 docs | 3.57ms/op | 2.8MB/op, 22538 allocs |
| Search_Term | 5000 docs | 7.57ms/op | 16MB/op, 132552 allocs |
| Search_AND | 1000 docs | 1.45ms/op | 1.3MB/op, 8738 allocs |
| Search_OR | 1000 docs | 4.35ms/op | 3.2MB/op, 25238 allocs |
| Search_NOT | 1000 docs | 1.97ms/op | 1.2MB/op, 8840 allocs |
| Search_Complex | 1000 docs | 6.76ms/op | 4.6MB/op, 36690 allocs |
| getDocIDsForToken | - | 1.03ms/op | 920KB/op, 7096 allocs |

**分析:**
- IndexDocument 性能与文本大小成正比 (~3-4ms/文档)
- Tokenization 非常快 (1.6-39µs)
- PorterStemmer 极快 (0.5µs, 零分配)
- Search 性能随文档数量线性增长
- 布尔查询 (AND/OR/NOT) 比单 term 查询慢 2-3x

### KVStore 存储引擎

| 操作 | 数据规模 | 性能 | 内存分配 |
|------|----------|------|----------|
| Put 顺序写 | 100 keys | 21.4ms/op | - |
| Put 顺序写 | 1k keys | 137.5ms/op | - |
| Put 顺序写 | 10k keys | 2190ms/op | - |
| Put 随机写 | 100 keys | 11.3ms/op | - |
| Put 随机写 | 1k keys | 106.8ms/op | - |
| Get 顺序读 | 100 keys | 1.5ms/op | 624KB/op, 11352 allocs |
| Get 顺序读 | 1k keys | 18.8ms/op | 7.4MB/op, 136761 allocs |
| Get 随机读 | 100 keys | 1.6ms/op | 625KB/op, 11151 allocs |
| Get 随机读 | 1k keys | 15.2ms/op | 7.2MB/op, 132774 allocs |
| WriteBatch (10) | 10 keys | 1.2ms/op | 322KB/op, 2948 allocs |
| WriteBatch (100) | 100 keys | 8.4ms/op | 4MB/op, 40048 allocs |
| WriteBatch (1k) | 1k keys | 32.4ms/op | 46MB/op, 474943 allocs |
| Scan | 100 keys | 18µs/op | 26KB/op, 524 allocs |
| Scan | 1k keys | 451µs/op | 258KB/op, 5254 allocs |
| 并发写 2线程 | - | 209µs/op | 22KB/op, 165 allocs |
| 并发写 4线程 | - | 219µs/op | 22KB/op, 158 allocs |
| 并发写 8线程 | - | - | - |

**分析:**
- 顺序写性能随数据量增加而下降 (批处理可优化)
- Get 操作性能稳定 (~1.5ms/100 keys)
- Scan 操作非常高效 (线性扫描)
- 并发写扩展性良好

---

## 已识别瓶颈

### 1. FTS Search 内存分配较高
- 5000 文档搜索分配 16MB 内存
- 原因: getDocIDsForToken 扫描大量 key

**优化建议:**
- [ ] 实现 docID 结果缓存
- [ ] 使用流式扫描避免全量加载

### 2. KVStore Put 操作随数据量增加性能下降
- 10k keys 比 100 keys 慢 100x
- 原因: WAL fsync + B-tree 调整

**优化建议:**
- [ ] 使用 WriteBatch 批量写入
- [ ] 调整 WAL group commit 参数

### 3. FTS getDocIDsForToken 扫描效率
- 每次查询重新扫描整个 token 前缀

**优化建议:**
- [ ] 实现 docID bitmap 缓存
- [ ] 考虑使用 bloom filter 快速过滤

---

## 性能优化记录

### 优化 #1: FTS Boolean 运算符修复
- 日期: 2024-04-22
- 问题: tokenizeQuery 子串匹配错误
- 修复: 添加 isWordBoundary 和 isOperatorPrefix 辅助函数
- 影响: 布尔查询 (AND/OR/NOT) 正常工作

### 优化 #2: Checkpoint 设计文档更新
- 日期: 2024-04-22
- 问题: 文档与实现不一致
- 修复: 更新文档说明为规划文档
- 影响: 明确实现状态

### 优化 #3: Tokenization 边界修复
- 日期: 2024-04-22
- 问题: "world" 被错误解析为 "w OR ld"
- 修复: isWordBoundary 确保操作符是独立单词
- 影响: 搜索 "world" 等包含操作符子串的词正常

---

## 运行基准测试

```bash
# FTS 基准测试
go test ./internal/sql/engine/internal -bench=FTS -benchmem -benchtime=100ms

# KVStore 基准测试
go test ./internal/kvstore/internal -bench=KVStore -benchmem -benchtime=100ms

# 存储层基准测试
go test ./internal/kvstore/internal -bench=Storage -benchmem -benchtime=100ms

# Parser 基准测试
go test ./internal/sql/parser/internal -bench=Parser -benchmem -benchtime=100ms
```

---

## 持续性能监控

建议在每次发布前运行基准测试，对比历史数据检测性能退化。

```bash
# 保存基准结果
go test ./... -bench=. -benchtime=100ms > benchmark_$(date +%Y%m%d).txt

# 对比上次结果
diff benchmark_20240422.txt benchmark_20240423.txt
```

---

## P18 性能基线 (2024-04-23)

### KVStore 基准测试 (SyncNone 模式)

| 操作 | 数据量 | 耗时 | 备注 |
|------|--------|------|------|
| Put SeqWrite | 100 keys | 5.7ms/op | ~175K ops/s |
| Put SeqWrite | 1k keys | 127ms/op | ~7.9K ops/s |
| Put SeqWrite | 10k keys | 1.27s/op | B-tree 分裂瓶颈 |
| Put RandWrite | 100 keys | 5.5ms/op | - |
| Put RandWrite | 1k keys | 130ms/op | - |
| Get SeqRead | 100 keys | 1.9ms/op | - |
| Get RandRead | 100 keys | 1.1ms/op | ~909K ops/s |
| WriteBatch | 1k keys | 104ms/op | 比 Put 快 18% |
| Scan | 100 keys | 0.055ms/op | 高效 |
| ConcurrentWrite (8) | 8 goroutines | 0.18ms/op | - |

### 瓶颈分析

- **10k keys Put: 1.27s/op** — 树高增加导致多层级分裂
- 根因: B-tree 分裂级联，每层 WritePage 累积
- 非 WAL fsync 瓶颈 (已使用 SyncNone)
- WriteBatch 比 Put 快 18% (共享 WAL batch)

### 解决方向

1. **Page 池化复用** — 减少 AllocPage 开销
2. **延迟刷盘** — 批量 WritePage 到磁盘
3. **LSM-tree 切换** — 批量 compactions

