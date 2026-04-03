# go-fast-kv

高性能键值存储数据库，采用 B-link-tree + 追加写入 + 异步 WAL 架构。

## 核心特性

| 特性 | 描述 |
|------|------|
| B-link-tree | 页面级锁，支持并发读取 |
| 追加写入 | 无随机写入，提升 I/O 效率 |
| 异步 WAL | 3层通道架构，无锁写入 |
| 分层 GC | 阈值 + 稳定性检查 |

## 性能

| Benchmark | 结果 |
|-----------|------|
| Sequential Put | ~2-3.5M ops/sec |
| Sequential Get | ~6-8M ops/sec |
| Concurrent Writes | >500K ops/sec |

## 快速开始

```bash
# 克隆
git clone https://github.com/akzj/go-fast-kv.git
cd go-fast-kv

# 编译
go build ./cmd/gofastkv

# 运行
./gofastkv -db /tmp/kvdb set key value
./gofastkv -db /tmp/kvdb get key

# 测试
go test ./... -v

# 性能测试
go test ./pkg/kvstore/internal/... -bench=. -benchmem
```

## 项目结构

```
pkg/
├── objectstore/    # 统一对象存储 (Page + Blob)
├── wal/           # 异步 WAL (3层通道)
├── btree/         # B-link-tree (页面级锁)
├── gc/            # GC 控制器
└── kvstore/       # KV 数据库集成
```

每个模块遵循 **模块隔离原则**:
- `api/api.go` - 公共接口
- `internal/` - 私有实现
- `{mod}.go` - 导出

## 架构设计

详见 [docs/architecture.md](docs/architecture.md)

## License

MIT
