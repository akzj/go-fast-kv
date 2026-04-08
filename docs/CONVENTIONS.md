# go-fast-kv 开发规范

**目的**：锚定实现阶段的开发方法论，防止漂移和遗忘。
**来源**：System Design Skill 方法论 + v1-v3 失败教训 + v4 设计 review 经验。

---

## 1. 核心约束

> **AI generates code well. AI modifies code poorly.**

每一条规范都服务于同一个目标：**减少实现时需要阅读的上下文量**。

上下文预算（128k tokens）：
- 读 5 个 500 行文件 ≈ 25k tokens — 已消耗一半文件预算
- 所以：**能读 1 个文件解决的，绝不读 5 个**

---

## 2. 三大支柱

### 支柱 1：Interface-First（接口先行）

**每个模块第一步：写 `api/api.go`，定义所有公开接口。**

```
internal/{module}/
├── api/api.go          ← 所有公开接口（只读这一个文件就能理解模块）
├── {module}.go         ← 实现
├── {module}_test.go    ← 测试
└── internal/           ← 内部实现细节（如有需要）
```

规则：
- 所有接口、公开类型、错误定义集中在 `api/api.go`
- 接口小而精（1-3 个方法），不暴露实现细节
- 不用 `interface{}`/`any` 做返回类型 — 让编译器帮我检查
- 接口文档要完整：参数、返回值、错误场景

```go
// 示例：internal/segment/api/api.go
package segmentapi

import "errors"

var (
    ErrSegmentFull   = errors.New("segment: full")
    ErrInvalidVAddr  = errors.New("segment: invalid vaddr")
)

type VAddr struct {
    SegmentID uint32
    Offset    uint32
}

// SegmentManager manages append-only segment files.
type SegmentManager interface {
    // Append writes data to the active segment, returns its VAddr.
    // Returns ErrSegmentFull if active segment is sealed (caller should Rotate first).
    Append(data []byte) (VAddr, error)

    // ReadAt reads size bytes starting at addr.
    // Returns ErrInvalidVAddr if addr is out of bounds.
    ReadAt(addr VAddr, size uint32) ([]byte, error)

    // Rotate seals the active segment and opens a new one.
    Rotate() error

    // RemoveSegment deletes a sealed segment file (used by GC).
    RemoveSegment(segID uint32) error

    // Close flushes and closes all segment files.
    Close() error
}
```

### 支柱 2：Dependency Inversion（依赖反转）

**下层不导入上层。依赖方向永远向下。**

```
kvstore  →  btree, txn, wal
btree    →  pagestore, blobstore, txn/visibility
pagestore →  segment, wal
blobstore →  segment, wal
txn      →  wal
segment  →  (os)
wal      →  (os)
vacuum   →  btree, pagestore, blobstore, txn
gc       →  pagestore, blobstore, segment
```

- 模块之间通过接口交互，不直接导入实现
- 组装在 `store.go`（composition root）中完成
- 编译器在组装点自动验证接口兼容性

### 支柱 3：Small Modules（小模块）

| 粒度 | 上限 | 原因 |
|------|------|------|
| 模块 | < 2000 行 | 能完整放入上下文 |
| 文件 | < 500 行 | 一次读完 |
| 函数 | < 100 行 | 完整推理 |

go-fast-kv 的 7 个模块天然满足这个约束 — 每个模块职责单一，预估 500-1500 行。

---

## 3. 实现流程

### 每个模块的实现步骤

```
Step 1: 写 api/api.go
  - 从 DESIGN.md 对应章节提取接口
  - 定义错误类型
  - 写完整文档注释
  - go build 验证编译通过

Step 2: fork 实现
  - fork_explore 创建分支，只需读 api/api.go 就能实现
  - 实现放在 {module}.go
  - 每写完一个函数就 go build

Step 3: 写测试
  - 测试标准来自 DESIGN.md §6
  - go test -v 验证通过

Step 4: git commit
  - 一个模块一个 commit（或多个小 commit）

Step 5: 下一个模块
```

### 模块实现顺序（来自 DESIGN.md §6）

| 阶段 | 模块 | 依赖 | 测试标准 |
|------|------|------|---------|
| 1 | segment | (os) | Append + ReadAt + Rotate |
| 2 | wal | (os) | Append + Sync + Replay + Truncate |
| 3 | pagestore | segment, wal | Alloc + Write + Read + checkpoint 恢复 |
| 4 | blobstore | segment, wal | Write + Read + Delete + checkpoint 恢复 |
| 5 | btree | pagestore, blobstore | Put/Get/Delete/Scan 1000 keys（含 MVCC 字段） |
| 6 | btree + pagestore 集成 | - | 持久化后重启，数据完整 |
| 6.5 | txn | wal | XID + CLOG + Snapshot + Visibility |
| 7 | kvstore | btree, txn, wal | 公开接口 + 大 value + auto-commit + 崩溃恢复 |
| 8 | gc | pagestore, blobstore, segment | 写入→删除→GC→验证空间回收 |
| 8.5 | vacuum | btree, pagestore, blobstore, txn | 旧版本清理 + 空间回收 |

---

## 4. 工具选择原则

### fork_explore vs delegate_task

```
fork_explore = 共享机制（我自己的手脚）
  ├── 继承完整上下文（对话历史、WM、LTM）
  ├── 适合：修改文件、实现代码、需要项目全貌的任务
  ├── 任务描述锚定方向，不需要重复解释背景
  └── ⚠️ 写同一文件必须串行

delegate_task = 隔离机制（独立承包商）
  ├── 从干净上下文开始（只有 goal/background/constraints）
  ├── 适合：独立的只读分析（review）、不需要项目历史的任务
  ├── 不受父 agent 认知偏见影响（review 更客观）
  └── 风险：缺乏上下文可能遗漏隐含约束
```

**选择规则**：

| 场景 | 工具 | 原因 |
|------|------|------|
| 实现一个模块 | fork_explore | 需要读 DESIGN.md + api/api.go，共享认知 |
| Review 代码/文档 | delegate_task | 隔离上下文，独立审查更客观 |
| 多个只读分析 | 并行 fork 或 delegate | 不写文件，安全并行 |
| 多个写文件任务 | 串行 fork | 避免文件冲突 |

### 模块边界 = Fork 边界

**终极检验**：branch agent 只读 `api/api.go` 就能实现这个模块吗？

- ✅ 能 → 模块设计合格
- ❌ 不能 → 模块太耦合，需要拆分或补充接口文档

---

## 5. 防漂移检查清单

### 每个模块开始前
- [ ] 先写 `api/api.go`，不直接写实现
- [ ] 接口从 DESIGN.md 对应章节提取
- [ ] 错误类型定义在 `api/api.go` 中
- [ ] 依赖方向正确（只依赖下层模块的 api/）

### 每次代码修改后
- [ ] `go build ./...` 编译通过
- [ ] `go test ./...` 测试通过
- [ ] 文件 < 500 行
- [ ] 函数 < 100 行

### 每个模块完成后
- [ ] 模块 < 2000 行
- [ ] 所有公开接口在 `api/api.go` 中
- [ ] 测试覆盖 DESIGN.md §6 中的测试标准
- [ ] git commit 完成

### 遇到困难时
- [ ] 同一文件失败 3 次 → 停止，revert，fork 调查
- [ ] 需要读 5+ 文件 → fork 而不是自己硬撑
- [ ] 修改前先说假设："我认为根因是 X 因为 Y"
- [ ] 不要批量修改 — 每改一处就验证

---

## 6. v1-v3 教训（永远记住）

1. **API 和实现脱节** — 接口写了 PageID，实现用 VAddr → **Interface-First 防止此问题**
2. **空壳模块** — GC/Compaction 有接口无实现 → **每个模块必须有测试证明它工作**
3. **fork 报告不可信** — 必须亲自验证 → **delegate review 验证，不盲信**
4. **补丁思维** — 在错误架构上打补丁 → **Generate, Don't Modify**
5. **过度分析** — 200+ turns 分析一个 bug → **3-Strike Rule，及时止损**
6. **上下文过载** — 同时读 10+ 文件改一个 bug → **Fork When Context Gets Heavy**
