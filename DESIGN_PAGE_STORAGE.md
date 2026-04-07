# Page Storage Service 设计提案

## 当前问题

当前代码中 `InternalEntry.Child` 使用 `VAddr`（存储地址），但这与 Append-Only 存储设计冲突。

## 设计目标

**存储层（Append-Only Page Storage）**:
```
PageID (逻辑地址) ←→ Page Storage ←→ VAddr (物理地址, Append-Only)
```

B-tree 只使用 PageID，不感知底层存储地址。

## 接口设计

### 1. PageStorage 接口

```go
// PageStorage 提供 Page 读写服务，内部维护 PageID → VAddr 映射
type PageStorage interface {
    // CreatePage 创建新页面，返回 PageID（自增唯一）
    CreatePage(data []byte) (PageID, error)
    
    // WritePage 更新页面：追加到存储，更新 PageID → VAddr 映射
    WritePage(pageID PageID, data []byte) (VAddr, error)
    
    // ReadPage 通过 PageID 查找 VAddr，读取页面数据
    ReadPage(pageID PageID) ([]byte, error)
    
    // DeletePage 删除页面（标记删除，保留映射）
    DeletePage(pageID PageID) error
}
```

### 2. B-tree NodeManager 接口（修改后）

```go
type NodeManager interface {
    // 创建新页面
    CreateLeaf() (*NodeFormat, PageID)      // 返回节点和 PageID
    CreateInternal(level uint8) (*NodeFormat, PageID)
    
    // 持久化：写入页面
    Persist(node *NodeFormat, pageID PageID) (VAddr, error)
    
    // 加载：通过 PageID 读取
    Load(pageID PageID) (*NodeFormat, error)
    
    // 更新父节点指向
    UpdateParent(parentPageID, oldChild, newChild PageID, splitKey PageID) error
}
```

### 3. InternalEntry 结构（修改后）

```go
// 当前（错误）
type InternalEntry struct {
    Key   PageID
    Child VAddr  // ❌ 直接用存储地址
}

// 修改后
type InternalEntry struct {
    Key   PageID
    Child PageID  // ✅ 用逻辑 PageID，Load 时查映射表
}
```

## 数据流

### 插入流程
```
1. Insert(key=100, value) 
2. 找到叶节点 PageID=5
3. 叶节点满了，需要分裂
4. CreatePage() → pageID=6 (新叶节点)
5. WritePage(5, left_leaf_data) → 追加存储，返回 vaddr1
6. WritePage(6, right_leaf_data) → 追加存储，返回 vaddr2
7. 更新内部节点: UpdateParent(parentPageID=2, oldChild=5, newChild=6, splitKey=50)
```

### 读取流程
```
1. Get(key=100)
2. search(rootPageID=1, key=100)
3. Load(1) → 查映射 1→vaddr1，读取根节点
4. 找到 Child=5 (PageID)
5. Load(5) → 查映射 5→vaddr5，读取叶节点
```

## 关键点

1. **PageID 是逻辑地址**：B-tree 内部使用，稳定不变
2. **VAddr 是物理地址**：每次 WritePage 可能变化（Append）
3. **映射表在 PageStorage 内部**：PageID → VAddr
4. **Append-Only**：旧版本数据保留，可用于 MVCC 或 GC

## 需要修改的文件

1. `internal/blinktree/api/api.go` - NodeManager 接口、InternalEntry 结构
2. `internal/blinktree/internal/manager.go` - NodeManager 实现，使用 PageStorage
3. `internal/blinktree/internal/tree.go` - InternalEntry 字段改为 PageID
4. `internal/storage/` - 实现 PageStorage（或新增 PageStorage 接口）
5. `internal/kvstore/internal/` - 适配 kvStore 使用

## 现有代码对比

| 组件 | 当前 | 目标 |
|------|------|------|
| InternalEntry.Child | VAddr | PageID |
| NodeManager.CreateLeaf() | → VAddr | → PageID |
| NodeManager.Persist() | (node) → VAddr | (node, pageID) → VAddr |
| NodeManager.Load() | (vaddr) | (pageID) |
| Load 路径 | VAddr → Segment | PageID → 映射 → VAddr → Segment |
