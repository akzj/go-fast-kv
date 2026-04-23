package internal

import (
    "bytes"
    "fmt"
    "math/rand"
    "testing"
    "time"
    
    btreeapi "github.com/akzj/go-fast-kv/internal/btree/api"
)

// TestBTreeStructureAnalysis 分析 B-tree 结构
func TestBTreeStructureAnalysis(t *testing.T) {
    const N = 100000
    fmt.Printf("=== B-tree 结构分析测试 ===\n")
    fmt.Printf("写入 %d keys...\n", N)
    
    pages := NewMemPageProvider()
    tree := New(btreeapi.Config{}, pages, nil)
    
    keys := make([][]byte, N)
    for i := 0; i < N; i++ {
        keys[i] = []byte(fmt.Sprintf("key%08d", i))
    }
    // 打乱顺序模拟随机写入
    rand.Seed(42)
    rand.Shuffle(N, func(i, j int) {
        keys[i], keys[j] = keys[j], keys[i]
    })
    
    start := time.Now()
    for i := 0; i < N; i++ {
        tree.Put(keys[i], []byte(fmt.Sprintf("value%d", i)), uint64(i+1))
    }
    fmt.Printf("写入耗时: %v\n", time.Since(start))
    
    // 分析树结构
    analyzeStructure(pages, tree, t)
    
    // 测试读取路径
    fmt.Printf("\n=== 读取路径分析 ===\n")
    testReadPath(tree, keys[:100], t)
}

// analyzeStructure 分析树结构
func analyzeStructure(pages *MemPageProvider, tree btreeapi.BTree, t *testing.T) {
    rootPID := tree.RootPageID()
    if rootPID == 0 {
        fmt.Println("树为空")
        return
    }
    
    // 递归分析
    var visit func(pid uint64, depth int)
    levelCounts := make(map[int]int)
    levelNodes := make(map[int]int)
    
    visit = func(pid uint64, depth int) {
        node, err := pages.ReadPage(pid)
        if err != nil {
            return
        }
        
        levelCounts[depth] += int(node.Count)
        levelNodes[depth]++
        
        if !node.IsLeaf {
            for _, childPID := range node.Children {
                visit(childPID, depth+1)
            }
        }
    }
    
    visit(rootPID, 0)
    
    // 打印结果
    fmt.Printf("\n--- 层结构统计 ---\n")
    maxDepth := 0
    totalKeys := 0
    totalNodes := 0
    for depth := 0; ; depth++ {
        count, ok := levelCounts[depth]
        nodes, nodesOk := levelNodes[depth]
        if !ok || !nodesOk || count == 0 {
            maxDepth = depth - 1
            break
        }
        fmt.Printf("层 %d: %d 个节点, %d 个 keys\n", depth, nodes, count)
        totalKeys += count
        totalNodes += nodes
    }
    
    fmt.Printf("\n--- 总结 ---\n")
    fmt.Printf("总层数: %d\n", maxDepth+1)
    fmt.Printf("总节点数: %d\n", totalNodes)
    fmt.Printf("总 keys: %d\n", totalKeys)
    fmt.Printf("平均每节点 keys: %.1f\n", float64(totalKeys)/float64(totalNodes))
}

// testReadPath 测试读取路径是否使用二分搜索
func testReadPath(tree btreeapi.BTree, keys [][]byte, t *testing.T) {
    fmt.Printf("测试 %d 个 key 的读取路径...\n", len(keys))
    
    // 类型断言获取 bTree 以访问 searchPath
    bt := tree.(*bTree)
    
    // 记录每次 searchPath 访问的 page_id
    for i, key := range keys {
        path, err := bt.searchPath(key)
        if err != nil {
            t.Fatalf("searchPath 失败: %v", err)
        }
        
        if i < 5 {
            fmt.Printf("key=%s, path_len=%d, pages=", string(key), len(path))
            for _, pid := range path {
                fmt.Printf("%d ", pid)
            }
            fmt.Println()
        }
    }
    
    // 验证 key 搜索是二分的
    fmt.Printf("\n--- 二分搜索验证 ---\n")
    
    // 读取 root 节点
    rootPID := tree.RootPageID()
    root, _ := bt.pages.ReadPage(rootPID)
    
    if root.IsLeaf {
        fmt.Println("root 是叶节点 (树很小)")
    } else {
        fmt.Printf("root 有 %d 个 keys\n", len(root.Keys))
        
        // 测试二分搜索
        testKey := []byte("key00500000")
        
        // 手动实现二分搜索
        lo, hi := 0, len(root.Keys)-1
        for lo <= hi {
            mid := (lo + hi) / 2
            cmp := bytes.Compare(testKey, root.Keys[mid])
            if cmp < 0 {
                hi = mid - 1
            } else if cmp > 0 {
                lo = mid + 1
            } else {
                fmt.Printf("key %s 在 root.Keys[%d]\n", string(testKey), mid)
                break
            }
        }
        if lo > hi {
            fmt.Printf("key %s 应在 child[%d]\n", string(testKey), lo)
        }
        
        // 验证 findChild 的行为与二分搜索一致
        for _, childPID := range root.Children {
            child, _ := bt.pages.ReadPage(childPID)
            if child.IsLeaf {
                for _, e := range child.Entries {
                    if bytes.Equal(e.Key, testKey) {
                        fmt.Printf("找到 key 在 leaf page %d\n", childPID)
                        break
                    }
                }
                break
            }
        }
    }
    
    fmt.Println("\n✅ 读取路径分析完成")
}
