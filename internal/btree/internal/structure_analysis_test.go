package internal

import (
    "bytes"
    "fmt"
    "math/rand"
    "testing"
    "time"
    
    btreeapi "github.com/akzj/go-fast-kv/internal/btree/api"
)

// TestBTreeStructureAnalysis analyzes B-tree structure.
func TestBTreeStructureAnalysis(t *testing.T) {
    const N = 100000
    fmt.Printf("=== B-tree Structure Analysis ===\n")
    fmt.Printf("Writing %d keys...\n", N)
    
    pages := NewMemPageProvider()
    tree := New(btreeapi.Config{}, pages, nil)
    
    keys := make([][]byte, N)
    for i := 0; i < N; i++ {
        keys[i] = []byte(fmt.Sprintf("key%08d", i))
    }
    rand.Seed(42)
    rand.Shuffle(N, func(i, j int) {
        keys[i], keys[j] = keys[j], keys[i]
    })
    
    start := time.Now()
    for i := 0; i < N; i++ {
        tree.Put(keys[i], []byte(fmt.Sprintf("value%d", i)), uint64(i+1))
    }
    fmt.Printf("Write time: %v\n", time.Since(start))
    
    analyzeStructure(pages, tree, t)
    
    fmt.Printf("\n=== Read Path Analysis ===\n")
    testReadPath(tree, keys[:100], t)
}

func analyzeStructure(pages *MemPageProvider, tree btreeapi.BTree, t *testing.T) {
    rootPID := tree.RootPageID()
    if rootPID == 0 {
        fmt.Println("Tree is empty")
        return
    }
    
    var visit func(pid uint64, depth int)
    levelCounts := make(map[int]int)
    levelNodes := make(map[int]int)
    
    visit = func(pid uint64, depth int) {
        node, err := pages.ReadPage(pid)
        if err != nil {
            return
        }
        
        levelCounts[depth] += node.Count()
        levelNodes[depth]++
        
        if !node.IsLeaf() {
            // Visit child0
            visit(node.Child0(), depth+1)
            // Visit children from internal entries
            for i := 0; i < node.Count(); i++ {
                visit(node.InternalChild(i), depth+1)
            }
        }
    }
    
    visit(rootPID, 0)
    
    fmt.Printf("\n--- Level Statistics ---\n")
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
        fmt.Printf("Level %d: %d nodes, %d keys\n", depth, nodes, count)
        totalKeys += count
        totalNodes += nodes
    }
    
    fmt.Printf("\n--- Summary ---\n")
    fmt.Printf("Total levels: %d\n", maxDepth+1)
    fmt.Printf("Total nodes: %d\n", totalNodes)
    fmt.Printf("Total keys: %d\n", totalKeys)
    fmt.Printf("Avg keys/node: %.1f\n", float64(totalKeys)/float64(totalNodes))
}

func testReadPath(tree btreeapi.BTree, keys [][]byte, t *testing.T) {
    fmt.Printf("Testing read path for %d keys...\n", len(keys))
    
    bt := tree.(*bTree)
    
    for i, key := range keys {
        path, err := bt.searchPath(key)
        if err != nil {
            t.Fatalf("searchPath failed: %v", err)
        }
        
        if i < 5 {
            fmt.Printf("key=%s, path_len=%d, pages=", string(key), len(path))
            for _, pid := range path {
                fmt.Printf("%d ", pid)
            }
            fmt.Println()
        }
    }
    
    fmt.Printf("\n--- Binary Search Verification ---\n")
    
    rootPID := tree.RootPageID()
    root, _ := bt.pages.ReadPage(rootPID)
    
    if root.IsLeaf() {
        fmt.Println("root is leaf (small tree)")
    } else {
        fmt.Printf("root has %d keys\n", root.Count())
        
        testKey := []byte("key00500000")
        
        // Verify findChild binary search
        childPID := root.FindChild(testKey)
        child, _ := bt.pages.ReadPage(childPID)
        if child.IsLeaf() {
            lo := child.SearchLeaf(testKey)
            if lo < child.Count() && bytes.Equal(child.EntryKey(lo), testKey) {
                fmt.Printf("Found key in leaf page %d\n", childPID)
            }
        }
    }
    
    fmt.Println("\n✅ Read path analysis complete")
}
