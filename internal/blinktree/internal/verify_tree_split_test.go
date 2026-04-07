package internal

import (
    "fmt"
    "testing"
    vaddr "github.com/akzj/go-fast-kv/internal/vaddr"
)

func TestVerifyTreeSplit(t *testing.T) {
    ops := NewNodeOperations()
    mgr := &inMemoryNodeManager{
        nodeOps:  ops,
        nodes:    make(map[vaddr.VAddr]*NodeFormat),
        nextAddr: vaddr.VAddr{SegmentID: 1, Offset: 4096},
    }
    
    tree := NewTreeMutator(ops, mgr)
    tree.Open("")
    
    // Insert 60 keys (triggers at least one split)
    for i := 1; i <= 60; i++ {
        err := tree.Put(PageID(i), InlineValue{})
        if err != nil {
            t.Fatalf("Put %d failed: %v", i, err)
        }
    }
    
    // Scan
    iter, _ := tree.Scan(0, 0)
    count := 0
    for iter.Next() {
        count++
    }
    fmt.Printf("In-memory tree: Inserted 60, Scanned %d\n", count)
}
