package internal

import (
	"fmt"
	"testing"

	"github.com/akzj/go-fast-kv/internal/storage"
)

func TestVerifyTreeSplit(t *testing.T) {
	ops := NewNodeOperations()
	pageStorage := storage.NewMemoryPageStorage()
	mgr := newPageNodeManager(pageStorage, ops)

	tree := newTreeImpl(ops, mgr, true)
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
	if count != 60 {
		t.Errorf("expected 60 scanned, got %d", count)
	}
}
