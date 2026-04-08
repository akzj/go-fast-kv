package internal

import (
	"fmt"
	"testing"

	"github.com/akzj/go-fast-kv/internal/storage"
)

// TestCoWPersistentPath tests with PageStorage (appending storage).
func TestCoWPersistentPath(t *testing.T) {
	// Use MemoryPageStorage — append-only, maps PageID → VAddr internally
	pageStorage := storage.NewMemoryPageStorage()
	ops := NewNodeOperations()
	nodeMgr := newPageNodeManager(pageStorage, ops)

	tree := newTreeImpl(ops, nodeMgr, true)
	tree.Open("")

	// Insert 60 keys
	for i := 1; i <= 60; i++ {
		err := tree.Put(PageID(i), MakeInlineValue([]byte{byte(i)}))
		if err != nil {
			t.Fatalf("Put %d failed: %v", i, err)
		}
	}

	// Verify Get
	getCount := 0
	for i := 1; i <= 60; i++ {
		_, err := tree.Get(PageID(i))
		if err == nil {
			getCount++
		} else {
			t.Logf("Get(%d) failed: %v", i, err)
		}
	}

	// Verify Scan
	iter, err := tree.Scan(0, 0)
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	scanCount := 0
	for iter.Next() {
		scanCount++
	}
	iter.Close()

	fmt.Printf("Persistent path: Get=%d/60, Scan=%d/60\n", getCount, scanCount)
	if getCount != 60 {
		t.Errorf("Get: expected 60, got %d", getCount)
	}
	if scanCount != 60 {
		t.Errorf("Scan: expected 60, got %d", scanCount)
	}
}
