package internal

import (
	"testing"
	"fmt"
)

func TestSplitTrace(t *testing.T) {
	tree := NewInMemoryTree()
	defer tree.Close()

	// Insert 56 keys (fills one leaf)
	for i := 1; i <= 56; i++ {
		_ = tree.Put(PageID(i*10), MakeInlineValue([]byte("val")))
	}
	
	// Print all keys before 57th insert
	fmt.Println("Before 57th insert:")
	iter, _ := tree.Scan(0, 0)
	count := 0
	for iter.Next() {
		fmt.Printf("  key=%d\n", iter.Key())
		count++
	}
	iter.Close()
	fmt.Printf("Total: %d\n", count)

	// Insert 57th key
	fmt.Println("\nInserting key=285:")
	_ = tree.Put(285, MakeInlineValue([]byte("newval")))

	// Print all keys after 57th insert
	fmt.Println("\nAfter 57th insert:")
	iter2, _ := tree.Scan(0, 0)
	count2 := 0
	for iter2.Next() {
		fmt.Printf("  key=%d\n", iter2.Key())
		count2++
	}
	iter2.Close()
	fmt.Printf("Total: %d\n", count2)

	// Now scan [5000, 6000)
	fmt.Println("\nScanning [5000, 6000):")
	iter3, _ := tree.Scan(5000, 6000)
	count3 := 0
	for iter3.Next() {
		fmt.Printf("  returned key=%d\n", iter3.Key())
		count3++
	}
	iter3.Close()
	fmt.Printf("Total: %d (expected 0)\n", count3)
}
