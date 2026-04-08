package internal

import (
	"testing"
)

func TestDebug(t *testing.T) {
	tree := NewInMemoryTree()
	defer tree.Close()
	for i := 1; i <= 85; i++ {
		tree.Put(intKey(uint64(i)), MakeInlineValue([]byte{byte(i)}))
	}
	// Check key 29
	_, err := tree.Get(intKey(29))
	if err != nil {
		t.Logf("key 29: MISSING")
	} else {
		t.Logf("key 29: found")
	}
}
