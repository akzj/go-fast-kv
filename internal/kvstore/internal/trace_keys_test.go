package internal

import (
	"fmt"
	"testing"
)

func TestTraceKeys(t *testing.T) {
	dir := tempDir(t)
	store := newStore(t, dir)
	defer closeAndCleanup(t, store, dir)
	
	// Insert 60 keys
	inserted := make(map[string]bool)
	for i := 0; i < 60; i++ {
		key := []byte{byte('a' + i%26), byte('0' + i/26)}
		keyStr := string(key)
		if err := store.Put(key, []byte("value")); err != nil {
			fmt.Printf("Put error at i=%d key=%q: %v\n", i, keyStr, err)
		} else {
			inserted[keyStr] = true
		}
	}
	
	fmt.Printf("Inserted: %d keys\n", len(inserted))
	
	// Scan and find missing
	iter, _ := store.Scan(nil, nil)
	defer iter.Close()
	
	scanned := make(map[string]bool)
	missing := []string{}
	for iter.Next() {
		key := string(iter.Key())
		scanned[key] = true
	}
	
	for k := range inserted {
		if !scanned[k] {
			missing = append(missing, k)
		}
	}
	
	fmt.Printf("Scanned: %d keys\n", len(scanned))
	fmt.Printf("Missing keys: %v\n", missing)
}
