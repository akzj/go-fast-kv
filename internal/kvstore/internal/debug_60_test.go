package internal

import (
    "fmt"
    "testing"
)

func TestDebug60Keys(t *testing.T) {
    dir := tempDir(t)
    store := newStore(t, dir)
    defer closeAndCleanup(t, store, dir)
    
    // Insert 60 keys with sequential PageID-like values
    for i := 0; i < 60; i++ {
        key := []byte{byte(i + 1)} // Simple 1-byte keys: 1, 2, 3, ..., 60
        store.Put(key, []byte("value"))
    }
    
    // Scan count
    iter, _ := store.Scan(nil, nil)
    scanCount := 0
    for iter.Next() {
        scanCount++
    }
    iter.Close()
    
    fmt.Printf("Inserted 60 keys\n")
    fmt.Printf("Scan count: %d\n", scanCount)
    
    // Try Get for each key
    getCount := 0
    for i := 0; i < 60; i++ {
        key := []byte{byte(i + 1)}
        _, err := store.Get(key)
        if err == nil {
            getCount++
        }
    }
    fmt.Printf("Get successful: %d\n", getCount)
}
