package internal

import (
    "testing"
)

func TestDebugExternal(t *testing.T) {
    dir := tempDir(t)
    store, err := NewKVStore(Config{Directory: dir})
    if err != nil {
        t.Fatalf("create store: %v", err)
    }
    
    // Store a 100-byte value (should be external)
    value := make([]byte, 100)
    for i := range value {
        value[i] = byte(i)
    }
    
    err = store.Put([]byte("large"), value)
    t.Logf("Put error: %v", err)
    
    // Try to get it back
    val, err := store.Get([]byte("large"))
    t.Logf("Get result: val=%v (len=%d), err=%v", val != nil, len(val), err)
    
    if err != nil {
        t.Logf("Error type: %T", err)
    }
    
    store.Close()
}
