package internal

import (
    "testing"
    btreeapi "github.com/akzj/go-fast-kv/internal/btree/api"
)

func BenchmarkSinglePut(b *testing.B) {
    pages := NewMemPageProvider()
    tree := New(btreeapi.Config{}, pages, nil)
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        tree.Put([]byte("key"), []byte("value"), uint64(i+1))
    }
}

func BenchmarkSingleGet(b *testing.B) {
    pages := NewMemPageProvider()
    tree := New(btreeapi.Config{}, pages, nil)
    tree.Put([]byte("key"), []byte("value"), uint64(1))
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        tree.Get([]byte("key"), uint64(2))
    }
}
