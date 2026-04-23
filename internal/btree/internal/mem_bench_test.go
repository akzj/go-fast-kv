package internal

import (
    "fmt"
    "testing"
    
    btreeapi "github.com/akzj/go-fast-kv/internal/btree/api"
)

func BenchmarkMem_Put_1k(b *testing.B) {
    for i := 0; i < b.N; i++ {
        pages := NewMemPageProvider()
        tree := New(btreeapi.Config{}, pages, nil)
        
        for j := 0; j < 1000; j++ {
            tree.Put([]byte(fmt.Sprintf("key%08d", j)), []byte(fmt.Sprintf("value%d", j)), uint64(j+1))
        }
    }
}

func BenchmarkMem_Get_1k(b *testing.B) {
    for i := 0; i < b.N; i++ {
        pages := NewMemPageProvider()
        tree := New(btreeapi.Config{}, pages, nil)
        
        for j := 0; j < 1000; j++ {
            tree.Put([]byte(fmt.Sprintf("key%08d", j)), []byte(fmt.Sprintf("value%d", j)), uint64(j+1))
        }
        
        for j := 0; j < 1000; j++ {
            tree.Get([]byte(fmt.Sprintf("key%08d", j)), uint64(1001))
        }
    }
}

func BenchmarkMem_Scan_1k(b *testing.B) {
    for i := 0; i < b.N; i++ {
        pages := NewMemPageProvider()
        tree := New(btreeapi.Config{}, pages, nil)
        
        for j := 0; j < 1000; j++ {
            tree.Put([]byte(fmt.Sprintf("key%08d", j)), []byte(fmt.Sprintf("value%d", j)), uint64(j+1))
        }
        
        iter := tree.Scan([]byte("key00000000"), []byte("key00001000"), uint64(1001))
        count := 0
        for iter.Next() {
            count++
        }
    }
}
