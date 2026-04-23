package internal

import (
	"fmt"
	"testing"

	btreeapi "github.com/akzj/go-fast-kv/internal/btree/api"
)

// BenchmarkCachedSinglePut measures pure B-tree insert performance
// without Serialize/Deserialize overhead.
func BenchmarkCachedSinglePut(b *testing.B) {
	pages := NewCachedMemPageProvider()
	tree := New(btreeapi.Config{}, pages, nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Put([]byte("key"), []byte("value"), uint64(i+1))
	}
}

// BenchmarkCachedMem_Put_1k measures B-tree insert performance for 1k keys.
func BenchmarkCachedMem_Put_1k(b *testing.B) {
	for i := 0; i < b.N; i++ {
		pages := NewCachedMemPageProvider()
		tree := New(btreeapi.Config{}, pages, nil)
		for j := 0; j < 1000; j++ {
			tree.Put([]byte(fmt.Sprintf("key%08d", j)), []byte(fmt.Sprintf("value%d", j)), uint64(j+1))
		}
	}
}

// BenchmarkCachedMem_Get_1k measures B-tree read performance for 1k keys.
func BenchmarkCachedMem_Get_1k(b *testing.B) {
	for i := 0; i < b.N; i++ {
		pages := NewCachedMemPageProvider()
		tree := New(btreeapi.Config{}, pages, nil)
		for j := 0; j < 1000; j++ {
			tree.Put([]byte(fmt.Sprintf("key%08d", j)), []byte(fmt.Sprintf("value%d", j)), uint64(j+1))
		}
		for j := 0; j < 1000; j++ {
			tree.Get([]byte(fmt.Sprintf("key%08d", j)), uint64(1001))
		}
	}
}