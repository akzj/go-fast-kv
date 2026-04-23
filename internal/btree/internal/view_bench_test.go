package internal

import (
	"bytes"
	"fmt"
	"testing"

	btreeapi "github.com/akzj/go-fast-kv/internal/btree/api"
	"github.com/akzj/go-fast-kv/internal/lock"
)

// BenchmarkViewPut benchmarks CachedMemPageProvider (no Serialize/Deserialize, no clone on Read).
// This represents the "ideal" in-memory B-tree with view semantics.
func BenchmarkViewPut(b *testing.B) {
	provider := NewCachedMemPageProvider()

	// Bootstrap root
	pid := provider.AllocPage()
	root := &btreeapi.Node{IsLeaf: true}
	provider.WritePage(pid, root)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))

		// Read (returns *Node directly, no clone)
		node, _ := provider.ReadPage(pid)

		// Insert
		entry := btreeapi.LeafEntry{
			Key:    cloneBytes(key),
			TxnMin: uint64(i),
			TxnMax: btreeapi.TxnMaxInfinity,
			Value:  btreeapi.Value{Inline: cloneBytes(value)},
		}
		pos := 0
		for pos < len(node.Entries) && bytes.Compare(key, node.Entries[pos].Key) > 0 {
			pos++
		}
		node.Entries = append(node.Entries, btreeapi.LeafEntry{})
		copy(node.Entries[pos+1:], node.Entries[pos:])
		node.Entries[pos] = entry

		// Write (returns *Node directly, no Serialize)
		provider.WritePage(pid, node)
	}
}

// BenchmarkViewGet benchmarks Get on CachedMemPageProvider.
func BenchmarkViewGet(b *testing.B) {
	provider := NewCachedMemPageProvider()

	// Populate with 1000 entries
	pid := provider.AllocPage()
	node := &btreeapi.Node{IsLeaf: true}
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		value := []byte(fmt.Sprintf("value-%04d", i))
		node.Entries = append(node.Entries, btreeapi.LeafEntry{
			Key:    key,
			TxnMin: uint64(i),
			TxnMax: btreeapi.TxnMaxInfinity,
			Value:  btreeapi.Value{Inline: value},
		})
	}
	node.Count = uint16(len(node.Entries))
	provider.WritePage(pid, node)

	txnID := uint64(999)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i%1000))

		// Read (direct pointer, no clone)
		node, _ := provider.ReadPage(pid)

		// Search
		for j := range node.Entries {
			e := &node.Entries[j]
			if bytes.Equal(e.Key, key) && e.TxnMin <= txnID && e.TxnMax > txnID {
				_ = cloneBytes(e.Value.Inline) // simulate value copy
				break
			}
		}
	}
}

// BenchmarkViewFullTree benchmarks full B-tree Put with CachedMemPageProvider.
func BenchmarkViewFullTree(b *testing.B) {
	for _, n := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			provider := NewCachedMemPageProvider()
			tree := newBTreeForTest(provider)

			// Pre-populate
			for i := 0; i < n; i++ {
				key := []byte(fmt.Sprintf("key-%04d", i))
				value := []byte(fmt.Sprintf("value-%04d", i))
				tree.Put(key, value, uint64(i))
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := []byte(fmt.Sprintf("key-%04d", n+i))
				value := []byte(fmt.Sprintf("value-%04d", n+i))
				tree.Put(key, value, uint64(n+i))
			}
		})
	}
}

// BenchmarkViewFullTreeGet benchmarks full B-tree Get with CachedMemPageProvider.
func BenchmarkViewFullTreeGet(b *testing.B) {
	for _, n := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			provider := NewCachedMemPageProvider()
			tree := newBTreeForTest(provider)

			// Pre-populate
			for i := 0; i < n; i++ {
				key := []byte(fmt.Sprintf("key-%04d", i))
				value := []byte(fmt.Sprintf("value-%04d", i))
				tree.Put(key, value, uint64(i))
			}

			txnID := uint64(n - 1)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := []byte(fmt.Sprintf("key-%04d", i%n))
				tree.Get(key, txnID)
			}
		})
	}
}

// newBTreeForTest creates a BTree with CachedMemPageProvider for testing.
func newBTreeForTest(pages *CachedMemPageProvider) *bTree {
	tree := &bTree{
		pages:       pages,
		serializer:  NewNodeSerializer(),
		pageLocks:   lock.New(),
	}
	return tree
}
