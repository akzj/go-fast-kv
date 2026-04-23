package internal

import (
	"bytes"
	"fmt"
	"math"
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
	root := NewLeafPage()
	provider.WritePage(pid, root)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))

		// Read (returns *Page directly, no clone)
		page, _ := provider.ReadPage(pid)

		// Insert at end (simplified — real btree does binary search)
		pos := page.Count()
		err := page.InsertLeafEntry(pos, key, uint64(i), math.MaxUint64, value, 0)
		if err != nil {
			// Page full — create new page (simplified)
			pid = provider.AllocPage()
			page = NewLeafPage()
			page.InsertLeafEntry(0, key, uint64(i), math.MaxUint64, value, 0)
		}

		// Write (returns *Page directly, no Serialize)
		provider.WritePage(pid, page)
	}
}

// BenchmarkViewGet benchmarks Get on CachedMemPageProvider.
func BenchmarkViewGet(b *testing.B) {
	provider := NewCachedMemPageProvider()

	// Populate with entries (fill as many as fit in one page)
	pid := provider.AllocPage()
	page := NewLeafPage()
	count := 0
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		value := []byte(fmt.Sprintf("value-%04d", i))
		err := page.InsertLeafEntry(count, key, uint64(i), math.MaxUint64, value, 0)
		if err != nil {
			break // page full
		}
		count++
	}
	provider.WritePage(pid, page)

	txnID := uint64(999)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i%count))

		// Read (direct pointer, no clone)
		p, _ := provider.ReadPage(pid)

		// Search using binary search
		lo := p.SearchLeaf(key)
		if lo < p.Count() {
			eKey := p.EntryKey(lo)
			if bytes.Equal(eKey, key) {
				eTxnMin := p.EntryTxnMin(lo)
				eTxnMax := p.EntryTxnMax(lo)
				if eTxnMin <= txnID && eTxnMax > txnID {
					_ = cloneBytes(p.EntryInlineValue(lo)) // simulate value copy
				}
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
		inlineThres: btreeapi.InlineThreshold,
		pageLocks:   lock.New(),
	}
	return tree
}
