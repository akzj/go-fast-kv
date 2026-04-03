package internal

import (
	"testing"
)

// BenchmarkTreeInsert measures key insertion throughput (1000 iterations per op).
func BenchmarkTreeInsert(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tree := NewInMemoryTree()
		for j := 0; j < 1000; j++ {
			err := tree.Write(TreeOperation{
				Type:  OpPut,
				Key:   PageID(j),
				Value: MakeInlineValue([]byte{byte(j)}),
			})
			if err != nil {
				b.Fatal(err)
			}
		}
		tree.Close()
	}
}

// BenchmarkTreeInsertSequential measures sequential key insertion.
func BenchmarkTreeInsertSequential(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	tree := NewInMemoryTree()
	defer tree.Close()

	for i := 0; i < b.N; i++ {
		err := tree.Write(TreeOperation{
			Type:  OpPut,
			Key:   PageID(i),
			Value: MakeInlineValue([]byte{byte(i)}),
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTreeInsertRandom measures random key insertion.
func BenchmarkTreeInsertRandom(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	tree := NewInMemoryTree()
	defer tree.Close()

	// Use predictable pseudo-random for reproducibility
	for i := 0; i < b.N; i++ {
		key := PageID((uint64(i) * 1103515245) % 100000)
		err := tree.Write(TreeOperation{
			Type:  OpPut,
			Key:   key,
			Value: MakeInlineValue([]byte{byte(key)}),
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTreeGet measures point lookup performance.
func BenchmarkTreeGet(b *testing.B) {
	tree := NewInMemoryTree()
	defer tree.Close()

	// Pre-populate with 10000 keys
	for i := 0; i < 10000; i++ {
		err := tree.Write(TreeOperation{
			Type:  OpPut,
			Key:   PageID(i),
			Value: MakeInlineValue([]byte{byte(i % 256)}),
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := PageID(i % 10000)
		_, err := tree.Get(key)
		if err != nil && err != ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}

// BenchmarkTreeGetRandom measures random point lookups.
func BenchmarkTreeGetRandom(b *testing.B) {
	tree := NewInMemoryTree()
	defer tree.Close()

	// Pre-populate
	for i := 0; i < 10000; i++ {
		err := tree.Write(TreeOperation{
			Type:  OpPut,
			Key:   PageID(i),
			Value: MakeInlineValue([]byte{byte(i % 256)}),
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := PageID((uint64(i) * 1103515245) % 10000)
		_, err := tree.Get(key)
		if err != nil && err != ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}

// BenchmarkTreeScan measures range scan performance.
func BenchmarkTreeScan(b *testing.B) {
	tree := NewInMemoryTree()
	defer tree.Close()

	// Pre-populate with 10000 sequential keys
	for i := 0; i < 10000; i++ {
		err := tree.Write(TreeOperation{
			Type:  OpPut,
			Key:   PageID(i),
			Value: MakeInlineValue([]byte{byte(i % 256)}),
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		iter, err := tree.Scan(0, 10000)
		if err != nil {
			b.Fatal(err)
		}
		count := 0
		for iter.Next() {
			count++
		}
		iter.Close()
	}
}

// BenchmarkTreeScan100 measures scanning 100 consecutive keys.
func BenchmarkTreeScan100(b *testing.B) {
	tree := NewInMemoryTree()
	defer tree.Close()

	// Pre-populate
	for i := 0; i < 10000; i++ {
		err := tree.Write(TreeOperation{
			Type:  OpPut,
			Key:   PageID(i),
			Value: MakeInlineValue([]byte{byte(i % 256)}),
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		start := (i % 9900)
		iter, err := tree.Scan(PageID(start), PageID(start+100))
		if err != nil {
			b.Fatal(err)
		}
		count := 0
		for iter.Next() {
			count++
		}
		iter.Close()
	}
}

// BenchmarkTreeDelete measures key deletion performance.
// Note: Delete operation has known issues in current implementation.
func BenchmarkTreeDelete(b *testing.B) {
	b.Skip("Delete operation has known issues in current implementation")
}

// BenchmarkTreeMixedWorkload measures mixed read/write workload.
func BenchmarkTreeMixedWorkload(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	tree := NewInMemoryTree()
	defer tree.Close()

	// Pre-populate
	for i := 0; i < 5000; i++ {
		err := tree.Write(TreeOperation{
			Type:  OpPut,
			Key:   PageID(i),
			Value: MakeInlineValue([]byte{byte(i % 256)}),
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	// Mixed workload: 80% reads, 20% writes
	for i := 0; i < b.N; i++ {
		op := i % 10
		if op < 8 {
			// Read
			key := PageID(i % 5000)
			tree.Get(key)
		} else {
			// Write
			key := PageID(5000 + i)
			tree.Write(TreeOperation{
				Type:  OpPut,
				Key:   key,
				Value: MakeInlineValue([]byte{byte(i % 256)}),
			})
		}
	}
}
