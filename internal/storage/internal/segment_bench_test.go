package internal

import (
	"os"
	"testing"

	vaddr "github.com/akzj/go-fast-kv/internal/vaddr"
)

// BenchmarkSegmentCreate measures segment creation overhead.
func BenchmarkSegmentCreate(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		dir, err := os.MkdirTemp("", "storage_bench")
		if err != nil {
			b.Fatal(err)
		}

		config := Config{Directory: dir, SegmentSize: 1 << 22}
		sm, err := NewSegmentManager(config)
		if err != nil {
			b.Fatal(err)
		}

		seg, err := sm.CreateSegment()
		if err != nil {
			b.Fatal(err)
		}

		// Add some data
		data := make([]byte, vaddr.PageSize)
		for j := 0; j < 10; j++ {
			_, err := seg.Append(data)
			if err != nil {
				b.Fatal(err)
			}
		}

		if err := seg.Close(); err != nil {
			b.Fatal(err)
		}
		sm.Close()
		os.RemoveAll(dir)
	}
}

// BenchmarkSegmentAppend measures sequential append performance with page-aligned data.
func BenchmarkSegmentAppend(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		dir, err := os.MkdirTemp("", "storage_bench")
		if err != nil {
			b.Fatal(err)
		}

		config := Config{Directory: dir, SegmentSize: 1 << 22}
		sm, err := NewSegmentManager(config)
		if err != nil {
			b.Fatal(err)
		}

		seg, err := sm.CreateSegment()
		if err != nil {
			b.Fatal(err)
		}

		// Use page-sized data
		data := make([]byte, vaddr.PageSize)
		for j := 0; j < 50; j++ { // Limited to stay within 2GB limit
			_, err := seg.Append(data)
			if err != nil {
				break // Stop if segment full
			}
		}

		seg.Close()
		sm.Close()
		os.RemoveAll(dir)
	}
}

// BenchmarkSegmentAppend4KB measures append with 4KB records.
func BenchmarkSegmentAppend4KB(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		dir, err := os.MkdirTemp("", "storage_bench")
		if err != nil {
			b.Fatal(err)
		}

		config := Config{Directory: dir, SegmentSize: 1 << 22}
		sm, err := NewSegmentManager(config)
		if err != nil {
			b.Fatal(err)
		}

		seg, err := sm.CreateSegment()
		if err != nil {
			b.Fatal(err)
		}

		data := make([]byte, 4096)
		for j := 0; j < 50; j++ { // Limited to stay within 2GB limit
			_, err := seg.Append(data)
			if err != nil {
				break
			}
		}

		seg.Close()
		sm.Close()
		os.RemoveAll(dir)
	}
}

// BenchmarkSegmentReadAt measures random read performance.
func BenchmarkSegmentReadAt(b *testing.B) {
	dir, err := os.MkdirTemp("", "storage_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	config := Config{Directory: dir, SegmentSize: 1 << 26}
	sm, err := NewSegmentManager(config)
	if err != nil {
		b.Fatal(err)
	}
	defer sm.Close()

	seg, err := sm.CreateSegment()
	if err != nil {
		b.Fatal(err)
	}
	defer seg.Close()

	// Pre-populate segment
	data := make([]byte, vaddr.PageSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	addrs := make([]vaddr.VAddr, 1000)
	for i := 0; i < 1000; i++ {
		addr, err := seg.Append(data)
		if err != nil {
			b.Fatal(err)
		}
		addrs[i] = addr
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		addr := addrs[i%len(addrs)]
		_, err := seg.ReadAt(int64(addr.Offset), vaddr.PageSize)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSegmentSync measures fsync overhead.
func BenchmarkSegmentSync(b *testing.B) {
	dir, err := os.MkdirTemp("", "storage_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	config := Config{Directory: dir, SegmentSize: 1 << 22}
	sm, err := NewSegmentManager(config)
	if err != nil {
		b.Fatal(err)
	}
	defer sm.Close()

	seg, err := sm.CreateSegment()
	if err != nil {
		b.Fatal(err)
	}
	defer seg.Close()

	// Pre-populate
	data := make([]byte, vaddr.PageSize)
	for i := 0; i < 100; i++ {
		_, err := seg.Append(data)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := seg.Sync(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSegmentSeal measures segment sealing overhead.
func BenchmarkSegmentSeal(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		dir, err := os.MkdirTemp("", "storage_bench")
		if err != nil {
			b.Fatal(err)
		}

		config := Config{Directory: dir, SegmentSize: 1 << 22}
		sm, err := NewSegmentManager(config)
		if err != nil {
			b.Fatal(err)
		}

		seg, err := sm.CreateSegment()
		if err != nil {
			b.Fatal(err)
		}

		// Add some data
		data := make([]byte, vaddr.PageSize)
		for j := 0; j < 10; j++ {
			_, err := seg.Append(data)
			if err != nil {
				b.Fatal(err)
			}
		}

		if err := seg.Close(); err != nil {
			b.Fatal(err)
		}
		sm.Close()
		os.RemoveAll(dir)
	}
}

// BenchmarkSegmentWriteAndSync measures append + sync as atomic operation.
func BenchmarkSegmentWriteAndSync(b *testing.B) {
	dir, err := os.MkdirTemp("", "storage_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	config := Config{Directory: dir, SegmentSize: 1 << 22}
	sm, err := NewSegmentManager(config)
	if err != nil {
		b.Fatal(err)
	}
	defer sm.Close()

	seg, err := sm.CreateSegment()
	if err != nil {
		b.Fatal(err)
	}
	defer seg.Close()

	data := make([]byte, vaddr.PageSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := seg.Append(data)
		if err != nil {
			b.Fatal(err)
		}
		if err := seg.Sync(); err != nil {
			b.Fatal(err)
		}
	}
}
