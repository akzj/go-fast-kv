package internal

import (
	"os"
	"testing"
)

// BenchmarkWALAppend measures WAL record append throughput.
func BenchmarkWALAppend(b *testing.B) {
	dir, err := os.MkdirTemp("", "wal_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	wal, err := NewWAL(WALConfig{
		Directory:   dir,
		SegmentSize: 1 << 22,
		SyncWrites:  false,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer wal.Close()

	record := &WALRecord{
		RecordType: WALPageAlloc,
		Payload:    make([]byte, 256),
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := wal.Append(record)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWALAppend1KB measures append with 1KB payload.
func BenchmarkWALAppend1KB(b *testing.B) {
	dir, err := os.MkdirTemp("", "wal_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	wal, err := NewWAL(WALConfig{
		Directory:   dir,
		SegmentSize: 1 << 22,
		SyncWrites:  false,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer wal.Close()

	record := &WALRecord{
		RecordType: WALNodeWrite,
		Payload:    make([]byte, 1024),
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := wal.Append(record)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWALAppend4KB measures append with 4KB payload (page-sized).
func BenchmarkWALAppend4KB(b *testing.B) {
	dir, err := os.MkdirTemp("", "wal_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	wal, err := NewWAL(WALConfig{
		Directory:   dir,
		SegmentSize: 1 << 22,
		SyncWrites:  false,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer wal.Close()

	record := &WALRecord{
		RecordType: WALNodeWrite,
		Payload:    make([]byte, 4096),
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := wal.Append(record)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWALReadAt measures random read performance.
func BenchmarkWALReadAt(b *testing.B) {
	dir, err := os.MkdirTemp("", "wal_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	wal, err := NewWAL(WALConfig{
		Directory:   dir,
		SegmentSize: 1 << 22,
		SyncWrites:  false,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer wal.Close()

	// Pre-populate with records
	lsns := make([]uint64, 1000)
	for i := 0; i < 1000; i++ {
		lsn, err := wal.Append(&WALRecord{
			RecordType: WALPageAlloc,
			Payload:    make([]byte, 256),
		})
		if err != nil {
			b.Fatal(err)
		}
		lsns[i] = lsn
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		lsn := lsns[i%len(lsns)]
		_, err := wal.ReadAt(lsn)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWALReadFrom measures sequential iteration performance.
func BenchmarkWALReadFrom(b *testing.B) {
	dir, err := os.MkdirTemp("", "wal_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	wal, err := NewWAL(WALConfig{
		Directory:   dir,
		SegmentSize: 1 << 22,
		SyncWrites:  false,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer wal.Close()

	// Pre-populate with 1000 records
	for i := 0; i < 1000; i++ {
		_, err := wal.Append(&WALRecord{
			RecordType: WALPageAlloc,
			Payload:    make([]byte, 256),
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		iter, err := wal.ReadFrom(1)
		if err != nil {
			b.Fatal(err)
		}
		count := 0
		for iter.Next() {
			count++
		}
		iter.Close()
		if count != 1000 {
			b.Fatalf("expected 1000 records, got %d", count)
		}
	}
}

// BenchmarkWALFlush measures flush overhead.
func BenchmarkWALFlush(b *testing.B) {
	dir, err := os.MkdirTemp("", "wal_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	wal, err := NewWAL(WALConfig{
		Directory:   dir,
		SegmentSize: 1 << 22,
		SyncWrites:  false,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer wal.Close()

	// Pre-populate
	for i := 0; i < 100; i++ {
		_, err := wal.Append(&WALRecord{
			RecordType: WALPageAlloc,
			Payload:    make([]byte, 256),
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := wal.Flush(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWALConcurrentAppend measures concurrent append throughput.
func BenchmarkWALConcurrentAppend(b *testing.B) {
	dir, err := os.MkdirTemp("", "wal_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	wal, err := NewWAL(WALConfig{
		Directory:   dir,
		SegmentSize: 1 << 22,
		SyncWrites:  false,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer wal.Close()

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		record := &WALRecord{
			RecordType: WALPageAlloc,
			Payload:    make([]byte, 256),
		}
		for pb.Next() {
			_, err := wal.Append(record)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
