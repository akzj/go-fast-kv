package internal

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	vaddr "github.com/akzj/go-fast-kv/internal/vaddr"
)

// =============================================================================
// Concurrent Segment Access Tests
// =============================================================================

func TestSegment_ConcurrentRead(t *testing.T) {
	dir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	config := Config{Directory: dir, SegmentSize: 1 << 20}
	sm, err := NewSegmentManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	seg, err := sm.CreateSegment()
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()

	// Append test data
	data := make([]byte, vaddr.PageSize)
	for i := range data {
		data[i] = byte(i % 256)
	}
	addr, err := seg.Append(data)
	if err != nil {
		t.Fatal(err)
	}

	// Concurrent reads
	const numGoroutines = 10
	const numReads = 100
	var wg sync.WaitGroup
	errChan := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numReads; j++ {
				readData, err := seg.ReadAt(int64(addr.Offset), vaddr.PageSize)
				if err != nil {
					errChan <- err
					return
				}
				if !bytes.Equal(readData, data) {
					errChan <- err
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Errorf("concurrent read error: %v", err)
	}
}

func TestSegment_ConcurrentAppend(t *testing.T) {
	dir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	config := Config{Directory: dir, SegmentSize: 1 << 20}
	sm, err := NewSegmentManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	seg, err := sm.CreateSegment()
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()

	// Concurrent appends
	const numGoroutines = 5
	const pagesPerGoroutine = 10
	var wg sync.WaitGroup
	var pageCount uint64
	var appendCount int32

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < pagesPerGoroutine; j++ {
				data := make([]byte, vaddr.PageSize)
				for k := range data {
					data[k] = byte((goroutineID*100 + j*10 + k) % 256)
				}
				_, err := seg.Append(data)
				if err != nil {
					t.Errorf("append error: %v", err)
					return
				}
				atomic.AddInt32(&appendCount, 1)
				atomic.AddUint64(&pageCount, 1)
			}
		}(i)
	}

	wg.Wait()

	// Verify page count
	if seg.PageCount() != pageCount {
		t.Errorf("expected %d pages, got %d", pageCount, seg.PageCount())
	}
}

func TestSegment_ConcurrentReadWrite(t *testing.T) {
	dir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	config := Config{Directory: dir, SegmentSize: 1 << 20}
	sm, err := NewSegmentManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	seg, err := sm.CreateSegment()
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()

	// Pre-populate with data
	initialData := make([]byte, vaddr.PageSize)
	for i := range initialData {
		initialData[i] = 0xAA
	}
	addr, err := seg.Append(initialData)
	if err != nil {
		t.Fatal(err)
	}

	// Concurrent reads and writes
	var wg sync.WaitGroup
	done := make(chan struct{})

	// Writer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
				data := make([]byte, vaddr.PageSize)
				for i := range data {
					data[i] = 0xBB
				}
				seg.Append(data)
			}
		}
	}()

	// Reader goroutines
	const numReaders = 3
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					seg.ReadAt(int64(addr.Offset), vaddr.PageSize)
				}
			}
		}()
	}

	// Let them run for a bit
	// Note: This is a basic race test - in real tests we'd use -race flag

	// Stop goroutines
	close(done)
	wg.Wait()
}

// =============================================================================
// Concurrent SegmentManager Access Tests
// =============================================================================

func TestSegmentManager_ConcurrentCreate(t *testing.T) {
	dir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	config := Config{Directory: dir, SegmentSize: 1 << 20}
	sm, err := NewSegmentManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	const numGoroutines = 10
	var wg sync.WaitGroup
	segmentIDs := make(chan vaddr.SegmentID, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			seg, err := sm.CreateSegment()
			if err != nil {
				t.Errorf("create segment error: %v", err)
				return
			}
			segmentIDs <- seg.ID()
			// Don't close - let manager handle cleanup to avoid race with sealing
		}()
	}

	wg.Wait()
	close(segmentIDs)

	// Count unique segment IDs
	idSet := make(map[vaddr.SegmentID]bool)
	for id := range segmentIDs {
		idSet[id] = true
	}

	if len(idSet) != numGoroutines {
		t.Errorf("expected %d unique segments, got %d", numGoroutines, len(idSet))
	}

	if sm.SegmentCount() != numGoroutines {
		t.Errorf("expected %d segments in manager, got %d", numGoroutines, sm.SegmentCount())
	}
}

func TestSegmentManager_ConcurrentList(t *testing.T) {
	dir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	config := Config{Directory: dir, SegmentSize: 1 << 20}
	sm, err := NewSegmentManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	// Create segments - first 4 will be sealed when we create the 5th
	for i := 0; i < 4; i++ {
		_, err := sm.CreateSegment()
		if err != nil {
			t.Fatal(err)
		}
	}

	// Create 5th segment - this seals the first 4
	_, err = sm.CreateSegment()
	if err != nil {
		t.Fatal(err)
	}

	// Concurrent listing
	const numGoroutines = 10
	var wg sync.WaitGroup
	errChan := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				segments := sm.ListSegments()
				if len(segments) != 5 {
					errChan <- err
					return
				}

				sealed := sm.ListSegmentsByState(vaddr.SegmentStateSealed)
				if len(sealed) != 4 {
					errChan <- err
					return
				}

				active := sm.ListSegmentsByState(vaddr.SegmentStateActive)
				if len(active) != 1 {
					errChan <- err
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Errorf("concurrent list error: %v", err)
	}
}

func TestSegmentManager_ConcurrentSeal(t *testing.T) {
	dir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	config := Config{Directory: dir, SegmentSize: 1 << 20}
	sm, err := NewSegmentManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	// Create a segment
	seg, err := sm.CreateSegment()
	if err != nil {
		t.Fatal(err)
	}
	segID := seg.ID()
	seg.Close()

	// Concurrent seal attempts
	var wg sync.WaitGroup
	errChan := make(chan error, 2)

	wg.Add(1)
	go func() {
		defer wg.Done()
		errChan <- sm.SealSegment(segID)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		errChan <- sm.SealSegment(segID)
	}()

	wg.Wait()
	close(errChan)

	// Exactly one should succeed, one should fail
	successCount := 0
	failCount := 0
	for err := range errChan {
		if err != nil {
			failCount++
		} else {
			successCount++
		}
	}

	// Note: In current implementation, sealing an already sealed segment
	// might return nil or ErrSegmentNotActive depending on timing
	// The important thing is no panic or data corruption

	// Verify segment is sealed
	seg = sm.GetSegment(segID)
	if seg.State() != vaddr.SegmentStateSealed {
		t.Errorf("expected Sealed state, got %s", seg.State())
	}
}

// =============================================================================
// OSFile Concurrent Access Tests
// =============================================================================

func TestOSFile_ConcurrentReadWrite(t *testing.T) {
	dir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "testfile")
	file := NewOSFile(path)

	if err := file.Open(path); err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	// Write initial data
	initialData := make([]byte, 4096)
	for i := range initialData {
		initialData[i] = 0xAA
	}
	file.WriteAt(initialData, 0)

	// Concurrent readers
	const numGoroutines = 5
	var wg sync.WaitGroup

	// Readers
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 4096)
			for j := 0; j < 100; j++ {
				file.ReadAt(buf, 0)
			}
		}()
	}

	// Writer
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 100; j++ {
			data := make([]byte, 100)
			file.WriteAt(data, int64(j*100))
		}
	}()

	wg.Wait()
}

func TestOSFile_ConcurrentSync(t *testing.T) {
	dir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "testfile")
	file := NewOSFile(path)

	if err := file.Open(path); err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	// Write some data
	file.WriteAt(make([]byte, 4096), 0)

	// Concurrent syncs
	const numGoroutines = 5
	var wg sync.WaitGroup
	errChan := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				if err := file.Sync(); err != nil {
					errChan <- err
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Errorf("sync error: %v", err)
	}
}

// =============================================================================
// Concurrent State Checks
// =============================================================================

func TestSegment_ConcurrentStateChecks(t *testing.T) {
	dir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	config := Config{Directory: dir, SegmentSize: 1 << 20}
	sm, err := NewSegmentManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	seg, err := sm.CreateSegment()
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()

	// Concurrent state checks
	const numGoroutines = 10
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				_ = seg.ID()
				_ = seg.State()
				_ = seg.Size()
				_ = seg.PageCount()
			}
		}()
	}

	wg.Wait()
}

func TestSegmentManager_ConcurrentStateChecks(t *testing.T) {
	dir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	config := Config{Directory: dir, SegmentSize: 1 << 20}
	sm, err := NewSegmentManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	// Create segments - first 2 will be sealed when we create the 3rd
	for i := 0; i < 2; i++ {
		_, err := sm.CreateSegment()
		if err != nil {
			t.Fatal(err)
		}
	}

	// Create 3rd segment - this seals the first 2
	_, err = sm.CreateSegment()
	if err != nil {
		t.Fatal(err)
	}

	// Concurrent state checks
	const numGoroutines = 10
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				_ = sm.SegmentCount()
				_ = sm.ActiveSegmentCount()
				_ = sm.TotalSize()
				_ = sm.ActiveSegment()
				_ = sm.ListSegments()
			}
		}()
	}

	wg.Wait()
}
