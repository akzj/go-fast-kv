package internal

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	lsmapi "github.com/akzj/go-fast-kv/internal/lsm/api"
	segmentapi "github.com/akzj/go-fast-kv/internal/segment/api"
	walapi "github.com/akzj/go-fast-kv/internal/wal/api"
)

// mockWAL is a test double for walapi.WAL.
type mockWAL struct {
	mu       sync.Mutex
	records  []walapi.Record
	curLSN   uint64
	batches  []*walapi.Batch
	closed   bool
}

func newMockWAL() *mockWAL {
	return &mockWAL{}
}

func (w *mockWAL) WriteBatch(batch *walapi.Batch) (lastLSN uint64, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, walapi.ErrClosed
	}
	for _, rec := range batch.Records {
		w.curLSN++
		r := rec
		r.LSN = w.curLSN
		w.records = append(w.records, r)
	}
	w.batches = append(w.batches, batch)
	lastLSN = w.curLSN
	return lastLSN, nil
}

func (w *mockWAL) Replay(afterLSN uint64, fn func(walapi.Record) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, r := range w.records {
		if r.LSN <= afterLSN {
			continue
		}
		if err := fn(r); err != nil {
			return err
		}
	}
	return nil
}

func (w *mockWAL) CurrentLSN() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.curLSN
}

func (w *mockWAL) Truncate(upToLSN uint64) error { return nil }
func (w *mockWAL) Rotate() error                 { return nil }
func (w *mockWAL) DeleteSegmentsBefore(lsn uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	var kept []walapi.Record
	for _, r := range w.records {
		if r.LSN > lsn {
			kept = append(kept, r)
		}
	}
	w.records = kept
	return nil
}
func (w *mockWAL) ListSegments() []string { return nil }
func (w *mockWAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	return nil
}


func TestMemtableBasic(t *testing.T) {
	m := newMemtable()

	// Test page mappings
	m.SetPageMapping(1, 100)
	m.SetPageMapping(2, 200)

	vaddr, ok := m.GetPageMapping(1)
	if !ok || vaddr != 100 {
		t.Errorf("GetPageMapping(1): got (%d, %v), want (100, true)", vaddr, ok)
	}

	vaddr, ok = m.GetPageMapping(2)
	if !ok || vaddr != 200 {
		t.Errorf("GetPageMapping(2): got (%d, %v), want (200, true)", vaddr, ok)
	}

	// Test not found
	_, ok = m.GetPageMapping(999)
	if ok {
		t.Errorf("GetPageMapping(999): got ok=true, want ok=false")
	}

	// Test blob mappings
	m.SetBlobMapping(10, 1000, 500)
	vaddr, size, ok := m.GetBlobMapping(10)
	if !ok || vaddr != 1000 || size != 500 {
		t.Errorf("GetBlobMapping(10): got (%d, %d, %v), want (1000, 500, true)", vaddr, size, ok)
	}

	// Test delete
	m.DeleteBlobMapping(10)
	_, _, ok = m.GetBlobMapping(10)
	if ok {
		t.Errorf("GetBlobMapping(10) after delete: got ok=true, want ok=false")
	}
}

func TestMemtableUpdate(t *testing.T) {
	m := newMemtable()

	// Update same key
	m.SetPageMapping(1, 100)
	m.SetPageMapping(1, 200)

	vaddr, ok := m.GetPageMapping(1)
	if !ok || vaddr != 200 {
		t.Errorf("GetPageMapping(1) after update: got (%d, %v), want (200, true)", vaddr, ok)
	}
}

func TestSSTableWriteRead(t *testing.T) {
	Dir := t.TempDir()
	path := filepath.Join(Dir, "test.sst")

	// Create test data
	pages := []sstEntry{
		{key: 1, value: 100},
		{key: 5, value: 500},
		{key: 10, value: 1000},
	}
	blobs := []sstEntry{
		{key: 100, value: 10000, size: 1000},
		{key: 200, value: 20000, size: 2000},
	}

	// Write SSTable
	if err := writeSSTable(path, pages, blobs); err != nil {
		t.Fatalf("writeSSTable: %v", err)
	}

	// Read SSTable
	pages2, blobs2, err := readSSTable(path)
	if err != nil {
		t.Fatalf("readSSTable: %v", err)
	}

	if len(pages2) != len(pages) {
		t.Errorf("page count: got %d, want %d", len(pages2), len(pages))
	}

	if len(blobs2) != len(blobs) {
		t.Errorf("blob count: got %d, want %d", len(blobs2), len(blobs))
	}

	// Verify data
	for i, p := range pages2 {
		if p.key != pages[i].key || p.value != pages[i].value {
			t.Errorf("page[%d]: got (%d, %d), want (%d, %d)", i, p.key, p.value, pages[i].key, pages[i].value)
		}
	}
}

func TestManifest(t *testing.T) {
	Dir := t.TempDir()

	m, err := newManifest(Dir)
	if err != nil {
		t.Fatalf("newManifest: %v", err)
	}

	// Test AddSegmentWithLevel
	if err := m.AddSegmentWithLevel("segment-001.sst", 0, 0, 0); err != nil {
		t.Fatalf("AddSegmentWithLevel: %v", err)
	}

	segs := m.Segments()
	if len(segs) != 1 || segs[0] != "segment-001.sst" {
		t.Errorf("Segments: got %v, want [segment-001.sst]", segs)
	}

	// Test NextID
	id1 := m.NextID()
	id2 := m.NextID()
	if id2 != id1+1 {
		t.Errorf("NextID: got %d, %d, want consecutive", id1, id2)
	}

	// Test RemoveSegment
	if err := m.RemoveSegment("segment-001.sst"); err != nil {
		t.Fatalf("RemoveSegment: %v", err)
	}

	segs = m.Segments()
	if len(segs) != 0 {
		t.Errorf("Segments after remove: got %v, want []", segs)
	}

	// Flush async saves before temp dir cleanup
	m.Flush()
}

// mockSegmentManagerForDelete is a test double for segmentapi.SegmentManager.
type mockSegmentManagerForDelete struct {
	removedSegments map[uint32]bool
}

func newMockSegmentManagerForDelete() *mockSegmentManagerForDelete {
	return &mockSegmentManagerForDelete{
		removedSegments: make(map[uint32]bool),
	}
}

func (m *mockSegmentManagerForDelete) Append(data []byte) (segmentapi.VAddr, error) {
	return segmentapi.VAddr{}, nil
}

func (m *mockSegmentManagerForDelete) ReadAt(addr segmentapi.VAddr, size uint32) ([]byte, error) {
	return nil, nil
}

func (m *mockSegmentManagerForDelete) ReadAtInto(addr segmentapi.VAddr, buf []byte) error {
	return nil
}

func (m *mockSegmentManagerForDelete) Sync() error { return nil }

func (m *mockSegmentManagerForDelete) Rotate() error { return nil }

func (m *mockSegmentManagerForDelete) RemoveSegment(segID uint32) error {
	m.removedSegments[segID] = true
	return nil
}

func (m *mockSegmentManagerForDelete) ActiveSegmentID() uint32 { return 0 }

func (m *mockSegmentManagerForDelete) SegmentSize(segID uint32) (int64, error) { return 0, nil }

func (m *mockSegmentManagerForDelete) SealedSegments() []uint32 { return nil }

func (m *mockSegmentManagerForDelete) Close() error { return nil }

func (m *mockSegmentManagerForDelete) StorageDir() string { return "" }

func (m *mockSegmentManagerForDelete) Reserve(size int) (segmentapi.VAddr, []byte, error) {
	return segmentapi.VAddr{}, nil, fmt.Errorf("reserve not supported in mock")
}

func (m *mockSegmentManagerForDelete) WasRemoved(segID uint32) bool {
	return m.removedSegments[segID]
}

// TestTryDeleteRefcountRaceFix tests that TryDelete atomically checks refcount
// and prevents deletion while segment is pinned by checkpoint.
// This is the fix for the race: CanDelete() → Pin() → Delete would cause data loss.
func TestTryDeleteRefcountRaceFix(t *testing.T) {
	Dir := t.TempDir()

	m, err := newManifest(Dir)
	if err != nil {
		t.Fatalf("newManifest: %v", err)
	}

	// Add a segment
	if err := m.AddSegmentWithLevel("segment-001.sst", 0, 0, 0); err != nil {
		t.Fatalf("AddSegmentWithLevel: %v", err)
	}

	segMgr := newMockSegmentManagerForDelete()

	// Test 1: TryDelete should succeed when refcount == 0
	if !m.TryDelete(segMgr, 1) {
		t.Error("TryDelete(segID=1): expected true when refcount==0, got false")
	}
	if !segMgr.WasRemoved(1) {
		t.Error("TryDelete: segment file was not removed from segMgr")
	}

	// Add another segment
	if err := m.AddSegmentWithLevel("segment-002.sst", 0, 0, 0); err != nil {
		t.Fatalf("AddSegmentWithLevel: %v", err)
	}

	// Test 2: TryDelete should fail when refcount > 0 (pinned by checkpoint)
	m.Pin("segment-002.sst")
	if m.TryDelete(segMgr, 2) {
		t.Error("TryDelete(segID=2): expected false when refcount>0 (pinned), got true")
	}
	if segMgr.WasRemoved(2) {
		t.Error("TryDelete: segment file should NOT be removed when pinned")
	}

	// Unpin and verify deletion now succeeds
	m.Unpin("segment-002.sst")
	if !m.TryDelete(segMgr, 2) {
		t.Error("TryDelete(segID=2): expected true after Unpin, got false")
	}

	// Flush async saves before temp dir cleanup
	m.Flush()
}

// TestTryDeleteConcurrentPinRace tests the race condition that TryDelete prevents:
// concurrent Pin() and TryDelete() should never result in deletion while pinned.
func TestTryDeleteConcurrentPinRace(t *testing.T) {
	Dir := t.TempDir()

	m, err := newManifest(Dir)
	if err != nil {
		t.Fatalf("newManifest: %v", err)
	}

	// Add segments
	for i := uint64(1); i <= 10; i++ {
		if err := m.AddSegmentWithLevel(fmt.Sprintf("segment-%03d.sst", i), 0, 0, 0); err != nil {
			t.Fatalf("AddSegmentWithLevel: %v", err)
		}
	}

	var wg sync.WaitGroup
	errors := make(chan error, 100)

	// Concurrently: pin segments, unpin segments, and try to delete them
	for i := 1; i <= 10; i++ {
		segID := uint32(i)
		segName := fmt.Sprintf("segment-%03d.sst", i)

		wg.Add(1)
		go func() {
			defer wg.Done()
			// Rapidly pin and unpin
			for j := 0; j < 100; j++ {
				m.Pin(segName)
				m.Unpin(segName)
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			segMgr := newMockSegmentManagerForDelete()
			// Try to delete - should never succeed while pinned
			for j := 0; j < 100; j++ {
				if m.TryDelete(segMgr, segID) {
					// Deletion succeeded - verify segment was NOT pinned at time of deletion
					// TryDelete holds write lock during check+delete, so Pin cannot race
					// But we can verify: after deletion, segment is gone from manifest
					if m.Refcount(segName) != -1 {
						// Segment still exists but was "deleted" - this shouldn't happen
						select {
						case errors <- fmt.Errorf("TryDelete succeeded but segment still in manifest"):
						default:
						}
					}
				}
			}
		}()
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}

	// Flush async saves before temp dir cleanup
	m.Flush()
}

func TestLSMStoreBasic(t *testing.T) {
	Dir := t.TempDir()
	cfg := lsmapi.Config{Dir: Dir, MemtableSize: 1024}
	l, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer l.Close()

	// Test page mappings
	l.SetPageMapping(1, 100)
	l.SetPageMapping(2, 200)

	vaddr, ok := l.GetPageMapping(1)
	if !ok || vaddr != 100 {
		t.Errorf("GetPageMapping(1): got (%d, %v), want (100, true)", vaddr, ok)
	}

	// Test blob mappings
	l.SetBlobMapping(10, 1000, 500)
	vaddr, size, ok := l.GetBlobMapping(10)
	if !ok || vaddr != 1000 || size != 500 {
		t.Errorf("GetBlobMapping(10): got (%d, %d, %v), want (1000, 500, true)", vaddr, size, ok)
	}

	// Test delete
	l.DeleteBlobMapping(10)
	_, _, ok = l.GetBlobMapping(10)
	if ok {
		t.Errorf("GetBlobMapping(10) after delete: got ok=true, want ok=false")
	}
}

func TestLSMCheckpoint(t *testing.T) {
	Dir := t.TempDir()
	cfg := lsmapi.Config{Dir: Dir, MemtableSize: 1024}
	l, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer l.Close()

	// Set checkpoint LSN
	if err := l.Checkpoint(100); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	if l.CheckpointLSN() != 100 {
		t.Errorf("CheckpointLSN: got %d, want 100", l.CheckpointLSN())
	}
}

func TestLSMCompact(t *testing.T) {
	Dir := t.TempDir()
	cfg := lsmapi.Config{Dir: Dir, MemtableSize: 100} // Small size to trigger compaction
	l, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer l.Close()

	// Add enough data to trigger compaction
	for i := uint64(1); i <= 100; i++ {
		l.SetPageMapping(i, i*100)
	}

	// Trigger compaction
	if err := l.MaybeCompact(); err != nil {
		t.Fatalf("MaybeCompact: %v", err)
	}

	// Wait for background compaction to complete
	time.Sleep(100 * time.Millisecond)

	// Verify data still accessible after compaction
	vaddr, ok := l.GetPageMapping(50)
	if !ok || vaddr != 5000 {
		t.Errorf("GetPageMapping(50) after compaction: got (%d, %v), want (5000, true)", vaddr, ok)
	}
}

func TestLSMRecovery(t *testing.T) {
	Dir := t.TempDir()
	cfg := lsmapi.Config{Dir: Dir}

	// Create and populate store
	l, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	l.SetPageMapping(1, 100)
	l.SetPageMapping(2, 200)
	l.SetBlobMapping(10, 1000, 500)

	// Close to flush to SSTable
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Create recovery store
	rs, err := NewRecoveryStore(Dir)
	if err != nil {
		t.Fatalf("NewRecoveryStore: %v", err)
	}

	// Build from SSTables
	if err := rs.Build(); err != nil {
		t.Fatalf("Build: %v", err)
	}

	// Verify data
	vaddr, ok := rs.lsm.GetPageMapping(1)
	if !ok || vaddr != 100 {
		t.Errorf("GetPageMapping(1) after recovery: got (%d, %v), want (100, true)", vaddr, ok)
	}

	vaddr, size, ok := rs.lsm.GetBlobMapping(10)
	if !ok || vaddr != 1000 || size != 500 {
		t.Errorf("GetBlobMapping(10) after recovery: got (%d, %d, %v), want (1000, 500, true)", vaddr, size, ok)
	}
}

func TestLSMClose(t *testing.T) {
	Dir := t.TempDir()
	cfg := lsmapi.Config{Dir: Dir}
	l, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Add data
	l.SetPageMapping(1, 100)
	l.SetBlobMapping(10, 1000, 500)

	// Close
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Second close should return error
	if err := l.Close(); err != lsmapi.ErrClosed {
		t.Errorf("Second Close: got %v, want ErrClosed", err)
	}
}

func TestLSMRecoveryStoreApply(t *testing.T) {
	Dir := t.TempDir()
	rs, err := NewRecoveryStore(Dir)
	if err != nil {
		t.Fatalf("NewRecoveryStore: %v", err)
	}

	// Apply page mapping
	rs.ApplyPageMapping(1, 100)
	vaddr, ok := rs.lsm.GetPageMapping(1)
	if !ok || vaddr != 100 {
		t.Errorf("GetPageMapping(1): got (%d, %v), want (100, true)", vaddr, ok)
	}

	// Apply blob mapping
	rs.ApplyBlobMapping(10, 1000, 500)
	vaddr, size, ok := rs.lsm.GetBlobMapping(10)
	if !ok || vaddr != 1000 || size != 500 {
		t.Errorf("GetBlobMapping(10): got (%d, %d, %v), want (1000, 500, true)", vaddr, size, ok)
	}

	// Apply blob delete
	rs.ApplyBlobDelete(10)
	_, _, ok = rs.lsm.GetBlobMapping(10)
	if ok {
		t.Errorf("GetBlobMapping(10) after delete: got ok=true, want ok=false")
	}

	// Set checkpoint LSN
	rs.SetCheckpointLSN(1000)
	if rs.lsm.CheckpointLSN() != 1000 {
		t.Errorf("CheckpointLSN: got %d, want 1000", rs.lsm.CheckpointLSN())
	}
}

// ─── WAL Integration Tests ───────────────────────────────────────────

func TestLSMWALFlush(t *testing.T) {
	lsmDir := t.TempDir()

	w := newMockWAL()
	cfg := lsmapi.Config{Dir: lsmDir, MemtableSize: 1024}
	l, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	l.SetWAL(w)
	defer l.Close()

	// Write page and blob mappings
	l.SetPageMapping(1, 100)
	l.SetPageMapping(2, 200)
	l.SetBlobMapping(10, 1000, 500)

	// Flush to WAL
	lastLSN, err := l.FlushToWAL()
	if err != nil {
		t.Fatalf("FlushToWAL: %v", err)
	}
	if lastLSN == 0 {
		t.Errorf("FlushToWAL: expected non-zero LSN, got 0")
	}

	// Verify data still accessible after flush
	vaddr, ok := l.GetPageMapping(1)
	if !ok || vaddr != 100 {
		t.Errorf("GetPageMapping(1): got (%d, %v), want (100, true)", vaddr, ok)
	}

	vaddr, size, ok := l.GetBlobMapping(10)
	if !ok || vaddr != 1000 || size != 500 {
		t.Errorf("GetBlobMapping(10): got (%d, %d, %v), want (1000, 500, true)", vaddr, size, ok)
	}

	// Verify LastLSN
	if l.LastLSN() != lastLSN {
		t.Errorf("LastLSN: got %d, want %d", l.LastLSN(), lastLSN)
	}

	// Verify WAL received correct record types
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.records) < 3 {
		t.Errorf("expected ≥3 WAL records, got %d", len(w.records))
	}
	for _, r := range w.records {
		if r.ModuleType != walapi.ModuleLSM {
			t.Errorf("ModuleType: got %v, want ModuleLSM", r.ModuleType)
		}
	}
}

func TestLSMWALCheckpoint(t *testing.T) {
	lsmDir := t.TempDir()

	w := newMockWAL()
	cfg := lsmapi.Config{Dir: lsmDir, MemtableSize: 1024}
	l, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	l.SetWAL(w)
	defer l.Close()

	// Write without checkpoint — no WAL flush yet
	l.SetPageMapping(1, 100)

	// Checkpoint flushes WAL and records LSN
	if err := l.Checkpoint(42); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	if l.CheckpointLSN() != 42 {
		t.Errorf("CheckpointLSN: got %d, want 42", l.CheckpointLSN())
	}

	// Verify data persisted
	vaddr, ok := l.GetPageMapping(1)
	if !ok || vaddr != 100 {
		t.Errorf("GetPageMapping(1): got (%d, %v), want (100, true)", vaddr, ok)
	}

	// Verify WAL received the entry
	w.mu.Lock()
	hasPageMap := false
	for _, r := range w.records {
		if r.Type == walapi.RecordPageMap && r.ID == 1 {
			hasPageMap = true
			break
		}
	}
	w.mu.Unlock()
	if !hasPageMap {
		t.Errorf("WAL did not receive RecordPageMap for pageID=1")
	}
}

func TestLSMWALDeleteBlob(t *testing.T) {
	lsmDir := t.TempDir()

	w := newMockWAL()
	cfg := lsmapi.Config{Dir: lsmDir, MemtableSize: 1024}
	l, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	l.SetWAL(w)
	defer l.Close()

	// Write blob then delete
	l.SetBlobMapping(10, 1000, 500)
	l.DeleteBlobMapping(10)

	// Flush to WAL
	_, err = l.FlushToWAL()
	if err != nil {
		t.Fatalf("FlushToWAL: %v", err)
	}

	// Verify deleted
	_, _, ok := l.GetBlobMapping(10)
	if ok {
		t.Errorf("GetBlobMapping(10) after delete: got ok=true, want ok=false")
	}

	// Verify WAL has both BlobMap and BlobFree records
	w.mu.Lock()
	hasMap, hasFree := false, false
	for _, r := range w.records {
		if r.Type == walapi.RecordBlobMap && r.ID == 10 {
			hasMap = true
		}
		if r.Type == walapi.RecordBlobFree && r.ID == 10 {
			hasFree = true
		}
	}
	w.mu.Unlock()
	if !hasMap {
		t.Errorf("WAL missing RecordBlobMap for blobID=10")
	}
	if !hasFree {
		t.Errorf("WAL missing RecordBlobFree for blobID=10")
	}
}

func TestLSMWALReplay(t *testing.T) {
	lsmDir := t.TempDir()

	// Create LSM with mock WAL, write and checkpoint
	w := newMockWAL()
	cfg := lsmapi.Config{Dir: lsmDir, MemtableSize: 1024}
	l, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	l.SetWAL(w)

	l.SetPageMapping(1, 100)
	l.SetBlobMapping(10, 1000, 500)
	l.Checkpoint(1)

	l.Close()

	// Verify WAL collected entries
	w.mu.Lock()
	pageRecs, blobRecs := 0, 0
	for _, r := range w.records {
		if r.ModuleType == walapi.ModuleLSM {
			switch r.Type {
			case walapi.RecordPageMap:
				pageRecs++
			case walapi.RecordBlobMap:
				blobRecs++
			}
		}
	}
	w.mu.Unlock()

	if pageRecs == 0 {
		t.Errorf("expected at least 1 page WAL record, got 0")
	}
	if blobRecs == 0 {
		t.Errorf("expected at least 1 blob WAL record, got 0")
	}

	// Replay into recovery store
	lsmDir2 := t.TempDir()
	rs, err := NewRecoveryStore(lsmDir2)
	if err != nil {
		t.Fatalf("NewRecoveryStore: %v", err)
	}

	for _, r := range w.records {
		if r.ModuleType != walapi.ModuleLSM {
			continue
		}
		switch r.Type {
		case walapi.RecordPageMap:
			rs.ApplyPageMapping(r.ID, r.VAddr)
		case walapi.RecordBlobMap:
			rs.ApplyBlobMapping(r.ID, r.VAddr, r.Size)
		case walapi.RecordBlobFree:
			rs.ApplyBlobDelete(r.ID)
		}
	}

	// Verify replayed data
	vaddr, ok := rs.lsm.GetPageMapping(1)
	if !ok || vaddr != 100 {
		t.Errorf("GetPageMapping(1) after replay: got (%d, %v), want (100, true)", vaddr, ok)
	}

	vaddr, size, ok := rs.lsm.GetBlobMapping(10)
	if !ok || vaddr != 1000 || size != 500 {
		t.Errorf("GetBlobMapping(10) after replay: got (%d, %d, %v), want (1000, 500, true)", vaddr, size, ok)
	}
}

// ─── Bloom Filter Benchmark ──────────────────────────────────────────

// BenchmarkLSMBloomFilter benchmarks read performance with bloom filters.
// With bloom filters, negative lookups (key not present) skip SSTables quickly.
func BenchmarkLSMBloomFilter(b *testing.B) {
	dir := b.TempDir()

	// Create LSM with small memtable to force multiple SSTables
	cfg := lsmapi.Config{Dir: dir, MemtableSize: 512} // Very small to force many SSTables
	l, err := New(cfg)
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	defer l.Close()

	// Insert enough data to create multiple SSTables
	// Each SSTable will have ~30 entries (512 bytes / ~17 bytes per entry)
	for i := uint64(1); i <= 1000; i++ {
		l.SetPageMapping(i, i*100)
	}

	// Trigger compaction to create SSTables
	l.MaybeCompact()
	time.Sleep(200 * time.Millisecond)

	b.ResetTimer()
	b.Run("PositiveLookup", func(b *testing.B) {
		// Lookup existing keys - should find in earliest SSTable
		for i := 0; i < b.N; i++ {
			pageID := uint64((i % 1000) + 1)
			l.GetPageMapping(pageID)
		}
	})

	b.Run("NegativeLookup", func(b *testing.B) {
		// Lookup non-existing keys - bloom filters should skip many SSTables
		for i := 0; i < b.N; i++ {
			pageID := uint64(1000000 + i) // Keys definitely not in any SSTable
			l.GetPageMapping(pageID)
		}
	})
}

// ─── Level Compaction Tests ─────────────────────────────────────────

func TestLevelCompactionIntegration(t *testing.T) {
	Dir := t.TempDir()
	cfg := lsmapi.Config{Dir: Dir, MemtableSize: 64}
	l, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer l.Close()

	// Insert data to create multiple SSTables at L0
	// Small memtable size (64 bytes) forces frequent flushes
	for i := uint64(1); i <= 10; i++ {
		l.SetPageMapping(i, i*100)
	}

	// Trigger compaction to create SSTables
	if err := l.MaybeCompact(); err != nil {
		t.Fatalf("MaybeCompact: %v", err)
	}
	// Wait for background flush
	time.Sleep(200 * time.Millisecond)
	l.WaitForCompaction()

	// Verify we have L0 segments
	l0Count := l.manifest.CountLevel(0)
	t.Logf("L0 segments after first compaction: %d", l0Count)

	// Verify data is accessible after compaction
	vaddr, ok := l.GetPageMapping(1)
	if !ok || vaddr != 100 {
		t.Errorf("GetPageMapping(1) after compaction: got (%d, %v), want (100, true)", vaddr, ok)
	}

	// Insert more data to exceed L0 capacity
	for i := uint64(100); i <= 300; i++ {
		l.SetPageMapping(i, i*100)
	}

	// Trigger more compactions to create more L0 segments
	for i := 0; i < 3; i++ {
		if err := l.MaybeCompact(); err != nil {
			t.Fatalf("MaybeCompact %d: %v", i, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	l.WaitForCompaction()

	// Check segment counts
	l0Count = l.manifest.CountLevel(0)
	l1Count := l.manifest.CountLevel(1)
	t.Logf("After compactions - L0: %d, L1: %d", l0Count, l1Count)

	// Verify all data still accessible from memtables
	for i := uint64(1); i <= 10; i++ {
		vaddr, ok := l.GetPageMapping(i)
		if !ok || vaddr != i*100 {
			t.Errorf("GetPageMapping(%d): got (%d, %v), want (%d, true)", i, vaddr, ok, i*100)
		}
	}
}

func TestManifestLevelTracking(t *testing.T) {
	Dir := t.TempDir()
	m, err := newManifest(Dir)
	if err != nil {
		t.Fatalf("newManifest: %v", err)
	}

	// Add segments at different levels with key ranges
	m.AddSegmentWithLevel("seg-l0-1.sst", 0, 1, 100)
	m.AddSegmentWithLevel("seg-l0-2.sst", 0, 50, 150)
	m.AddSegmentWithLevel("seg-l1-1.sst", 1, 1, 100)
	m.AddSegmentWithLevel("seg-l1-2.sst", 1, 200, 300)

	// Test GetSegmentsByLevel
	l0Segs := m.GetSegmentsByLevel(0)
	if len(l0Segs) != 2 {
		t.Errorf("GetSegmentsByLevel(0): got %d, want 2", len(l0Segs))
	}

	l1Segs := m.GetSegmentsByLevel(1)
	if len(l1Segs) != 2 {
		t.Errorf("GetSegmentsByLevel(1): got %d, want 2", len(l1Segs))
	}

	// Test GetOverlappingSegments - seg-l0-1 has range [1,100]
	// Should overlap with seg-l1-1 [1,100] but not seg-l1-2 [200,300]
	overlapping := m.GetOverlappingSegments(1, 1, 100)
	if len(overlapping) != 1 || overlapping[0].name != "seg-l1-1.sst" {
		t.Errorf("GetOverlappingSegments(1, [1,100]): got %v, want [seg-l1-1.sst]", overlapping)
	}

	// Test CountLevel
	if m.CountLevel(0) != 2 {
		t.Errorf("CountLevel(0): got %d, want 2", m.CountLevel(0))
	}
	if m.CountLevel(1) != 2 {
		t.Errorf("CountLevel(1): got %d, want 2", m.CountLevel(1))
	}

	// Test GetLevel/SetLevel
	if m.GetLevel("seg-l0-1.sst") != 0 {
		t.Errorf("GetLevel(seg-l0-1.sst): got %d, want 0", m.GetLevel("seg-l0-1.sst"))
	}

	m.SetLevel("seg-l0-1.sst", 1)
	if m.GetLevel("seg-l0-1.sst") != 1 {
		t.Errorf("GetLevel after SetLevel: got %d, want 1", m.GetLevel("seg-l0-1.sst"))
	}

	// Test GetKeyRange
	minKey, maxKey := m.GetKeyRange("seg-l0-2.sst")
	if minKey != 50 || maxKey != 150 {
		t.Errorf("GetKeyRange(seg-l0-2.sst): got (%d, %d), want (50, 150)", minKey, maxKey)
	}

}

// TestTryDeleteRefcountRaceFix tests that TryDelete atomically checks refcount	// Flush async saves before temp dir cleanup
