package internal

import (
	"path/filepath"
	"sync"
	"testing"
	"time"

	lsmapi "github.com/akzj/go-fast-kv/internal/lsm/api"
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

	// Test AddSegment
	if err := m.AddSegment("segment-001.sst"); err != nil {
		t.Fatalf("AddSegment: %v", err)
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
