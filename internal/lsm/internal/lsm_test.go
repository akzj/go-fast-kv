package internal

import (
	"path/filepath"
	"testing"
	"time"

	lsmapi "github.com/akzj/go-fast-kv/internal/lsm/api"
)

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
