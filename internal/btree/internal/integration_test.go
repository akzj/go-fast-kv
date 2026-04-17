package internal

import (
	"fmt"
	"math"
	"path/filepath"
	"testing"

	btreeapi "github.com/akzj/go-fast-kv/internal/btree/api"
	"github.com/akzj/go-fast-kv/internal/pagestore"
	pagestoreapi "github.com/akzj/go-fast-kv/internal/pagestore/api"
	"github.com/akzj/go-fast-kv/internal/segment"
	segmentapi "github.com/akzj/go-fast-kv/internal/segment/api"
	walapi "github.com/akzj/go-fast-kv/internal/wal/api"
)

// ─── Helpers ────────────────────────────────────────────────────────

type realSetup struct {
	tree     btreeapi.BTree
	provider *RealPageProvider
	ps       pagestoreapi.PageStore
	segMgr   segmentapi.SegmentManager
	segDir   string
}

func setupReal(t *testing.T) *realSetup {
	t.Helper()
	dir := t.TempDir()
	segDir := filepath.Join(dir, "segments")

	segMgr, err := segment.New(segmentapi.Config{Dir: segDir})
	if err != nil {
		t.Fatalf("segment.New: %v", err)
	}

	ps := pagestore.New(pagestoreapi.Config{}, segMgr, newMockLSM())
	provider := NewRealPageProvider(ps, 0)
	tree := New(btreeapi.Config{}, provider, nil)

	return &realSetup{
		tree:     tree,
		provider: provider,
		ps:       ps,
		segMgr:   segMgr,
		segDir:   segDir,
	}
}

func (s *realSetup) close(t *testing.T) {
	t.Helper()
	if err := s.tree.Close(); err != nil {
		t.Fatalf("tree.Close: %v", err)
	}
	if err := s.ps.Close(); err != nil {
		t.Fatalf("ps.Close: %v", err)
	}
	if err := s.segMgr.Close(); err != nil {
		t.Fatalf("segMgr.Close: %v", err)
	}
}

// reopen creates a new btree+pagestore+segment stack from the same directory,
// restoring the mapping from the collected WAL entries.
func (s *realSetup) reopen(t *testing.T) *realSetup {
	t.Helper()
	savedRootPageID := s.tree.RootPageID()
	savedNextPageID := s.ps.NextPageID()
	walEntries := s.provider.WALEntries()

	s.close(t)

	segMgr2, err := segment.New(segmentapi.Config{Dir: s.segDir})
	if err != nil {
		t.Fatalf("segment.New (reopen): %v", err)
	}

	ps2 := pagestore.New(pagestoreapi.Config{}, segMgr2, newMockLSM())
	recovery := ps2.(pagestoreapi.PageStoreRecovery)

	// Replay WAL entries to restore the mapping table
	for _, e := range walEntries {
		if e.Type == 1 { // RecordPageMap
			recovery.ApplyPageMap(e.ID, e.VAddr)
		} else if e.Type == 4 { // RecordPageFree
			recovery.ApplyPageFree(e.ID)
		}
	}
	recovery.SetNextPageID(savedNextPageID)

	provider2 := NewRealPageProvider(ps2, 0)
	tree2 := New(btreeapi.Config{}, provider2, nil)
	tree2.SetRootPageID(savedRootPageID)

	return &realSetup{
		tree:     tree2,
		provider: provider2,
		ps:       ps2,
		segMgr:   segMgr2,
		segDir:   s.segDir,
	}
}

func testKey(i int) []byte   { return []byte(fmt.Sprintf("key-%06d", i)) }
func testValue(i int) []byte { return []byte(fmt.Sprintf("value-%06d", i)) }

// maxTxn is a large txnID used for "see everything" reads.
// Cannot use math.MaxUint64 because the visibility check is TxnMax > txnID,
// and TxnMaxInfinity (MaxUint64) > MaxUint64 is false.
const maxTxn = math.MaxUint64 - 1

func assertEqual(t *testing.T, expected, actual []byte, msg string) {
	t.Helper()
	if string(expected) != string(actual) {
		t.Fatalf("%s: expected %q, got %q", msg, expected, actual)
	}
}

// ─── Tests ──────────────────────────────────────────────────────────


// mockLSMForTests is a simple in-memory LSM MappingStore for tests.
type mockLSMForTests struct {
	pages map[uint64]uint64
	blobs map[uint64]struct{ vaddr uint64; size uint32 }
}
func newMockLSM() *mockLSMForTests {
	return &mockLSMForTests{pages: make(map[uint64]uint64), blobs: make(map[uint64]struct{ vaddr uint64; size uint32 })}
}
func (m *mockLSMForTests) SetPageMapping(pageID uint64, vaddr uint64) { m.pages[pageID] = vaddr }
func (m *mockLSMForTests) GetPageMapping(pageID uint64) (uint64, bool) {
	v, ok := m.pages[pageID]; return v, ok
}
func (m *mockLSMForTests) SetBlobMapping(blobID uint64, vaddr uint64, size uint32) {
	m.blobs[blobID] = struct{ vaddr uint64; size uint32 }{vaddr, size}
}
func (m *mockLSMForTests) GetBlobMapping(blobID uint64) (uint64, uint32, bool) {
	b, ok := m.blobs[blobID]; return b.vaddr, b.size, ok
}
func (m *mockLSMForTests) DeleteBlobMapping(blobID uint64) { delete(m.blobs, blobID) }
func (m *mockLSMForTests) SetWAL(wal walapi.WAL) {}
func (m *mockLSMForTests) FlushToWAL() (uint64, error) { return 0, nil }
func (m *mockLSMForTests) LastLSN() uint64 { return 0 }
func (m *mockLSMForTests) Checkpoint(lsn uint64) error { return nil }
func (m *mockLSMForTests) CheckpointLSN() uint64 { return 0 }
func (m *mockLSMForTests) MaybeCompact() error { return nil }
func (m *mockLSMForTests) Close() error { return nil }


func TestIntegrationPutGet(t *testing.T) {
	s := setupReal(t)
	defer s.close(t)

	for i := 0; i < 100; i++ {
		if err := s.tree.Put(testKey(i), testValue(i), 1); err != nil {
			t.Fatalf("Put key %d: %v", i, err)
		}
	}

	for i := 0; i < 100; i++ {
		val, err := s.tree.Get(testKey(i), maxTxn)
		if err != nil {
			t.Fatalf("Get key %d: %v", i, err)
		}
		assertEqual(t, testValue(i), val, fmt.Sprintf("key %d", i))
	}
}

func TestIntegration1000Keys(t *testing.T) {
	s := setupReal(t)
	defer s.close(t)

	for i := 0; i < 1000; i++ {
		if err := s.tree.Put(testKey(i), testValue(i), 1); err != nil {
			t.Fatalf("Put key %d: %v", i, err)
		}
	}

	for i := 0; i < 1000; i++ {
		val, err := s.tree.Get(testKey(i), maxTxn)
		if err != nil {
			t.Fatalf("Get key %d: %v", i, err)
		}
		assertEqual(t, testValue(i), val, fmt.Sprintf("key %d", i))
	}
}

func TestIntegrationScan(t *testing.T) {
	s := setupReal(t)
	defer s.close(t)

	for i := 0; i < 500; i++ {
		if err := s.tree.Put(testKey(i), testValue(i), 1); err != nil {
			t.Fatalf("Put key %d: %v", i, err)
		}
	}

	// Scan [key-000100, key-000200)
	start := testKey(100)
	end := testKey(200)
	iter := s.tree.Scan(start, end, maxTxn)
	defer iter.Close()

	count := 0
	for iter.Next() {
		expected := testKey(100 + count)
		assertEqual(t, expected, iter.Key(), fmt.Sprintf("scan key %d", count))
		assertEqual(t, testValue(100+count), iter.Value(), fmt.Sprintf("scan val %d", count))
		count++
	}
	if err := iter.Err(); err != nil {
		t.Fatalf("Scan error: %v", err)
	}
	if count != 100 {
		t.Fatalf("expected 100 scan results, got %d", count)
	}
}

func TestIntegrationPersistence(t *testing.T) {
	s := setupReal(t)

	// Write 200 keys
	for i := 0; i < 200; i++ {
		if err := s.tree.Put(testKey(i), testValue(i), 1); err != nil {
			t.Fatalf("Put key %d: %v", i, err)
		}
	}

	// Reopen (close + restore from segment files + WAL entries)
	s2 := s.reopen(t)
	defer s2.close(t)

	// Verify all 200 keys
	for i := 0; i < 200; i++ {
		val, err := s2.tree.Get(testKey(i), maxTxn)
		if err != nil {
			t.Fatalf("Get key %d after reopen: %v", i, err)
		}
		assertEqual(t, testValue(i), val, fmt.Sprintf("key %d after reopen", i))
	}

	// Verify scan also works after reopen
	iter := s2.tree.Scan(testKey(0), testKey(200), maxTxn)
	defer iter.Close()
	count := 0
	for iter.Next() {
		count++
	}
	if err := iter.Err(); err != nil {
		t.Fatalf("Scan error after reopen: %v", err)
	}
	if count != 200 {
		t.Fatalf("expected 200 scan results after reopen, got %d", count)
	}
}

func TestIntegrationOverwritePersistence(t *testing.T) {
	s := setupReal(t)

	key := []byte("abc")

	// Write v1
	if err := s.tree.Put(key, []byte("v1"), 1); err != nil {
		t.Fatalf("Put v1: %v", err)
	}

	// Overwrite with v2 (higher txnID)
	if err := s.tree.Put(key, []byte("v2"), 2); err != nil {
		t.Fatalf("Put v2: %v", err)
	}

	// Verify v2 is current
	val, err := s.tree.Get(key, maxTxn)
	if err != nil {
		t.Fatalf("Get before reopen: %v", err)
	}
	assertEqual(t, []byte("v2"), val, "before reopen")

	// Reopen
	s2 := s.reopen(t)
	defer s2.close(t)

	// Verify v2 persisted
	val, err = s2.tree.Get(key, maxTxn)
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	assertEqual(t, []byte("v2"), val, "after reopen")
}

func TestIntegrationDeletePersistence(t *testing.T) {
	s := setupReal(t)

	key := []byte("abc")

	// Write
	if err := s.tree.Put(key, []byte("value"), 1); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Delete (txnID=2)
	if err := s.tree.Delete(key, 2); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Verify deleted
	_, err := s.tree.Get(key, maxTxn)
	if err != btreeapi.ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound before reopen, got: %v", err)
	}

	// Reopen
	s2 := s.reopen(t)
	defer s2.close(t)

	// Verify still deleted after reopen
	_, err = s2.tree.Get(key, maxTxn)
	if err != btreeapi.ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound after reopen, got: %v", err)
	}
}

func TestIntegration1000KeysPersistence(t *testing.T) {
	s := setupReal(t)

	// Write 1000 keys
	for i := 0; i < 1000; i++ {
		if err := s.tree.Put(testKey(i), testValue(i), 1); err != nil {
			t.Fatalf("Put key %d: %v", i, err)
		}
	}

	// Reopen
	s2 := s.reopen(t)
	defer s2.close(t)

	// Verify all 1000 keys after reopen
	for i := 0; i < 1000; i++ {
		val, err := s2.tree.Get(testKey(i), maxTxn)
		if err != nil {
			t.Fatalf("Get key %d after reopen: %v", i, err)
		}
		assertEqual(t, testValue(i), val, fmt.Sprintf("key %d after reopen", i))
	}
}
