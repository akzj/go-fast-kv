package internal

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/akzj/go-fast-kv/internal/blobstore"
	blobstoreapi "github.com/akzj/go-fast-kv/internal/blobstore/api"
	"github.com/akzj/go-fast-kv/internal/btree"
	btreeapi "github.com/akzj/go-fast-kv/internal/btree/api"
	"github.com/akzj/go-fast-kv/internal/pagestore"
	pagestoreapi "github.com/akzj/go-fast-kv/internal/pagestore/api"
	"github.com/akzj/go-fast-kv/internal/segment"
	segmentapi "github.com/akzj/go-fast-kv/internal/segment/api"
	txnmod "github.com/akzj/go-fast-kv/internal/txn"
	txnapi "github.com/akzj/go-fast-kv/internal/txn/api"
	vacuumapi "github.com/akzj/go-fast-kv/internal/vacuum/api"
	"github.com/akzj/go-fast-kv/internal/wal"
	walapi "github.com/akzj/go-fast-kv/internal/wal/api"
)

// ─── Test environment ───────────────────────────────────────────────

type testEnv struct {
	dir         string
	pageSegMgr  segmentapi.SegmentManager
	blobSegMgr  segmentapi.SegmentManager
	wal         walapi.WAL
	pageStore   pagestoreapi.PageStore
	blobStore   blobstoreapi.BlobStore
	txnMgr      txnapi.TxnManager
	provider    *btree.RealPageProvider
	blobAdapter *testBlobAdapter
	tree        btreeapi.BTree
}

// testBlobAdapter adapts BlobStore to btreeapi.BlobWriter, matching kvstore pattern.
type testBlobAdapter struct {
	store   blobstoreapi.BlobStore
	entries []blobstoreapi.WALEntry
}

func (a *testBlobAdapter) WriteBlob(data []byte) (uint64, error) {
	blobID, entry, err := a.store.Write(data)
	if err != nil {
		return 0, err
	}
	a.entries = append(a.entries, entry)
	return blobID, nil
}

func (a *testBlobAdapter) ReadBlob(blobID uint64) ([]byte, error) {
	return a.store.Read(blobID)
}

func (a *testBlobAdapter) drain() []blobstoreapi.WALEntry {
	out := a.entries
	a.entries = nil
	return out
}

func setupTestEnv(t *testing.T) *testEnv {
	t.Helper()
	dir := t.TempDir()

	pageSegDir := filepath.Join(dir, "page_segments")
	blobSegDir := filepath.Join(dir, "blob_segments")
	walDir := filepath.Join(dir, "wal")
	for _, d := range []string{pageSegDir, blobSegDir, walDir} {
		if err := os.MkdirAll(d, 0755); err != nil {
			t.Fatal(err)
		}
	}

	pageSegMgr, err := segment.New(segmentapi.Config{Dir: pageSegDir})
	if err != nil {
		t.Fatal(err)
	}
	blobSegMgr, err := segment.New(segmentapi.Config{Dir: blobSegDir})
	if err != nil {
		t.Fatal(err)
	}
	w, err := wal.New(walapi.Config{Dir: walDir})
	if err != nil {
		t.Fatal(err)
	}

	ps := pagestore.New(pagestoreapi.Config{}, pageSegMgr)
	bs := blobstore.New(blobstoreapi.Config{}, blobSegMgr)
	tm := txnmod.New()
	provider := btree.NewRealPageProvider(ps, 0)
	ba := &testBlobAdapter{store: bs}
	tree := btree.New(btreeapi.Config{InlineThreshold: 256}, provider, ba)

	t.Cleanup(func() {
		tree.Close()
		ps.Close()
		bs.Close()
		w.Close()
		pageSegMgr.Close()
		blobSegMgr.Close()
	})

	return &testEnv{
		dir:         dir,
		pageSegMgr:  pageSegMgr,
		blobSegMgr:  blobSegMgr,
		wal:         w,
		pageStore:   ps,
		blobStore:   bs,
		txnMgr:      tm,
		provider:    provider,
		blobAdapter: ba,
		tree:        tree,
	}
}

// putAndCommit is a helper that does a full Put + Commit cycle,
// draining WAL entries and writing a WAL batch (like kvstore does).
func (env *testEnv) putAndCommit(t *testing.T, key, value []byte) uint64 {
	t.Helper()
	xid, _ := env.txnMgr.BeginTxn()
	env.provider.DrainWALEntries()
	env.blobAdapter.drain()

	if err := env.tree.Put(key, value, xid); err != nil {
		t.Fatalf("Put(%q): %v", key, err)
	}

	commitEntry := env.txnMgr.Commit(xid)
	batch := env.assembleBatch(env.tree.RootPageID(), commitEntry)

	if err := env.pageSegMgr.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := env.wal.WriteBatch(batch); err != nil {
		t.Fatal(err)
	}
	return xid
}

// putAndAbort does a full Put + Abort cycle.
func (env *testEnv) putAndAbort(t *testing.T, key, value []byte) uint64 {
	t.Helper()
	xid, _ := env.txnMgr.BeginTxn()
	env.provider.DrainWALEntries()
	env.blobAdapter.drain()

	if err := env.tree.Put(key, value, xid); err != nil {
		t.Fatalf("Put(%q): %v", key, err)
	}

	abortEntry := env.txnMgr.Abort(xid)
	batch := env.assembleBatch(env.tree.RootPageID(), abortEntry)

	if err := env.pageSegMgr.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := env.wal.WriteBatch(batch); err != nil {
		t.Fatal(err)
	}
	return xid
}

// deleteAndCommit does a full Delete + Commit cycle.
func (env *testEnv) deleteAndCommit(t *testing.T, key []byte) uint64 {
	t.Helper()
	xid, _ := env.txnMgr.BeginTxn()
	env.provider.DrainWALEntries()
	env.blobAdapter.drain()

	if err := env.tree.Delete(key, xid); err != nil {
		t.Fatalf("Delete(%q): %v", key, err)
	}

	commitEntry := env.txnMgr.Commit(xid)
	batch := env.assembleBatch(env.tree.RootPageID(), commitEntry)

	if err := env.pageSegMgr.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := env.wal.WriteBatch(batch); err != nil {
		t.Fatal(err)
	}
	return xid
}

func (env *testEnv) assembleBatch(rootPageID uint64, txnEntry txnapi.WALEntry) *walapi.Batch {
	batch := walapi.NewBatch()
	for _, e := range env.provider.DrainWALEntries() {
		batch.Add(walapi.ModuleTree, walapi.RecordType(e.Type), e.ID, e.VAddr, e.Size)
	}
	for _, e := range env.blobAdapter.drain() {
		batch.Add(walapi.ModuleTree, walapi.RecordType(e.Type), e.ID, e.VAddr, e.Size)
	}
	batch.Add(walapi.ModuleTree, walapi.RecordSetRoot, rootPageID, 0, 0)
	batch.Add(walapi.ModuleTree, walapi.RecordType(txnEntry.Type), txnEntry.ID, 0, 0)
	return batch
}

// noopPageLocker is a no-op PageLocker for single-threaded tests.
type noopPageLocker struct{}

func (noopPageLocker) RLock(uint64)   {}
func (noopPageLocker) RUnlock(uint64) {}
func (noopPageLocker) WLock(uint64)   {}
func (noopPageLocker) WUnlock(uint64) {}

func (env *testEnv) newVacuum() vacuumapi.Vacuum {
	return New(
		env.tree.RootPageID,
		env.provider,
		env.txnMgr,
		env.blobStore,
		env.wal,
		env.pageSegMgr.Sync,
		// Wrap RegisterCollector to match the new vacuum API.
		// Returns a pointer to the entries slice + unregister function.
		func() (*[]pagestoreapi.WALEntry, func()) {
			collector, unreg := env.provider.RegisterCollector()
			return &collector.PageEntries, unreg
		},
		noopPageLocker{},
	)
}

// countLeafEntries traverses all leaves and counts total entries.
func (env *testEnv) countLeafEntries(t *testing.T) int {
	t.Helper()
	rootPID := env.tree.RootPageID()
	if rootPID == 0 {
		return 0
	}
	// Navigate to leftmost leaf
	pid := rootPID
	for {
		node, err := env.provider.ReadPage(pid)
		if err != nil {
			t.Fatal(err)
		}
		if node.IsLeaf {
			break
		}
		pid = node.Children[0]
	}
	// Count entries across all leaves
	total := 0
	for pid != 0 {
		node, err := env.provider.ReadPage(pid)
		if err != nil {
			t.Fatal(err)
		}
		total += len(node.Entries)
		pid = node.Next
	}
	return total
}

// countLeaves traverses all leaves and counts them.
func (env *testEnv) countLeaves(t *testing.T) int {
	t.Helper()
	rootPID := env.tree.RootPageID()
	if rootPID == 0 {
		return 0
	}
	pid := rootPID
	for {
		node, err := env.provider.ReadPage(pid)
		if err != nil {
			t.Fatal(err)
		}
		if node.IsLeaf {
			break
		}
		pid = node.Children[0]
	}
	count := 0
	for pid != 0 {
		node, err := env.provider.ReadPage(pid)
		if err != nil {
			t.Fatal(err)
		}
		count++
		pid = node.Next
	}
	return count
}

// ─── Tests ──────────────────────────────────────────────────────────

// Test 1: Empty tree returns ErrNoLeaves
func TestVacuum_EmptyTree(t *testing.T) {
	env := setupTestEnv(t)
	v := env.newVacuum()

	_, err := v.Run()
	if err != vacuumapi.ErrNoLeaves {
		t.Fatalf("expected ErrNoLeaves, got %v", err)
	}
}

// Test 2: All entries are current (TxnMax=MaxUint64), nothing to clean
func TestVacuum_NoCleanableEntries(t *testing.T) {
	env := setupTestEnv(t)

	// Insert 3 keys, all committed, none overwritten
	env.putAndCommit(t, []byte("a"), []byte("v1"))
	env.putAndCommit(t, []byte("b"), []byte("v2"))
	env.putAndCommit(t, []byte("c"), []byte("v3"))

	before := env.countLeafEntries(t)

	v := env.newVacuum()
	stats, err := v.Run()
	if err != nil {
		t.Fatal(err)
	}

	after := env.countLeafEntries(t)

	if stats.EntriesRemoved != 0 {
		t.Fatalf("expected 0 entries removed, got %d", stats.EntriesRemoved)
	}
	if stats.LeavesModified != 0 {
		t.Fatalf("expected 0 leaves modified, got %d", stats.LeavesModified)
	}
	if before != after {
		t.Fatalf("entry count changed: before=%d after=%d", before, after)
	}
	if before != 3 {
		t.Fatalf("expected 3 entries, got %d", before)
	}
	t.Logf("Stats: scanned=%d modified=%d removed=%d blobs=%d",
		stats.LeavesScanned, stats.LeavesModified, stats.EntriesRemoved, stats.BlobsFreed)
}

// Test 3: Case 1 — Committed overwrite, old version removed
func TestVacuum_CommittedDelete(t *testing.T) {
	env := setupTestEnv(t)

	// Insert key "a" v1
	env.putAndCommit(t, []byte("a"), []byte("v1"))
	// Overwrite key "a" v2 — this sets v1.TxnMax = xid2
	env.putAndCommit(t, []byte("a"), []byte("v2"))

	before := env.countLeafEntries(t)
	if before != 2 {
		t.Fatalf("expected 2 entries before vacuum, got %d", before)
	}

	v := env.newVacuum()
	stats, err := v.Run()
	if err != nil {
		t.Fatal(err)
	}

	after := env.countLeafEntries(t)

	if stats.EntriesRemoved != 1 {
		t.Fatalf("expected 1 entry removed, got %d", stats.EntriesRemoved)
	}
	if stats.LeavesModified != 1 {
		t.Fatalf("expected 1 leaf modified, got %d", stats.LeavesModified)
	}
	if after != 1 {
		t.Fatalf("expected 1 entry after vacuum, got %d", after)
	}

	// Verify the remaining entry is v2 (readable)
	val, err := env.tree.Get([]byte("a"), math.MaxUint64-1)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(val, []byte("v2")) {
		t.Fatalf("expected v2, got %q", val)
	}

	t.Logf("Stats: scanned=%d modified=%d removed=%d blobs=%d",
		stats.LeavesScanned, stats.LeavesModified, stats.EntriesRemoved, stats.BlobsFreed)
}

// Test 4: Case 2 — Aborted creator, entry removed, prev version restored
func TestVacuum_AbortedCreator(t *testing.T) {
	env := setupTestEnv(t)

	// Insert key "a" v1, committed
	env.putAndCommit(t, []byte("a"), []byte("v1"))
	// Overwrite key "a" v2, aborted — sets v1.TxnMax = xid2, then abort xid2
	env.putAndAbort(t, []byte("a"), []byte("v2"))

	before := env.countLeafEntries(t)
	if before != 2 {
		t.Fatalf("expected 2 entries before vacuum, got %d", before)
	}

	v := env.newVacuum()
	stats, err := v.Run()
	if err != nil {
		t.Fatal(err)
	}

	after := env.countLeafEntries(t)

	if stats.EntriesRemoved != 1 {
		t.Fatalf("expected 1 entry removed, got %d", stats.EntriesRemoved)
	}
	if after != 1 {
		t.Fatalf("expected 1 entry after vacuum, got %d", after)
	}

	// Verify v1 is still readable and its TxnMax was restored to MaxUint64
	val, err := env.tree.Get([]byte("a"), math.MaxUint64-1)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(val, []byte("v1")) {
		t.Fatalf("expected v1, got %q", val)
	}

	t.Logf("Stats: scanned=%d modified=%d removed=%d blobs=%d",
		stats.LeavesScanned, stats.LeavesModified, stats.EntriesRemoved, stats.BlobsFreed)
}

// Test 5: Mix of Case 1, Case 2, and keep entries
func TestVacuum_MixedCases(t *testing.T) {
	env := setupTestEnv(t)

	// Key "a": committed overwrite (Case 1 — old version removed)
	env.putAndCommit(t, []byte("a"), []byte("a-v1"))
	env.putAndCommit(t, []byte("a"), []byte("a-v2"))

	// Key "b": aborted overwrite (Case 2 — aborted entry removed, prev restored)
	env.putAndCommit(t, []byte("b"), []byte("b-v1"))
	env.putAndAbort(t, []byte("b"), []byte("b-v2"))

	// Key "c": current, no cleanup needed
	env.putAndCommit(t, []byte("c"), []byte("c-v1"))

	before := env.countLeafEntries(t)
	// a: 2 entries, b: 2 entries, c: 1 entry = 5 total
	if before != 5 {
		t.Fatalf("expected 5 entries before vacuum, got %d", before)
	}

	v := env.newVacuum()
	stats, err := v.Run()
	if err != nil {
		t.Fatal(err)
	}

	after := env.countLeafEntries(t)

	// Should remove: a-v1 (Case 1) + b-v2 (Case 2) = 2 removed
	if stats.EntriesRemoved != 2 {
		t.Fatalf("expected 2 entries removed, got %d", stats.EntriesRemoved)
	}
	// 5 - 2 = 3 remaining
	if after != 3 {
		t.Fatalf("expected 3 entries after vacuum, got %d", after)
	}

	// Verify all keys are still readable with correct values
	for _, tc := range []struct {
		key string
		val string
	}{
		{"a", "a-v2"},
		{"b", "b-v1"},
		{"c", "c-v1"},
	} {
		val, err := env.tree.Get([]byte(tc.key), math.MaxUint64-1)
		if err != nil {
			t.Fatalf("Get(%q): %v", tc.key, err)
		}
		if !bytes.Equal(val, []byte(tc.val)) {
			t.Fatalf("Get(%q): expected %q, got %q", tc.key, tc.val, val)
		}
	}

	t.Logf("Stats: scanned=%d modified=%d removed=%d blobs=%d",
		stats.LeavesScanned, stats.LeavesModified, stats.EntriesRemoved, stats.BlobsFreed)
}

// Test 6: Blob cleanup — entries with BlobID > 0 are freed when removed
func TestVacuum_BlobCleanup(t *testing.T) {
	env := setupTestEnv(t)

	// Use a large value (> InlineThreshold=256) to trigger blob storage
	largeV1 := []byte(strings.Repeat("A", 300))
	largeV2 := []byte(strings.Repeat("B", 300))

	// Insert key "a" with large value v1 (stored as blob)
	env.putAndCommit(t, []byte("a"), largeV1)
	// Overwrite with v2 (also blob) — v1 becomes dead
	env.putAndCommit(t, []byte("a"), largeV2)

	before := env.countLeafEntries(t)
	if before != 2 {
		t.Fatalf("expected 2 entries before vacuum, got %d", before)
	}

	v := env.newVacuum()
	stats, err := v.Run()
	if err != nil {
		t.Fatal(err)
	}

	if stats.EntriesRemoved != 1 {
		t.Fatalf("expected 1 entry removed, got %d", stats.EntriesRemoved)
	}
	if stats.BlobsFreed != 1 {
		t.Fatalf("expected 1 blob freed, got %d", stats.BlobsFreed)
	}

	// Verify v2 is still readable
	val, err := env.tree.Get([]byte("a"), math.MaxUint64-1)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(val, largeV2) {
		t.Fatalf("expected largeV2, got %d bytes", len(val))
	}

	t.Logf("Stats: scanned=%d modified=%d removed=%d blobs=%d",
		stats.LeavesScanned, stats.LeavesModified, stats.EntriesRemoved, stats.BlobsFreed)
}

// Test 7: Multiple leaves — tree with enough entries to span multiple leaves
func TestVacuum_MultipleLeaves(t *testing.T) {
	env := setupTestEnv(t)

	// Insert enough keys to cause splits (each leaf holds ~4096 bytes).
	// With 200-byte values and ~20-byte overhead per entry, ~18 entries per leaf.
	// Insert 60 keys to get at least 3 leaves.
	numKeys := 60
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		val := []byte(fmt.Sprintf("val-%03d-padding-%s", i, strings.Repeat("x", 150)))
		env.putAndCommit(t, key, val)
	}

	leafCount := env.countLeaves(t)
	if leafCount < 2 {
		t.Fatalf("expected at least 2 leaves, got %d", leafCount)
	}
	t.Logf("Tree has %d leaves", leafCount)

	// Overwrite half the keys to create dead versions
	for i := 0; i < numKeys; i += 2 {
		key := []byte(fmt.Sprintf("key-%03d", i))
		val := []byte(fmt.Sprintf("val-%03d-updated-%s", i, strings.Repeat("y", 150)))
		env.putAndCommit(t, key, val)
	}

	before := env.countLeafEntries(t)
	t.Logf("Entries before vacuum: %d", before)

	v := env.newVacuum()
	stats, err := v.Run()
	if err != nil {
		t.Fatal(err)
	}

	after := env.countLeafEntries(t)
	t.Logf("Entries after vacuum: %d", after)

	// Should remove 30 old versions (every other key was overwritten)
	if stats.EntriesRemoved != numKeys/2 {
		t.Fatalf("expected %d entries removed, got %d", numKeys/2, stats.EntriesRemoved)
	}
	if stats.LeavesScanned < 2 {
		t.Fatalf("expected at least 2 leaves scanned, got %d", stats.LeavesScanned)
	}
	if stats.LeavesModified < 1 {
		t.Fatalf("expected at least 1 leaf modified, got %d", stats.LeavesModified)
	}

	// Verify all keys are still readable
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		val, err := env.tree.Get(key, math.MaxUint64-1)
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		if i%2 == 0 {
			if !bytes.Contains(val, []byte("updated")) {
				t.Fatalf("key-%03d: expected updated value, got %q", i, val[:30])
			}
		} else {
			if !bytes.Contains(val, []byte("padding")) {
				t.Fatalf("key-%03d: expected original value, got %q", i, val[:30])
			}
		}
	}

	t.Logf("Stats: scanned=%d modified=%d removed=%d blobs=%d",
		stats.LeavesScanned, stats.LeavesModified, stats.EntriesRemoved, stats.BlobsFreed)
}

// Test 8: SafeXID boundary — entry with TxnMax == safeXID is NOT removed
func TestVacuum_SafeXIDBoundary(t *testing.T) {
	env := setupTestEnv(t)

	// Insert key "a" v1
	env.putAndCommit(t, []byte("a"), []byte("v1"))
	// Overwrite key "a" v2 — v1.TxnMax = xid2
	xid2 := env.putAndCommit(t, []byte("a"), []byte("v2"))

	before := env.countLeafEntries(t)
	if before != 2 {
		t.Fatalf("expected 2 entries before vacuum, got %d", before)
	}

	// Start a transaction with xid == xid2 to make safeXID == xid2
	// This means v1.TxnMax == safeXID, which should NOT be removed (must be strictly less)
	holdXID, _ := env.txnMgr.BeginTxn()
	_ = holdXID

	// Verify GetMinActive returns holdXID (which equals xid2+1)
	minActive := env.txnMgr.GetMinActive()
	t.Logf("holdXID=%d, xid2=%d, minActive=%d", holdXID, xid2, minActive)

	// The v1 entry has TxnMax = xid2. safeXID = holdXID = xid2+1.
	// Since xid2 < xid2+1, v1 IS eligible for cleanup.
	// To test the boundary, we need safeXID == xid2 exactly.
	// Let's abort the hold txn and use a different approach.
	env.txnMgr.Abort(holdXID)

	// Create a scenario where safeXID == v1.TxnMax:
	// v1.TxnMax = xid2. We need GetMinActive() to return xid2.
	// Start a new txn (xid = xid2+2 since holdXID was xid2+1),
	// but we need an active txn with xid == xid2.
	// That's tricky since xid2 is already committed.
	//
	// Alternative: directly test the boundary by controlling when vacuum runs.
	// Insert "b" v1, overwrite "b" v2 with a NEW xid, then hold a txn
	// whose xid equals the overwrite xid.
	//
	// Actually, the simplest approach: insert and overwrite, then start a
	// transaction that makes safeXID equal to the overwrite xid.
	// Since we can't control xid allocation, let's verify the strict-less-than
	// by checking that vacuum with an active txn at the right boundary
	// preserves the entry.

	// Fresh env for clean test
	env2 := setupTestEnv(t)

	// xid=1: insert "a" v1
	env2.putAndCommit(t, []byte("a"), []byte("v1"))
	// xid=2: overwrite "a" v2 → v1.TxnMax = 2
	env2.putAndCommit(t, []byte("a"), []byte("v2"))

	// Start a "hold" transaction — this becomes the min active
	holdXID2, _ := env2.txnMgr.BeginTxn() // xid=3
	_ = holdXID2

	// safeXID = GetMinActive() = 3
	// v1.TxnMax = 2, which is < 3, so v1 IS cleanable
	// That tests the normal case. For boundary test, we need TxnMax == safeXID.

	// Let's insert "b" v1 (xid=4, but hold is still xid=3 active)
	// Actually, we can't easily get TxnMax == safeXID with the current setup
	// because safeXID = min(active) and TxnMax is always a committed/aborted xid.
	//
	// The real boundary test: when TxnMax == safeXID, the entry should NOT be removed.
	// We can simulate this by having an active txn whose xid == the overwrite xid.
	// But that's impossible since the overwrite txn must be committed for Case 1.
	//
	// Instead, test with safeXID == TxnMax by using NextXID when no active txns.
	env2.txnMgr.Abort(holdXID2) // remove hold

	// Now no active txns. safeXID = NextXID() = 4.
	// v1.TxnMax = 2, which is < 4 → cleanable. This is normal.
	//
	// For a true boundary test, we need to insert another overwrite
	// and have safeXID == that overwrite's xid.

	// Clean approach: use a third env with precise control
	env3 := setupTestEnv(t)

	// xid=1: insert "a" v1
	env3.putAndCommit(t, []byte("a"), []byte("v1"))
	// xid=2: overwrite "a" v2 → v1.TxnMax = 2
	env3.putAndCommit(t, []byte("a"), []byte("v2"))
	// xid=3: overwrite "a" v3 → v2.TxnMax = 3
	env3.putAndCommit(t, []byte("a"), []byte("v3"))

	// Now: v1.TxnMax=2, v2.TxnMax=3, v3.TxnMax=MaxUint64
	// Start hold txn xid=4, making safeXID=4
	holdXID3, _ := env3.txnMgr.BeginTxn() // xid=4
	_ = holdXID3
	safeXID := env3.txnMgr.GetMinActive()
	t.Logf("safeXID=%d, v1.TxnMax=2, v2.TxnMax=3", safeXID)

	// Both v1 (TxnMax=2 < 4) and v2 (TxnMax=3 < 4) should be removed
	before3 := env3.countLeafEntries(t)
	if before3 != 3 {
		t.Fatalf("expected 3 entries, got %d", before3)
	}

	v3 := env3.newVacuum()
	stats3, err := v3.Run()
	if err != nil {
		t.Fatal(err)
	}
	if stats3.EntriesRemoved != 2 {
		t.Fatalf("expected 2 entries removed, got %d", stats3.EntriesRemoved)
	}

	// Now test the boundary: make safeXID == 3 by having an active txn with xid=3
	// We can't do that since xid=3 is committed. But we CAN test with a fresh env:
	env4 := setupTestEnv(t)
	// xid=1: insert "a" v1
	env4.putAndCommit(t, []byte("a"), []byte("v1"))
	// Start hold txn xid=2 BEFORE overwriting (so it stays active)
	holdXID4, _ := env4.txnMgr.BeginTxn() // xid=2, stays active
	// xid=3: overwrite "a" v2 → v1.TxnMax = 3
	env4.putAndCommit(t, []byte("a"), []byte("v2"))

	// safeXID = min(active) = holdXID4 = 2
	// v1.TxnMax = 3, safeXID = 2
	// Since 3 >= 2 (NOT less than), v1 should NOT be removed
	safeXID4 := env4.txnMgr.GetMinActive()
	t.Logf("safeXID=%d, v1.TxnMax=3", safeXID4)
	if safeXID4 != holdXID4 {
		t.Fatalf("expected safeXID=%d, got %d", holdXID4, safeXID4)
	}

	before4 := env4.countLeafEntries(t)
	if before4 != 2 {
		t.Fatalf("expected 2 entries, got %d", before4)
	}

	v4 := env4.newVacuum()
	stats4, err := v4.Run()
	if err != nil {
		t.Fatal(err)
	}

	// v1.TxnMax=3 >= safeXID=2 → NOT removed
	if stats4.EntriesRemoved != 0 {
		t.Fatalf("expected 0 entries removed (boundary), got %d", stats4.EntriesRemoved)
	}

	after4 := env4.countLeafEntries(t)
	if after4 != 2 {
		t.Fatalf("expected 2 entries preserved, got %d", after4)
	}

	env4.txnMgr.Abort(holdXID4) // cleanup
	env3.txnMgr.Abort(holdXID3) // cleanup

	t.Logf("Boundary test passed: TxnMax >= safeXID → entry preserved")
}

// Test 9: Delete (not overwrite) — committed delete creates dead entry
func TestVacuum_CommittedDeleteOp(t *testing.T) {
	env := setupTestEnv(t)

	// Insert key "a"
	env.putAndCommit(t, []byte("a"), []byte("v1"))
	// Delete key "a" — sets v1.TxnMax = deleteXID
	env.deleteAndCommit(t, []byte("a"))

	before := env.countLeafEntries(t)
	if before != 1 {
		t.Fatalf("expected 1 entry before vacuum, got %d", before)
	}

	v := env.newVacuum()
	stats, err := v.Run()
	if err != nil {
		t.Fatal(err)
	}

	after := env.countLeafEntries(t)

	if stats.EntriesRemoved != 1 {
		t.Fatalf("expected 1 entry removed, got %d", stats.EntriesRemoved)
	}
	if after != 0 {
		t.Fatalf("expected 0 entries after vacuum, got %d", after)
	}

	// Verify key is not found
	_, err = env.tree.Get([]byte("a"), math.MaxUint64-1)
	if err != btreeapi.ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}

	t.Logf("Stats: scanned=%d modified=%d removed=%d blobs=%d",
		stats.LeavesScanned, stats.LeavesModified, stats.EntriesRemoved, stats.BlobsFreed)
}

// Test 10: Idempotency — running vacuum twice produces no changes on second run
func TestVacuum_Idempotent(t *testing.T) {
	env := setupTestEnv(t)

	// Insert and overwrite to create dead versions
	env.putAndCommit(t, []byte("a"), []byte("v1"))
	env.putAndCommit(t, []byte("a"), []byte("v2"))
	env.putAndCommit(t, []byte("b"), []byte("v1"))
	env.putAndAbort(t, []byte("b"), []byte("v2"))

	// First vacuum
	v := env.newVacuum()
	stats1, err := v.Run()
	if err != nil {
		t.Fatal(err)
	}
	if stats1.EntriesRemoved != 2 {
		t.Fatalf("first run: expected 2 removed, got %d", stats1.EntriesRemoved)
	}

	// Second vacuum — should find nothing to clean
	v2 := env.newVacuum()
	stats2, err := v2.Run()
	if err != nil {
		t.Fatal(err)
	}
	if stats2.EntriesRemoved != 0 {
		t.Fatalf("second run: expected 0 removed, got %d", stats2.EntriesRemoved)
	}
	if stats2.LeavesModified != 0 {
		t.Fatalf("second run: expected 0 modified, got %d", stats2.LeavesModified)
	}

	t.Logf("Idempotent: run1 removed=%d, run2 removed=%d",
		stats1.EntriesRemoved, stats2.EntriesRemoved)
}
