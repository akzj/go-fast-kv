package internal

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	blobstoreapi "github.com/akzj/go-fast-kv/internal/blobstore/api"
	gcapi "github.com/akzj/go-fast-kv/internal/gc/api"
	pagestoreapi "github.com/akzj/go-fast-kv/internal/pagestore/api"
	segmentapi "github.com/akzj/go-fast-kv/internal/segment/api"
	walapi "github.com/akzj/go-fast-kv/internal/wal/api"

	"github.com/akzj/go-fast-kv/internal/blobstore"
	"github.com/akzj/go-fast-kv/internal/pagestore"
	"github.com/akzj/go-fast-kv/internal/segment"
	"github.com/akzj/go-fast-kv/internal/wal"
)

// ─── Helpers ────────────────────────────────────────────────────────

func newSegMgr(t *testing.T, subdir string) segmentapi.SegmentManager {
	t.Helper()
	dir := filepath.Join(t.TempDir(), subdir)
	mgr, err := segment.New(segmentapi.Config{Dir: dir})
	if err != nil {
		t.Fatalf("segment.New(%s): %v", subdir, err)
	}
	t.Cleanup(func() { mgr.Close() })
	return mgr
}

func newWAL(t *testing.T) walapi.WAL {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "wal")
	w, err := wal.New(walapi.Config{Dir: dir})
	if err != nil {
		t.Fatalf("wal.New: %v", err)
	}
	t.Cleanup(func() { w.Close() })
	return w
}

// makePage creates a 4096-byte page filled with the given byte value.
func makePage(fill byte) []byte {
	data := make([]byte, pagestoreapi.PageSize)
	for i := range data {
		data[i] = fill
	}
	return data
}

// writePageViaStore writes a page through PageStore and writes the WAL batch.
// Returns the WAL entry for reference.
func writePageViaStore(t *testing.T, ps pagestoreapi.PageStore, w walapi.WAL, segMgr segmentapi.SegmentManager, pageID uint64, data []byte) {
	t.Helper()
	entry, err := ps.Write(pageID, data)
	if err != nil {
		t.Fatalf("PageStore.Write(%d): %v", pageID, err)
	}
	if err := segMgr.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	batch := walapi.NewBatch()
	batch.Add(walapi.ModuleTree, walapi.RecordType(entry.Type), entry.ID, entry.VAddr, entry.Size)
	if _, err := w.WriteBatch(batch); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
}

// writeBlobViaStore writes a blob through BlobStore and writes the WAL batch.
func writeBlobViaStore(t *testing.T, bs blobstoreapi.BlobStore, w walapi.WAL, segMgr segmentapi.SegmentManager, data []byte) uint64 {
	t.Helper()
	blobID, entry, err := bs.Write(data)
	if err != nil {
		t.Fatalf("BlobStore.Write: %v", err)
	}
	if err := segMgr.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	batch := walapi.NewBatch()
	batch.Add(walapi.ModuleTree, walapi.RecordType(entry.Type), entry.ID, entry.VAddr, entry.Size)
	if _, err := w.WriteBatch(batch); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	return blobID
}

// ═══════════════════════════════════════════════════════════════════
// PAGE GC TESTS
// ═══════════════════════════════════════════════════════════════════

// ─── Test 1: No sealed segments → ErrNoSegmentsToGC ─────────────

func TestPageGC_NoSealedSegments(t *testing.T) {
	segMgr := newSegMgr(t, "pages")
	w := newWAL(t)
	ps := pagestore.New(pagestoreapi.Config{}, segMgr, newMockLSM())
	recovery := ps.(pagestoreapi.PageStoreRecovery)

	gc := NewPageGC(segMgr, ps, recovery, w)
	_, err := gc.CollectOne()
	if !errors.Is(err, gcapi.ErrNoSegmentsToGC) {
		t.Fatalf("expected ErrNoSegmentsToGC, got %v", err)
	}
}

// ─── Test 2: All pages dead (overwritten) → all skipped ─────────

func TestPageGC_AllDead(t *testing.T) {
	segMgr := newSegMgr(t, "pages")
	w := newWAL(t)
	ps := pagestore.New(pagestoreapi.Config{}, segMgr, newMockLSM())
	recovery := ps.(pagestoreapi.PageStoreRecovery)

	// Write 3 pages into segment 1.
	id1 := ps.Alloc()
	id2 := ps.Alloc()
	id3 := ps.Alloc()
	writePageViaStore(t, ps, w, segMgr, id1, makePage(0x01))
	writePageViaStore(t, ps, w, segMgr, id2, makePage(0x02))
	writePageViaStore(t, ps, w, segMgr, id3, makePage(0x03))

	// Rotate → segment 1 is now sealed.
	if err := segMgr.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}

	// Overwrite all 3 pages → new data goes to segment 2 (active).
	// The old records in segment 1 are now dead.
	writePageViaStore(t, ps, w, segMgr, id1, makePage(0x11))
	writePageViaStore(t, ps, w, segMgr, id2, makePage(0x12))
	writePageViaStore(t, ps, w, segMgr, id3, makePage(0x13))

	gc := NewPageGC(segMgr, ps, recovery, w)
	stats, err := gc.CollectOne()
	if err != nil {
		t.Fatalf("CollectOne: %v", err)
	}

	if stats.TotalRecords != 3 {
		t.Errorf("TotalRecords: got %d, want 3", stats.TotalRecords)
	}
	if stats.LiveRecords != 0 {
		t.Errorf("LiveRecords: got %d, want 0", stats.LiveRecords)
	}
	if stats.DeadRecords != 3 {
		t.Errorf("DeadRecords: got %d, want 3", stats.DeadRecords)
	}
	if stats.BytesFreed != 3*pagestoreapi.PageRecordSize {
		t.Errorf("BytesFreed: got %d, want %d", stats.BytesFreed, 3*pagestoreapi.PageRecordSize)
	}

	// Sealed segments should be empty now.
	if segs := segMgr.SealedSegments(); len(segs) != 0 {
		t.Errorf("sealed segments after GC: %v, want empty", segs)
	}

	// Pages should still be readable (from segment 2).
	data, err := ps.Read(id1)
	if err != nil {
		t.Fatalf("Read(%d) after GC: %v", id1, err)
	}
	if !bytes.Equal(data, makePage(0x11)) {
		t.Error("data mismatch after GC for page 1")
	}
}

// ─── Test 3: All pages live → all copied ────────────────────────

func TestPageGC_AllLive(t *testing.T) {
	segMgr := newSegMgr(t, "pages")
	w := newWAL(t)
	ps := pagestore.New(pagestoreapi.Config{}, segMgr, newMockLSM())
	recovery := ps.(pagestoreapi.PageStoreRecovery)

	// Write 3 pages into segment 1.
	id1 := ps.Alloc()
	id2 := ps.Alloc()
	id3 := ps.Alloc()
	writePageViaStore(t, ps, w, segMgr, id1, makePage(0xAA))
	writePageViaStore(t, ps, w, segMgr, id2, makePage(0xBB))
	writePageViaStore(t, ps, w, segMgr, id3, makePage(0xCC))

	// Rotate → segment 1 is sealed. No overwrites — all pages are live.
	if err := segMgr.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}

	gc := NewPageGC(segMgr, ps, recovery, w)
	stats, err := gc.CollectOne()
	if err != nil {
		t.Fatalf("CollectOne: %v", err)
	}

	if stats.TotalRecords != 3 {
		t.Errorf("TotalRecords: got %d, want 3", stats.TotalRecords)
	}
	if stats.LiveRecords != 3 {
		t.Errorf("LiveRecords: got %d, want 3", stats.LiveRecords)
	}
	if stats.DeadRecords != 0 {
		t.Errorf("DeadRecords: got %d, want 0", stats.DeadRecords)
	}

	// Old segment should be removed.
	if segs := segMgr.SealedSegments(); len(segs) != 0 {
		t.Errorf("sealed segments after GC: %v, want empty", segs)
	}

	// All pages should still be readable (now from the new active segment).
	for _, tc := range []struct {
		id   uint64
		fill byte
	}{
		{id1, 0xAA}, {id2, 0xBB}, {id3, 0xCC},
	} {
		data, err := ps.Read(tc.id)
		if err != nil {
			t.Fatalf("Read(%d) after GC: %v", tc.id, err)
		}
		if !bytes.Equal(data, makePage(tc.fill)) {
			t.Errorf("data mismatch for page %d after GC", tc.id)
		}
	}
}

// ─── Test 4: Mixed liveness → some copied, some skipped ─────────

func TestPageGC_MixedLiveness(t *testing.T) {
	segMgr := newSegMgr(t, "pages")
	w := newWAL(t)
	ps := pagestore.New(pagestoreapi.Config{}, segMgr, newMockLSM())
	recovery := ps.(pagestoreapi.PageStoreRecovery)

	// Write 5 pages into segment 1.
	ids := make([]uint64, 5)
	for i := range ids {
		ids[i] = ps.Alloc()
		writePageViaStore(t, ps, w, segMgr, ids[i], makePage(byte(i+1)))
	}

	// Rotate → segment 1 is sealed.
	if err := segMgr.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}

	// Overwrite pages 0, 2, 4 (making them dead in segment 1).
	// Pages 1, 3 remain live in segment 1.
	writePageViaStore(t, ps, w, segMgr, ids[0], makePage(0xF0))
	writePageViaStore(t, ps, w, segMgr, ids[2], makePage(0xF2))
	writePageViaStore(t, ps, w, segMgr, ids[4], makePage(0xF4))

	gc := NewPageGC(segMgr, ps, recovery, w)
	stats, err := gc.CollectOne()
	if err != nil {
		t.Fatalf("CollectOne: %v", err)
	}

	if stats.TotalRecords != 5 {
		t.Errorf("TotalRecords: got %d, want 5", stats.TotalRecords)
	}
	if stats.LiveRecords != 2 {
		t.Errorf("LiveRecords: got %d, want 2", stats.LiveRecords)
	}
	if stats.DeadRecords != 3 {
		t.Errorf("DeadRecords: got %d, want 3", stats.DeadRecords)
	}

	// Verify all pages are still readable with correct data.
	expected := []byte{0xF0, 0x02, 0xF2, 0x04, 0xF4}
	for i, id := range ids {
		data, err := ps.Read(id)
		if err != nil {
			t.Fatalf("Read(%d) after GC: %v", id, err)
		}
		if !bytes.Equal(data, makePage(expected[i])) {
			t.Errorf("page %d: data mismatch after GC", id)
		}
	}
}

// ─── Test 5: Page written multiple times in same segment ────────

func TestPageGC_DuplicatePageInSegment(t *testing.T) {
	segMgr := newSegMgr(t, "pages")
	w := newWAL(t)
	ps := pagestore.New(pagestoreapi.Config{}, segMgr, newMockLSM())
	recovery := ps.(pagestoreapi.PageStoreRecovery)

	// Write page 1 three times in segment 1. Only the last write is live.
	id := ps.Alloc()
	writePageViaStore(t, ps, w, segMgr, id, makePage(0x01))
	writePageViaStore(t, ps, w, segMgr, id, makePage(0x02))
	writePageViaStore(t, ps, w, segMgr, id, makePage(0x03)) // this one is live

	if err := segMgr.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}

	gc := NewPageGC(segMgr, ps, recovery, w)
	stats, err := gc.CollectOne()
	if err != nil {
		t.Fatalf("CollectOne: %v", err)
	}

	if stats.TotalRecords != 3 {
		t.Errorf("TotalRecords: got %d, want 3", stats.TotalRecords)
	}
	if stats.LiveRecords != 1 {
		t.Errorf("LiveRecords: got %d, want 1", stats.LiveRecords)
	}
	if stats.DeadRecords != 2 {
		t.Errorf("DeadRecords: got %d, want 2", stats.DeadRecords)
	}

	// The page should still be readable with the latest data.
	data, err := ps.Read(id)
	if err != nil {
		t.Fatalf("Read after GC: %v", err)
	}
	if !bytes.Equal(data, makePage(0x03)) {
		t.Error("data mismatch: expected last write's data")
	}
}

// ═══════════════════════════════════════════════════════════════════
// BLOB GC TESTS
// ═══════════════════════════════════════════════════════════════════

// ─── Test 6: No sealed segments → ErrNoSegmentsToGC ─────────────

func TestBlobGC_NoSealedSegments(t *testing.T) {
	segMgr := newSegMgr(t, "blobs")
	w := newWAL(t)
	bs := blobstore.New(blobstoreapi.Config{}, segMgr)
	recovery := bs.(blobstoreapi.BlobStoreRecovery)

	gc := NewBlobGC(segMgr, bs, recovery, w)
	_, err := gc.CollectOne()
	if !errors.Is(err, gcapi.ErrNoSegmentsToGC) {
		t.Fatalf("expected ErrNoSegmentsToGC, got %v", err)
	}
}

// ─── Test 7: All blobs dead → all skipped ───────────────────────

func TestBlobGC_AllDead(t *testing.T) {
	segMgr := newSegMgr(t, "blobs")
	w := newWAL(t)
	bs := blobstore.New(blobstoreapi.Config{}, segMgr)
	recovery := bs.(blobstoreapi.BlobStoreRecovery)

	// Write 3 blobs into segment 1.
	bid1 := writeBlobViaStore(t, bs, w, segMgr, []byte("blob-one"))
	bid2 := writeBlobViaStore(t, bs, w, segMgr, []byte("blob-two"))
	bid3 := writeBlobViaStore(t, bs, w, segMgr, []byte("blob-three"))

	// Rotate → segment 1 is sealed.
	if err := segMgr.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}

	// Delete all blobs → they become dead in segment 1.
	bs.Delete(bid1)
	bs.Delete(bid2)
	bs.Delete(bid3)

	gc := NewBlobGC(segMgr, bs, recovery, w)
	stats, err := gc.CollectOne()
	if err != nil {
		t.Fatalf("CollectOne: %v", err)
	}

	if stats.TotalRecords != 3 {
		t.Errorf("TotalRecords: got %d, want 3", stats.TotalRecords)
	}
	if stats.LiveRecords != 0 {
		t.Errorf("LiveRecords: got %d, want 0", stats.LiveRecords)
	}
	if stats.DeadRecords != 3 {
		t.Errorf("DeadRecords: got %d, want 3", stats.DeadRecords)
	}

	// Sealed segments should be empty.
	if segs := segMgr.SealedSegments(); len(segs) != 0 {
		t.Errorf("sealed segments after GC: %v, want empty", segs)
	}
}

// ─── Test 8: All blobs live → all copied ────────────────────────

func TestBlobGC_AllLive(t *testing.T) {
	segMgr := newSegMgr(t, "blobs")
	w := newWAL(t)
	bs := blobstore.New(blobstoreapi.Config{}, segMgr)
	recovery := bs.(blobstoreapi.BlobStoreRecovery)

	// Write 3 blobs into segment 1.
	bid1 := writeBlobViaStore(t, bs, w, segMgr, []byte("alpha"))
	bid2 := writeBlobViaStore(t, bs, w, segMgr, []byte("beta-longer-blob"))
	bid3 := writeBlobViaStore(t, bs, w, segMgr, []byte("g"))

	// Rotate → segment 1 is sealed. No deletes — all live.
	if err := segMgr.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}

	gc := NewBlobGC(segMgr, bs, recovery, w)
	stats, err := gc.CollectOne()
	if err != nil {
		t.Fatalf("CollectOne: %v", err)
	}

	if stats.TotalRecords != 3 {
		t.Errorf("TotalRecords: got %d, want 3", stats.TotalRecords)
	}
	if stats.LiveRecords != 3 {
		t.Errorf("LiveRecords: got %d, want 3", stats.LiveRecords)
	}
	if stats.DeadRecords != 0 {
		t.Errorf("DeadRecords: got %d, want 0", stats.DeadRecords)
	}

	// All blobs should still be readable.
	for _, tc := range []struct {
		id   uint64
		data string
	}{
		{bid1, "alpha"}, {bid2, "beta-longer-blob"}, {bid3, "g"},
	} {
		got, err := bs.Read(tc.id)
		if err != nil {
			t.Fatalf("Read(%d) after GC: %v", tc.id, err)
		}
		if !bytes.Equal(got, []byte(tc.data)) {
			t.Errorf("blob %d: got %q, want %q", tc.id, got, tc.data)
		}
	}
}

// ─── Test 9: Mixed blob liveness ────────────────────────────────

func TestBlobGC_MixedLiveness(t *testing.T) {
	segMgr := newSegMgr(t, "blobs")
	w := newWAL(t)
	bs := blobstore.New(blobstoreapi.Config{}, segMgr)
	recovery := bs.(blobstoreapi.BlobStoreRecovery)

	// Write 4 blobs.
	bid1 := writeBlobViaStore(t, bs, w, segMgr, []byte("keep-me"))
	bid2 := writeBlobViaStore(t, bs, w, segMgr, []byte("delete-me"))
	bid3 := writeBlobViaStore(t, bs, w, segMgr, []byte("keep-me-too"))
	bid4 := writeBlobViaStore(t, bs, w, segMgr, []byte("also-delete"))

	// Rotate.
	if err := segMgr.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}

	// Delete blobs 2 and 4.
	bs.Delete(bid2)
	bs.Delete(bid4)

	gc := NewBlobGC(segMgr, bs, recovery, w)
	stats, err := gc.CollectOne()
	if err != nil {
		t.Fatalf("CollectOne: %v", err)
	}

	if stats.TotalRecords != 4 {
		t.Errorf("TotalRecords: got %d, want 4", stats.TotalRecords)
	}
	if stats.LiveRecords != 2 {
		t.Errorf("LiveRecords: got %d, want 2", stats.LiveRecords)
	}
	if stats.DeadRecords != 2 {
		t.Errorf("DeadRecords: got %d, want 2", stats.DeadRecords)
	}

	// Live blobs should still be readable.
	got1, err := bs.Read(bid1)
	if err != nil {
		t.Fatalf("Read(%d): %v", bid1, err)
	}
	if !bytes.Equal(got1, []byte("keep-me")) {
		t.Errorf("blob %d: got %q, want %q", bid1, got1, "keep-me")
	}

	got3, err := bs.Read(bid3)
	if err != nil {
		t.Fatalf("Read(%d): %v", bid3, err)
	}
	if !bytes.Equal(got3, []byte("keep-me-too")) {
		t.Errorf("blob %d: got %q, want %q", bid3, got3, "keep-me-too")
	}
}

// ═══════════════════════════════════════════════════════════════════
// CROSS-CUTTING TESTS
// ═══════════════════════════════════════════════════════════════════

// ─── Test 10: Multiple sealed segments — collect one at a time ──


// mockLSMForTests is a simple in-memory LSM MappingStore for tests.
type mockLSMForTests struct {
	pages map[uint64]uint64
	blobs map[uint64]struct{ vaddr uint64; size uint32 }
}

func newMockLSM() *mockLSMForTests {
	return &mockLSMForTests{pages: make(map[uint64]uint64), blobs: make(map[uint64]struct{ vaddr uint64; size uint32 })}
}

func (m *mockLSMForTests) SetPageMapping(pageID uint64, vaddr uint64) {
	m.pages[pageID] = vaddr
}
func (m *mockLSMForTests) GetPageMapping(pageID uint64) (uint64, bool) {
	v, ok := m.pages[pageID]
	return v, ok
}
func (m *mockLSMForTests) SetBlobMapping(blobID uint64, vaddr uint64, size uint32) {
	m.blobs[blobID] = struct{ vaddr uint64; size uint32 }{vaddr, size}
}
func (m *mockLSMForTests) GetBlobMapping(blobID uint64) (uint64, uint32, bool) {
	b, ok := m.blobs[blobID]
	return b.vaddr, b.size, ok
}
func (m *mockLSMForTests) DeleteBlobMapping(blobID uint64) { delete(m.blobs, blobID) }
func (m *mockLSMForTests) SetWAL(wal walapi.WAL) {}
func (m *mockLSMForTests) FlushToWAL() (uint64, error)    { return 0, nil }
func (m *mockLSMForTests) LastLSN() uint64                { return 0 }
func (m *mockLSMForTests) Checkpoint(lsn uint64) error    { return nil }
func (m *mockLSMForTests) CheckpointLSN() uint64          { return 0 }
func (m *mockLSMForTests) MaybeCompact() error            { return nil }
func (m *mockLSMForTests) Close() error                  { return nil }

// LSMLifecycle methods required by pagestoreapi.LSMLifecycle.
func (m *mockLSMForTests) ApplyPageMapping(pageID uint64, vaddr uint64) {
	m.pages[pageID] = vaddr
}
func (m *mockLSMForTests) ApplyPageDelete(pageID uint64) {
	delete(m.pages, pageID)
}
func (m *mockLSMForTests) ApplyBlobMapping(blobID uint64, vaddr uint64, size uint32) {
	m.blobs[blobID] = struct{ vaddr uint64; size uint32 }{vaddr, size}
}
func (m *mockLSMForTests) ApplyBlobDelete(blobID uint64) {
	delete(m.blobs, blobID)
}
func (m *mockLSMForTests) SetCheckpointLSN(lsn uint64) {}
func (m *mockLSMForTests) DrainCollector() []walapi.Record { return nil }


func TestGC_MultipleSegments(t *testing.T) {
	segMgr := newSegMgr(t, "pages")
	w := newWAL(t)
	ps := pagestore.New(pagestoreapi.Config{}, segMgr, newMockLSM())
	recovery := ps.(pagestoreapi.PageStoreRecovery)

	// Segment 1: write 2 pages.
	id1 := ps.Alloc()
	id2 := ps.Alloc()
	writePageViaStore(t, ps, w, segMgr, id1, makePage(0x01))
	writePageViaStore(t, ps, w, segMgr, id2, makePage(0x02))
	if err := segMgr.Rotate(); err != nil {
		t.Fatalf("Rotate 1: %v", err)
	}

	// Segment 2: write 2 more pages.
	id3 := ps.Alloc()
	id4 := ps.Alloc()
	writePageViaStore(t, ps, w, segMgr, id3, makePage(0x03))
	writePageViaStore(t, ps, w, segMgr, id4, makePage(0x04))
	if err := segMgr.Rotate(); err != nil {
		t.Fatalf("Rotate 2: %v", err)
	}

	// We should have 2 sealed segments.
	sealed := segMgr.SealedSegments()
	if len(sealed) != 2 {
		t.Fatalf("expected 2 sealed segments, got %d: %v", len(sealed), sealed)
	}

	gc := NewPageGC(segMgr, ps, recovery, w)

	// Collect first segment.
	stats1, err := gc.CollectOne()
	if err != nil {
		t.Fatalf("CollectOne 1: %v", err)
	}
	if stats1.TotalRecords != 2 || stats1.LiveRecords != 2 {
		t.Errorf("first GC: total=%d live=%d, want 2/2", stats1.TotalRecords, stats1.LiveRecords)
	}

	// Should have 1 sealed segment remaining.
	sealed = segMgr.SealedSegments()
	if len(sealed) != 1 {
		t.Fatalf("expected 1 sealed segment after first GC, got %d", len(sealed))
	}

	// Collect second segment.
	stats2, err := gc.CollectOne()
	if err != nil {
		t.Fatalf("CollectOne 2: %v", err)
	}
	if stats2.TotalRecords != 2 || stats2.LiveRecords != 2 {
		t.Errorf("second GC: total=%d live=%d, want 2/2", stats2.TotalRecords, stats2.LiveRecords)
	}

	// No more sealed segments.
	sealed = segMgr.SealedSegments()
	if len(sealed) != 0 {
		t.Fatalf("expected 0 sealed segments after both GCs, got %d", len(sealed))
	}

	// Third attempt → no segments to GC.
	_, err = gc.CollectOne()
	if !errors.Is(err, gcapi.ErrNoSegmentsToGC) {
		t.Fatalf("expected ErrNoSegmentsToGC, got %v", err)
	}

	// All 4 pages should still be readable.
	for _, tc := range []struct {
		id   uint64
		fill byte
	}{
		{id1, 0x01}, {id2, 0x02}, {id3, 0x03}, {id4, 0x04},
	} {
		data, err := ps.Read(tc.id)
		if err != nil {
			t.Fatalf("Read(%d): %v", tc.id, err)
		}
		if !bytes.Equal(data, makePage(tc.fill)) {
			t.Errorf("page %d: data mismatch", tc.id)
		}
	}
}
