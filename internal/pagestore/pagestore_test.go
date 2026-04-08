package pagestore

import (
	"bytes"
	"testing"

	pagestoreapi "github.com/akzj/go-fast-kv/internal/pagestore/api"
	"github.com/akzj/go-fast-kv/internal/segment"
	segmentapi "github.com/akzj/go-fast-kv/internal/segment/api"
)

// newTestSegMgr creates a real SegmentManager in a temp directory.
func newTestSegMgr(t *testing.T) segmentapi.SegmentManager {
	t.Helper()
	dir := t.TempDir()
	mgr, err := segment.New(segmentapi.Config{Dir: dir})
	if err != nil {
		t.Fatalf("failed to create segment manager: %v", err)
	}
	t.Cleanup(func() { mgr.Close() })
	return mgr
}

// newTestPageStore creates a PageStore backed by a real SegmentManager.
func newTestPageStore(t *testing.T) *pageStore {
	t.Helper()
	segMgr := newTestSegMgr(t)
	ps := New(pagestoreapi.Config{}, segMgr)
	return ps.(*pageStore)
}

// makePage creates a 4096-byte page filled with the given byte value.
func makePage(fill byte) []byte {
	data := make([]byte, pagestoreapi.PageSize)
	for i := range data {
		data[i] = fill
	}
	return data
}

func TestAllocIncrementing(t *testing.T) {
	ps := newTestPageStore(t)

	id1 := ps.Alloc()
	id2 := ps.Alloc()
	id3 := ps.Alloc()

	if id1 != 1 {
		t.Errorf("first Alloc: got %d, want 1", id1)
	}
	if id2 != 2 {
		t.Errorf("second Alloc: got %d, want 2", id2)
	}
	if id3 != 3 {
		t.Errorf("third Alloc: got %d, want 3", id3)
	}
}

func TestWriteAndRead(t *testing.T) {
	ps := newTestPageStore(t)

	pid := ps.Alloc()
	data := makePage(0xAB)

	_, err := ps.Write(pid, data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	got, err := ps.Read(pid)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if !bytes.Equal(got, data) {
		t.Errorf("Read data mismatch: got[0]=%x, want[0]=%x", got[0], data[0])
	}
}

func TestWriteOverwrite(t *testing.T) {
	ps := newTestPageStore(t)

	pid := ps.Alloc()

	data1 := makePage(0x11)
	_, err := ps.Write(pid, data1)
	if err != nil {
		t.Fatalf("Write 1 failed: %v", err)
	}

	data2 := makePage(0x22)
	_, err = ps.Write(pid, data2)
	if err != nil {
		t.Fatalf("Write 2 failed: %v", err)
	}

	got, err := ps.Read(pid)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if !bytes.Equal(got, data2) {
		t.Errorf("Read after overwrite: got[0]=%x, want[0]=%x", got[0], data2[0])
	}
}

func TestReadUnallocated(t *testing.T) {
	ps := newTestPageStore(t)

	_, err := ps.Read(999)
	if err != pagestoreapi.ErrPageNotFound {
		t.Errorf("Read unallocated: got %v, want ErrPageNotFound", err)
	}
}

func TestFreeAndRead(t *testing.T) {
	ps := newTestPageStore(t)

	pid := ps.Alloc()
	data := makePage(0xCC)
	_, err := ps.Write(pid, data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	ps.Free(pid)

	_, err = ps.Read(pid)
	if err != pagestoreapi.ErrPageNotFound {
		t.Errorf("Read after Free: got %v, want ErrPageNotFound", err)
	}
}

func TestWriteInvalidSize(t *testing.T) {
	ps := newTestPageStore(t)

	pid := ps.Alloc()

	// Too short
	_, err := ps.Write(pid, make([]byte, 100))
	if err != pagestoreapi.ErrInvalidPageSize {
		t.Errorf("Write short data: got %v, want ErrInvalidPageSize", err)
	}

	// Too long
	_, err = ps.Write(pid, make([]byte, 5000))
	if err != pagestoreapi.ErrInvalidPageSize {
		t.Errorf("Write long data: got %v, want ErrInvalidPageSize", err)
	}
}

func TestWALEntryValues(t *testing.T) {
	ps := newTestPageStore(t)

	pid := ps.Alloc()
	data := makePage(0xDD)

	entry, err := ps.Write(pid, data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if entry.Type != 1 {
		t.Errorf("WALEntry.Type: got %d, want 1 (RecordPageMap)", entry.Type)
	}
	if entry.ID != pid {
		t.Errorf("WALEntry.ID: got %d, want %d", entry.ID, pid)
	}
	if entry.VAddr == 0 {
		t.Error("WALEntry.VAddr should not be 0")
	}
	if entry.Size != 0 {
		t.Errorf("WALEntry.Size: got %d, want 0", entry.Size)
	}

	// Free entry
	freeEntry := ps.Free(pid)
	if freeEntry.Type != 4 {
		t.Errorf("Free WALEntry.Type: got %d, want 4 (RecordPageFree)", freeEntry.Type)
	}
	if freeEntry.ID != pid {
		t.Errorf("Free WALEntry.ID: got %d, want %d", freeEntry.ID, pid)
	}
	if freeEntry.VAddr != 0 {
		t.Errorf("Free WALEntry.VAddr: got %d, want 0", freeEntry.VAddr)
	}
}

func TestRecoveryLoadMapping(t *testing.T) {
	segMgr := newTestSegMgr(t)
	ps := New(pagestoreapi.Config{}, segMgr).(*pageStore)

	// Write a page to get a real VAddr
	pid := ps.Alloc()
	data := makePage(0xEE)
	walEntry, err := ps.Write(pid, data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Create a new PageStore and recover via LoadMapping
	ps2 := New(pagestoreapi.Config{}, segMgr).(*pageStore)
	recovery := pagestoreapi.PageStoreRecovery(ps2)
	recovery.LoadMapping([]pagestoreapi.MappingEntry{
		{PageID: pid, VAddr: walEntry.VAddr},
	})
	recovery.SetNextPageID(pid + 1)

	got, err := ps2.Read(pid)
	if err != nil {
		t.Fatalf("Read after LoadMapping failed: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("Data mismatch after LoadMapping: got[0]=%x, want[0]=%x", got[0], data[0])
	}
}

func TestRecoveryApplyPageMap(t *testing.T) {
	segMgr := newTestSegMgr(t)
	ps := New(pagestoreapi.Config{}, segMgr).(*pageStore)

	// Write a page
	pid := ps.Alloc()
	data := makePage(0xFF)
	walEntry, err := ps.Write(pid, data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// New PageStore, apply via ApplyPageMap
	ps2 := New(pagestoreapi.Config{}, segMgr).(*pageStore)
	recovery := pagestoreapi.PageStoreRecovery(ps2)
	recovery.ApplyPageMap(pid, walEntry.VAddr)
	recovery.SetNextPageID(pid + 1)

	got, err := ps2.Read(pid)
	if err != nil {
		t.Fatalf("Read after ApplyPageMap failed: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("Data mismatch after ApplyPageMap: got[0]=%x, want[0]=%x", got[0], data[0])
	}
}

func TestRecoveryApplyPageFree(t *testing.T) {
	segMgr := newTestSegMgr(t)
	ps := New(pagestoreapi.Config{}, segMgr).(*pageStore)

	// Write a page
	pid := ps.Alloc()
	data := makePage(0xAA)
	walEntry, err := ps.Write(pid, data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// New PageStore, load then free
	ps2 := New(pagestoreapi.Config{}, segMgr).(*pageStore)
	recovery := pagestoreapi.PageStoreRecovery(ps2)
	recovery.ApplyPageMap(pid, walEntry.VAddr)
	recovery.ApplyPageFree(pid)

	_, err = ps2.Read(pid)
	if err != pagestoreapi.ErrPageNotFound {
		t.Errorf("Read after ApplyPageFree: got %v, want ErrPageNotFound", err)
	}
}

func TestCloseReturnsErrClosed(t *testing.T) {
	ps := newTestPageStore(t)

	if err := ps.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	_, err := ps.Write(1, makePage(0x00))
	if err != pagestoreapi.ErrClosed {
		t.Errorf("Write after Close: got %v, want ErrClosed", err)
	}

	_, err = ps.Read(1)
	if err != pagestoreapi.ErrClosed {
		t.Errorf("Read after Close: got %v, want ErrClosed", err)
	}
}

func TestNextPageID(t *testing.T) {
	ps := newTestPageStore(t)

	if ps.NextPageID() != 1 {
		t.Errorf("initial NextPageID: got %d, want 1", ps.NextPageID())
	}

	ps.Alloc()
	if ps.NextPageID() != 2 {
		t.Errorf("after 1 Alloc: got %d, want 2", ps.NextPageID())
	}

	ps.Alloc()
	ps.Alloc()
	if ps.NextPageID() != 4 {
		t.Errorf("after 3 Allocs: got %d, want 4", ps.NextPageID())
	}
}

func TestMultiplePagesReadWrite(t *testing.T) {
	ps := newTestPageStore(t)

	// Write 50 pages with different content
	pages := make(map[pagestoreapi.PageID][]byte)
	for i := 0; i < 50; i++ {
		pid := ps.Alloc()
		data := makePage(byte(i))
		_, err := ps.Write(pid, data)
		if err != nil {
			t.Fatalf("Write page %d failed: %v", pid, err)
		}
		pages[pid] = data
	}

	// Read back all pages and verify
	for pid, expected := range pages {
		got, err := ps.Read(pid)
		if err != nil {
			t.Fatalf("Read page %d failed: %v", pid, err)
		}
		if !bytes.Equal(got, expected) {
			t.Errorf("Page %d data mismatch: got[0]=%x, want[0]=%x", pid, got[0], expected[0])
		}
	}
}
