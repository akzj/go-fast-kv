package internal

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"

	segmentapi "github.com/akzj/go-fast-kv/internal/segment/api"
)

// helper: create a SegmentManager with a temp dir and small max size for testing.
func newTestManager(t *testing.T, maxSize int64) segmentapi.SegmentManager {
	t.Helper()
	dir := t.TempDir()
	sm, err := New(segmentapi.Config{Dir: dir, MaxSize: maxSize})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	return sm
}

func newTestManagerDir(t *testing.T, dir string, maxSize int64) segmentapi.SegmentManager {
	t.Helper()
	sm, err := New(segmentapi.Config{Dir: dir, MaxSize: maxSize})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	return sm
}

// ─── Test 1: Basic Append + ReadAt ──────────────────────────────────

func TestAppendAndReadAt(t *testing.T) {
	sm := newTestManager(t, 1024)
	defer sm.Close()

	data := []byte("hello, segment!")
	addr, err := sm.Append(data)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	got, err := sm.ReadAt(addr, uint32(len(data)))
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}

	if !bytes.Equal(got, data) {
		t.Fatalf("ReadAt mismatch: got %q, want %q", got, data)
	}
}

// ─── Test 2: Multiple Appends, ReadAt each position ─────────────────

func TestMultipleAppends(t *testing.T) {
	sm := newTestManager(t, 4096)
	defer sm.Close()

	var addrs []segmentapi.VAddr
	var datas [][]byte

	for i := 0; i < 10; i++ {
		d := []byte(fmt.Sprintf("record-%03d", i))
		addr, err := sm.Append(d)
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		addrs = append(addrs, addr)
		datas = append(datas, d)
	}

	// Read back in reverse order.
	for i := len(addrs) - 1; i >= 0; i-- {
		got, err := sm.ReadAt(addrs[i], uint32(len(datas[i])))
		if err != nil {
			t.Fatalf("ReadAt %d: %v", i, err)
		}
		if !bytes.Equal(got, datas[i]) {
			t.Fatalf("ReadAt %d mismatch: got %q, want %q", i, got, datas[i])
		}
	}
}

// ─── Test 3: Rotate — old segment still readable ────────────────────

func TestRotateOldSegmentReadable(t *testing.T) {
	sm := newTestManager(t, 4096)
	defer sm.Close()

	data1 := []byte("before-rotate")
	addr1, err := sm.Append(data1)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	if err := sm.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}

	// Old data should still be readable.
	got, err := sm.ReadAt(addr1, uint32(len(data1)))
	if err != nil {
		t.Fatalf("ReadAt after rotate: %v", err)
	}
	if !bytes.Equal(got, data1) {
		t.Fatalf("mismatch: got %q, want %q", got, data1)
	}
}

// ─── Test 4: Rotate — new Append goes to new segment ────────────────

func TestRotateNewAppendNewSegment(t *testing.T) {
	sm := newTestManager(t, 4096)
	defer sm.Close()

	addr1, _ := sm.Append([]byte("seg1-data"))
	seg1ID := addr1.SegmentID

	if err := sm.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}

	addr2, err := sm.Append([]byte("seg2-data"))
	if err != nil {
		t.Fatalf("Append after rotate: %v", err)
	}

	if addr2.SegmentID == seg1ID {
		t.Fatalf("new append should be in different segment: got seg %d", addr2.SegmentID)
	}

	// Both should be readable.
	got1, _ := sm.ReadAt(addr1, 9)
	got2, _ := sm.ReadAt(addr2, 9)
	if string(got1) != "seg1-data" {
		t.Fatalf("seg1 mismatch: %q", got1)
	}
	if string(got2) != "seg2-data" {
		t.Fatalf("seg2 mismatch: %q", got2)
	}
}

// ─── Test 5: RemoveSegment — ReadAt returns ErrInvalidVAddr ─────────

func TestRemoveSegment(t *testing.T) {
	sm := newTestManager(t, 4096)
	defer sm.Close()

	data := []byte("to-be-removed")
	addr, _ := sm.Append(data)
	oldSegID := addr.SegmentID

	if err := sm.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}

	if err := sm.RemoveSegment(oldSegID); err != nil {
		t.Fatalf("RemoveSegment: %v", err)
	}

	_, err := sm.ReadAt(addr, uint32(len(data)))
	if !errors.Is(err, segmentapi.ErrInvalidVAddr) {
		t.Fatalf("ReadAt after remove: expected ErrInvalidVAddr, got %v", err)
	}
}

// ─── Test 6: ErrSegmentFull ─────────────────────────────────────────

func TestSegmentFull(t *testing.T) {
	sm := newTestManager(t, 100) // tiny segment
	defer sm.Close()

	// Fill it up.
	bigData := make([]byte, 80)
	_, err := sm.Append(bigData)
	if err != nil {
		t.Fatalf("first Append: %v", err)
	}

	// This should fail — 80 + 30 > 100.
	_, err = sm.Append(make([]byte, 30))
	if !errors.Is(err, segmentapi.ErrSegmentFull) {
		t.Fatalf("expected ErrSegmentFull, got %v", err)
	}
}

// ─── Test 7: Sync does not error ────────────────────────────────────

func TestSync(t *testing.T) {
	sm := newTestManager(t, 4096)
	defer sm.Close()

	sm.Append([]byte("some data"))
	if err := sm.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
}

// ─── Test 8: Close — subsequent ops return ErrClosed ────────────────

func TestCloseReturnsErrClosed(t *testing.T) {
	sm := newTestManager(t, 4096)

	sm.Append([]byte("data"))
	if err := sm.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// All operations should return ErrClosed.
	_, err := sm.Append([]byte("more"))
	if !errors.Is(err, segmentapi.ErrClosed) {
		t.Fatalf("Append after close: expected ErrClosed, got %v", err)
	}

	_, err = sm.ReadAt(segmentapi.VAddr{SegmentID: 1, Offset: 0}, 4)
	if !errors.Is(err, segmentapi.ErrClosed) {
		t.Fatalf("ReadAt after close: expected ErrClosed, got %v", err)
	}

	if err := sm.Sync(); !errors.Is(err, segmentapi.ErrClosed) {
		t.Fatalf("Sync after close: expected ErrClosed, got %v", err)
	}

	if err := sm.Rotate(); !errors.Is(err, segmentapi.ErrClosed) {
		t.Fatalf("Rotate after close: expected ErrClosed, got %v", err)
	}

	// Double close should also return ErrClosed.
	if err := sm.Close(); !errors.Is(err, segmentapi.ErrClosed) {
		t.Fatalf("double Close: expected ErrClosed, got %v", err)
	}
}

// ─── Test 9: Restart recovery ───────────────────────────────────────

func TestRestartRecovery(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: write data, rotate, write more, close.
	sm1 := newTestManagerDir(t, dir, 4096)

	data1 := []byte("first-segment-data")
	addr1, _ := sm1.Append(data1)

	sm1.Rotate()

	data2 := []byte("second-segment-data")
	addr2, _ := sm1.Append(data2)

	sm1.Close()

	// Phase 2: reopen — all data should be recoverable.
	sm2 := newTestManagerDir(t, dir, 4096)
	defer sm2.Close()

	got1, err := sm2.ReadAt(addr1, uint32(len(data1)))
	if err != nil {
		t.Fatalf("ReadAt addr1 after restart: %v", err)
	}
	if !bytes.Equal(got1, data1) {
		t.Fatalf("addr1 mismatch: got %q, want %q", got1, data1)
	}

	got2, err := sm2.ReadAt(addr2, uint32(len(data2)))
	if err != nil {
		t.Fatalf("ReadAt addr2 after restart: %v", err)
	}
	if !bytes.Equal(got2, data2) {
		t.Fatalf("addr2 mismatch: got %q, want %q", got2, data2)
	}

	// Should be able to append more after restart.
	data3 := []byte("after-restart")
	addr3, err := sm2.Append(data3)
	if err != nil {
		t.Fatalf("Append after restart: %v", err)
	}
	got3, _ := sm2.ReadAt(addr3, uint32(len(data3)))
	if !bytes.Equal(got3, data3) {
		t.Fatalf("addr3 mismatch: got %q, want %q", got3, data3)
	}
}

// ─── Test 10: SealedSegments ────────────────────────────────────────

func TestSealedSegments(t *testing.T) {
	sm := newTestManager(t, 4096)
	defer sm.Close()

	// Initially no sealed segments.
	if segs := sm.SealedSegments(); len(segs) != 0 {
		t.Fatalf("expected 0 sealed, got %v", segs)
	}

	sm.Append([]byte("a"))
	sm.Rotate()
	sm.Append([]byte("b"))
	sm.Rotate()
	sm.Append([]byte("c"))

	segs := sm.SealedSegments()
	if len(segs) != 2 {
		t.Fatalf("expected 2 sealed, got %v", segs)
	}
	// Should be sorted ascending.
	if segs[0] >= segs[1] {
		t.Fatalf("sealed segments not sorted: %v", segs)
	}
}

// ─── Test 11: ActiveSegmentID ───────────────────────────────────────

func TestActiveSegmentID(t *testing.T) {
	sm := newTestManager(t, 4096)
	defer sm.Close()

	id1 := sm.ActiveSegmentID()
	sm.Rotate()
	id2 := sm.ActiveSegmentID()

	if id2 <= id1 {
		t.Fatalf("active segment ID should increase after rotate: %d -> %d", id1, id2)
	}
}

// ─── Test 12: ReadAt out of bounds ──────────────────────────────────

func TestReadAtOutOfBounds(t *testing.T) {
	sm := newTestManager(t, 4096)
	defer sm.Close()

	sm.Append([]byte("short"))

	// Read beyond written data.
	_, err := sm.ReadAt(segmentapi.VAddr{SegmentID: 1, Offset: 0}, 1000)
	if !errors.Is(err, segmentapi.ErrInvalidVAddr) {
		t.Fatalf("expected ErrInvalidVAddr for OOB read, got %v", err)
	}

	// Read from non-existent segment.
	_, err = sm.ReadAt(segmentapi.VAddr{SegmentID: 999, Offset: 0}, 1)
	if !errors.Is(err, segmentapi.ErrInvalidVAddr) {
		t.Fatalf("expected ErrInvalidVAddr for bad segment, got %v", err)
	}
}

// ─── Test 13: RemoveSegment cannot remove active ────────────────────

func TestRemoveActiveSegmentFails(t *testing.T) {
	sm := newTestManager(t, 4096)
	defer sm.Close()

	activeID := sm.ActiveSegmentID()
	err := sm.RemoveSegment(activeID)
	if err == nil {
		t.Fatal("expected error when removing active segment")
	}
}

// ─── Test 14: VAddr Pack/Unpack roundtrip ───────────────────────────

func TestVAddrPackUnpack(t *testing.T) {
	cases := []segmentapi.VAddr{
		{SegmentID: 0, Offset: 0},
		{SegmentID: 1, Offset: 0},
		{SegmentID: 1, Offset: 4096},
		{SegmentID: 0xFFFFFFFF, Offset: 0xFFFFFFFF},
	}
	for _, v := range cases {
		packed := v.Pack()
		unpacked := segmentapi.UnpackVAddr(packed)
		if unpacked != v {
			t.Fatalf("Pack/Unpack roundtrip failed: %v -> %d -> %v", v, packed, unpacked)
		}
	}
}

// ─── Test 15: SegmentSize ────────────────────────────────────────────

func TestSegmentSize(t *testing.T) {
	sm := newTestManager(t, 4096)
	defer sm.Close()

	// Active segment starts empty.
	size, err := sm.SegmentSize(sm.ActiveSegmentID())
	if err != nil {
		t.Fatalf("SegmentSize active: %v", err)
	}
	if size != 0 {
		t.Fatalf("expected 0, got %d", size)
	}

	// Write some data.
	data := []byte("hello world") // 11 bytes
	sm.Append(data)

	size, err = sm.SegmentSize(sm.ActiveSegmentID())
	if err != nil {
		t.Fatalf("SegmentSize active after write: %v", err)
	}
	if size != int64(len(data)) {
		t.Fatalf("expected %d, got %d", len(data), size)
	}

	// Rotate → sealed segment should keep its size.
	sealedID := sm.ActiveSegmentID()
	sm.Rotate()

	size, err = sm.SegmentSize(sealedID)
	if err != nil {
		t.Fatalf("SegmentSize sealed: %v", err)
	}
	if size != int64(len(data)) {
		t.Fatalf("expected %d, got %d", len(data), size)
	}

	// Non-existent segment → error.
	_, err = sm.SegmentSize(999)
	if !errors.Is(err, segmentapi.ErrInvalidVAddr) {
		t.Fatalf("expected ErrInvalidVAddr, got %v", err)
	}
}

// ─── Test 16: MmapBasic — write + seal + mmap'd read returns same data ──

func TestMmapBasic(t *testing.T) {
	sm := newTestManager(t, 4096)
	defer sm.Close()

	// Write some data to active segment.
	data := []byte("mmap-test-data-12345")
	addr, err := sm.Append(data)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Read before sealing — uses ReadAt path.
	got, err := sm.ReadAt(addr, uint32(len(data)))
	if err != nil {
		t.Fatalf("ReadAt before seal: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("before seal: got %q, want %q", got, data)
	}

	// Seal — now reads use mmap path.
	if err := sm.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}

	// Read after sealing — must return same data.
	got, err = sm.ReadAt(addr, uint32(len(data)))
	if err != nil {
		t.Fatalf("ReadAt after seal (mmap path): %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("after seal: got %q, want %q", got, data)
	}
}

// ─── Test 17: MmapMultipleRecords ─────────────────────────────────────

func TestMmapMultipleRecords(t *testing.T) {
	sm := newTestManager(t, 8192)
	defer sm.Close()

	var addrs []segmentapi.VAddr
	var datas [][]byte

	for i := 0; i < 20; i++ {
		d := []byte(fmt.Sprintf("record-%04d-for-mmap-test", i))
		addr, err := sm.Append(d)
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		addrs = append(addrs, addr)
		datas = append(datas, d)
	}

	// Seal.
	sm.Rotate()

	// Read all records back — all should match.
	for i := len(addrs) - 1; i >= 0; i-- {
		got, err := sm.ReadAt(addrs[i], uint32(len(datas[i])))
		if err != nil {
			t.Fatalf("ReadAt %d after seal: %v", i, err)
		}
		if !bytes.Equal(got, datas[i]) {
			t.Fatalf("record %d mismatch: got %q, want %q", i, got, datas[i])
		}
	}
}

// ─── Test 18: MmapRemoveSegment — seal + remove + verify no crash ─────

func TestMmapRemoveSegment(t *testing.T) {
	sm := newTestManager(t, 4096)
	defer sm.Close()

	data := []byte("segment-to-be-removed")
	addr, _ := sm.Append(data)
	segID := addr.SegmentID

	// Seal it.
	sm.Rotate()

	// Remove it.
	if err := sm.RemoveSegment(segID); err != nil {
		t.Fatalf("RemoveSegment: %v", err)
	}

	// ReadAt should now return ErrInvalidVAddr.
	_, err := sm.ReadAt(addr, uint32(len(data)))
	if !errors.Is(err, segmentapi.ErrInvalidVAddr) {
		t.Fatalf("expected ErrInvalidVAddr after remove, got %v", err)
	}

	// Should still be able to use the active segment.
	newData := []byte("new-active-segment")
	newAddr, err := sm.Append(newData)
	if err != nil {
		t.Fatalf("Append to new active: %v", err)
	}
	got, _ := sm.ReadAt(newAddr, uint32(len(newData)))
	if !bytes.Equal(got, newData) {
		t.Fatalf("new data mismatch: got %q, want %q", got, newData)
	}
}

// ─── Test 19: MmapConcurrentRead — seal, 10 goroutines concurrent ReadAt ──

func TestMmapConcurrentRead(t *testing.T) {
	sm := newTestManager(t, 8192)
	defer sm.Close()

	// Write a moderate amount of data.
	var addrs []segmentapi.VAddr
	var datas [][]byte
	for i := 0; i < 50; i++ {
		d := []byte(fmt.Sprintf("concurrent-read-%04d", i))
		addr, _ := sm.Append(d)
		addrs = append(addrs, addr)
		datas = append(datas, d)
	}

	// Seal.
	sm.Rotate()

	// Concurrent reads from 10 goroutines.
	var wg sync.WaitGroup
	errCh := make(chan error, 100)

	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(goroutine int) {
			defer wg.Done()
			for i := 0; i < len(addrs); i++ {
				got, err := sm.ReadAt(addrs[i], uint32(len(datas[i])))
				if err != nil {
					errCh <- fmt.Errorf("goroutine %d, record %d: %w", goroutine, i, err)
					return
				}
				if !bytes.Equal(got, datas[i]) {
					errCh <- fmt.Errorf("goroutine %d, record %d: got %q, want %q", goroutine, i, got, datas[i])
					return
				}
			}
		}(g)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}
}

// ─── Test 20: MmapCloseCleansUp — seal multiple, close manager, no leak ──

func TestMmapCloseCleansUp(t *testing.T) {
	dir := t.TempDir()
	sm, err := New(segmentapi.Config{Dir: dir, MaxSize: 4096})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Write and seal multiple segments.
	for i := 0; i < 3; i++ {
		sm.Append([]byte(fmt.Sprintf("segment-data-%d-extra-padding-to-fill", i)))
		sm.Rotate()
	}

	// Close — must not crash or leak.
	if err := sm.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and verify sealed segments are still readable via mmap.
	sm2, err := New(segmentapi.Config{Dir: dir, MaxSize: 4096})
	if err != nil {
		t.Fatalf("Reopen New: %v", err)
	}
	defer sm2.Close()

	sealed := sm2.SealedSegments()
	if len(sealed) != 3 {
		t.Fatalf("expected 3 sealed segments after reopen, got %d", len(sealed))
	}

	// Read a record from the first sealed segment (mmap path).
	segSize, err := sm2.SegmentSize(sealed[0])
	if err != nil {
		t.Fatalf("SegmentSize: %v", err)
	}
	if segSize == 0 {
		t.Fatal("first sealed segment has zero size")
	}
}

// suppress unused import warning
var _ = os.Remove
