package internal

import (
	"testing"
)

// TestFreeListBasic tests basic FreeList operations.
func TestFreeListBasic(t *testing.T) {
	fl := newFreeList()

	// Empty list
	if pageID, ok := fl.Pop(); ok || pageID != 0 {
		t.Errorf("Pop on empty list: got (%d, %v), want (0, false)", pageID, ok)
	}
	if fl.Len() != 0 {
		t.Errorf("Len on empty list: got %d, want 0", fl.Len())
	}

	// Push some pages
	fl.Push(1)
	fl.Push(2)
	fl.Push(3)

	if fl.Len() != 3 {
		t.Errorf("Len after 3 pushes: got %d, want 3", fl.Len())
	}

	// Pop pages (LIFO order)
	id, ok := fl.Pop()
	if !ok || id == 0 {
		t.Errorf("Pop: got (%d, %v), want (non-zero, true)", id, ok)
	}
	if fl.Len() != 2 {
		t.Errorf("Len after 1 pop: got %d, want 2", fl.Len())
	}

	// Clear
	fl.Clear()
	if fl.Len() != 0 {
		t.Errorf("Len after clear: got %d, want 0", fl.Len())
	}
}

// TestFreeListFIFO tests that pages are reused in FIFO order.
func TestFreeListFIFO(t *testing.T) {
	fl := newFreeList()

	// Push pages
	fl.Push(10)
	fl.Push(20)
	fl.Push(30)

	// Pop all
	var ids [3]PageID
	for i := 0; i < 3; i++ {
		pageID, ok := fl.Pop()
		if !ok {
			t.Errorf("Pop %d: got (0, false), want non-zero", i)
			break
		}
		ids[i] = pageID
	}

	// Last pushed should be popped first (LIFO)
	if ids[0] != 30 || ids[1] != 20 || ids[2] != 10 {
		t.Errorf("Order: got %v, want [30, 20, 10]", ids)
	}
}

// TestDenseArrayBasic tests basic DenseArray operations.
func TestDenseArrayBasic(t *testing.T) {
	da := newDenseArray(16)

	// Get on empty array
	if vaddr := da.Get(1); !isZeroVAddr(vaddr) {
		t.Errorf("Get(1) on empty: got %v, want zero", vaddr)
	}

	// Put and Get
	var vaddr [16]byte
	vaddr[0] = 1 // SegmentID = 1
	vaddr[8] = 100 // Offset = 100

	da.Put(1, vaddr)

	result := da.Get(1)
	if result[0] != 1 || result[8] != 100 {
		t.Errorf("Get(1): got %v, want vaddr with seg=1, offset=100", result)
	}

	// Live count
	if da.LiveCount() != 1 {
		t.Errorf("LiveCount: got %d, want 1", da.LiveCount())
	}

	// ByteSize
	size := da.ByteSize()
	if size < 24*16 {
		t.Errorf("ByteSize: got %d, want >= %d", size, 24*16)
	}
}

// TestDenseArrayGrowth tests that array grows correctly.
func TestDenseArrayGrowth(t *testing.T) {
	da := newDenseArray(4) // Start small

	var vaddr [16]byte
	vaddr[0] = 1

	// Insert many entries to force growth
	for i := PageID(1); i <= 100; i++ {
		da.Put(i, vaddr)
	}

	// Verify we can retrieve them
	for i := PageID(1); i <= 100; i++ {
		if got := da.Get(i); isZeroVAddr(got) {
			t.Errorf("Get(%d) after growth: got zero, want vaddr", i)
		}
	}

	if da.LiveCount() != 100 {
		t.Errorf("LiveCount: got %d, want 100", da.LiveCount())
	}
}

// TestDenseArrayRangeQuery tests range queries.
func TestDenseArrayRangeQuery(t *testing.T) {
	da := newDenseArray(32)

	var vaddr [16]byte
	vaddr[0] = 1

	// Put entries at specific PageIDs
	da.Put(5, vaddr)
	da.Put(10, vaddr)
	da.Put(15, vaddr)
	da.Put(20, vaddr)

	// Query range [5, 20)
	entries := da.RangeQuery(5, 20)
	if len(entries) != 3 {
		t.Errorf("RangeQuery(5, 20): got %d entries, want 3", len(entries))
	}
}

// TestDenseArrayIter tests iteration.
func TestDenseArrayIter(t *testing.T) {
	da := newDenseArray(16)

	var vaddr [16]byte
	vaddr[0] = 1

	da.Put(1, vaddr)
	da.Put(2, vaddr)
	da.Put(3, vaddr)

	count := 0
	da.Iter(func(pageID PageID, v [16]byte) {
		count++
	})

	if count != 3 {
		t.Errorf("Iter count: got %d, want 3", count)
	}
}

// TestPageManagerIndexEntry tests the entry structure.
func TestPageManagerIndexEntry(t *testing.T) {
	entry := PageManagerIndexEntry{
		PageID: 42,
		VAddr:  [16]byte{0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 100},
	}

	if entry.PageID != 42 {
		t.Errorf("PageID: got %d, want 42", entry.PageID)
	}
	if entry.VAddr[7] != 1 || entry.VAddr[15] != 100 {
		t.Errorf("VAddr: got %v, want seg=1, offset=100", entry.VAddr)
	}
}
