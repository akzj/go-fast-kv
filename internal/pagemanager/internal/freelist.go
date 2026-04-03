package internal

import (
	"sync/atomic"
	"unsafe"
)

// freeList implements the api.FreeList interface using a lock-free stack.
// Design: Treiber stack with CAS operations for page reuse.
type freeList struct {
	head unsafe.Pointer // *freeNode - top of stack
	len  atomic.Uint64
}

// freeNode is a node in the free list stack.
type freeNode struct {
	pageID uint64
	next  unsafe.Pointer // *freeNode
}

// newFreeList creates a new empty free list.
func newFreeList() *freeList {
	return &freeList{}
}

// Pop returns a freed PageID or false if the list is empty.
// Invariant: Pop returns PageID > 0.
func (f *freeList) Pop() (PageID, bool) {
	for {
		head := atomic.LoadPointer(&f.head)
		if head == nil {
			return 0, false
		}

		node := (*freeNode)(head)
		next := atomic.LoadPointer(&node.next)

		// Try to remove head
		if atomic.CompareAndSwapPointer(&f.head, head, next) {
			f.len.Add(^uint64(0)) // Decrement
			return PageID(node.pageID), true
		}
		// CAS failed, retry
	}
}

// Push adds a PageID to the free list.
// Invariant: Push is idempotent (caller ensures no double-free).
func (f *freeList) Push(pageID PageID) {
	if pageID == 0 {
		return
	}

	node := &freeNode{
		pageID: uint64(pageID),
		next:   atomic.LoadPointer(&f.head),
	}

	// Keep trying until we succeed
	for {
		if atomic.CompareAndSwapPointer(&f.head, node.next, unsafe.Pointer(node)) {
			f.len.Add(1)
			return
		}
		// CAS failed, update next and retry
		node.next = atomic.LoadPointer(&f.head)
	}
}

// Len returns the number of pages on the free list.
func (f *freeList) Len() uint64 {
	return f.len.Load()
}

// Clear removes all entries from the free list.
func (f *freeList) Clear() {
	for {
		head := atomic.LoadPointer(&f.head)
		if head == nil {
			f.len.Store(0)
			return
		}
		if atomic.CompareAndSwapPointer(&f.head, head, nil) {
			f.len.Add(^uint64(0)) // Decrement
			// Continue until head is nil
		}
	}
}
