// Package lock provides a sharded per-page RwLock manager for the B-link tree.
//
// Each PageID gets its own sync.RWMutex, created on demand. Locks are
// organized in 16 shards (pageID % 16) to reduce contention on the
// internal map operations.
//
// The B-link tree protocol guarantees that at most one lock is held at
// a time per goroutine, making deadlocks impossible.
//
// Design reference: docs/DESIGN.md §3.8.1
package lock

import (
	"sync"
)

const numShards = 16

// PageRWLocks manages per-page RwLocks with sharding.
//
// Thread safety: all methods are safe for concurrent use.
type PageRWLocks struct {
	shards [numShards]lockShard
}

type lockShard struct {
	mu    sync.Mutex
	locks map[uint64]*sync.RWMutex
}

// New creates a new PageRWLocks manager.
func New() *PageRWLocks {
	l := &PageRWLocks{}
	for i := range l.shards {
		l.shards[i].locks = make(map[uint64]*sync.RWMutex)
	}
	return l
}

// GetLock returns the RWMutex for the given PageID, creating one if needed.
func (l *PageRWLocks) GetLock(pid uint64) *sync.RWMutex {
	shard := &l.shards[pid%numShards]
	shard.mu.Lock()
	rw, ok := shard.locks[pid]
	if !ok {
		rw = &sync.RWMutex{}
		shard.locks[pid] = rw
	}
	shard.mu.Unlock()
	return rw
}

// RLock acquires a read lock on the given PageID.
func (l *PageRWLocks) RLock(pid uint64) {
	l.GetLock(pid).RLock()
}

// RUnlock releases the read lock on the given PageID.
func (l *PageRWLocks) RUnlock(pid uint64) {
	l.GetLock(pid).RUnlock()
}

// WLock acquires a write lock on the given PageID.
func (l *PageRWLocks) WLock(pid uint64) {
	l.GetLock(pid).Lock()
}

// WUnlock releases the write lock on the given PageID.
func (l *PageRWLocks) WUnlock(pid uint64) {
	l.GetLock(pid).Unlock()
}
