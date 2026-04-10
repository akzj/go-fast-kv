// Package ssi implements Serializable Snapshot Isolation (SSI) indexes.
package ssi

import (
	"sync"

	ssiapi "github.com/akzj/go-fast-kv/internal/ssi/api"
)

// ─── SSI Index ───────────────────────────────────────────────────

// index implements ssiapi.Index for Serializable Snapshot Isolation.
type index struct {
	mu sync.RWMutex

	// SIndex: Key → last committed write info
	// Used to detect RW-conflicts (my read vs your write)
	SIndex map[ssiapi.Key]*ssiapi.WriteInfo

	// TIndex: Key → last reading transaction ID
	// Used to detect WW-conflicts (my write vs your read)
	TIndex map[ssiapi.Key]uint64
}

// NewIndex creates a new SSI index.
func NewIndex() ssiapi.Index {
	return &index{
		SIndex: make(map[ssiapi.Key]*ssiapi.WriteInfo),
		TIndex: make(map[ssiapi.Key]uint64),
	}
}

// ─── SIndex Operations ────────────────────────────────────────────

// GetWriteInfo returns the last committed write info for a key.
// Returns nil if no committed write exists.
func (i *index) GetWriteInfo(key ssiapi.Key) *ssiapi.WriteInfo {
	i.mu.RLock()
	defer i.mu.RUnlock()

	info, ok := i.SIndex[key]
	if !ok {
		return nil
	}
	return info
}

// SetWriteInfo sets the write info for a key after transaction commits.
func (i *index) SetWriteInfo(key ssiapi.Key, info *ssiapi.WriteInfo) {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.SIndex[key] = info
}

// ─── TIndex Operations ────────────────────────────────────────────

// GetReader returns the transaction ID that last read the key.
// Returns 0 if no reader exists.
func (i *index) GetReader(key ssiapi.Key) uint64 {
	i.mu.RLock()
	defer i.mu.RUnlock()

	return i.TIndex[key]
}

// SetReader sets the reader transaction ID for a key.
func (i *index) SetReader(key ssiapi.Key, txnID uint64) {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.TIndex[key] = txnID
}

// ─── GC ─────────────────────────────────────────────────────────

// GC removes entries older than minXID from the indexes.
func (i *index) GC(minXID uint64) {
	i.mu.Lock()
	defer i.mu.Unlock()

	// GC SIndex: remove entries where CommitTS < minXID
	for key, info := range i.SIndex {
		if info.CommitTS < minXID {
			delete(i.SIndex, key)
		}
	}

	// GC TIndex: remove entries where txnID < minXID
	for key, txnID := range i.TIndex {
		if txnID < minXID {
			delete(i.TIndex, key)
		}
	}
}

// ─── Stats ─────────────────────────────────────────────────────

// Size returns the current size of both indexes.
func (i *index) Size() (sindexSize, tindexSize int) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	return len(i.SIndex), len(i.TIndex)
}
