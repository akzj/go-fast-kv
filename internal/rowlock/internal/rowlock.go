// Package internal implements the RowLockManager with 16-shard locking.
//
// This package provides row-level locking for database operations, distinct
// from page-level B-tree locks in internal/lock/. Row locks operate on
// data keys (tableID:rowID) and support both shared and exclusive modes.
//
// Design:
//   - 16 shards reduce contention by distributing locks across independent maps
//   - Each shard uses a sync.Mutex for thread-safe access
//   - Blocking acquires use retry with sleep for simplicity
//   - Lock ordering (sorted rowKey) is the caller's responsibility
package internal

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/akzj/go-fast-kv/internal/rowlock/api"
)

const numShards = 16

// lockEntry represents a lock held on a specific row.
// Supports both shared (multiple holders) and exclusive (single holder) modes.
type lockEntry struct {
	mu      sync.Mutex
	mode    api.LockMode          // Current lock mode
	holders map[api.TxnID]struct{} // Set of transaction holders (for shared locks)
}

// shard manages locks for a subset of rowKeys.
type shard struct {
	mu    sync.Mutex
	locks map[string]*lockEntry
}

// RowLockManager implements the api.LockManager interface using 16 shards.
type RowLockManager struct {
	shards [numShards]shard
}

// New creates a new RowLockManager with 16 shards.
func New() *RowLockManager {
	m := &RowLockManager{}
	for i := range m.shards {
		m.shards[i].locks = make(map[string]*lockEntry)
	}
	return m
}

// shardIndex returns the shard index for a given rowKey.
func (m *RowLockManager) shardIndex(rowKey string) int {
	// Use string hash to distribute across shards
	h := 0
	for _, c := range rowKey {
		h = h*31 + int(c)
	}
	return (h & 0xFFFF) % numShards
}

// getShard returns the shard for a given rowKey.
func (m *RowLockManager) getShard(rowKey string) *shard {
	return &m.shards[m.shardIndex(rowKey)]
}

// canAcquire checks if a lock can be acquired without blocking.
// Returns true if the lock can be acquired immediately.
func (m *RowLockManager) canAcquire(entry *lockEntry, txnID api.TxnID, mode api.LockMode) bool {
	if entry == nil {
		// No existing lock - can acquire
		return true
	}

	// Check if this transaction already holds the lock
	if _, exists := entry.holders[txnID]; exists {
		// Same transaction holds the lock
		if mode == api.LockExclusive && entry.mode == api.LockShared {
			// Upgrade from shared to exclusive: only allowed if we're the ONLY holder
			return len(entry.holders) == 1
		}
		// Re-entrant same-mode or downgrade: always allowed
		return true
	}

	// Different transaction holds the lock
	if entry.mode == api.LockExclusive {
		// Someone has exclusive lock - can't acquire
		return false
	}

	// Existing lock is shared
	if mode == api.LockShared {
		// Multiple shared locks allowed
		return true
	}

	// Exclusive requested but someone has shared - conflict
	return false
}

// Acquire attempts to acquire a lock, blocking until available or timeout.
// Returns true if acquired, false if timeout occurred.
func (m *RowLockManager) Acquire(rowKey string, ctx api.LockContext, mode api.LockMode) bool {
	s := m.getShard(rowKey)
	timeout := ctx.Timeout()
	deadline := time.Now().Add(timeout)

	for {
		s.mu.Lock()

		entry, exists := s.locks[rowKey]
		if !exists {
			// No existing lock - create new entry
			entry = &lockEntry{
				mode:    mode,
				holders: map[api.TxnID]struct{}{ctx.TxnID: {}},
			}
			s.locks[rowKey] = entry
			s.mu.Unlock()
			incAcquire()
			return true
		}

		if m.canAcquire(entry, ctx.TxnID, mode) {
			// Can acquire - add to holders
			entry.holders[ctx.TxnID] = struct{}{}
			if entry.mode == api.LockShared && mode == api.LockExclusive {
				// Upgrade from shared to exclusive (same txn)
				entry.mode = api.LockExclusive
			}
			s.mu.Unlock()
			incAcquire()
			return true
		}

		s.mu.Unlock()

		// Check timeout
		if timeout > 0 && time.Now().After(deadline) {
			return false
		}
		// Short sleep before retry
		time.Sleep(10 * time.Millisecond)
	}
}

// TryAcquire attempts to acquire a lock without blocking.
// Returns true if acquired, false if lock is held by another transaction.
func (m *RowLockManager) TryAcquire(rowKey string, txnID api.TxnID, mode api.LockMode) bool {
	s := m.getShard(rowKey)
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.locks[rowKey]
	if !exists {
		// No existing lock - create new entry
		s.locks[rowKey] = &lockEntry{
			mode:    mode,
			holders: map[api.TxnID]struct{}{txnID: {}},
		}
		incAcquire()
		return true
	}

	if m.canAcquire(entry, txnID, mode) {
		entry.holders[txnID] = struct{}{}
		if entry.mode == api.LockShared && mode == api.LockExclusive {
			entry.mode = api.LockExclusive
		}
		incAcquire()
		return true
	}

	return false
}

// Release releases a lock held by the given transaction.
func (m *RowLockManager) Release(rowKey string, txnID api.TxnID) {
	s := m.getShard(rowKey)
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.locks[rowKey]
	if !exists {
		return
	}

	if _, held := entry.holders[txnID]; !held {
		// Transaction doesn't hold this lock
		return
	}

	// Remove from holders
	delete(entry.holders, txnID)

	if len(entry.holders) == 0 {
		// No more holders - remove the entry
		delete(s.locks, rowKey)
	}
	incRelease()
}

// ReleaseAll releases all locks held by the given transaction.
func (m *RowLockManager) ReleaseAll(txnID api.TxnID) {
	for i := range m.shards {
		s := &m.shards[i]
		s.mu.Lock()

		for rowKey, entry := range s.locks {
			if _, held := entry.holders[txnID]; held {
				delete(entry.holders, txnID)
				if len(entry.holders) == 0 {
					delete(s.locks, rowKey)
				}
				incRelease()
			}
		}

		s.mu.Unlock()
	}
}

// IsLocked returns true if rowKey is currently locked.
func (m *RowLockManager) IsLocked(rowKey string) bool {
	s := m.getShard(rowKey)
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.locks[rowKey]
	return exists && len(entry.holders) > 0
}

// IsLockedByTxn returns true if rowKey is locked by the specified transaction.
func (m *RowLockManager) IsLockedByTxn(rowKey string, txnID api.TxnID) bool {
	s := m.getShard(rowKey)
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.locks[rowKey]
	if !exists {
		return false
	}
	_, held := entry.holders[txnID]
	return held
}

// GetLockMode returns the current lock mode for rowKey, or LockShared+LockExclusive+1 if unlocked.
// Note: Returns the mode of the existing lock (Shared or Exclusive).
func (m *RowLockManager) GetLockMode(rowKey string) api.LockMode {
	s := m.getShard(rowKey)
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.locks[rowKey]
	if !exists || len(entry.holders) == 0 {
		return api.LockMode(255) // Invalid mode for unlocked state
	}
	return entry.mode
}

// LockStats returns statistics about the lock manager.
func (m *RowLockManager) LockStats() api.LockStats {
	stats := api.LockStats{
		ShardStats: make([]api.ShardStat, numShards),
	}

	for i := range m.shards {
		s := &m.shards[i]
		s.mu.Lock()

		count := 0
		for _, entry := range s.locks {
			if len(entry.holders) > 0 {
				count++
			}
		}
		stats.ShardStats[i] = api.ShardStat{
			ShardID: i,
			Locks:   count,
			Waiters: 0,
		}
		stats.TotalLocks += int64(count)

		s.mu.Unlock()
	}

	return stats
}

// Close releases all resources.
func (m *RowLockManager) Close() {
	for i := range m.shards {
		s := &m.shards[i]
		s.mu.Lock()
		s.locks = make(map[string]*lockEntry)
		s.mu.Unlock()
	}
}

// Atomic counters for lock operations
var lockAcquires uint64
var lockReleases uint64

func incAcquire() {
	atomic.AddUint64(&lockAcquires, 1)
}

func incRelease() {
	atomic.AddUint64(&lockReleases, 1)
}
