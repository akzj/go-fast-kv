// Package internal provides the B+Tree implementation.
package internal

import (
	"sync"
)

// Latch provides fine-grained locking for B+Tree pages.
// Uses a read-write mutex to support concurrent readers.
type Latch struct {
	mu sync.RWMutex
}

// newLatch creates a new Latch.
func newLatch() *Latch {
	return &Latch{}
}

// Lock acquires an exclusive (write) lock.
func (l *Latch) Lock() {
	l.mu.Lock()
}

// Unlock releases the exclusive lock.
func (l *Latch) Unlock() {
	l.mu.Unlock()
}

// RLock acquires a shared (read) lock.
func (l *Latch) RLock() {
	l.mu.RLock()
}

// RUnlock releases the shared lock.
func (l *Latch) RUnlock() {
	l.mu.RUnlock()
}

// TryLock attempts to acquire an exclusive lock without blocking.
func (l *Latch) TryLock() bool {
	return l.mu.TryLock()
}

// TryRLock attempts to acquire a shared lock without blocking.
func (l *Latch) TryRLock() bool {
	return l.mu.TryRLock()
}
