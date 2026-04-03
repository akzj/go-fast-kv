package internal

import (
	"sync"

	concurrency "github.com/akzj/go-fast-kv/internal/concurrency/api"
	vaddr "github.com/akzj/go-fast-kv/internal/vaddr"
)

// LatchManager interface for tree operations
type LatchManager interface {
	Acquire(addr vaddr.VAddr, mode concurrency.LatchMode)
	Release(addr vaddr.VAddr, mode concurrency.LatchMode)
	TryAcquire(addr vaddr.VAddr, mode concurrency.LatchMode) bool
	Upgrade(addr vaddr.VAddr) error
	IsWriteLocked(addr vaddr.VAddr) bool
	ReaderCount(addr vaddr.VAddr) int
}

// latchManager implements LatchManager for B-link tree nodes.
type latchManagerImpl struct {
	latches map[vaddr.VAddr]*nodeLatch
	mu      sync.RWMutex
}

type nodeLatch struct {
	mu      sync.RWMutex
	readers int
	writer  bool
}

func NewLatchManager() LatchManager {
	return &latchManagerImpl{
		latches: make(map[vaddr.VAddr]*nodeLatch),
	}
}

func (lm *latchManagerImpl) getLatch(addr vaddr.VAddr) *nodeLatch {
	lm.mu.RLock()
	l, ok := lm.latches[addr]
	lm.mu.RUnlock()
	if ok {
		return l
	}

	lm.mu.Lock()
	defer lm.mu.Unlock()
	if l, ok = lm.latches[addr]; ok {
		return l
	}
	l = &nodeLatch{}
	lm.latches[addr] = l
	return l
}

func (lm *latchManagerImpl) Acquire(addr vaddr.VAddr, mode concurrency.LatchMode) {
	l := lm.getLatch(addr)
	l.mu.Lock()
	defer l.mu.Unlock()

	if mode == concurrency.LatchWrite {
		for l.readers > 0 {
			l.mu.Unlock()
			l.mu.Lock()
		}
		l.writer = true
	} else {
		l.readers++
	}
}

func (lm *latchManagerImpl) Release(addr vaddr.VAddr, mode concurrency.LatchMode) {
	l := lm.getLatch(addr)
	l.mu.Lock()
	defer l.mu.Unlock()

	if mode == concurrency.LatchWrite {
		l.writer = false
	} else {
		l.readers--
	}
}

func (lm *latchManagerImpl) TryAcquire(addr vaddr.VAddr, mode concurrency.LatchMode) bool {
	l := lm.getLatch(addr)
	l.mu.Lock()
	defer l.mu.Unlock()

	if mode == concurrency.LatchWrite {
		if l.readers > 0 || l.writer {
			return false
		}
		l.writer = true
		return true
	}
	l.readers++
	return true
}

func (lm *latchManagerImpl) Upgrade(addr vaddr.VAddr) error {
	l := lm.getLatch(addr)
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.readers != 1 {
		return concurrency.ErrUpgradeNotSupported
	}
	return nil
}

func (lm *latchManagerImpl) IsWriteLocked(addr vaddr.VAddr) bool {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	if l, ok := lm.latches[addr]; ok {
		l.mu.RLock()
		defer l.mu.RUnlock()
		return l.writer
	}
	return false
}

func (lm *latchManagerImpl) ReaderCount(addr vaddr.VAddr) int {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	if l, ok := lm.latches[addr]; ok {
		l.mu.RLock()
		defer l.mu.RUnlock()
		return l.readers
	}
	return 0
}
