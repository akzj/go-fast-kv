package compaction

import (
	"sync"

	api "github.com/akzj/go-fast-kv/internal/compaction/api"
	vaddr "github.com/akzj/go-fast-kv/internal/vaddr"
)

// epochManager implements EpochManager using epoch-based MVCC.
type epochManager struct {
	mu          sync.RWMutex
	epoch       vaddr.EpochID
	gracePeriod vaddr.EpochID
	epochCount  map[vaddr.EpochID]int
	compactable map[vaddr.SegmentID]struct{}
}

// NewEpochManager creates a new EpochManager with the given grace period.
func NewEpochManager(gracePeriod uint) api.EpochManager {
	gp := uint64(vaddr.EpochGracePeriod)
	if gracePeriod > 0 {
		gp = uint64(gracePeriod)
	}
	return &epochManager{
		gracePeriod: vaddr.EpochID(gp),
		epoch:        0,
		epochCount:   make(map[vaddr.EpochID]int),
		compactable:  make(map[vaddr.SegmentID]struct{}),
	}
}

// RegisterEpoch creates a new epoch and returns its ID.
func (e *epochManager) RegisterEpoch() api.EpochID {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.epoch++
	e.epochCount[e.epoch]++
	return api.EpochID(e.epoch)
}

// UnregisterEpoch releases all references to an epoch.
func (e *epochManager) UnregisterEpoch(epoch api.EpochID) {
	e.mu.Lock()
	defer e.mu.Unlock()
	id := vaddr.EpochID(epoch)
	if count, ok := e.epochCount[id]; ok {
		if count > 0 {
			e.epochCount[id] = count - 1
		}
		if e.epochCount[id] == 0 {
			delete(e.epochCount, id)
		}
	}
}

// IsVisible returns true if vaddr is visible in the given epoch.
func (e *epochManager) IsVisible(v api.VAddr, epoch api.EpochID) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	segID := vaddr.SegmentID(v.SegmentID)
	if _, compactable := e.compactable[segID]; compactable {
		return false
	}
	return true
}

// IsSafeToReclaim returns true if vaddr can be safely reclaimed.
func (e *epochManager) IsSafeToReclaim(v api.VAddr) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	segID := vaddr.SegmentID(v.SegmentID)
	if _, ok := e.compactable[segID]; !ok {
		return false
	}
	oldest := e.oldestActiveEpochLocked()
	if oldest == 0 {
		return true
	}
	if oldest <= vaddr.EpochID(e.gracePeriod) {
		return false
	}
	return true
}

// oldestActiveEpochLocked returns the oldest active epoch. Caller must hold lock.
func (e *epochManager) oldestActiveEpochLocked() vaddr.EpochID {
	var oldest vaddr.EpochID
	for epoch, count := range e.epochCount {
		if count > 0 && (oldest == 0 || epoch < oldest) {
			oldest = epoch
		}
	}
	return oldest
}

// MarkCompactionComplete marks old segments as successfully compacted.
func (e *epochManager) MarkCompactionComplete(oldSegments []api.SegmentID) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, seg := range oldSegments {
		e.compactable[vaddr.SegmentID(seg)] = struct{}{}
	}
}

// CurrentEpoch returns the current epoch ID.
func (e *epochManager) CurrentEpoch() api.EpochID {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return api.EpochID(e.epoch)
}
