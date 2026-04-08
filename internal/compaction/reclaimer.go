package compaction

import (
	"sync"

	api "github.com/akzj/go-fast-kv/internal/compaction/api"
)

// reclaimer tracks live VAddrs and determines safe reclamation windows
// using epoch-based MVCC from EpochManager.
type reclaimer struct {
	mu       sync.RWMutex
	epochMgr api.EpochManager
	// liveVAddrs tracks VAddrs that are currently live (visible to readers).
	liveVAddrs map[string]struct{}
}

// NewReclaimer creates a new Reclaimer using the given EpochManager.
func NewReclaimer(epochMgr api.EpochManager) api.Reclaimer {
	return &reclaimer{
		epochMgr:  epochMgr,
		liveVAddrs: make(map[string]struct{}),
	}
}

// RegisterVAddr records a VAddr as live (visible to readers).
func (r *reclaimer) RegisterVAddr(v api.VAddr) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.liveVAddrs[vaddrKey(v)] = struct{}{}
}

// UnregisterVAddr removes a VAddr from live tracking.
func (r *reclaimer) UnregisterVAddr(v api.VAddr) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.liveVAddrs, vaddrKey(v))
	return nil
}

// TryReclaim attempts to reclaim a VAddr.
// Returns true if the VAddr was successfully reclaimed.
// Returns false if the VAddr is still visible to active readers.
func (r *reclaimer) TryReclaim(v api.VAddr) (bool, error) {
	// Check if it's safe to reclaim using EpochManager
	if !r.epochMgr.IsSafeToReclaim(v) {
		return false, nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, live := r.liveVAddrs[vaddrKey(v)]; live {
		return false, nil
	}
	delete(r.liveVAddrs, vaddrKey(v))
	return true, nil
}

// BatchTryReclaim attempts to reclaim multiple VAddrs.
// Returns VAddrs that were successfully reclaimed.
func (r *reclaimer) BatchTryReclaim(vaddrs []api.VAddr) ([]api.VAddr, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var reclaimed []api.VAddr
	for _, v := range vaddrs {
		if !r.epochMgr.IsSafeToReclaim(v) {
			continue
		}
		key := vaddrKey(v)
		if _, live := r.liveVAddrs[key]; live {
			continue
		}
		delete(r.liveVAddrs, key)
		reclaimed = append(reclaimed, v)
	}
	return reclaimed, nil
}

// LiveVAddrCount returns the number of VAddrs currently tracked as live.
func (r *reclaimer) LiveVAddrCount() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return uint64(len(r.liveVAddrs))
}

// vaddrKey returns a string key for a VAddr (for map indexing).
func vaddrKey(v api.VAddr) string {
	bytes := v.ToBytes()
	return string(bytes[:])
}
