package internal

import (
	"sync"

	"github.com/akzj/go-fast-kv/pkg/objectstore/api"
)

// MappingIndexImpl provides thread-safe in-memory object location mapping.
// Implements MappingIndex interface.
//
// Invariant: All accesses are protected by RWMutex - reads can proceed
// concurrently, writes are exclusive.
//
// Why not sync.Map? RWMutex gives better read-heavy performance and
// simpler iteration semantics for our fixed key-value pattern.
type MappingIndexImpl struct {
	mu  sync.RWMutex
	m   map[api.ObjectID]api.ObjectLocation
}

// Ensure MappingIndexImpl implements api.MappingIndex
var _ api.MappingIndex = (*MappingIndexImpl)(nil)

// NewMappingIndex creates a new MappingIndexImpl.
func NewMappingIndex() *MappingIndexImpl {
	return &MappingIndexImpl{
		m: make(map[api.ObjectID]api.ObjectLocation),
	}
}

// Get retrieves the location for objID.
// Returns (location, true) if found, (zero, false) if not.
//
// Thread-safe: read lock held during map access.
func (m *MappingIndexImpl) Get(objID api.ObjectID) (api.ObjectLocation, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	loc, ok := m.m[objID]
	return loc, ok
}

// Put stores or updates the location for objID.
// If objID already exists, the location is overwritten.
//
// Thread-safe: write lock held during map access.
func (m *MappingIndexImpl) Put(objID api.ObjectID, loc api.ObjectLocation) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.m[objID] = loc
}

// Delete removes the mapping for objID.
// No-op if objID doesn't exist.
//
// Thread-safe: write lock held during map access.
func (m *MappingIndexImpl) Delete(objID api.ObjectID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.m, objID)
}

// Iterate calls fn for each (objID, location) pair.
// Iteration order is not guaranteed.
//
// Thread-safe: read lock held during iteration.
// Why RLock not Lock? Allows concurrent reads during iteration.
// Warning: fn must not mutate the map.
func (m *MappingIndexImpl) Iterate(fn func(objID api.ObjectID, loc api.ObjectLocation)) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for objID, loc := range m.m {
		fn(objID, loc)
	}
}
