// Package lock provides a sharded per-page RwLock manager for the B-link tree.
//
// Design reference: docs/DESIGN.md §3.8.1
package lock

import (
	"github.com/akzj/go-fast-kv/internal/lock/internal"
)

// PageRWLocks manages per-page RwLocks with sharding.
type PageRWLocks = internal.PageRWLocks

// New creates a new PageRWLocks manager.
func New() *PageRWLocks {
	return internal.New()
}
