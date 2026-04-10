// Package btree provides the B-link tree index with MVCC versioning
// and per-page RwLocks for concurrent access.
//
// Design reference: docs/DESIGN.md §3.5, §3.8
package btree

import (
	btreeapi "github.com/akzj/go-fast-kv/internal/btree/api"
	"github.com/akzj/go-fast-kv/internal/btree/internal"
	pagestoreapi "github.com/akzj/go-fast-kv/internal/pagestore/api"
)

// Types (re-exported from internal).
type (
	RealPageProvider = internal.RealPageProvider
	MemPageProvider  = internal.MemPageProvider
	WALCollector     = internal.WALCollector
)

// New creates a new BTree.
func New(cfg btreeapi.Config, pages btreeapi.PageProvider, blobs btreeapi.BlobWriter) btreeapi.BTree {
	return internal.New(cfg, pages, blobs)
}

// NewNodeSerializer creates a new NodeSerializer.
func NewNodeSerializer() btreeapi.NodeSerializer {
	return internal.NewNodeSerializer()
}

// NewRealPageProvider creates a PageProvider backed by a PageStore.
// cacheSize is the maximum number of B-tree page entries in the LRU cache.
// Defaults to 8192 if zero.
func NewRealPageProvider(store pagestoreapi.PageStore, cacheSize int) *RealPageProvider {
	return internal.NewRealPageProvider(store, cacheSize)
}

// NewMemPageProvider creates an in-memory PageProvider for testing.
func NewMemPageProvider() *MemPageProvider {
	return internal.NewMemPageProvider()
}
