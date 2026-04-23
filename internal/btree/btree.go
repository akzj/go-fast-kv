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
	NodePageAdapter  = internal.NodePageAdapter
)

// New creates a new BTree backed by a MemPageProvider (for testing).
func New(cfg btreeapi.Config, pages *MemPageProvider, blobs btreeapi.BlobWriter) btreeapi.BTree {
	return internal.New(cfg, pages, blobs)
}

// NewWithRealProvider creates a new BTree backed by a RealPageProvider.
func NewWithRealProvider(cfg btreeapi.Config, pages *RealPageProvider, blobs btreeapi.BlobWriter) btreeapi.BTree {
	return internal.New(cfg, pages, blobs)
}

// NewNodeSerializer creates a new NodeSerializer.
func NewNodeSerializer() btreeapi.NodeSerializer {
	return internal.NewNodeSerializer()
}

// NewRealPageProvider creates a PageProvider backed by a PageStore.
func NewRealPageProvider(store pagestoreapi.PageStore, cacheSize int) *RealPageProvider {
	return internal.NewRealPageProvider(store, cacheSize)
}

// NewMemPageProvider creates an in-memory PageProvider for testing.
func NewMemPageProvider() *MemPageProvider {
	return internal.NewMemPageProvider()
}

// NewNodePageAdapter creates an adapter that wraps *RealPageProvider
// and implements btreeapi.PageProvider for backward compatibility
// with consumers (vacuum, etc.) that still use *btreeapi.Node.
func NewNodePageAdapter(provider *RealPageProvider) *NodePageAdapter {
	return internal.NewNodePageAdapter(provider)
}
