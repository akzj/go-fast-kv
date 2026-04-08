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
func NewRealPageProvider(store pagestoreapi.PageStore) *RealPageProvider {
	return internal.NewRealPageProvider(store)
}

// NewMemPageProvider creates an in-memory PageProvider for testing.
func NewMemPageProvider() *MemPageProvider {
	return internal.NewMemPageProvider()
}
