// Package vacuum provides MVCC old version cleanup.
//
// Design reference: docs/DESIGN.md §3.10
package vacuum

import (
	blobstoreapi "github.com/akzj/go-fast-kv/internal/blobstore/api"
	btreeapi "github.com/akzj/go-fast-kv/internal/btree/api"
	pagestoreapi "github.com/akzj/go-fast-kv/internal/pagestore/api"
	txnapi "github.com/akzj/go-fast-kv/internal/txn/api"
	vacuumapi "github.com/akzj/go-fast-kv/internal/vacuum/api"
	"github.com/akzj/go-fast-kv/internal/vacuum/internal"
	walapi "github.com/akzj/go-fast-kv/internal/wal/api"
)

// New creates a new Vacuum instance.
func New(
	rootPageIDFn func() uint64,
	pages btreeapi.PageProvider,
	txnMgr txnapi.TxnManager,
	blobStore blobstoreapi.BlobStore,
	wal walapi.WAL,
	segSync func() error,
	drainPageWAL func() []pagestoreapi.WALEntry,
) vacuumapi.Vacuum {
	return internal.New(rootPageIDFn, pages, txnMgr, blobStore, wal, segSync, drainPageWAL)
}
