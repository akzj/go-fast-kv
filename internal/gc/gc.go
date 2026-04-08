// Package gc provides Page GC and Blob GC for reclaiming storage space.
//
// Design reference: docs/DESIGN.md §3.7
package gc

import (
	blobstoreapi "github.com/akzj/go-fast-kv/internal/blobstore/api"
	gcapi "github.com/akzj/go-fast-kv/internal/gc/api"
	"github.com/akzj/go-fast-kv/internal/gc/internal"
	pagestoreapi "github.com/akzj/go-fast-kv/internal/pagestore/api"
	segmentapi "github.com/akzj/go-fast-kv/internal/segment/api"
	walapi "github.com/akzj/go-fast-kv/internal/wal/api"
)

// NewPageGC creates a new PageGC instance.
func NewPageGC(
	pageSegMgr segmentapi.SegmentManager,
	pageStore pagestoreapi.PageStore,
	pageStoreRecovery pagestoreapi.PageStoreRecovery,
	wal walapi.WAL,
) gcapi.PageGC {
	return internal.NewPageGC(pageSegMgr, pageStore, pageStoreRecovery, wal)
}

// NewBlobGC creates a new BlobGC instance.
func NewBlobGC(
	blobSegMgr segmentapi.SegmentManager,
	blobStore blobstoreapi.BlobStore,
	blobStoreRecovery blobstoreapi.BlobStoreRecovery,
	wal walapi.WAL,
) gcapi.BlobGC {
	return internal.NewBlobGC(blobSegMgr, blobStore, blobStoreRecovery, wal)
}
