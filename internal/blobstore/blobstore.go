// Package blobstore provides the BlobStore — large value storage with
// dense array mapping from BlobID to VAddr.
//
// Design reference: docs/DESIGN.md §3.4
package blobstore

import (
	blobstoreapi "github.com/akzj/go-fast-kv/internal/blobstore/api"
	"github.com/akzj/go-fast-kv/internal/blobstore/internal"
	segmentapi "github.com/akzj/go-fast-kv/internal/segment/api"
)

// New creates a new BlobStore.
func New(cfg blobstoreapi.Config, segMgr segmentapi.SegmentManager) blobstoreapi.BlobStore {
	return internal.New(cfg, segMgr)
}
