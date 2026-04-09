// Package pagestore provides the PageStore — page-level storage with
// dense array mapping from PageID to VAddr.
//
// Design reference: docs/DESIGN.md §3.3
package pagestore

import (
	pagestoreapi "github.com/akzj/go-fast-kv/internal/pagestore/api"
	"github.com/akzj/go-fast-kv/internal/pagestore/internal"
	segmentapi "github.com/akzj/go-fast-kv/internal/segment/api"
)

// New creates a new PageStore.
func New(cfg pagestoreapi.Config, segMgr segmentapi.SegmentManager) pagestoreapi.PageStore {
	return internal.New(cfg, segMgr)
}
