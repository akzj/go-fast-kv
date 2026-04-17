// Package pagestore provides the PageStore — page-level storage with
// LSM-backed mapping from PageID to VAddr.
//
// Design reference: docs/DESIGN.md §3.3
package pagestore

import (
	lsmapi "github.com/akzj/go-fast-kv/internal/lsm/api"
	pagestoreapi "github.com/akzj/go-fast-kv/internal/pagestore/api"
	"github.com/akzj/go-fast-kv/internal/pagestore/internal"
	segmentapi "github.com/akzj/go-fast-kv/internal/segment/api"
)

// New creates a new PageStore backed by an LSM MappingStore for page→VAddr mapping.
func New(cfg pagestoreapi.Config, segMgr segmentapi.SegmentManager, lsmStore lsmapi.MappingStore) pagestoreapi.PageStore {
	return internal.New(cfg, segMgr, lsmStore)
}
