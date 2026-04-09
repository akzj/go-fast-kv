// Package kvstore provides the top-level KVStore — the user-facing API
// for key-value operations with MVCC, checkpoint, and crash recovery.
//
// Design reference: docs/DESIGN.md §3.6
package kvstore

import (
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
	"github.com/akzj/go-fast-kv/internal/kvstore/internal"
)

// Open creates and opens a KVStore at the given directory.
func Open(cfg kvstoreapi.Config) (kvstoreapi.Store, error) {
	return internal.Open(cfg)
}
