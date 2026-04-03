// Package kvstore provides a high-level key-value store that integrates
// ObjectStore, WAL, and BTree into a crash-safe database.
// Root package re-exports types from api package for backward compatibility.
package kvstore

import (
	"github.com/akzj/go-fast-kv/pkg/kvstore/api"
	"github.com/akzj/go-fast-kv/pkg/kvstore/internal"
)

// Re-export types from api package
type (
	KVStore = api.KVStore
	Config  = api.Config
	Stats   = api.Stats
)

// Re-export internal types
type (
	KVStoreImpl = internal.KVStoreImpl
)

// Re-export constants
const (
	PageSize        = api.PageSize
	InlineThreshold = api.InlineThreshold
)

// Open opens or creates a KVStore.
var Open = internal.Open
