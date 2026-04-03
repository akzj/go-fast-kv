// Package btree provides B+Tree implementation.
// Root package re-exports types from api package for backward compatibility.
package btree

import (
	"github.com/akzj/go-fast-kv/pkg/btree/api"
	"github.com/akzj/go-fast-kv/pkg/btree/internal"
)

// Re-export types from api package
type (
	BTree       = api.BTree
	BTreeIter   = api.BTreeIter
	BTreeConfig = api.BTreeConfig
	PageID      = api.PageID
	BTreeValueFlag = api.BTreeValueFlag
	BTreeError  = api.BTreeError
)

// Re-export constants
const (
	BTreeValueInline = api.BTreeValueInline
	BTreeValueBlob   = api.BTreeValueBlob
)

// Re-export errors
var (
	ErrInvalidValue = api.ErrInvalidValue
)

// Re-export functions
var (
	DefaultBTreeConfig = api.DefaultBTreeConfig
)

// BTreeImpl is the concrete BTree implementation.
// Defined in internal/btree_impl.go.
type BTreeImpl = internal.BTreeImpl
