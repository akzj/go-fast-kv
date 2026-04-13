// Package engine provides table and index CRUD operations mapped to KV storage.
package engine

import (
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
	encodingapi "github.com/akzj/go-fast-kv/internal/sql/encoding/api"
	"github.com/akzj/go-fast-kv/internal/sql/engine/api"
	"github.com/akzj/go-fast-kv/internal/sql/engine/internal"
)

// Re-export types for convenience.
type Row = api.Row
type RowIterator = api.RowIterator
type RowIDIterator = api.RowIDIterator

// Re-export errors.
var (
	ErrRowNotFound  = api.ErrRowNotFound
	ErrDuplicateKey = api.ErrDuplicateKey
	ErrTableIDNotSet = api.ErrTableIDNotSet
)

// NewTableEngine creates a new TableEngine.
func NewTableEngine(store kvstoreapi.Store, encoder encodingapi.KeyEncoder, codec encodingapi.RowCodec) api.TableEngine {
	return internal.NewTableEngine(store, encoder, codec)
}

// NewIndexEngine creates a new IndexEngine.
func NewIndexEngine(store kvstoreapi.Store, encoder encodingapi.KeyEncoder) api.IndexEngine {
	return internal.NewIndexEngine(store, encoder)
}
