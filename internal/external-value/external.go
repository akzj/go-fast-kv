// Package externalvalue provides the external-value module API.
package externalvalue

import (
	apipkg "github.com/akzj/go-fast-kv/internal/external-value/api"
	"github.com/akzj/go-fast-kv/internal/external-value/internal"
	"github.com/akzj/go-fast-kv/internal/storage"
)

// Re-export types and functions from api package.
type (
	ExternalValueStore = apipkg.ExternalValueStore
	VAddr             = apipkg.VAddr
	Metrics           = apipkg.Metrics
	Config            = apipkg.Config
)

// Re-export functions from internal package.
func NewExternalValueStore(segmentMgr storage.SegmentManager, config apipkg.Config) (apipkg.ExternalValueStore, error) {
	return internal.NewExternalValueStore(segmentMgr, config)
}

// Re-export helper functions from api package.
var (
	Threshold              = apipkg.Threshold
	ShouldStoreExternally = apipkg.ShouldStoreExternally
	InlineCapacity        = apipkg.InlineCapacity
	DefaultConfig         = apipkg.DefaultConfig
)

// Re-export error types.
var (
	ErrValueNotFound   = apipkg.ErrValueNotFound
	ErrValueTooLarge   = apipkg.ErrValueTooLarge
	ErrInvalidVAddr    = apipkg.ErrInvalidVAddr
	ErrCorruptedValue  = apipkg.ErrCorruptedValue
	ErrStoreClosed     = apipkg.ErrStoreClosed
	ErrPartialRead     = apipkg.ErrPartialRead
)
