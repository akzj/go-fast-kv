// Package objectstore provides object storage functionality.
// Root package re-exports types from api package for backward compatibility.
package objectstore

import "github.com/akzj/go-fast-kv/pkg/objectstore/api"

// Re-export types from api package
type (
	ObjectStore     = api.ObjectStore
	MappingIndex    = api.MappingIndex
	ObjectID        = api.ObjectID
	ObjectType      = api.ObjectType
	ObjectLocation  = api.ObjectLocation
	ObjectHeader    = api.ObjectHeader
	SegmentType     = api.SegmentType
	SegmentID       = api.SegmentID
	SegmentMeta     = api.SegmentMeta
	ObjectStoreError = api.ObjectStoreError
)

// Re-export constants
const (
	ObjectTypePage        = api.ObjectTypePage
	ObjectTypeBlob        = api.ObjectTypeBlob
	ObjectTypeLarge       = api.ObjectTypeLarge
	ObjectTypeMax         = api.ObjectTypeMax
	SegmentTypePage        = api.SegmentTypePage
	SegmentTypeBlob       = api.SegmentTypeBlob
	SegmentTypeLarge      = api.SegmentTypeLarge
	PageSize              = api.PageSize
	ObjectHeaderSize      = api.ObjectHeaderSize
	PageSegmentMaxSize     = api.PageSegmentMaxSize
	BlobSegmentMaxSize    = api.BlobSegmentMaxSize
	LargeBlobThreshold    = api.LargeBlobThreshold
	MagicByte1            = api.MagicByte1
	MagicByte2            = api.MagicByte2
	HeaderVersion         = api.HeaderVersion
	WALEntryTypeObjectStore = api.WALEntryTypeObjectStore
)

// Re-export errors
var (
	ErrObjectNotFound      = api.ErrObjectNotFound
	ErrInvalidObjectID     = api.ErrInvalidObjectID
	ErrSegmentFull         = api.ErrSegmentFull
	ErrInvalidSegment      = api.ErrInvalidSegment
	ErrChecksumMismatch    = api.ErrChecksumMismatch
	ErrSegmentTypeNotMatch = api.ErrSegmentTypeNotMatch
	ErrInvalidHeader       = api.ErrInvalidHeader
)

// Re-export functions
var (
	MakeObjectID        = api.MakeObjectID
)
