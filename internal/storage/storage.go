// Package storage provides segment lifecycle management for append-only storage.
//
// Usage:
//
//	import "github.com/akzj/go-fast-kv/internal/storage"
//
// This package provides the factory functions by wiring to the internal implementation.
package storage

import (
	storageapi "github.com/akzj/go-fast-kv/internal/storage/api"
	"github.com/akzj/go-fast-kv/internal/storage/internal"
)

// Re-export all interfaces.
type (
	FileType       = storageapi.FileType
	FileOperations = storageapi.FileOperations
	Segment        = storageapi.Segment
	SegmentManager = storageapi.SegmentManager
)

// Re-export types.
type (
	StorageConfig = storageapi.StorageConfig
)

// Re-export constants.
const (
	FileTypeWAL            = storageapi.FileTypeWAL
	FileTypeSegment        = storageapi.FileTypeSegment
	FileTypeExternalValue  = storageapi.FileTypeExternalValue
	FileTypeIndex          = storageapi.FileTypeIndex
	FileTypeCheckpoint     = storageapi.FileTypeCheckpoint
)

// Re-export functions that don't need implementation.
var (
	NewSegment      = storageapi.NewSegment
	DefaultConfig   = storageapi.DefaultConfig
)

// Re-export errors.
var (
	ErrSegmentNotFound   = storageapi.ErrSegmentNotFound
	ErrSegmentNotActive  = storageapi.ErrSegmentNotActive
	ErrSegmentNotSealed  = storageapi.ErrSegmentNotSealed
	ErrSegmentFull       = storageapi.ErrSegmentFull
	ErrMaxSegments       = storageapi.ErrMaxSegments
	ErrStorageClosed     = storageapi.ErrStorageClosed
	ErrInvalidOffset     = storageapi.ErrInvalidOffset
	ErrInvalidSegmentID  = storageapi.ErrInvalidSegmentID
)

// OpenSegmentManager opens or creates a segment manager.
func OpenSegmentManager(config StorageConfig) (SegmentManager, error) {
	return internal.NewSegmentManager(internal.Config{
		Directory:       config.Directory,
		SegmentSize:     config.SegmentSize,
		MaxSegmentCount: config.MaxSegmentCount,
	})
}
