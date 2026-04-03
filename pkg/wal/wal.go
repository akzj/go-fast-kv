// Package wal provides Write-Ahead Logging functionality.
// Root package re-exports types from api package for backward compatibility.
package wal

import "github.com/akzj/go-fast-kv/pkg/wal/api"
import "github.com/akzj/go-fast-kv/pkg/wal/internal"

// Re-export types from api package
type (
	WAL         = api.WAL
	WALEntry    = api.WALEntry
	WALEntryType = api.WALEntryType
	LSN         = api.LSN
	WALStats    = api.WALStats
	ObjectStoreWALPayload = api.ObjectStoreWALPayload
	BTreeWALPayload       = api.BTreeWALPayload
)

// Re-export constants
const (
	WALEntryTypeObjectStore = api.WALEntryTypeObjectStore
	WALEntryTypeBTree       = api.WALEntryTypeBTree
	WALEntryTypeCheckpoint  = api.WALEntryTypeCheckpoint
	WALFilePrefix           = api.WALFilePrefix
	WALFileExt              = api.WALFileExt
	DefaultBufferSize       = api.DefaultBufferSize
	MaxWALFileSize          = api.MaxWALFileSize
)

// NewWAL creates a new WAL instance.
// Delegates to internal package implementation.
var NewWAL = api.NewWALFunc(func(dir string, bufferSize int) (api.WAL, error) {
	return internal.NewWAL(dir, bufferSize)
})
