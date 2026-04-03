// Package api provides the public interfaces for wal module.
// This package contains ONLY interfaces, types, and constants - NO implementation.
// Invariant: Any concrete implementation MUST live in the internal/ package.
package api

import "context"

// =============================================================================
// Types
// =============================================================================

// WALEntryType defines the operation type of a WAL entry.
// Used to distinguish operations from different modules (ObjectStore, B+Tree, etc.).
type WALEntryType uint8

const (
	WALEntryTypeObjectStore WALEntryType = 0x01 // ObjectStore operation
	WALEntryTypeBTree       WALEntryType = 0x02 // B+Tree operation
	WALEntryTypeCheckpoint  WALEntryType = 0xFF // Checkpoint marker
)

// WALEntry is the basic write unit of WAL.
// Layout:
//   - Type[1]: WALEntryType
//   - Length[4]: Entry total length (excluding Length field itself)
//   - Payload[n]: Module-specific payload data
//
// Invariant: Each entry is atomically written; no cross-entry data.
type WALEntry struct {
	Type    WALEntryType
	Payload []byte // Serialized payload data
}

// ObjectStoreWALPayload is the WAL payload for ObjectStore module operations.
// Used for ObjectStore operation persistence and replay.
type ObjectStoreWALPayload struct {
	Operation uint8 // 0=Alloc, 1=Write, 2=Delete
	ObjectID  uint64
	Location  struct {
		SegmentID uint64
		Offset    uint32
		Size      uint32
	}
}

// BTreeWALPayload is the WAL payload for B+Tree module operations.
type BTreeWALPayload struct {
	Operation uint8 // 0=Put, 1=Delete
	PageID    uint64 // B+Tree virtual page_id
	Key       []byte
	Value     []byte
}

// LSN (Log Sequence Number) is the unique sequential number of a WAL entry.
// Monotonically increasing, used for positioning and replay.
type LSN uint64

// WALStats contains WAL statistics.
type WALStats struct {
	TotalEntries uint64 // Total entry count
	TotalBytes   uint64 // Total bytes
	LastLSN      LSN    // Last LSN
	FileCount    int    // WAL file count
}

// =============================================================================
// Constants
// =============================================================================

const (
	// WALFilePrefix is the WAL file name prefix.
	WALFilePrefix = "wal"

	// WALFileExt is the WAL file extension.
	WALFileExt = ".wal"

	// DefaultBufferSize is the default WAL buffer size (4MB).
	DefaultBufferSize = 4 * 1024 * 1024

	// MaxWALFileSize is the maximum size for a single WAL file (256MB).
	MaxWALFileSize = 256 * 1024 * 1024
)

// =============================================================================
// Errors
// =============================================================================

// WAL-related errors are defined in the internal package.
// API package defines only structural types.

// =============================================================================
// Interfaces
// =============================================================================

// WAL provides the Write-Ahead Logging interface.
// Design principle: Batch fsync for performance; modules distinguish by Type.
type WAL interface {
	// Write appends a WAL entry.
	// Entry is written to memory buffer first, not flushed to disk until Sync.
	// Invariant: Entry type must match payload data.
	Write(entry *WALEntry) error

	// Sync flushes all entries in buffer to disk and fsyncs.
	// Returns the LSN after sync.
	// Why not auto-Sync per entry? Batch fsync significantly improves performance.
	Sync(ctx context.Context) (uint64, error)

	// Checkpoint marks a checkpoint.
	// Writes current Mapping Index snapshot to WAL.
	// Returns the checkpoint LSN.
	Checkpoint(ctx context.Context) (uint64, error)

	// Replay replays all WAL entries after the specified LSN.
	// Used for crash recovery.
	// handler: (entry *WALEntry) error, returning error stops replay.
	Replay(ctx context.Context, sinceLSN uint64, handler func(entry *WALEntry) error) error

	// GetLastLSN returns the LSN of the last valid entry.
	// Used to determine where to start replay after crash.
	GetLastLSN() uint64

	// Close closes the WAL and releases resources.
	Close() error
}

// =============================================================================
// Constructor Type
// =============================================================================

// NewWALFunc is the function type for creating a WAL instance.
// Why a function type? Allows internal package to hide concrete implementation.
type NewWALFunc func(dir string, bufferSize int) (WAL, error)
