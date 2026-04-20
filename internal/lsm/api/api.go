// Package lsmapi defines the interface for the LSM Store.
//
// LSM Store manages PageID→VAddr and BlobID→VAddr mappings.
// It uses a simplified LSM-tree with memtable + SSTable (no multi-level compaction).
//
// Design reference: docs/CHECKPOINT_DESIGN.md
package lsmapi

import (
	"errors"

	segmentapi "github.com/akzj/go-fast-kv/internal/segment/api"
	walapi "github.com/akzj/go-fast-kv/internal/wal/api"
)

// ─── Errors ─────────────────────────────────────────────────────────

var (
	// ErrNotFound is returned when a key is not found.
	ErrNotFound = errors.New("lsm: key not found")

	// ErrClosed is returned when operating on a closed store.
	ErrClosed = errors.New("lsm: closed")
)

// ─── Mapping Types ───────────────────────────────────────────────────

// PageMapping represents a page ID to virtual address mapping.
type PageMapping struct {
	PageID uint64
	VAddr  uint64
}

// BlobMapping represents a blob ID to (vaddr, size) mapping.
type BlobMapping struct {
	BlobID uint64
	VAddr  uint64
	Size   uint32
}

// ─── Config ─────────────────────────────────────────────────────────

// Config holds configuration for the LSM Store.
type Config struct {
	// Dir is the directory where LSM files are stored.
	Dir string

	// MemtableSize is the maximum size of the memtable before flushing.
	// Default is 64MB.
	MemtableSize int64

	// CompactInterval is the interval between compaction checks in milliseconds.
	// Default is 1000ms (1 second).
	CompactInterval int64

	// SegmentSize is the size threshold for triggering compaction.
	// Default is 64MB.
	SegmentSize int64
}

// ─── Interface ─────────────────────────────────────────────────────

// MappingStore manages page and blob mappings using a simplified LSM tree.
type MappingStore interface {
	// Page mappings
	SetPageMapping(pageID uint64, vaddr uint64)
	GetPageMapping(pageID uint64) (vaddr uint64, ok bool)
	GetAllPageMappings() []walapi.Record // returns all mappings for checkpoint persistence

	// Blob mappings
	SetBlobMapping(blobID uint64, vaddr uint64, size uint32)
	GetBlobMapping(blobID uint64) (vaddr uint64, size uint32, ok bool)
	DeleteBlobMapping(blobID uint64)

	// WAL integration: attach WAL and flush pending entries
	SetWAL(wal walapi.WAL)
	FlushToWAL() (lastLSN uint64, err error)
	LastLSN() uint64

	// Checkpoint: record the checkpoint LSN
	Checkpoint(lsn uint64) error

	// CheckpointLSN returns the last checkpoint LSN.
	CheckpointLSN() uint64

	// MaybeCompact triggers compaction if needed.
	MaybeCompact() error

	// Close closes the store.
	Close() error
}

// RecoveryStore is used during crash recovery to rebuild the mapping store.
type RecoveryStore interface {
	// ApplyPageMapping applies a page mapping update.
	ApplyPageMapping(pageID uint64, vaddr uint64)

	// ApplyPageDelete applies a page deletion (marks page as removed).
	ApplyPageDelete(pageID uint64)

	// ApplyBlobMapping applies a blob mapping update.
	ApplyBlobMapping(blobID uint64, vaddr uint64, size uint32)

	// ApplyBlobDelete applies a blob deletion.
	ApplyBlobDelete(blobID uint64)

	// SetCheckpointLSN sets the checkpoint LSN.
	SetCheckpointLSN(lsn uint64)

	// SetNextSegmentID sets the next segment ID for SSTable naming.
	SetNextSegmentID(id uint64)

	// Build rebuilds the in-memory structures from the applied records.
	Build() error
}

// Manifest provides access to SSTable segment management for checkpoint.
// Checkpoint pins segments during state capture to prevent GC from deleting them.
type Manifest interface {
	// PinAll atomically increments refcount for all current segments.
	// Returns the list of pinned segment names.
	PinAll() []string

	// UnpinAll atomically decrements refcount for the given segments.
	UnpinAll(names []string)

	// CanDelete returns true if a segment's refcount is 0.
	CanDelete(name string) bool

	// TryDelete atomically checks refcount and removes the segment if 0.
	// This prevents the race where checkpoint pins a segment between
	// CanDelete() returning true and RemoveSegment() being called.
	// Returns true if the segment was deleted, false if pinned (refcount > 0).
	// The segment manager's RemoveSegment is called to delete the file.
	TryDelete(segMgr segmentapi.SegmentManager, segID uint32) bool

	// GetSegmentName returns the filename for a given segment ID.
	// Used by GC to construct the segment name before checking CanDelete.
	GetSegmentName(segID uint64) string
}
