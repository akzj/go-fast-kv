// Package lsmapi defines the interface for the LSM Store.
//
// LSM Store manages PageID→VAddr and BlobID→VAddr mappings.
// It uses a simplified LSM-tree with memtable + SSTable (no multi-level compaction).
//
// Design reference: docs/CHECKPOINT_DESIGN.md
package lsmapi

import "errors"

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

	// Blob mappings
	SetBlobMapping(blobID uint64, vaddr uint64, size uint32)
	GetBlobMapping(blobID uint64) (vaddr uint64, size uint32, ok bool)
	DeleteBlobMapping(blobID uint64)

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
