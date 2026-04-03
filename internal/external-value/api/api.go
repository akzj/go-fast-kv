// Package externalvalue provides the public API for the External Value Store.
//
// This module stores values exceeding the inline threshold (48 bytes) in
// append-only segment storage, referenced by VAddr.
//
// Architecture:
//   - Values ≤ 48 bytes: stored inline in B-link tree leaf entries
//   - Values > 48 bytes: stored externally, VAddr stored inline
//
// Module boundaries:
//   - Public API: this file only
//   - External consumers: import this package
//   - Internal implementation: internal/ package (not importable by other modules)
package externalvalue

import (
	"errors"

	"github.com/akzj/go-fast-kv/internal/vaddr"
)

// =============================================================================
// Core Types
// =============================================================================

// VAddr encodes a physical address in the append-only address space.
// Invariant: VAddr is 16 bytes, never zero (SegmentID 0 is reserved).
//
// NOTE: This type alias imports VAddr from the shared vaddr module.
type VAddr = vaddr.VAddr

// =============================================================================
// Constants
// =============================================================================

const (
	// ExternalThreshold is the maximum size for inline values.
	// Values > 48 bytes are stored externally in the ExternalValueStore.
	// Why 48 bytes?
	//   - InlineValue.Data has 56 bytes available
	//   - When external: 1 byte length prefix + 16 bytes VAddr = 39 bytes wasted minimum
	//   - 48 bytes balances inline storage efficiency vs external store overhead
	ExternalThreshold = 48

	// ExternalValueHeaderSize is the size of the header prefix for each value.
	// Header: Magic(8) + Version(2) + ValueSize(8) + Reserved(4) = 32 bytes
	ExternalValueHeaderSize = 32

	// ExternalValueDataPerPage is the data bytes available per 4KB page.
	// PageSize(4096) - ExternalValueHeaderSize(32) = 4064 bytes
	ExternalValueDataPerPage = 4064

	// ExternalValueMagic is the magic number identifying external value pages.
	// Format: "EXTVAL" + 2 null bytes (8 bytes total).
	ExternalValueMagic = "EXTVAL\x00\x00"

	// ExternalValueVersion is the current format version.
	ExternalValueVersion = 1

	// DefaultMaxValueSize is the default maximum allowed value size.
	// 64 MB limit prevents runaway allocations.
	DefaultMaxValueSize = 64 * 1024 * 1024
)

// =============================================================================
// Errors
// =============================================================================

var (
	// ErrValueNotFound is returned when retrieving a non-existent VAddr.
	// Why not nil? Allows distinguishing "not found" from "invalid vaddr".
	ErrValueNotFound = errors.New("external value not found")

	// ErrValueTooLarge is returned when value exceeds MaxValueSize.
	ErrValueTooLarge = errors.New("value too large for external store")

	// ErrInvalidVAddr is returned when VAddr is invalid (zero or malformed).
	ErrInvalidVAddr = errors.New("invalid virtual address")

	// ErrCorruptedValue is returned when value data fails checksum verification.
	ErrCorruptedValue = errors.New("corrupted external value data")

	// ErrStoreClosed is returned when operating on a closed store.
	ErrStoreClosed = errors.New("external value store is closed")

	// ErrPartialRead is returned when RetrieveAt exceeds value boundaries.
	// Why not slice-panic? Callers may intentionally query partial ranges.
	ErrPartialRead = errors.New("partial read beyond value boundaries")
)

// =============================================================================
// ExternalValueStore Interface
// =============================================================================

// ExternalValueStore manages off-tree storage for large values.
// Invariant: All mutations are append-only; VAddrs are never reused.
// Thread-safe: implementations must handle concurrent Store/Retrieve calls.
type ExternalValueStore interface {
	// Store persists a value and returns its VAddr.
	// The returned VAddr can be used to Retrieve the value later.
	//
	// Store writes are idempotent with respect to VAddr allocation:
	// each call returns a unique VAddr, even if value content is identical.
	//
	// Why return VAddr instead of embedding in caller?
	//   - Decouples storage location from tree structure
	//   - Allows values to be moved during compaction without updating tree
	Store(value []byte) (VAddr, error)

	// Retrieve reads the complete value at the given VAddr.
	// Returns the full byte slice of the stored value.
	//
	// Why not streaming? Simpler API; callers can buffer if needed.
	// For very large values (64MB), use RetrieveAt for partial reads.
	Retrieve(vaddr VAddr) ([]byte, error)

	// RetrieveAt reads a slice of the value without loading the entire value.
	// Useful for large values where only a portion is needed.
	//
	// offset: byte offset from start of value data (0-indexed)
	// length: number of bytes to read
	//
	// Returns ErrPartialRead if offset+length exceeds value size.
	// Why not return truncated data? Explicit error forces caller awareness.
	RetrieveAt(vaddr VAddr, offset, length uint64) ([]byte, error)

	// Delete marks a value for future reclamation.
	// Actual space reclamation happens during compaction.
	//
	// Why not immediate reclamation?
	//   - Active readers may hold references to the value
	//   - Epoch-based GC requires grace period before reuse
	//
	// Delete is idempotent: deleting already-deleted value succeeds silently.
	Delete(vaddr VAddr) error

	// GetValueSize returns the size of the value at vaddr without loading data.
	// Performs O(1) lookup of metadata.
	//
	// Why not embed in Retrieve? Allows metadata-only access for pagination,
	// progress indicators, and range queries without I/O.
	GetValueSize(vaddr VAddr) (uint64, error)

	// Close releases resources held by the store.
	// After Close, all operations return ErrStoreClosed.
	Close() error
}

// =============================================================================
// Threshold Accessor
// =============================================================================

// Threshold returns the maximum size for inline values in the parent B-link tree.
// Values exceeding this threshold should be stored externally via Store().
//
// Why not a global constant? Allows per-instance threshold configuration
// while maintaining a consistent API across the module.
func Threshold() int {
	return ExternalThreshold
}

// =============================================================================
// Value Classification Utilities
// =============================================================================

// ShouldStoreExternally returns true if value should use external storage.
// Convenience function for callers deciding inline vs external storage.
//
// Why not inline this logic in B-link tree? ExternalValueStore is the
// authority on threshold; keeps classification logic centralized.
func ShouldStoreExternally(valueSize int) bool {
	return valueSize > ExternalThreshold
}

// InlineCapacity returns the maximum inline storage capacity.
// Alias for ExternalThreshold for clarity in inline storage context.
func InlineCapacity() int {
	return ExternalThreshold
}

// =============================================================================
// Metrics Interface (optional for monitoring)
// =============================================================================

// Metrics provides observability into ExternalValueStore operations.
// Implementations may expose these for monitoring/debugging.
//
// Why optional interface? Not all consumers need metrics; keeps core API clean.
type Metrics interface {
	// StoreCount returns the total number of Store operations.
	StoreCount() uint64

	// RetrieveCount returns the total number of Retrieve operations.
	RetrieveCount() uint64

	// ActiveValueCount returns the number of values not yet deleted.
	ActiveValueCount() uint64

	// TotalBytesStored returns the total bytes in active values.
	TotalBytesStored() uint64

	// DeletedBytes returns the total bytes in deleted (pending GC) values.
	DeletedBytes() uint64
}

// =============================================================================
// Builder for store configuration
// =============================================================================

// Config holds configuration for ExternalValueStore initialization.
type Config struct {
	// MaxValueSize is the maximum allowed value size.
	// Defaults to DefaultMaxValueSize (64 MB).
	// Values exceeding this return ErrValueTooLarge on Store.
	MaxValueSize uint64

	// SegmentSize is the target size for segment rotation.
	// When active segment exceeds this, a new segment is created.
	// Defaults to 1 GB.
	SegmentSize uint64

	// EnableMetrics enables Metrics interface.
	EnableMetrics bool
}

// DefaultConfig returns a Config with default values.
func DefaultConfig() Config {
	return Config{
		MaxValueSize: DefaultMaxValueSize,
		SegmentSize:  1 << 30, // 1 GB
		EnableMetrics: false,
	}
}
