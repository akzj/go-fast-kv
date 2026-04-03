// Package api provides the public interfaces for kvstore module.
// This package contains ONLY interfaces, types, and constants - NO implementation.
// Invariant: Any concrete implementation MUST live in the internal/ package.
package api

import "context"

// =============================================================================
// Types
// =============================================================================

// Config holds KVStore configuration options.
type Config struct {
	Dir             string // Database directory
	WALBufferSize   int    // WAL buffer size (default: 4MB)
	BTreePageSize   uint32 // BTree page size (default: 4KB)
	InlineThreshold uint32 // Size threshold for inline storage (default: 512)
}

// Stats holds database statistics.
type Stats struct {
	LastLSN uint64
}

// =============================================================================
// Constants
// =============================================================================

const (
	// PageSize is the BTree page size (4KB).
	PageSize = 4096

	// InlineThreshold is the size threshold for inline storage.
	// Values < 512B are stored inline in BTree.
	// Values >= 512B are stored as blobs in ObjectStore.
	InlineThreshold = 512
)

// =============================================================================
// Errors
// =============================================================================

// KVStore-related errors are defined in the internal package.

// =============================================================================
// Interfaces
// =============================================================================

// KVStore is the main database type that integrates ObjectStore, WAL, and BTree.
// This interface is defined here for reference; the concrete type is in internal/.
type KVStore interface {
	// Put inserts or updates a key-value pair.
	Put(ctx context.Context, key []byte, value []byte) error

	// Get retrieves a value by key.
	Get(ctx context.Context, key []byte) ([]byte, bool, error)

	// Delete removes a key from the store.
	Delete(ctx context.Context, key []byte) error

	// Scan performs a range scan from start to end (inclusive).
	Scan(ctx context.Context, start []byte, end []byte, handler func(key, value []byte) bool) error

	// Sync flushes all pending writes to disk.
	Sync(ctx context.Context) error

	// Close closes the KVStore gracefully.
	Close() error

	// Stats returns current database statistics.
	Stats() (Stats, error)
}

// OpenFunc is the function type for opening a KVStore.
type OpenFunc func(ctx context.Context, cfg Config) (KVStore, error)
