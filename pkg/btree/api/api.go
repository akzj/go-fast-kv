// Package api provides the public interfaces for btree module.
// This package contains ONLY interfaces, types, and constants - NO implementation.
// Invariant: Any concrete implementation MUST live in the internal/ package.
package api

import (
	"context"

	"github.com/akzj/go-fast-kv/pkg/objectstore/api"
)

// =============================================================================
// Types
// =============================================================================

// PageID is the virtual page ID of B+Tree.
type PageID uint64

// BTreeValueFlag indicates how B+Tree values are stored.
type BTreeValueFlag uint8

const (
	BTreeValueInline BTreeValueFlag = 0x00 // Value stored inline in node
	BTreeValueBlob   BTreeValueFlag = 0x01 // Value stored in ObjectStore
)

// BTreeConfig contains B+Tree configuration parameters.
type BTreeConfig struct {
	PageSize        uint32 // Default 4KB
	InlineThreshold uint32 // Default 512B
	MaxInlineSize   uint32
	Order           uint16
}

// DefaultBTreeConfig returns the default configuration.
func DefaultBTreeConfig() BTreeConfig {
	return BTreeConfig{
		PageSize:        4096,
		InlineThreshold: 512,
		MaxInlineSize:   512,
		Order:           256,
	}
}

// BTreeError represents a B+Tree error.
type BTreeError struct {
	msg string
}

func (e *BTreeError) Error() string {
	return e.msg
}

// BTree errors
var ErrInvalidValue = &BTreeError{msg: "invalid btree value format"}

// =============================================================================
// Interfaces
// =============================================================================

// BTree provides the key-value storage interface.
type BTree interface {
	// Get retrieves a value by key.
	Get(ctx context.Context, key []byte) ([]byte, bool, error)

	// Put inserts or updates a key-value pair.
	Put(ctx context.Context, key []byte, value []byte) error

	// Delete removes a key.
	Delete(ctx context.Context, key []byte) error

	// Scan performs a range scan.
	Scan(ctx context.Context, start []byte, end []byte, iterator func(key, value []byte) bool) error

	// CreateScanIter creates a scan iterator.
	CreateScanIter(start []byte, end []byte) (BTreeIter, error)

	// Load loads a page from ObjectStore by PageID.
	Load(ctx context.Context, pageID PageID) error

	// Flush flushes all dirty pages to ObjectStore.
	Flush(ctx context.Context) error

	// Close closes the BTree and releases resources.
	Close() error
}

// BTreeIter is the B+Tree scan iterator.
type BTreeIter interface {
	// Next returns the next key-value pair.
	// Returns (nil, nil, nil) when iteration is complete.
	Next() (key []byte, value []byte, err error)

	// Close closes the iterator.
	Close() error
}

// =============================================================================
// Marshal/Unmarshal helpers for BTree values
// =============================================================================

// MarshalBTreeValue marshals a value for BTree storage.
// Values < threshold are stored inline with flag byte.
// Values >= threshold are stored as blob references.
func MarshalBTreeValue(data []byte, threshold uint32) ([]byte, error) {
	if len(data) < int(threshold) {
		// Inline value: [flag:1][data:n]
		result := make([]byte, 1+len(data))
		result[0] = byte(BTreeValueInline)
		copy(result[1:], data)
		return result, nil
	}
	// Large value should be stored as blob, not marshaled here
	return nil, &BTreeError{msg: "value too large for inline storage"}
}

// UnmarshalBTreeValue unmarshals a BTree value.
// Returns (actualData, blobID, isBlob, error).
func UnmarshalBTreeValue(data []byte) ([]byte, api.ObjectID, bool, error) {
	if len(data) == 0 {
		return nil, 0, false, &BTreeError{msg: "empty value"}
	}

	flag := BTreeValueFlag(data[0])
	switch flag {
	case BTreeValueInline:
		// [flag:1][data:n]
		actualData := make([]byte, len(data)-1)
		copy(actualData, data[1:])
		return actualData, 0, false, nil
	case BTreeValueBlob:
		// [flag:1][blobID:8]
		if len(data) < 9 {
			return nil, 0, false, &BTreeError{msg: "invalid blob value: too short"}
		}
		blobID := api.ObjectID(getUint64BE(data[1:9]))
		return nil, blobID, true, nil
	default:
		return nil, 0, false, &BTreeError{msg: "invalid value flag"}
	}
}

// getUint64BE decodes uint64 from big-endian format.
func getUint64BE(b []byte) uint64 {
	return uint64(b[0])<<56 | uint64(b[1])<<48 | uint64(b[2])<<40 | uint64(b[3])<<32 |
		uint64(b[4])<<24 | uint64(b[5])<<16 | uint64(b[6])<<8 | uint64(b[7])
}

// =============================================================================
// Constructor Type
// =============================================================================

// NewBTreeFunc is the function type for creating a BTree instance.
// Why a function type? Allows internal package to hide concrete implementation.
type NewBTreeFunc func(store api.ObjectStore, config BTreeConfig) (*BTreeImpl, error)

// BTreeImpl is the concrete B+Tree implementation (exported for KVStore integration).
// Why exported? KVStore needs to create BTree directly.
// This is the only concrete type exposed; all other internals are private.
type BTreeImpl struct {
	// Implementation in internal package
}
