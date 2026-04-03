// Package kvstore provides the top-level public API for the KV store.
package kvstore

import (
	"errors"
	"os"

	"github.com/akzj/go-fast-kv/internal/kvstore/internal"
)

// =============================================================================
// Type Aliases — re-export from internal implementation
// =============================================================================

// VAddr is the physical address type (16 bytes: SegmentID[8] + Offset[8]).
// Defined in: vaddr package
// type VAddr = vaddr.VAddr

// KVStore is the primary interface for key-value operations.
type KVStore = internal.KVStore

// KVStoreWithTransactions extends KVStore with transaction support.
type KVStoreWithTransactions = internal.KVStoreWithTransactions

// Transaction provides atomic multi-key operations.
type Transaction = internal.Transaction

// Iterator provides sequential access to a range of key-value pairs.
type Iterator = internal.Iterator

// Batch performs multiple operations atomically.
type Batch = internal.Batch

// BatchCreator creates batches for atomic multi-operation.
type BatchCreator = internal.BatchCreator

// Config holds KVStore initialization parameters.
type Config = internal.Config

// TransactionOptions controls transaction behavior.
type TransactionOptions = internal.TransactionOptions

// IsolationLevel controls transaction isolation.
type IsolationLevel = internal.IsolationLevel

// =============================================================================
// Constants
// =============================================================================

const (
	DefaultMaxKeySize      = internal.DefaultMaxKeySize
	DefaultMaxValueSize    = internal.DefaultMaxValueSize
	DefaultNodeSize        = internal.DefaultNodeSize
	DefaultCacheSizeMB     = 256
	DefaultSyncWrites      = internal.DefaultSyncWrites
)

// Isolation levels for transactions.
const (
	IsolationSnapshot IsolationLevel = iota
)

// =============================================================================
// Error Types
// =============================================================================

var (
	ErrKeyNotFound        = internal.ErrKeyNotFound
	ErrStoreClosed        = internal.ErrStoreClosed
	ErrTransactionAborted = internal.ErrTransactionAborted
	ErrStoreFull          = internal.ErrStoreFull
	ErrKeyTooLarge        = internal.ErrKeyTooLarge
	ErrValueTooLarge      = internal.ErrValueTooLarge
	ErrWriteLocked        = internal.ErrWriteLocked
	ErrReadOnly           = internal.ErrReadOnly
	ErrTransactionFull    = internal.ErrTransactionFull
	ErrBatchCommitted     = internal.ErrBatchCommitted
)

// =============================================================================
// Factory Functions
// =============================================================================

// Open creates or opens a KVStore at the given directory.
func Open(directory string, config *Config) (KVStore, error) {
	if config == nil {
		c := DefaultConfig()
		c.Directory = directory
		config = &c
	}
	if config.Directory == "" {
		config.Directory = directory
	}
	return internal.NewKVStore(*config)
}

// OpenWithTransactions creates a store with transaction support.
func OpenWithTransactions(directory string, config *Config) (KVStoreWithTransactions, error) {
	store, err := Open(directory, config)
	if err != nil {
		return nil, err
	}
	// The underlying store implements KVStoreWithTransactions
	if txStore, ok := store.(KVStoreWithTransactions); ok {
		return txStore, nil
	}
	return nil, errors.New("store does not support transactions")
}

// Destroy removes all storage files in directory.
func Destroy(directory string) error {
	if directory == "" {
		return errors.New("kvstore: directory required for Destroy")
	}
	return os.RemoveAll(directory)
}

// DefaultConfig returns a Config with default values.
func DefaultConfig() Config {
	return internal.DefaultConfig()
}

// =============================================================================
// Helper Functions
// =============================================================================

// IsNotFound is a helper for checking ErrKeyNotFound.
func IsNotFound(err error) bool {
	return errors.Is(err, ErrKeyNotFound)
}

// IsClosed is a helper for checking ErrStoreClosed.
func IsClosed(err error) bool {
	return errors.Is(err, ErrStoreClosed)
}
