// Package wal provides Write-Ahead Log for crash recovery.
// This module ensures durability by logging all mutations before applying them.
//
// Architecture:
//   - WAL is append-only: records are never overwritten
//   - Checkpoint creates consistent snapshots for fast recovery
//   - Redo-only recovery: append-only storage never needs undo
//
// Design invariants:
//   - All mutations are recorded in WAL before being applied to data
//   - Checkpoint LSN indicates all prior records are durable
//   - WAL can be truncated after checkpoint (old records no longer needed)
//
// Module boundaries:
//   - wal has NO dependencies on other internal packages
//   - Other modules depend on wal via interfaces defined here
package wal

import (
	"errors"
	"time"

	"github.com/akzj/go-fast-kv/internal/wal/internal"
)

// Re-export types from internal implementation
type (
	WALRecordType         = internal.WALRecordType
	WALRecord             = internal.WALRecord
	WAL                   = internal.WAL
	WALIterator           = internal.WALIterator
	CheckpointManager     = internal.CheckpointManager
	Checkpoint            = internal.Checkpoint
	PageManagerSnapshot    = internal.PageManagerSnapshot
	ExternalValueSnapshot  = internal.ExternalValueSnapshot
	WALConfig              = internal.WALConfig
	CheckpointConfig       = internal.CheckpointConfig
)

// Re-export constants
const (
	WALPageAlloc       = internal.WALPageAlloc
	WALPageFree       = internal.WALPageFree
	WALNodeWrite       = internal.WALNodeWrite
	WALExternalValue  = internal.WALExternalValue
	WALRootUpdate      = internal.WALRootUpdate
	WALCheckpoint      = internal.WALCheckpoint
	WALIndexUpdate     = internal.WALIndexUpdate
	WALIndexRootUpdate = internal.WALIndexRootUpdate
)

// =============================================================================
// Errors
// =============================================================================

var (
	ErrWALClosed           = errors.New("wal: WAL is closed")
	ErrWALCorrupted        = errors.New("wal: WAL record corrupted")
	ErrInvalidLSN          = errors.New("wal: invalid LSN")
	ErrTruncateLSNTooLarge = errors.New("wal: truncate LSN too large")
	ErrCheckpointInProgress = errors.New("wal: checkpoint in progress")
	ErrNoCheckpoint        = errors.New("wal: no checkpoint found")
	ErrRecoveryFailed      = errors.New("wal: recovery failed")
)

// =============================================================================
// Default Configuration
// =============================================================================

func DefaultConfig() WALConfig {
	return internal.WALConfig{
		SegmentSize: 64 * 1024 * 1024,
		SyncWrites:  true,
		BufferSize:  1 * 1024 * 1024,
	}
}

func DefaultCheckpointConfig() CheckpointConfig {
	return internal.CheckpointConfig{
		Interval:              30 * time.Second,
		WALSizeLimit:          64 * 1024 * 1024,
		DirtyPageLimit:        1000,
		MinCheckpointInterval: 5 * time.Second,
	}
}

// =============================================================================
// Factory Functions
// =============================================================================

// OpenWAL opens or creates a WAL.
func OpenWAL(config WALConfig) (WAL, error) {
	return internal.NewWAL(config)
}

// OpenCheckpointManager opens or creates a checkpoint manager.
func OpenCheckpointManager(wal WAL, config WALConfig, checkpointConfig CheckpointConfig) (CheckpointManager, error) {
	walimpl, ok := wal.(*internal.WALImpl)
	if !ok {
		return nil, errors.New("wal: invalid WAL implementation")
	}
	return internal.NewCheckpointManager(walimpl, config, checkpointConfig)
}
