// Package wal provides Write-Ahead Log for crash recovery.
//
// Usage:
//
//	import "github.com/akzj/go-fast-kv/internal/wal"
//
// This package re-exports all public interfaces from the api package.
package wal

import walapi "github.com/akzj/go-fast-kv/internal/wal/api"

// Re-export all interfaces.
type (
    WALRecordType     = walapi.WALRecordType
    WALRecord         = walapi.WALRecord
    WAL               = walapi.WAL
    WALIterator       = walapi.WALIterator
    CheckpointManager = walapi.CheckpointManager
    Checkpoint        = walapi.Checkpoint
)

// Re-export types.
type (
    WALConfig         = walapi.WALConfig
    CheckpointConfig  = walapi.CheckpointConfig
    PageManagerSnapshot = walapi.PageManagerSnapshot
    ExternalValueSnapshot = walapi.ExternalValueSnapshot
)

// Re-export constants.
const (
    WALPageAlloc        = walapi.WALPageAlloc
    WALPageFree         = walapi.WALPageFree
    WALNodeWrite        = walapi.WALNodeWrite
    WALExternalValue    = walapi.WALExternalValue
    WALRootUpdate       = walapi.WALRootUpdate
    WALCheckpoint       = walapi.WALCheckpoint
    WALIndexUpdate      = walapi.WALIndexUpdate
    WALIndexRootUpdate  = walapi.WALIndexRootUpdate
)

// Re-export functions.
var (
    OpenWAL                 = walapi.OpenWAL
    OpenCheckpointManager   = walapi.OpenCheckpointManager
    DefaultConfig           = walapi.DefaultConfig
    DefaultCheckpointConfig = walapi.DefaultCheckpointConfig
)

// Re-export errors.
var (
    ErrWALClosed            = walapi.ErrWALClosed
    ErrWALCorrupted         = walapi.ErrWALCorrupted
    ErrInvalidLSN           = walapi.ErrInvalidLSN
    ErrTruncateLSNTooLarge  = walapi.ErrTruncateLSNTooLarge
    ErrCheckpointInProgress = walapi.ErrCheckpointInProgress
    ErrNoCheckpoint         = walapi.ErrNoCheckpoint
    ErrRecoveryFailed       = walapi.ErrRecoveryFailed
)
