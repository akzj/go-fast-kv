// Package checkpointapi defines the interface for the CheckpointManager.
package checkpointapi

import (
	"errors"
	"time"
)

// ─── Errors ─────────────────────────────────────────────────────────

var (
	// ErrCheckpointInProgress is returned when a checkpoint is already running.
	ErrCheckpointInProgress = errors.New("checkpoint: already in progress")

	// ErrNoCheckpoint is returned when no checkpoint exists.
	ErrNoCheckpoint = errors.New("checkpoint: no checkpoint found")
)

// ─── Module Metadata ─────────────────────────────────────────────────

// ModuleMetadata holds checkpoint information for a single module.
type ModuleMetadata struct {
	CheckpointLSN uint64 `json:"checkpoint_lsn"`
	// Module-specific fields stored as JSON
	Extra map[string]interface{} `json:"extra,omitempty"`
}

// ─── Checkpoint Metadata ─────────────────────────────────────────────

// Metadata represents the full checkpoint metadata.
type Metadata struct {
	Version   int                `json:"version"`
	Timestamp string             `json:"timestamp"`
	Modules   map[string]ModuleMetadata `json:"modules"`
}

// NewMetadata creates a new checkpoint metadata.
func NewMetadata() *Metadata {
	return &Metadata{
		Version:   1,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Modules:   make(map[string]ModuleMetadata),
	}
}

// GetModule returns the metadata for a specific module.
func (m *Metadata) GetModule(name string) (ModuleMetadata, bool) {
	mod, ok := m.Modules[name]
	return mod, ok
}

// SetModule sets the metadata for a specific module.
func (m *Metadata) SetModule(name string, meta ModuleMetadata) {
	m.Modules[name] = meta
}

// ─── Checkpointable Interface ─────────────────────────────────────────

// Checkpointable is implemented by modules that participate in checkpoint.
// All modules (LSM, Tree, Blob) must implement this interface.
type Checkpointable interface {
	// Checkpoint records the current checkpoint LSN for this module.
	// Called during Phase 1 of the two-phase checkpoint.
	Checkpoint(lsn uint64) error

	// CheckpointLSN returns the LSN of the last checkpoint.
	CheckpointLSN() uint64
}

// ─── CheckpointManager Interface ─────────────────────────────────────

// CheckpointManager coordinates the two-phase checkpoint process.
type CheckpointManager interface {
	// DoCheckpoint executes the two-phase checkpoint:
	//
	// Phase 1 (milliseconds, holds lock):
	//   1. Get current WAL LSN
	//   2. Call Checkpoint(lsn) on all registered modules
	//   3. Write metadata.json
	//   4. Release lock
	//
	// Phase 2 (background):
	//   1. Get min checkpoint LSN from all modules
	//   2. Call WAL.DeleteSegmentsBefore(min_lsn)
	//   3. Complete
	//
	// Returns error if Phase 1 fails. Phase 2 errors are logged.
	DoCheckpoint() error

	// GetMetadata returns the current checkpoint metadata.
	// Returns nil if no checkpoint exists.
	GetMetadata() *Metadata

	// RegisterModule registers a module for checkpoint coordination.
	RegisterModule(name string, module Checkpointable)

	// Close closes the checkpoint manager.
	Close() error
}

// ─── Config ─────────────────────────────────────────────────────────

// Config holds configuration for the CheckpointManager.
type Config struct {
	// Dir is the directory where checkpoint metadata is stored.
	Dir string
}
