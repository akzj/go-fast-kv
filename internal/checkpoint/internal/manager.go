// Package checkpoint implements the CheckpointManager.
package internal

import (
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	checkpointapi "github.com/akzj/go-fast-kv/internal/checkpoint/api"
	walapi "github.com/akzj/go-fast-kv/internal/wal/api"
)

// Compile-time interface check.
var _ checkpointapi.CheckpointManager = (*CheckpointManager)(nil)

// CheckpointManager implements the two-phase checkpoint.
type CheckpointManager struct {
	mu      sync.Mutex
	dir     string
	wal     walapi.WAL
	closed  bool

	// Registered modules
	modules map[string]checkpointapi.Checkpointable

	// Current metadata (nil if no checkpoint exists)
	metadata *checkpointapi.Metadata

	// Phase 2 state
	phase2InProgress atomic.Bool
	phase2Wg         sync.WaitGroup // Tracks Phase 2 completion for Close coordination
}

// New creates a new CheckpointManager.
func New(dir string, wal walapi.WAL) (checkpointapi.CheckpointManager, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("checkpoint: mkdir: %w", err)
	}

	cm := &CheckpointManager{
		dir:     dir,
		wal:     wal,
		modules: make(map[string]checkpointapi.Checkpointable),
	}

	// Load existing metadata if any
	meta, err := readMetadata(dir)
	if err != nil && err != checkpointapi.ErrNoCheckpoint {
		return nil, fmt.Errorf("checkpoint: read metadata: %w", err)
	}
	cm.metadata = meta

	return cm, nil
}

// RegisterModule registers a module for checkpoint coordination.
func (cm *CheckpointManager) RegisterModule(name string, module checkpointapi.Checkpointable) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.modules[name] = module
}

// DoCheckpoint executes the two-phase checkpoint.
func (cm *CheckpointManager) DoCheckpoint() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Phase 1: Milliseconds, holds lock
	metadata, err := cm.doCheckpointPhase1()
	if err != nil {
		return fmt.Errorf("checkpoint phase 1: %w", err)
	}

	// Phase 2: Background, no lock held
	cm.phase2Wg.Add(1)
	go func() {
		defer cm.phase2Wg.Done()
		cm.doCheckpointPhase2(metadata)
	}()

	return nil
}

// doCheckpointPhase1 performs the fast phase of checkpoint.
// Caller must hold cm.mu.
func (cm *CheckpointManager) doCheckpointPhase1() (*checkpointapi.Metadata, error) {
	if cm.closed {
		return nil, fmt.Errorf("checkpoint: manager closed")
	}

	// 1. Get current WAL LSN
	currentLSN := cm.wal.CurrentLSN()

	// 2. Call Checkpoint(lsn) on all registered modules
	for name, module := range cm.modules {
		if err := module.Checkpoint(currentLSN); err != nil {
			return nil, fmt.Errorf("checkpoint module %s: %w", name, err)
		}
	}

	// 3. Build metadata
	meta := checkpointapi.NewMetadata()
	meta.Timestamp = time.Now().UTC().Format(time.RFC3339)

	for name, module := range cm.modules {
		meta.SetModule(name, checkpointapi.ModuleMetadata{
			CheckpointLSN: module.CheckpointLSN(),
		})
	}

	// 4. Write metadata.json
	if err := writeMetadata(cm.dir, meta); err != nil {
		return nil, fmt.Errorf("write metadata: %w", err)
	}

	cm.metadata = meta
	return meta, nil
}

// doCheckpointPhase2 performs the slow phase of checkpoint in background.
func (cm *CheckpointManager) doCheckpointPhase2(meta *checkpointapi.Metadata) {
	// Prevent concurrent phase 2
	if !cm.phase2InProgress.CompareAndSwap(false, true) {
		log.Println("checkpoint: phase 2 already in progress, skipping")
		return
	}
	defer cm.phase2InProgress.Store(false)

	// 1. Get min checkpoint LSN from all modules
	cm.mu.Lock()
	minLSN := uint64(^uint64(0)) // Max uint64
	for _, module := range cm.modules {
		lsn := module.CheckpointLSN()
		if lsn < minLSN {
			minLSN = lsn
		}
	}
	cm.mu.Unlock()

	// 2. Delete WAL segments before min_lsn
	if minLSN > 0 {
		if err := cm.wal.DeleteSegmentsBefore(minLSN); err != nil {
			log.Printf("checkpoint: WAL cleanup failed: %v", err)
		}
	}
}

// GetMetadata returns the current checkpoint metadata.
func (cm *CheckpointManager) GetMetadata() *checkpointapi.Metadata {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.metadata
}

// Close closes the checkpoint manager.
func (cm *CheckpointManager) Close() error {
	cm.mu.Lock()
	cm.closed = true
	cm.mu.Unlock()
	// Wait for any in-flight Phase 2 to complete before returning
	cm.phase2Wg.Wait()
	return nil
}
