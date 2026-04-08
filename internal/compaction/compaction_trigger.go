package compaction

import (
	"sync"
	"time"

	api "github.com/akzj/go-fast-kv/internal/compaction/api"
)

// compactionTrigger implements CompactionTrigger using space usage and time thresholds.
type compactionTrigger struct {
	mu               sync.RWMutex
	config           *api.CompactionConfig
	lastCompaction   time.Time
	usedBytes        uint64
	totalBytes       uint64
	lastTriggerCheck time.Time
}

// NewCompactionTrigger creates a new CompactionTrigger.
func NewCompactionTrigger(config *api.CompactionConfig) api.CompactionTrigger {
	if config == nil {
		config = api.DefaultCompactionConfig()
	}
	return &compactionTrigger{
		config: config,
	}
}

// Evaluate checks if compaction should run now.
func (t *compactionTrigger) Evaluate() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Check time interval
	interval := t.config.CompactionInterval
	if interval > 0 && time.Since(t.lastCompaction) < interval {
		return false
	}

	// Check space usage threshold
	if t.totalBytes > 0 {
		usage := float64(t.usedBytes) / float64(t.totalBytes)
		if usage >= t.config.SpaceUsageThreshold {
			return true
		}
	}

	return false
}

// LastCompactionTime returns when the last compaction completed.
func (t *compactionTrigger) LastCompactionTime() time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.lastCompaction
}

// UpdateSpaceUsage records current disk space usage.
func (t *compactionTrigger) UpdateSpaceUsage(usedBytes, totalBytes uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.usedBytes = usedBytes
	t.totalBytes = totalBytes
}

// RecordCompaction records that compaction just completed (used internally).
func (t *compactionTrigger) RecordCompaction() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lastCompaction = time.Now()
}
