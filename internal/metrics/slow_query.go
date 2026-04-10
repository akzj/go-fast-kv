// Package metrics provides observability metrics for go-fast-kv.
package metrics

import (
	"log"
	"os"
	"sync"
	"time"
)

// ─── Slow Query Logger ──────────────────────────────────────────

// SlowQueryLogger logs slow operations.
type SlowQueryLogger struct {
	mu         sync.Mutex
	threshold  time.Duration
	logger     *log.Logger
	enabled    bool
}

// NewSlowQueryLogger creates a new slow query logger.
func NewSlowQueryLogger(threshold time.Duration) *SlowQueryLogger {
	return &SlowQueryLogger{
		threshold: threshold,
		logger:    log.New(os.Stderr, "[slow-query] ", log.LstdFlags),
		enabled:   threshold > 0,
	}
}

// SetThreshold updates the slow query threshold.
func (l *SlowQueryLogger) SetThreshold(d time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.threshold = d
	l.enabled = d > 0
}

// Log logs a slow operation.
func (l *SlowQueryLogger) Log(op string, key string, latency time.Duration) {
	if !l.enabled {
		return
	}
	
	l.mu.Lock()
	defer l.mu.Unlock()
	
	if latency > l.threshold {
		l.logger.Printf("op=%s key=%q latency=%v threshold=%v", op, key, latency, l.threshold)
	}
}

// ─── Latency Tracker ────────────────────────────────────────────

// LatencyTracker tracks operation latencies for reporting.
type LatencyTracker struct {
	mu         sync.Mutex
	operations map[string]*OpStats
}

type OpStats struct {
	count   int64
	totalNs int64
	minNs   int64
	maxNs   int64
}

// NewLatencyTracker creates a new latency tracker.
func NewLatencyTracker() *LatencyTracker {
	return &LatencyTracker{
		operations: make(map[string]*OpStats),
	}
}

// Record records an operation latency.
func (t *LatencyTracker) Record(op string, latencyNs int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	
	stats, ok := t.operations[op]
	if !ok {
		stats = &OpStats{minNs: latencyNs, maxNs: latencyNs}
		t.operations[op] = stats
	}
	
	stats.count++
	stats.totalNs += latencyNs
	if latencyNs < stats.minNs {
		stats.minNs = latencyNs
	}
	if latencyNs > stats.maxNs {
		stats.maxNs = latencyNs
	}
}

// Report returns a report of all operation latencies.
func (t *LatencyTracker) Report() map[string]LatencyReport {
	t.mu.Lock()
	defer t.mu.Unlock()
	
	result := make(map[string]LatencyReport)
	for op, stats := range t.operations {
		var avg int64
		if stats.count > 0 {
			avg = stats.totalNs / stats.count
		}
		result[op] = LatencyReport{
			Count:    stats.count,
			AvgNs:    avg,
			MinNs:    stats.minNs,
			MaxNs:    stats.maxNs,
		}
	}
	return result
}

// LatencyReport is a latency report for an operation.
type LatencyReport struct {
	Count int64
	AvgNs int64
	MinNs int64
	MaxNs int64
}
