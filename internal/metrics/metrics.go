// Package metrics provides observability metrics for go-fast-kv.
package metrics

import (
	"sync/atomic"
	"time"
)

// ─── Metrics ─────────────────────────────────────────────────────

// Metrics holds all observable metrics for the KV store.
type Metrics struct {
	// WAL metrics
	WALSizeBytes    atomic.Int64
	WALSegments     atomic.Int64
	WALWriteOps     atomic.Int64
	WALWriteBytes   atomic.Int64
	WALFsyncOps     atomic.Int64

	// Compaction metrics
	CompactionOps   atomic.Int64
	CompactionBytes atomic.Int64
	MemtableFlushes atomic.Int64
	MemtableSizeBytes atomic.Int64

	// Transaction metrics
	ActiveTxns     atomic.Int64
	TxnCommits     atomic.Int64
	TxnAborts      atomic.Int64
	SSIConflicts   atomic.Int64

	// Operation metrics
	GetOps          atomic.Int64
	GetLatencyNs    atomic.Int64
	PutOps          atomic.Int64
	PutLatencyNs    atomic.Int64
	DeleteOps       atomic.Int64
	DeleteLatencyNs atomic.Int64
	SlowQueries     atomic.Int64

	// Slow query threshold (0 = disabled)
	SlowQueryThreshold time.Duration
}

// Global metrics instance.
var global = &Metrics{}

// Global returns the global metrics instance.
func Global() *Metrics {
	return global
}

// ─── WAL ────────────────────────────────────────────────────────

// SetWALSize sets the current WAL size in bytes.
func (m *Metrics) SetWALSize(bytes int64) {
	m.WALSizeBytes.Store(bytes)
}

// SetWALSegments sets the number of WAL segments.
func (m *Metrics) SetWALSegments(count int64) {
	m.WALSegments.Store(count)
}

// WALWrite records a WAL write operation.
func (m *Metrics) WALWrite(bytes int64) {
	m.WALWriteOps.Add(1)
	m.WALWriteBytes.Add(bytes)
}

// WALFsync records a WAL fsync operation.
func (m *Metrics) WALFsync() {
	m.WALFsyncOps.Add(1)
}

// ─── Compaction ─────────────────────────────────────────────────

// Compaction records a compaction operation.
func (m *Metrics) Compaction(bytes int64) {
	m.CompactionOps.Add(1)
	m.CompactionBytes.Add(bytes)
}

// MemtableFlush records a memtable flush operation.
func (m *Metrics) MemtableFlush(bytes int64) {
	m.MemtableFlushes.Add(1)
	m.MemtableSizeBytes.Store(bytes)
}

// SetMemtableSize sets the current memtable size in bytes.
func (m *Metrics) SetMemtableSize(bytes int64) {
	m.MemtableSizeBytes.Store(bytes)
}

// ─── Transaction ────────────────────────────────────────────────

// TxnStarted records a new active transaction.
func (m *Metrics) TxnStarted() {
	m.ActiveTxns.Add(1)
}

// TxnEnded records a transaction ending (committed or aborted).
func (m *Metrics) TxnEnded(committed bool) {
	if committed {
		m.TxnCommits.Add(1)
	} else {
		m.TxnAborts.Add(1)
	}
	m.ActiveTxns.Add(-1)
}

// SSIConflict records an SSI serialization conflict.
func (m *Metrics) SSIConflict() {
	m.SSIConflicts.Add(1)
}

// ─── Operations ─────────────────────────────────────────────────

// Get records a GET operation with latency.
func (m *Metrics) Get(latencyNs int64) {
	m.GetOps.Add(1)
	m.GetLatencyNs.Add(latencyNs)
	m.checkSlowQuery("Get", latencyNs)
}

// Put records a PUT operation with latency.
func (m *Metrics) Put(latencyNs int64) {
	m.PutOps.Add(1)
	m.PutLatencyNs.Add(latencyNs)
	m.checkSlowQuery("Put", latencyNs)
}

// Delete records a DELETE operation with latency.
func (m *Metrics) Delete(latencyNs int64) {
	m.DeleteOps.Add(1)
	m.DeleteLatencyNs.Add(latencyNs)
	m.checkSlowQuery("Delete", latencyNs)
}

// SlowQuery records a slow query.
func (m *Metrics) SlowQuery() {
	m.SlowQueries.Add(1)
}

// checkSlowQuery checks if the operation is slow and records it.
func (m *Metrics) checkSlowQuery(op string, latencyNs int64) {
	if m.SlowQueryThreshold > 0 {
		if latencyNs > m.SlowQueryThreshold.Nanoseconds() {
			m.SlowQueries.Add(1)
		}
	}
}

// SetSlowQueryThreshold sets the slow query threshold.
func (m *Metrics) SetSlowQueryThreshold(d time.Duration) {
	m.SlowQueryThreshold = d
}

// ─── Snapshot ───────────────────────────────────────────────────

// Snapshot returns a point-in-time snapshot of all metrics.
func (m *Metrics) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		WALSizeBytes:       m.WALSizeBytes.Load(),
		WALSegments:        m.WALSegments.Load(),
		WALWriteOps:        m.WALWriteOps.Load(),
		WALWriteBytes:      m.WALWriteBytes.Load(),
		WALFsyncOps:        m.WALFsyncOps.Load(),
		CompactionOps:       m.CompactionOps.Load(),
		CompactionBytes:     m.CompactionBytes.Load(),
		MemtableFlushes:    m.MemtableFlushes.Load(),
		MemtableSizeBytes:  m.MemtableSizeBytes.Load(),
		ActiveTxns:         m.ActiveTxns.Load(),
		TxnCommits:          m.TxnCommits.Load(),
		TxnAborts:          m.TxnAborts.Load(),
		SSIConflicts:        m.SSIConflicts.Load(),
		GetOps:             m.GetOps.Load(),
		PutOps:             m.PutOps.Load(),
		DeleteOps:          m.DeleteOps.Load(),
		SlowQueries:        m.SlowQueries.Load(),
	}
}

// MetricsSnapshot is a point-in-time snapshot of metrics.
type MetricsSnapshot struct {
	WALSizeBytes       int64
	WALSegments        int64
	WALWriteOps        int64
	WALWriteBytes      int64
	WALFsyncOps        int64
	CompactionOps       int64
	CompactionBytes     int64
	MemtableFlushes    int64
	MemtableSizeBytes  int64
	ActiveTxns         int64
	TxnCommits          int64
	TxnAborts          int64
	SSIConflicts        int64
	GetOps             int64
	PutOps             int64
	DeleteOps          int64
	SlowQueries        int64
}
