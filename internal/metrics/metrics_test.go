package metrics

import (
	"testing"
)

func TestMetricsGlobal(t *testing.T) {
	m := Global()
	if m == nil {
		t.Fatal("Global() returned nil")
	}
}

func TestMetricsWAL(t *testing.T) {
	m := &Metrics{}
	
	m.SetWALSize(1024)
	if m.WALSizeBytes.Load() != 1024 {
		t.Errorf("expected 1024, got %d", m.WALSizeBytes.Load())
	}
	
	m.SetWALSegments(5)
	if m.WALSegments.Load() != 5 {
		t.Errorf("expected 5, got %d", m.WALSegments.Load())
	}
	
	m.WALWrite(100)
	if m.WALWriteOps.Load() != 1 {
		t.Errorf("expected 1, got %d", m.WALWriteOps.Load())
	}
	if m.WALWriteBytes.Load() != 100 {
		t.Errorf("expected 100, got %d", m.WALWriteBytes.Load())
	}
	
	m.WALFsync()
	if m.WALFsyncOps.Load() != 1 {
		t.Errorf("expected 1, got %d", m.WALFsyncOps.Load())
	}
}

func TestMetricsCompaction(t *testing.T) {
	m := &Metrics{}
	
	m.Compaction(1024 * 1024)
	if m.CompactionOps.Load() != 1 {
		t.Errorf("expected 1, got %d", m.CompactionOps.Load())
	}
	if m.CompactionBytes.Load() != 1024*1024 {
		t.Errorf("expected 1MB, got %d", m.CompactionBytes.Load())
	}
	
	m.MemtableFlush(1024)
	if m.MemtableFlushes.Load() != 1 {
		t.Errorf("expected 1, got %d", m.MemtableFlushes.Load())
	}
}

func TestMetricsTxn(t *testing.T) {
	m := &Metrics{}
	
	m.TxnStarted()
	if m.ActiveTxns.Load() != 1 {
		t.Errorf("expected 1, got %d", m.ActiveTxns.Load())
	}
	
	m.TxnStarted()
	if m.ActiveTxns.Load() != 2 {
		t.Errorf("expected 2, got %d", m.ActiveTxns.Load())
	}
	
	m.TxnEnded(true) // commit
	if m.ActiveTxns.Load() != 1 {
		t.Errorf("expected 1, got %d", m.ActiveTxns.Load())
	}
	if m.TxnCommits.Load() != 1 {
		t.Errorf("expected 1, got %d", m.TxnCommits.Load())
	}
	
	m.TxnEnded(false) // abort
	if m.ActiveTxns.Load() != 0 {
		t.Errorf("expected 0, got %d", m.ActiveTxns.Load())
	}
	if m.TxnAborts.Load() != 1 {
		t.Errorf("expected 1, got %d", m.TxnAborts.Load())
	}
	
	m.SSIConflict()
	if m.SSIConflicts.Load() != 1 {
		t.Errorf("expected 1, got %d", m.SSIConflicts.Load())
	}
}

func TestMetricsOps(t *testing.T) {
	m := &Metrics{}
	
	m.Get(1000)
	if m.GetOps.Load() != 1 {
		t.Errorf("expected 1, got %d", m.GetOps.Load())
	}
	if m.GetLatencyNs.Load() != 1000 {
		t.Errorf("expected 1000, got %d", m.GetLatencyNs.Load())
	}
	
	m.Put(2000)
	if m.PutOps.Load() != 1 {
		t.Errorf("expected 1, got %d", m.PutOps.Load())
	}
	if m.PutLatencyNs.Load() != 2000 {
		t.Errorf("expected 2000, got %d", m.PutLatencyNs.Load())
	}
	
	m.Delete(1500)
	if m.DeleteOps.Load() != 1 {
		t.Errorf("expected 1, got %d", m.DeleteOps.Load())
	}
}

func TestMetricsSnapshot(t *testing.T) {
	m := &Metrics{}
	m.SetWALSize(1024)
	m.SetWALSegments(3)
	m.Get(100)
	
	snap := m.Snapshot()
	
	if snap.WALSizeBytes != 1024 {
		t.Errorf("expected 1024, got %d", snap.WALSizeBytes)
	}
	if snap.WALSegments != 3 {
		t.Errorf("expected 3, got %d", snap.WALSegments)
	}
	if snap.GetOps != 1 {
		t.Errorf("expected 1, got %d", snap.GetOps)
	}
}
