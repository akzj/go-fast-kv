// Package internal provides the GC controller implementation.
package internal

import (
	"context"
	"testing"

	"github.com/akzj/go-fast-kv/pkg/gc/api"
	objstore "github.com/akzj/go-fast-kv/pkg/objectstore/api"
)

// mockObjectStore implements a minimal ObjectStore for GC testing.
type mockObjectStore struct {
	segmentIDs  []uint64
	segmentMeta map[uint64]*mockSegmentMeta
}

type mockSegmentMeta struct {
	segmentType  uint8 // 0=Page, 1=Blob, 2=Large
	garbageSize  uint64
	totalSize    uint64
	liveCount    uint64
	garbageCount uint64
}

func newMockObjectStore() *mockObjectStore {
	return &mockObjectStore{
		segmentIDs: []uint64{1, 2, 3},
		segmentMeta: map[uint64]*mockSegmentMeta{
			1: {segmentType: 0, garbageSize: 10 * 1024 * 1024, totalSize: 64 * 1024 * 1024, liveCount: 100, garbageCount: 50},
			2: {segmentType: 1, garbageSize: 150 * 1024 * 1024, totalSize: 256 * 1024 * 1024, liveCount: 200, garbageCount: 100},
			3: {segmentType: 2, garbageSize: 0, totalSize: 256 * 1024 * 1024, liveCount: 0, garbageCount: 0},
		},
	}
}

var _ objstore.ObjectStore = (*mockObjectStore)(nil)

func (m *mockObjectStore) AllocPage(ctx context.Context) (objstore.ObjectID, error) {
	return objstore.ObjectID(0), nil
}

func (m *mockObjectStore) WritePage(ctx context.Context, id objstore.ObjectID, data []byte) (objstore.ObjectID, error) {
	return id, nil
}

func (m *mockObjectStore) ReadPage(ctx context.Context, id objstore.ObjectID) ([]byte, error) {
	return nil, nil
}

func (m *mockObjectStore) DeletePage(ctx context.Context, id objstore.ObjectID) error {
	return nil
}

func (m *mockObjectStore) WriteBlob(ctx context.Context, data []byte) (objstore.ObjectID, error) {
	return objstore.ObjectID(0), nil
}

func (m *mockObjectStore) ReadBlob(ctx context.Context, id objstore.ObjectID) ([]byte, error) {
	return nil, nil
}

func (m *mockObjectStore) DeleteBlob(ctx context.Context, id objstore.ObjectID) error {
	return nil
}

func (m *mockObjectStore) Sync(ctx context.Context) error {
	return nil
}

func (m *mockObjectStore) Close() error {
	return nil
}

func (m *mockObjectStore) Delete(ctx context.Context, id objstore.ObjectID) error {
	return nil
}

func (m *mockObjectStore) GetLocation(ctx context.Context, id objstore.ObjectID) (objstore.ObjectLocation, error) {
	return objstore.ObjectLocation{}, nil
}

func (m *mockObjectStore) GetSegmentIDs(ctx context.Context) []uint64 {
	return m.segmentIDs
}

func (m *mockObjectStore) GetSegmentType(ctx context.Context, segID uint64) objstore.SegmentType {
	if meta, ok := m.segmentMeta[segID]; ok {
		return objstore.SegmentType(meta.segmentType)
	}
	return objstore.SegmentTypePage
}

func (m *mockObjectStore) GetSegmentMeta(ctx context.Context, segID uint64) (*objstore.SegmentMeta, error) {
	if meta, ok := m.segmentMeta[segID]; ok {
		return &objstore.SegmentMeta{
			SegmentID:     segID,
			GarbageSize:   meta.garbageSize,
			TotalSize:     meta.totalSize,
			LiveSize:      meta.totalSize - meta.garbageSize,
			GarbageCount:  meta.garbageCount,
			LiveCount:     meta.liveCount,
		}, nil
	}
	return nil, objstore.ErrObjectNotFound
}

func (m *mockObjectStore) CompactSegment(ctx context.Context, segID uint64) error {
	return nil
}

func (m *mockObjectStore) DeleteSegment(ctx context.Context, segID uint64) error {
	return nil
}

func (m *mockObjectStore) MarkObjectDeleted(ctx context.Context, id objstore.ObjectID, size uint32) {
}

func (m *mockObjectStore) GetActiveSegmentID(ctx context.Context, segType objstore.SegmentType) (uint64, error) {
	return 1, nil
}

// TestModRateStabilityChecker tests the stability checker.
func TestModRateStabilityChecker(t *testing.T) {
	// Test 1: New checker should be stable (no data)
	checker := NewModRateStabilityChecker(10, 0.1)
	if !checker.IsStable() {
		t.Error("New checker should be stable")
	}

	// Test 2: Add samples below threshold - should be stable
	for i := 0; i < 10; i++ {
		checker.AddSample(0.01)
	}
	if !checker.IsStable() {
		t.Error("Low variance samples should be stable")
	}

	// Test 3: Clear should reset
	checker.Clear()
	if checker.SampleCount() != 0 {
		t.Errorf("Clear: expected 0 samples, got %d", checker.SampleCount())
	}

	// Test 4: Variance calculation
	checker.AddSample(0.0)
	checker.AddSample(0.1)
	v := checker.Variance()
	if v < 0 || v > 0.01 {
		t.Errorf("Variance: expected ~0.0025, got %f", v)
	}

	// Test 5: SampleCount
	if checker.SampleCount() != 2 {
		t.Errorf("SampleCount: expected 2, got %d", checker.SampleCount())
	}
}

// TestGCControllerShouldCompactPageSegment tests page segment compaction logic.
func TestGCControllerShouldCompactPageSegment(t *testing.T) {
	store := newMockObjectStore()
	policy := api.DefaultGCPolicy()
	gc := NewGCController(store, policy)

	// Test: below threshold should not compact
	if gc.ShouldCompactPageSegment(1, 0.3) {
		t.Error("30% should not trigger compaction (threshold 40%)")
	}

	// Test: above threshold should compact
	if !gc.ShouldCompactPageSegment(1, 0.5) {
		t.Error("50% should trigger compaction (threshold 40%)")
	}
}

// TestGCControllerShouldCompactBlobSegment tests blob segment compaction logic.
func TestGCControllerShouldCompactBlobSegment(t *testing.T) {
	store := newMockObjectStore()
	policy := api.DefaultGCPolicy()
	gc := NewGCController(store, policy)

	// Test: below garbage threshold should not compact
	if gc.ShouldCompactBlobSegment(1, 0.3, 0.05) {
		t.Error("30% garbage should not trigger (threshold 50%)")
	}

	// Test: above garbage, but high mod rate should not compact
	if gc.ShouldCompactBlobSegment(1, 0.6, 0.2) {
		t.Error("60% garbage + 20% mod rate should not trigger")
	}

	// Test: above garbage AND stable (low mod rate) should compact
	if !gc.ShouldCompactBlobSegment(1, 0.6, 0.05) {
		t.Error("60% garbage + 5% mod rate should trigger")
	}
}

// TestGCControllerGetStats tests stats retrieval.
func TestGCControllerGetStats(t *testing.T) {
	store := newMockObjectStore()
	policy := api.DefaultGCPolicy()
	gc := NewGCController(store, policy)

	stats := gc.GetStats()
	if stats.TotalCompactions != 0 {
		t.Errorf("Initial compactions should be 0, got %d", stats.TotalCompactions)
	}
	if stats.TotalBytesFreed != 0 {
		t.Errorf("Initial bytes freed should be 0, got %d", stats.TotalBytesFreed)
	}
}

// TestGCControllerUpdateSegmentMeta tests metadata update.
func TestGCControllerUpdateSegmentMeta(t *testing.T) {
	store := newMockObjectStore()
	policy := api.DefaultGCPolicy()
	gc := NewGCController(store, policy)

	gc.UpdateSegmentMeta(100, 1024*1024, 4096*1024)

	ratio := gc.GarbageRatio(100)
	expected := 0.25
	if ratio < expected-0.01 || ratio > expected+0.01 {
		t.Errorf("GarbageRatio: expected ~0.25, got %f", ratio)
	}
}

// TestGCControllerSetPolicy tests policy update.
func TestGCControllerSetPolicy(t *testing.T) {
	store := newMockObjectStore()
	policy := api.DefaultGCPolicy()
	gc := NewGCController(store, policy)

	// Update policy
	newPolicy := api.GCPolicy{
		PageSegmentThreshold:        0.50,
		BlobSegmentThreshold:        0.60,
		BlobSegmentModRateThreshold: 0.15,
		CheckIntervalMs:             5000,
	}
	gc.SetPolicy(newPolicy)

	// Verify new thresholds work
	if gc.ShouldCompactPageSegment(1, 0.45) {
		t.Error("45% should not trigger with 50% threshold")
	}
	if !gc.ShouldCompactPageSegment(1, 0.55) {
		t.Error("55% should trigger with 50% threshold")
	}
}

// TestGCControllerUpdateGCMeta tests detailed metadata update.
func TestGCControllerUpdateGCMeta(t *testing.T) {
	store := newMockObjectStore()
	policy := api.DefaultGCPolicy()
	gc := NewGCController(store, policy)

	gc.UpdateGCMeta(200, 50*1024*1024, 100*1024*1024, 1000, 2000)

	ratio := gc.GarbageRatio(200)
	expected := 0.5
	if ratio < expected-0.01 || ratio > expected+0.01 {
		t.Errorf("GarbageRatio: expected ~0.5, got %f", ratio)
	}
}

// TestGCControllerForceCompaction tests force compaction.
func TestGCControllerForceCompaction(t *testing.T) {
	store := newMockObjectStore()
	policy := api.DefaultGCPolicy()
	gc := NewGCController(store, policy)

	// Force compaction should succeed
	err := gc.ForceCompaction(nil, 1)
	if err != nil {
		t.Errorf("ForceCompaction failed: %v", err)
	}

	// Large segment should fail
	err = gc.ForceCompaction(nil, 3)
	if err == nil {
		t.Error("ForceCompaction on large segment should fail")
	}
}

// TestGCControllerForceDeleteSegment tests force delete.
func TestGCControllerForceDeleteSegment(t *testing.T) {
	store := newMockObjectStore()
	policy := api.DefaultGCPolicy()
	gc := NewGCController(store, policy)

	// Force delete large segment should succeed
	err := gc.ForceDeleteSegment(nil, 3)
	if err != nil {
		t.Errorf("ForceDeleteSegment failed: %v", err)
	}
}

// TestModRateStabilityCheckerWindowSize tests window size behavior.
func TestModRateStabilityCheckerWindowSize(t *testing.T) {
	checker := NewModRateStabilityChecker(5, 0.1)

	// Add more samples than window size
	for i := 0; i < 10; i++ {
		checker.AddSample(float64(i) * 0.1)
	}

	// Should only have 5 samples
	if checker.SampleCount() != 5 {
		t.Errorf("SampleCount: expected 5, got %d", checker.SampleCount())
	}
}

// TestGCPolicyDefaults tests default policy values.
func TestGCPolicyDefaults(t *testing.T) {
	policy := api.DefaultGCPolicy()

	if policy.PageSegmentThreshold != 0.40 {
		t.Errorf("PageSegmentThreshold: expected 0.40, got %f", policy.PageSegmentThreshold)
	}
	if policy.BlobSegmentThreshold != 0.50 {
		t.Errorf("BlobSegmentThreshold: expected 0.50, got %f", policy.BlobSegmentThreshold)
	}
	if policy.CheckIntervalMs != 1000 {
		t.Errorf("CheckIntervalMs: expected 1000, got %d", policy.CheckIntervalMs)
	}
}
