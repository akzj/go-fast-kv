// Package internal provides the GC controller implementation.
package internal

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/akzj/go-fast-kv/pkg/gc/api"
	objstore "github.com/akzj/go-fast-kv/pkg/objectstore/api"
)

// SegmentGCMeta holds per-segment GC metadata.
type SegmentGCMeta struct {
	SegmentID            uint64
	GarbageSize          uint64
	TotalSize            uint64
	LiveSize             uint64
	GarbageCount         uint64
	LiveCount            uint64
	ModificationRate     float64
	LastModificationTime int64
}

// GCControllerImpl implements GCController.
type GCControllerImpl struct {
	policy   api.GCPolicy
	store    objstore.ObjectStore
	ticker   *time.Ticker
	done     chan struct{}
	stats    api.GCStats
	mu       sync.Mutex
	segMeta  map[uint64]*SegmentGCMeta
	segMux   sync.RWMutex // per-segment locking for concurrent access

	// Stats counters using atomic
	compactions    atomic.Uint64
	bytesFreed     atomic.Uint64
	bytesMoved     atomic.Uint64
	lastCompaction atomic.Int64

	// Stability checkers per segment (blob segments only)
	stabilityCheckers map[uint64]*ModRateStabilityChecker
}

// Ensure GCControllerImpl satisfies GCController interface
var _ api.GCController = (*GCControllerImpl)(nil)

// NewGCController creates a new GC controller.
func NewGCController(store objstore.ObjectStore, policy api.GCPolicy) *GCControllerImpl {
	interval := time.Duration(policy.CheckIntervalMs) * time.Millisecond
	if interval <= 0 {
		interval = time.Second // default 1 second
	}

	return &GCControllerImpl{
		policy:             policy,
		store:              store,
		ticker:             time.NewTicker(interval),
		done:               make(chan struct{}),
		segMeta:            make(map[uint64]*SegmentGCMeta),
		stabilityCheckers:  make(map[uint64]*ModRateStabilityChecker),
	}
}

// ShouldCompactPageSegment returns true if page segment should be compacted.
// Condition: garbage ratio > threshold.
func (gc *GCControllerImpl) ShouldCompactPageSegment(segmentID uint64, garbageRatio float64) bool {
	return garbageRatio > gc.policy.PageSegmentThreshold
}

// ShouldCompactBlobSegment returns true if blob segment should be compacted.
// Condition: garbage ratio > threshold AND modification rate < threshold (stable).
func (gc *GCControllerImpl) ShouldCompactBlobSegment(segmentID uint64, garbageRatio float64, modRate float64) bool {
	if garbageRatio <= gc.policy.BlobSegmentThreshold {
		return false
	}
	return modRate < gc.policy.BlobSegmentModRateThreshold
}

// OnDeleteLargeBlob handles large blob deletion (immediate file delete).
func (gc *GCControllerImpl) OnDeleteLargeBlob(segmentID uint64) error {
	ctx := context.Background()
	return gc.store.DeleteSegment(ctx, segmentID)
}

// GetStats returns GC statistics.
func (gc *GCControllerImpl) GetStats() api.GCStats {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	return api.GCStats{
		TotalCompactions:   gc.compactions.Load(),
		TotalBytesFreed:    gc.bytesFreed.Load(),
		TotalBytesMoved:    gc.bytesMoved.Load(),
		LastCompactionTime: gc.lastCompaction.Load(),
		SegmentsUnderGC:    uint64(len(gc.segMeta)),
	}
}

// Run starts the GC background loop.
func (gc *GCControllerImpl) Run() {
	for {
		select {
		case <-gc.done:
			return
		case <-gc.ticker.C:
			gc.CheckAllSegments()
		}
	}
}

// Stop stops the GC background loop.
func (gc *GCControllerImpl) Stop() {
	close(gc.done)
	gc.ticker.Stop()
}

// CheckAllSegments scans all sealed segments and triggers compaction/deletion as needed.
func (gc *GCControllerImpl) CheckAllSegments() {
	ctx := context.Background()
	segmentIDs := gc.store.GetSegmentIDs(ctx)

	for _, segID := range segmentIDs {
		gc.checkSegment(ctx, segID)
	}
}

// checkSegment evaluates a single segment for GC eligibility.
func (gc *GCControllerImpl) checkSegment(ctx context.Context, segID uint64) {
	segType := gc.store.GetSegmentType(ctx, segID)
	meta, err := gc.store.GetSegmentMeta(ctx, segID)
	if err != nil {
		return
	}

	switch segType {
	case objstore.SegmentTypeLarge:
		// Large blob: delete if empty (live count = 0)
		if meta.LiveCount == 0 || meta.LiveSize == 0 {
			if err := gc.store.DeleteSegment(ctx, segID); err == nil {
				gc.bytesFreed.Add(meta.TotalSize)
			}
		}

	case objstore.SegmentTypePage:
		// Page segment: compact if garbage ratio > threshold
		if meta.TotalSize > 0 {
			garbageRatio := float64(meta.GarbageSize) / float64(meta.TotalSize)
			if gc.ShouldCompactPageSegment(segID, garbageRatio) {
				gc.compactSegment(ctx, segID, segType, meta)
			}
		}

	case objstore.SegmentTypeBlob:
		// Blob segment: compact if garbage ratio > threshold AND stable
		if meta.TotalSize > 0 {
			garbageRatio := float64(meta.GarbageSize) / float64(meta.TotalSize)
			modRate := gc.calcModRate(segID)
			if gc.ShouldCompactBlobSegment(segID, garbageRatio, modRate) {
				gc.compactSegment(ctx, segID, segType, meta)
			}
		}
	}
}

// calcModRate calculates modification rate for a segment.
func (gc *GCControllerImpl) calcModRate(segID uint64) float64 {
	gc.segMux.RLock()
	checker, ok := gc.stabilityCheckers[segID]
	gc.segMux.RUnlock()

	if !ok {
		checker = NewModRateStabilityChecker(10, gc.policy.BlobSegmentModRateThreshold)
		gc.segMux.Lock()
		gc.stabilityCheckers[segID] = checker
		gc.segMux.Unlock()
	}

	// Get current mod rate from segment metadata
	gc.segMux.RLock()
	meta, ok := gc.segMeta[segID]
	gc.segMux.RUnlock()

	var modRate float64
	if ok && meta.TotalSize > 0 {
		modRate = float64(meta.GarbageSize) / float64(meta.TotalSize)
	}

	checker.AddSample(modRate)
	return modRate
}

// compactSegment compacts a sealed segment.
func (gc *GCControllerImpl) compactSegment(ctx context.Context, segID uint64, segType objstore.SegmentType, meta *objstore.SegmentMeta) {
	// Perform compaction
	err := gc.store.CompactSegment(ctx, segID)
	if err != nil {
		return // Compaction failed, skip
	}

	// Update stats
	gc.compactions.Add(1)
	gc.bytesFreed.Add(meta.GarbageSize)
	gc.bytesMoved.Add(meta.LiveSize)
	gc.lastCompaction.Store(time.Now().Unix())
}

// UpdateSegmentMeta updates metadata for a segment.
func (gc *GCControllerImpl) UpdateSegmentMeta(segID uint64, garbageSize, totalSize uint64) {
	gc.segMux.Lock()
	defer gc.segMux.Unlock()

	meta := gc.segMeta[segID]
	if meta == nil {
		meta = &SegmentGCMeta{SegmentID: segID}
		gc.segMeta[segID] = meta
	}

	meta.GarbageSize = garbageSize
	meta.TotalSize = totalSize
	meta.LiveSize = totalSize - garbageSize
	meta.LastModificationTime = time.Now().Unix()
}

// NewModRateStabilityChecker creates a new modification rate stability checker.
func NewModRateStabilityChecker(windowSize int, threshold float64) *ModRateStabilityChecker {
	if windowSize <= 0 {
		windowSize = 10
	}
	return &ModRateStabilityChecker{
		windowSize: windowSize,
		samples:    make([]float64, 0, windowSize),
		threshold:  threshold,
	}
}

// ModRateStabilityChecker monitors modification rate stability.
type ModRateStabilityChecker struct {
	windowSize int
	samples    []float64
	threshold  float64
	mu         sync.Mutex
}

// AddSample adds a new modification rate sample.
func (c *ModRateStabilityChecker) AddSample(rate float64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.samples = append(c.samples, rate)

	// Keep only windowSize samples
	if len(c.samples) > c.windowSize {
		c.samples = c.samples[len(c.samples)-c.windowSize:]
	}
}

// IsStable returns true if the modification rate is stable (variance < threshold).
func (c *ModRateStabilityChecker) IsStable() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.samples) < 2 {
		return true // Not enough data, assume stable
	}

	// Calculate variance inline (don't call Variance() to avoid deadlock)
	var sum float64
	for _, s := range c.samples {
		sum += s
	}
	mean := sum / float64(len(c.samples))

	var varSum float64
	for _, s := range c.samples {
		diff := s - mean
		varSum += diff * diff
	}
	variance := varSum / float64(len(c.samples))

	return variance < c.threshold
}

// Variance returns the variance of modification rate samples.
func (c *ModRateStabilityChecker) Variance() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.samples) < 2 {
		return 0
	}

	// Calculate mean
	var sum float64
	for _, s := range c.samples {
		sum += s
	}
	mean := sum / float64(len(c.samples))

	// Calculate variance
	var varSum float64
	for _, s := range c.samples {
		diff := s - mean
		varSum += diff * diff
	}

	return varSum / float64(len(c.samples))
}

// Clear clears all samples.
func (c *ModRateStabilityChecker) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.samples = c.samples[:0]
}

// SampleCount returns the number of samples.
func (c *ModRateStabilityChecker) SampleCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.samples)
}

// UpdateGCMeta updates segment GC metadata in the controller.
func (gc *GCControllerImpl) UpdateGCMeta(segID uint64, garbageSize, totalSize uint64, garbageCount, liveCount uint64) {
	gc.segMux.Lock()
	defer gc.segMux.Unlock()

	meta := gc.segMeta[segID]
	if meta == nil {
		meta = &SegmentGCMeta{SegmentID: segID}
		gc.segMeta[segID] = meta
	}

	meta.GarbageSize = garbageSize
	meta.TotalSize = totalSize
	meta.LiveSize = totalSize - garbageSize
	meta.GarbageCount = garbageCount
	meta.LiveCount = liveCount
	meta.LastModificationTime = time.Now().Unix()

	// Update modification rate
	if totalSize > 0 {
		meta.ModificationRate = float64(garbageSize) / float64(totalSize)
	}
}

// GarbageRatio returns the garbage ratio for a segment.
func (gc *GCControllerImpl) GarbageRatio(segID uint64) float64 {
	gc.segMux.RLock()
	defer gc.segMux.RUnlock()

	meta, ok := gc.segMeta[segID]
	if !ok || meta.TotalSize == 0 {
		return 0
	}

	return float64(meta.GarbageSize) / float64(meta.TotalSize)
}

// SetPolicy updates the GC policy.
func (gc *GCControllerImpl) SetPolicy(policy api.GCPolicy) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	gc.policy = policy

	// Update ticker interval if needed
	interval := time.Duration(policy.CheckIntervalMs) * time.Millisecond
	if interval > 0 {
		gc.ticker.Reset(interval)
	}
}

// ForceCompaction forces compaction of a specific segment.
func (gc *GCControllerImpl) ForceCompaction(ctx context.Context, segID uint64) error {
	segType := gc.store.GetSegmentType(ctx, segID)
	if segType == objstore.SegmentTypeLarge {
		return fmt.Errorf("cannot force compact large segment, use DeleteSegment instead")
	}

	meta, err := gc.store.GetSegmentMeta(ctx, segID)
	if err != nil {
		return err
	}

	gc.compactSegment(ctx, segID, segType, meta)
	return nil
}

// ForceDeleteSegment forces deletion of a segment.
func (gc *GCControllerImpl) ForceDeleteSegment(ctx context.Context, segID uint64) error {
	segType := gc.store.GetSegmentType(ctx, segID)
	meta, err := gc.store.GetSegmentMeta(ctx, segID)
	if err != nil {
		return err
	}

	var errDel error
	switch segType {
	case objstore.SegmentTypeLarge:
		errDel = gc.store.DeleteSegment(ctx, segID)
	default:
		// For page/blob, need to compact first
		errDel = gc.store.CompactSegment(ctx, segID)
	}

	if errDel == nil {
		gc.bytesFreed.Add(meta.TotalSize)
	}

	return errDel
}

// ObjectStoreGCAdapter provides GCController with ObjectStore access.
type ObjectStoreGCAdapter struct {
	store objstore.ObjectStore
}

// NewObjectStoreGCAdapter creates a new adapter.
func NewObjectStoreGCAdapter(store objstore.ObjectStore) *ObjectStoreGCAdapter {
	return &ObjectStoreGCAdapter{store: store}
}

// GetSegmentIDs returns all sealed segment IDs.
func (a *ObjectStoreGCAdapter) GetSegmentIDs(ctx context.Context) []uint64 {
	return a.store.GetSegmentIDs(ctx)
}

// GetSegmentType returns the type of a segment.
func (a *ObjectStoreGCAdapter) GetSegmentType(ctx context.Context, segID uint64) objstore.SegmentType {
	return a.store.GetSegmentType(ctx, segID)
}

// GetSegmentMeta returns GC metadata for a segment.
func (a *ObjectStoreGCAdapter) GetSegmentMeta(ctx context.Context, segID uint64) (*objstore.SegmentMeta, error) {
	return a.store.GetSegmentMeta(ctx, segID)
}

// CompactSegment compacts a segment.
func (a *ObjectStoreGCAdapter) CompactSegment(ctx context.Context, segID uint64) error {
	return a.store.CompactSegment(ctx, segID)
}

// DeleteSegment deletes a segment.
func (a *ObjectStoreGCAdapter) DeleteSegment(ctx context.Context, segID uint64) error {
	return a.store.DeleteSegment(ctx, segID)
}
