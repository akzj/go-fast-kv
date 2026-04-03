// Package compaction provides the compaction subsystem API.
// This file defines ONLY interfaces — no implementation code.
//
// Design invariants:
//   - Compaction is non-blocking: readers continue during compaction
//   - Epoch-based MVCC: VAddrs safe to reclaim after EpochGracePeriod epochs
//   - Generational segments: Active → Sealed → Archived → Compacted
//   - Space reclamation is asynchronous and safe
//
// Why separate interfaces?
//   - CompactionTrigger: decides WHEN to compact
//   - Compactor: performs the compaction WORK
//   - Reclaimer: manages VAddr lifecycle for safe reclamation
//   - EpochManager: provides epoch-based visibility (imported from vaddr)
//
// Module boundaries:
//   - Compaction depends on vaddr package for VAddr, SegmentID, EpochID types
//   - Compaction does NOT import internal/ packages
package compaction

import (
	"errors"

	"github.com/akzj/go-fast-kv/internal/vaddr/api"
	"time"
)

// =============================================================================
// Type Aliases — Import from vaddr package
// =============================================================================

// VAddr is the physical address type (16 bytes: SegmentID[8] + Offset[8]).
// Defined in: vaddr package
type VAddr = vaddr.VAddr

// SegmentID identifies a segment file.
// Defined in: vaddr package
type SegmentID = vaddr.SegmentID

// EpochID identifies a compaction epoch for MVCC.
// Defined in: vaddr package
type EpochID = vaddr.EpochID

// =============================================================================
// Constants
// =============================================================================

const (
	// DefaultSpaceUsageThreshold is the default disk usage % that triggers compaction.
	// When space usage exceeds this threshold, compaction is triggered.
	DefaultSpaceUsageThreshold = 0.4 // 40%

	// DefaultCompactionInterval is the default minimum time between compaction runs.
	DefaultCompactionInterval = 1 * time.Hour

	// DefaultMinSegmentCount is the minimum archived segments before compaction runs.
	// Why not always compact? Compaction has I/O cost; needs sufficient work.
	DefaultMinSegmentCount = 3

	// DefaultGCThreshold is the default number of epochs before VAddrs are reclaimable.
	// Why 3? Absorbs slow readers without excessive memory retention.
	DefaultGCThreshold = 3

	// MinCompactionInterval is the minimum time between compaction cycles.
	// Prevents compaction from consuming too many resources.
	MinCompactionInterval = 1 * time.Minute
)

// =============================================================================
// Error Types
// =============================================================================

var (
	// ErrCompactionInProgress is returned when attempting to start compaction
	// while another compaction is already running.
	ErrCompactionInProgress = errors.New("compaction: already in progress")

	// ErrNoSegmentsToCompact is returned when no segments meet compaction criteria.
	ErrNoSegmentsToCompact = errors.New("compaction: no segments eligible for compaction")

	// ErrSegmentInUse is returned when a segment cannot be reclaimed
	// because it is still visible to active readers.
	ErrSegmentInUse = errors.New("compaction: segment still in use")

	// ErrEpochExpired is returned when an epoch has expired and cannot be unregistered.
	ErrEpochExpired = errors.New("compaction: epoch expired")

	// ErrStoreClosed is returned when operations are attempted on a closed compactor.
	ErrStoreClosed = errors.New("compaction: store is closed")

	// ErrCompactionFailed is returned when compaction encounters an unrecoverable error.
	ErrCompactionFailed = errors.New("compaction: failed")

	// ErrInvalidThreshold is returned when threshold is out of valid range [0, 1].
	ErrInvalidThreshold = errors.New("compaction: invalid threshold")
)

// =============================================================================
// Configuration Types
// =============================================================================

// CompactionConfig holds configuration for the compaction subsystem.
type CompactionConfig struct {
	// SpaceUsageThreshold triggers compaction when disk usage exceeds this.
	// Valid range: (0.0, 1.0]. Default: 0.4 (40%).
	SpaceUsageThreshold float64

	// TimeInterval triggers compaction after this duration since last run.
	// Default: 1 hour.
	TimeInterval time.Duration

	// MinSegmentCount requires at least this many archived segments.
	// Default: 3.
	MinSegmentCount int

	// GCThreshold is epochs before VAddrs are reclaimable.
	// Default: 3.
	GCThreshold int

	// CompactionInterval is minimum time between compaction cycles.
	// Default: 1 minute.
	CompactionInterval time.Duration

	// MaxConcurrentCompaction limits parallel compaction tasks.
	// Default: 1 (serial compaction).
	MaxConcurrentCompaction int
}

// DefaultCompactionConfig returns configuration with sensible defaults.
func DefaultCompactionConfig() *CompactionConfig {
	return &CompactionConfig{
		SpaceUsageThreshold:    DefaultSpaceUsageThreshold,
		TimeInterval:           DefaultCompactionInterval,
		MinSegmentCount:        DefaultMinSegmentCount,
		GCThreshold:            DefaultGCThreshold,
		CompactionInterval:     MinCompactionInterval,
		MaxConcurrentCompaction: 1,
	}
}

// =============================================================================
// CompactionResult
// =============================================================================

// CompactionResult contains the outcome of a compaction operation.
type CompactionResult struct {
	// OldSegments lists the segment IDs that were compacted.
	OldSegments []SegmentID

	// NewSegments lists the new segment IDs created by compaction.
	NewSegments []SegmentID

	// BytesReclaimed is the actual bytes freed by compaction.
	BytesReclaimed uint64

	// Duration is how long compaction took.
	Duration time.Duration

	// EpochID is the epoch in which compaction completed.
	EpochID EpochID
}

// =============================================================================
// Interface: CompactionTrigger
// =============================================================================

// CompactionTrigger decides when to initiate compaction.
// Implementations should be lightweight and thread-safe.
//
// Why separate from Compactor?
//   Trigger can run frequently (e.g., after every write) without cost.
//   Compactor is expensive and should only run when necessary.
//
// Why Evaluate() returns bool?
//   Simple signal: should we start compaction now?
type CompactionTrigger interface {
	// Evaluate checks if compaction should run now.
	// Returns true if compaction should start.
	// Thread-safe: multiple goroutines may call Evaluate concurrently.
	Evaluate() bool

	// LastCompactionTime returns when the last compaction completed.
	// Returns zero time if no compaction has run.
	LastCompactionTime() time.Time

	// UpdateSpaceUsage records current disk space usage.
	// Called periodically to track space pressure.
	UpdateSpaceUsage(usedBytes, totalBytes uint64)
}

// CompactionTriggerFunc is a functional adapter for CompactionTrigger.
type CompactionTriggerFunc func() bool

// Evaluate implements CompactionTrigger.
func (f CompactionTriggerFunc) Evaluate() bool { return f() }

// =============================================================================
// Interface: SegmentSelector
// =============================================================================

// SegmentSelector chooses which segments should be compacted.
// Different policies can be implemented (size-based, age-based, LSM-style).
//
// Why separate interface?
//   Allows pluggable selection strategies without changing Compactor logic.
type SegmentSelector interface {
	// Select returns segments eligible for compaction, ordered by priority.
	// Higher priority segments appear earlier in the returned slice.
	// Returns empty slice if no segments qualify.
	Select(archivedSegments []SegmentID) []SegmentID

	// Priority returns the compaction priority for a segment.
	// Higher number = more urgent compaction candidate.
	// Called only for segments that pass Select() filtering.
	Priority(segmentID SegmentID) int
}

// SegmentSelectorFunc is a functional adapter for SegmentSelector.
type SegmentSelectorFunc func([]SegmentID) []SegmentID

// Select implements SegmentSelector.
func (f SegmentSelectorFunc) Select(segments []SegmentID) []SegmentID { return f(segments) }

// Priority implements SegmentSelector (default: round-robin).
func (f SegmentSelectorFunc) Priority(segmentID SegmentID) int { return 0 }

// =============================================================================
// Interface: Compactor
// =============================================================================

// Compactor performs generational compaction on segments.
// Compaction rewrites live data from old segments to new segments,
// enabling space reclamation of tombstoned and overwritten data.
//
// Invariant: Compactor is single-threaded per instance.
// Invariant: Compaction is atomic: either all changes succeed or none do.
// Invariant: Compaction does not block readers (non-blocking design).
//
// Why non-blocking?
//   Blocking compaction would pause all writes during I/O.
//   Non-blocking allows writers to continue while compaction runs in background.
type Compactor interface {
	// Compact performs a single compaction cycle.
	// Returns result with affected segments and space reclaimed.
	// Returns ErrCompactionInProgress if another compaction is running.
	// Returns ErrNoSegmentsToCompact if no segments qualify.
	Compact() (*CompactionResult, error)

	// SetSelector sets the segment selection strategy.
	// Thread-safe: may be called while Compact is not running.
	SetSelector(selector SegmentSelector)

	// IsRunning returns true if compaction is currently in progress.
	IsRunning() bool

	// Cancel requests cancellation of the current compaction.
	// Cancellation is cooperative; Compact must check periodically.
	// Returns immediately; does not wait for cancellation.
	Cancel()

	// Close releases resources held by the compactor.
	// Waits for any in-progress compaction to complete.
	Close() error
}

// =============================================================================
// Interface: CompactionWriter
// =============================================================================

// CompactionWriter writes compacted data to new segments.
// Receives data during compaction and writes to the output.
//
// Why separate interface?
//   Allows different output strategies: single segment, multiple segments,
//   or streaming to a new active segment.
type CompactionWriter interface {
	// Open initializes the writer for a new compaction output.
	// Returns the segment ID for the output.
	Open() (SegmentID, error)

	// WriteNode writes a B-link tree node to the output.
	// Returns the VAddr where the node was written.
	WriteNode(data []byte) (VAddr, error)

	// WriteExternalValue writes an external value to the output.
	// Returns the VAddr where the value was written.
	WriteExternalValue(data []byte) (VAddr, error)

	// Commit finalizes the output and marks it as sealed.
	Commit() error

	// Abort discards the output and cleans up resources.
	Abort()
}

// =============================================================================
// Interface: Reclaimer
// =============================================================================

// Reclaimer manages VAddr lifecycle for safe space reclamation.
// Tracks which VAddrs are live and determines when reclamation is safe.
//
// Why not use reference counting?
//   Reference counting requires tracking every VAddr across tree nodes.
//   Epoch-based reclamation amortizes this: one epoch covers many VAddrs.
//
// Why separate from EpochManager?
//   Reclaimer handles VAddr-specific tracking.
//   EpochManager handles epoch lifecycle and visibility queries.
type Reclaimer interface {
	// RegisterVAddr records a VAddr as live (visible to readers).
	// Called when a new VAddr is created (e.g., after compaction write).
	RegisterVAddr(vaddr VAddr)

	// UnregisterVAddr removes a VAddr from live tracking.
	// Called when a VAddr is no longer referenced by the tree.
	UnregisterVAddr(vaddr VAddr) error

	// TryReclaim attempts to reclaim a VAddr.
	// Returns true if the VAddr was successfully reclaimed.
	// Returns false if the VAddr is still visible to active readers.
	// Returns error if reclamation failed.
	TryReclaim(vaddr VAddr) (bool, error)

	// BatchTryReclaim attempts to reclaim multiple VAddrs.
	// Returns VAddrs that were successfully reclaimed.
	// Non-reclaimed VAddrs remain in the live set.
	BatchTryReclaim(vaddrs []VAddr) ([]VAddr, error)

	// LiveVAddrCount returns the number of VAddrs currently tracked as live.
	LiveVAddrCount() uint64
}

// =============================================================================
// Interface: CompactionStrategy
// =============================================================================

// CompactionStrategy combines trigger, selector, and compactor logic.
// Provides a unified interface for configuring the compaction subsystem.
//
// Why this interface?
//   Allows different strategies (generational, leveled, tiered) to implement
//   the same interface while using different internal components.
type CompactionStrategy interface {
	// Trigger returns the compaction trigger for this strategy.
	Trigger() CompactionTrigger

	// Selector returns the segment selector for this strategy.
	Selector() SegmentSelector

	// Compactor returns the compactor for this strategy.
	Compactor() Compactor

	// Reclaimer returns the reclaimer for this strategy.
	Reclaimer() Reclaimer
}

// =============================================================================
// Factory Functions
// =============================================================================

// NewCompactionStrategy creates a default generational compaction strategy.
// Uses size-based triggering, age-based selection, and epoch-based reclamation.
func NewCompactionStrategy(config *CompactionConfig) CompactionStrategy {
	panic("TODO: implementation provided by branch")
}

// NewCompactor creates a new Compactor instance.
// The compactor uses the provided writer to output compacted data.
func NewCompactor(writer CompactionWriter, reclaimer Reclaimer) Compactor {
	panic("TODO: implementation provided by branch")
}

// NewCompactionTrigger creates a trigger based on configuration.
func NewCompactionTrigger(config *CompactionConfig) CompactionTrigger {
	panic("TODO: implementation provided by branch")
}

// NewReclaimer creates a new Reclaimer instance.
func NewReclaimer(epochManager EpochManager) Reclaimer {
	panic("TODO: implementation provided by branch")
}

// NewSegmentSelector creates a segment selector with the given policy.
func NewSegmentSelector(policy string) SegmentSelector {
	panic("TODO: implementation provided by branch")
}

// =============================================================================
// EpochManager Interface
// =============================================================================

// EpochManager manages compaction epochs for MVCC.
// Used by Reclaimer to determine when VAddrs can be reclaimed.
//
// Why epoch-based instead of reference counting?
//   Reference counting requires tracking every VAddr across tree nodes.
//   Epochs amortize tracking cost: one epoch covers all VAddrs visible to that snapshot.
//   Grace period absorbs slow readers without excessive memory retention.
//
// Note: This interface mirrors vaddr.EpochManager. Implementations should
// satisfy both interfaces to allow sharing across packages.
type EpochManager interface {
	// RegisterEpoch creates a new epoch and returns its ID.
	// Callers should hold references to the returned epoch ID.
	RegisterEpoch() EpochID

	// UnregisterEpoch releases all references to an epoch.
	// Safe to call when epoch has passed its grace period.
	UnregisterEpoch(epoch EpochID)

	// IsVisible returns true if vaddr is visible in the given epoch.
	// A VAddr is visible if it was registered before the epoch was created.
	IsVisible(vaddr VAddr, epoch EpochID) bool

	// IsSafeToReclaim returns true if vaddr can be safely reclaimed.
	// Requires current epoch >= vaddr's epoch + EpochGracePeriod.
	// Thread-safe: may be called concurrently with RegisterEpoch/UnregisterEpoch.
	IsSafeToReclaim(vaddr VAddr) bool

	// MarkCompactionComplete marks old segments as successfully compacted.
	// Allows their VAddrs to become eligible for reclamation.
	MarkCompactionComplete(oldSegments []SegmentID)

	// CurrentEpoch returns the current epoch ID.
	// Epoch advances on each compaction cycle.
	CurrentEpoch() EpochID
}