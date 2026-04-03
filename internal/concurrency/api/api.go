// Package concurrency provides concurrency control and crash recovery APIs.
// This file defines ONLY interfaces — no implementation code.
//
// Design invariants:
//   - Single-writer/multi-reader model: writes are serialized via exclusive latch
//   - Append-only storage: redo-only recovery (no undo needed)
//   - WAL ordering: records written synchronously before data mutations
//   - Checkpoint atomicity: all prior WAL records are durable at checkpoint
//
// Concurrency model (from solution-b.md §8):
//   - Single-writer: exactly one goroutine holds exclusive write access
//   - Multi-reader: readers never block each other via latch crabbing
//   - Lock-free reads: sibling chains enable readers to bypass latches
//
// Recovery model (from solution-b.md §9):
//   - WAL records never overwritten except via Truncate
//   - Checkpoint LSN indicates all prior records durable
//   - Redo-only: append-only storage never needs undo operations
//
// Module boundaries:
//   - This package provides public API for concurrency/recovery
//   - VAddr is imported from vaddr package (DO NOT re-define)
//   - LatchManager is used by blinktree for node-level locking
//   - WAL and Checkpoint are used by storage for durability
package concurrency

import (
	"context"
	"errors"
	"time"

	vaddr "github.com/akzj/go-fast-kv/internal/vaddr"
)

// =============================================================================
// Type Aliases — Do NOT re-define, import from vaddr package
// =============================================================================

// VAddr is the physical address type (16 bytes: SegmentID[8] + Offset[8]).
// Defined in: vaddr package
// type VAddr = vaddr.VAddr

// PageID is the logical identifier for a page (uint64).
// Defined in: vaddr package
// type PageID = vaddr.PageID

// =============================================================================
// Constants — Latch Modes
// =============================================================================

// LatchMode determines the type of latch acquisition.
type LatchMode int

const (
	// LatchRead is a shared latch for read-only access.
	// Multiple readers can hold LatchRead simultaneously on the same node.
	// Why shared? Readers outnumber writers 10x-100x in typical KV workloads.
	LatchRead LatchMode = iota

	// LatchWrite is an exclusive latch for mutations.
	// Only one writer can hold LatchWrite; no other latches allowed.
	LatchWrite
)

// =============================================================================
// Constants — WAL Record Types
// =============================================================================

// WALRecordType identifies the type of WAL record.
type WALRecordType uint8

const (
	// WALPageAlloc records a page allocation.
	WALPageAlloc WALRecordType = iota

	// WALPageFree records a page deallocation.
	WALPageFree

	// WALNodeWrite records a B-link node write.
	WALNodeWrite

	// WALExternalValue records an external value store operation.
	WALExternalValue

	// WALRootUpdate records a tree root change.
	WALRootUpdate

	// WALCheckpoint marks a checkpoint boundary.
	WALCheckpoint

	// WALIndexUpdate records an index mutation (for RadixTree).
	WALIndexUpdate

	// WALIndexRootUpdate records a RadixTree root change.
	WALIndexRootUpdate
)

// =============================================================================
// Constants — Checkpoint
// =============================================================================

const (
	// DefaultCheckpointInterval is the default time between checkpoints.
	DefaultCheckpointInterval = 30 * time.Second

	// DefaultWALSizeLimit is the default maximum WAL size before forcing checkpoint.
	DefaultWALSizeLimit = 64 * 1024 * 1024 // 64 MB

	// DefaultDirtyPageLimit is the default maximum dirty pages before checkpoint.
	DefaultDirtyPageLimit = 1000
)

// =============================================================================
// Error Types
// =============================================================================

var (
	// ErrWriteLocked is returned when a write latch cannot be acquired
	// because another writer holds it.
	ErrWriteLocked = errors.New("concurrency: write operation in progress")

	// ErrLatchNotHeld is returned when releasing or upgrading a latch
	// that is not held by the caller.
	ErrLatchNotHeld = errors.New("concurrency: latch not held")

	// ErrUpgradeNotSupported is returned when latch upgrade is not possible.
	// Why? Read→Write upgrade may deadlock if other readers exist.
	ErrUpgradeNotSupported = errors.New("concurrency: upgrade not supported")

	// ErrWALClosed is returned when operations are attempted on a closed WAL.
	ErrWALClosed = errors.New("concurrency: WAL is closed")

	// ErrWALCorrupted is returned when WAL record fails checksum validation.
	ErrWALCorrupted = errors.New("concurrency: WAL record corrupted")

	// ErrCheckpointInProgress is returned when checkpoint is already running.
	ErrCheckpointInProgress = errors.New("concurrency: checkpoint in progress")

	// ErrRecoveryFailed is returned when recovery cannot complete.
	ErrRecoveryFailed = errors.New("concurrency: recovery failed")

	// ErrNoCheckpointFound is returned when no valid checkpoint exists.
	ErrNoCheckpointFound = errors.New("concurrency: no checkpoint found")

	// ErrInvalidLSN is returned when LSN is invalid or out of order.
	ErrInvalidLSN = errors.New("concurrency: invalid LSN")

	// ErrStorageClosed is returned when storage is closed.
	ErrStorageClosed = errors.New("concurrency: storage closed")
)

// =============================================================================
// WAL Record Structure
// =============================================================================

// WALRecord is a single record in the write-ahead log.
// Invariant: LSN is monotonically increasing within a WAL.
// Invariant: Record is never modified after写入 (append-only).
//
// Why checksum? Detect corruption before replay.
//
// Layout:
//   - LSN: 8 bytes (uint64, big-endian)
//   - RecordType: 1 byte
//   - Length: 4 bytes (uint32, big-endian)
//   - Checksum: 4 bytes (CRC32c)
//   - Payload: variable length
type WALRecord struct {
	LSN        uint64        // Monotonically increasing log sequence number
	RecordType WALRecordType // Type of record
	Length     uint32        // Payload length in bytes
	Checksum   uint32        // CRC32c of payload
	Payload    []byte        // Record-specific data
}


// IsCheckpoint returns true if this is a checkpoint record.
func (r WALRecord) IsCheckpoint() bool {
	return r.RecordType == WALCheckpoint
}

// =============================================================================
// Interface: LatchManager
// =============================================================================

// LatchManager manages latches on B-link tree nodes for concurrency control.
// Invariant: LatchManager is goroutine-safe; concurrent Acquire/Release calls are safe.
//
// Why separate from Tree?
//   Latch management is orthogonal to tree operations. Allowing callers to
//   manage latches enables custom traversal strategies (e.g., lock-free reads).
//
// Latch Crabbing Protocol (from solution-b.md §8.3):
//   Phase 1: Acquire read latches top-down, descend
//   Phase 2: If split: acquire write latch on parent, release children
//   Phase 3: Update leaf, propagate changes upward
type LatchManager interface {
	// Acquire obtains a latch on the node at vaddr in the specified mode.
	// Block if latch is held by another goroutine until available.
	// Invariant: Caller must Release when done.
	Acquire(vaddr vaddr.VAddr, mode LatchMode)

	// Release drops a latch held by the caller.
	// mode must match the mode used in Acquire.
	// Invariant: Only the holder may release.
	Release(vaddr vaddr.VAddr, mode LatchMode)

	// TryAcquire attempts to acquire a latch without blocking.
	// Returns true if successful, false if latch is held by another goroutine.
	// Why non-blocking? Enables optimistic concurrency patterns.
	TryAcquire(vaddr vaddr.VAddr, mode LatchMode) bool

	// Upgrade attempts to upgrade a read latch to write latch.
	// Returns ErrUpgradeNotSupported if other readers exist.
	// Why not auto-upgrade? Prevents deadlock scenarios.
	Upgrade(vaddr vaddr.VAddr) error

	// IsWriteLocked returns true if vaddr is write-locked by anyone.
	// Why needed? Allows readers to detect potential blocking.
	IsWriteLocked(vaddr vaddr.VAddr) bool

	// ReaderCount returns the number of readers holding a read latch on vaddr.
	// Why needed? Enables upgrade decisions and deadlock detection.
	ReaderCount(vaddr vaddr.VAddr) int
}

// =============================================================================
// Interface: WAL
// =============================================================================

// WAL provides write-ahead logging for crash recovery.
// Invariant: WAL records are never overwritten; only appended.
//
// Why WAL before data?
//   Ensures durability: if crash occurs, WAL replay recovers all committed ops.
//   Appending to WAL is cheap; in-place data writes are expensive.
//
// Why separate interface?
//   Allows different WAL implementations (file-based, memory-mapped, distributed).
type WAL interface {
	// Append adds a record to the WAL.
	// Invariant: Record is durable before Append returns (if Sync enabled).
	// Invariant: LSN is assigned by WAL (monotonically increasing).
	Append(record *WALRecord) (uint64, error)

	// Read reads the record at the given LSN.
	// Returns ErrInvalidLSN if LSN does not exist.
	// Invariant: Records are immutable once written.
	Read(lsn uint64) (*WALRecord, error)

	// ReadFrom returns all records with LSN >= startLSN.
	// Used during recovery to replay uncommitted transactions.
	// Invariant: Records returned in ascending LSN order.
	ReadFrom(startLSN uint64) ([]*WALRecord, error)

	// Truncate removes all records with LSN <= cutoff.
	// Called after successful checkpoint to reclaim space.
	// Invariant: Cutoff must be <= last checkpoint LSN.
	Truncate(cutoff uint64) error

	// LastLSN returns the highest LSN in the WAL.
	// Returns 0 if WAL is empty.
	LastLSN() uint64

	// Size returns the current WAL size in bytes.
	Size() uint64

	// Flush ensures all pending writes are durable.
	// Invariant: After Flush returns, all prior Append calls are durable.
	Flush() error

	// Close releases resources and syncs final records.
	Close() error
}

// =============================================================================
// Interface: CheckpointManager
// =============================================================================

// CheckpointManager coordinates checkpoint creation and recovery.
// Invariant: Only one checkpoint can run at a time.
//
// Checkpoint Strategy (from solution-b.md §9.2):
//   - Time-based: checkpoint every N seconds
//   - Size-based: checkpoint when WAL exceeds limit
//   - Dirty-page based: checkpoint when dirty page count exceeds limit
//
// Checkpoint Atomicity:
//   - All WAL records before CheckpointLSN are durable
//   - Checkpoint records include TreeRoot, PageManager, ExternalValues snapshots
//   - Recovery replays WAL from CheckpointLSN to end
type CheckpointManager interface {
	// CreateCheckpoint creates a consistent snapshot of the database.
	// Returns the checkpoint metadata including CheckpointLSN.
	// Invariant: All WAL records before CheckpointLSN are durable.
	// Invariant: Checkpoint is atomic: either fully created or not at all.
	CreateCheckpoint() (*Checkpoint, error)

	// LatestCheckpoint returns the most recent valid checkpoint.
	// Used during recovery to find starting point.
	// Returns ErrNoCheckpointFound if no checkpoints exist.
	LatestCheckpoint() (*Checkpoint, error)

	// CheckpointByID returns a specific checkpoint by ID.
	// Returns ErrNoCheckpointFound if checkpoint doesn't exist.
	CheckpointByID(id uint64) (*Checkpoint, error)

	// DeleteCheckpoint removes an old checkpoint and its associated data.
	// Cannot delete checkpoints that haven't been superseded.
	// Invariant: At least one checkpoint always exists if database has data.
	DeleteCheckpoint(id uint64) error

	// CheckpointCount returns the number of stored checkpoints.
	CheckpointCount() int

	// RunAutoCheckpoints starts automatic checkpoint scheduling.
	// Stops when Close is called or context is cancelled.
	// Why background? Checkpoint timing is autonomous.
	RunAutoCheckpoints(ctx context.Context) error
}

// =============================================================================
// Interface: RecoveryManager
// =============================================================================

// RecoveryManager handles crash recovery using WAL and checkpoints.
// Invariant: Recovery is single-threaded; no other operations during recovery.
//
// Recovery Algorithm (from solution-b.md §9.3):
//   1. Find latest valid checkpoint
//   2. Load checkpoint state (TreeRoot, PageManager, ExternalValues)
//   3. Replay WAL from checkpoint LSN to end
//   4. Verify integrity
//   5. Truncate WAL past checkpoint LSN
//
// Why redo-only?
//   Append-only storage never overwrites data. Deleted data uses tombstones.
//   Therefore, only redo committed operations is needed.
type RecoveryManager interface {
	// Recover restores the database to a consistent state after crash.
	// Returns the recovered state (TreeRoot, PageManager, etc.).
	// Invariant: No other operations may be in progress during recovery.
	Recover() (*RecoveryResult, error)

	// VerifyIntegrity checks database integrity without recovery.
	// Returns list of corruptions found (empty if clean).
	// Why not part of Recover? Allows pre-recovery diagnostics.
	VerifyIntegrity() ([]IntegrityIssue, error)

	// SetWAL sets the WAL for recovery replay.
	// Why setter? Allows injection of mock WAL for testing.
	SetWAL(wal WAL)

	// SetCheckpointManager sets the checkpoint manager.
	SetCheckpointManager(mgr CheckpointManager)

	// IsRecovering returns true if recovery is in progress.
	IsRecovering() bool
}

// =============================================================================
// Supporting Types
// =============================================================================

// Checkpoint represents a consistent database snapshot.
type Checkpoint struct {
	// ID is the unique checkpoint identifier.
	ID uint64

	// LSN is the log sequence number of the checkpoint.
	// All WAL records before LSN are guaranteed durable.
	// Invariant: LSN is monotonically increasing across checkpoints.
	LSN uint64

	// TreeRoot is the VAddr of the B-link tree root at this checkpoint.
	TreeRoot vaddr.VAddr

	// PageManagerSnapshot is the VAddr of the PageManager index snapshot.
	PageManagerSnapshot vaddr.VAddr

	// ExternalValuesSnapshot captures the external value store state.
	ExternalValuesSnapshot vaddr.VAddr

	// Timestamp is when the checkpoint was created.
	Timestamp time.Time

	// WALTruncatedThrough is the LSN up to which WAL was truncated.
	// WAL records with LSN <= this value are no longer present.
	WALTruncatedThrough uint64
}

// IsValid returns true if this checkpoint has all required fields.
func (c Checkpoint) IsValid() bool {
	return c.ID != 0 && c.LSN != 0 && c.TreeRoot.IsValid()
}

// RecoveryResult contains the state after successful recovery.
type RecoveryResult struct {
	// TreeRoot is the recovered B-link tree root VAddr.
	TreeRoot vaddr.VAddr

	// PageManager is the recovered PageManager state.
	PageManager vaddr.VAddr

	// ExternalValues is the recovered external value store state.
	ExternalValues vaddr.VAddr

	// LastLSN is the highest LSN replayed during recovery.
	LastLSN uint64

	// RecordsRecovered is the number of WAL records replayed.
	RecordsRecovered int

	// RecoveredAt is when recovery completed.
	RecoveredAt time.Time
}

// IntegrityIssue represents a corruption detected during verification.
type IntegrityIssue struct {
	// Type is the kind of corruption.
	Type IntegrityIssueType

	// Location is where the corruption was found (e.g., VAddr, LSN).
	Location string

	// Description explains the issue.
	Description string

	// Severity indicates how serious this issue is.
	Severity IntegritySeverity
}

// IntegrityIssueType classifies integrity problems.
type IntegrityIssueType uint8

const (
	// IssueTypeChecksumMismatch indicates data failed checksum validation.
	IssueTypeChecksumMismatch IntegrityIssueType = iota

	// IssueTypeMissingRecord indicates a required WAL record is absent.
	IssueTypeMissingRecord

	// IssueTypeOrphanedData indicates data without valid references.
	IssueTypeOrphanedData

	// IssueTypeCorruptedNode indicates a B-link tree node is invalid.
	IssueTypeCorruptedNode
)

// IntegritySeverity indicates the seriousness of an issue.
type IntegritySeverity uint8

const (
	// SeverityLow indicates minor issue, recovery may succeed.
	SeverityLow IntegritySeverity = iota

	// SeverityMedium indicates moderate issue, manual intervention needed.
	SeverityMedium

	// SeverityHigh indicates severe issue, data loss possible.
	SeverityHigh

	// SeverityCritical indicates unrecoverable corruption.
	SeverityCritical
)

// CheckpointPolicy controls when checkpoints are triggered.
type CheckpointPolicy struct {
	// Interval is the time between checkpoints.
	// Default: 30 seconds.
	Interval time.Duration

	// WALSizeLimit triggers checkpoint when WAL exceeds this size.
	// Default: 64 MB.
	WALSizeLimit uint64

	// DirtyPageLimit triggers checkpoint when dirty page count exceeds.
	// Default: 1000 pages.
	DirtyPageLimit int
}

// DefaultCheckpointPolicy returns a policy with sensible defaults.
func DefaultCheckpointPolicy() *CheckpointPolicy {
	return &CheckpointPolicy{
		Interval:      DefaultCheckpointInterval,
		WALSizeLimit:  DefaultWALSizeLimit,
		DirtyPageLimit: DefaultDirtyPageLimit,
	}
}

// =============================================================================
// Concurrency Model Helpers
// =============================================================================

// SingleWriterModel represents the single-writer, multi-reader concurrency model.
// Why single-writer? B-link splits require coordinating parent/child atomically.
// Multiple writers would need distributed locking across splits.
type SingleWriterModel struct {
	// WriteGate allows exactly one writer at a time.
	// WriteGate chan struct{} // Capacity 1
}

// NewSingleWriterModel creates a new single-writer coordinator.
func NewSingleWriterModel() *SingleWriterModel {
	return &SingleWriterModel{}
}

// WriterMutex interface for exclusive write access.
type WriterMutex interface {
	// Lock acquires exclusive write access.
	// Blocks if another writer holds the lock.
	Lock()

	// Unlock releases exclusive write access.
	Unlock()

	// TryLock attempts to acquire lock without blocking.
	// Returns true if successful.
	TryLock() bool
}

// ReaderCounter tracks concurrent readers.
type ReaderCounter interface {
	// Increment adds one reader.
	Increment()

	// Decrement removes one reader.
	Decrement()

	// Count returns the current number of readers.
	Count() int64
}

// =============================================================================
// Factory Functions
// =============================================================================

// NewLatchManager creates a new latch manager.
// Implementation provided in internal package.
func NewLatchManager() LatchManager {
	panic("TODO: implementation provided by internal package")
}

// NewWAL creates a new write-ahead log at the given path.
// Implementation provided in internal package.
func NewWAL(path string, config *WALConfig) (WAL, error) {
	panic("TODO: implementation provided by internal package")
}

// NewCheckpointManager creates a new checkpoint manager.
// Implementation provided in internal package.
func NewCheckpointManager(wal WAL, storage interface {
	Snapshot() (vaddr.VAddr, vaddr.VAddr, error)
}) (CheckpointManager, error) {
	panic("TODO: implementation provided by internal package")
}

// NewRecoveryManager creates a new recovery manager.
// Implementation provided in internal package.
func NewRecoveryManager() RecoveryManager {
	panic("TODO: implementation provided by internal package")
}

// =============================================================================
// WAL Configuration
// =============================================================================

// WALConfig holds WAL initialization parameters.
type WALConfig struct {
	// Directory is where WAL files are stored.
	Directory string

	// SyncWrites enables synchronous writes (default: true for durability).
	SyncWrites bool

	// BufferSize is the write buffer size in bytes.
	// Default: 4 MB.
	BufferSize uint64

	// SegmentSize is the size of each WAL segment file.
	// Default: 64 MB.
	SegmentSize uint64

	// CreateIfMissing creates directory if it doesn't exist.
	CreateIfMissing bool
}

// DefaultWALConfig returns a configuration with sensible defaults.
func DefaultWALConfig(directory string) *WALConfig {
	return &WALConfig{
		Directory:        directory,
		SyncWrites:       true,
		BufferSize:       4 * 1024 * 1024,
		SegmentSize:      64 * 1024 * 1024,
		CreateIfMissing:  true,
	}
}

// =============================================================================
// Required Imports
// =============================================================================

// This import is required because the interfaces reference vaddr.VAddr.
// The actual import would be:
//   import vaddr "github.com/akzj/go-fast-kv/internal/vaddr"
