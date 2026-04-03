// Package vaddr provides the VAddr and Storage layer interfaces for the KV store.
//
// Architecture: This layer manages the append-only address space where data is
// persisted. VAddr encodes a physical address as SegmentID + Offset.
//
// Key invariants:
//   - VAddr uniqueness: No two allocations produce the same VAddr
//   - VAddr stability: A VAddr never changes after allocation
//   - Segment ID monotonicity: New segments have higher IDs than old
//   - Page alignment: All VAddr.Offset values are multiples of PageSize
//
// Module boundaries: This package is the public API. All internal/ implementation
// details are private to the vaddr module.
package vaddr

import (
	"errors"
	"time"
)

// =============================================================================
// Core Types
// =============================================================================

// VAddr encodes a physical address in the append-only address space.
// Invariant: VAddr is always 16 bytes (8 + 8), never zero (SegmentID 0 is reserved).
// Why 16 bytes? Allows segment IDs up to uint64_max with 8-byte offsets.
// Why big-endian? Natural byte ordering for segment comparisons.
//
//go:generate stringer -type=VAddr
type VAddr struct {
	SegmentID uint64 // Identifies the segment file
	Offset    uint64 // Byte offset within the segment
}

// IsValid returns true if this VAddr represents a valid address.
// Why not just check SegmentID != 0? Explicit validation aids debugging and
// makes the reserved value explicit.
func (v VAddr) IsValid() bool {
	return v.SegmentID != 0
}

// PageID is a logical identifier for a page.
// Invariant: PageID > 0 (0 is reserved for invalid/null).
type PageID uint64

// SegmentID identifies a segment file.
// Invariant: SegmentID > 0, monotonically increasing.
type SegmentID uint64

// SegmentState represents the lifecycle state of a segment.
type SegmentState uint8

// EpochID identifies a compaction epoch for MVCC.
type EpochID uint64

// =============================================================================
// Constants
// =============================================================================

var (
	// VAddrInvalid is the null/invalid address (reserved).
	// Why is 0 reserved? Allows zero-initialization to represent "not set".
	VAddrInvalid = VAddr{SegmentID: 0, Offset: 0}

	// VAddrMinValid is the minimum valid VAddr.
	VAddrMinValid = VAddr{SegmentID: 1, Offset: 0}

	// PageSize is aligned with OS page size (4KB).
	// Why 4096? Matches OS page size for efficient mmap and I/O.
	PageSize = 4096

	// PageDataSize is the usable data size within a page (4096 - 32 header).
	// Reserved for: Magic(8) + Version(2) + PageID(8) + Checksum(4) + Flags(2) + Reserved(8).
	PageDataSize = 4080

	// ExternalThreshold is the maximum inline value size in bytes.
	// Values > 48 bytes are stored externally.
	// Why 48? InlineValue.Data has 56 bytes. When external:
	// - Top bit of length = is_external flag (1 byte)
	// - 16 bytes for VAddr
	// - 48 leaves room for length prefix + alignment
	ExternalThreshold = 48

	// MaxSegmentSize is the maximum size before a segment is sealed.
	// Default: 1GB
	MaxSegmentSize uint64 = 1 << 30

	// DefaultMaxValueSize is the maximum value size (64 MB).
	DefaultMaxValueSize = 64 * 1024 * 1024

	// EpochGracePeriod is the number of epochs before VAddrs can be reclaimed.
	// Why 3? Absorbs slow readers without excessive memory retention.
	EpochGracePeriod = 3

	// PageIDInvalid is the reserved invalid page ID.
	PageIDInvalid PageID = 0

	// SegmentHeaderSize is the segment file header size.
	SegmentHeaderSize = 32

	// SegmentTrailerSize is the segment file trailer size.
	SegmentTrailerSize = 32

	// SegmentMagic is the magic number for segment files.
	// Why "FASTSEG"? Identifies valid segment files.
	SegmentMagic = "FASTSEG"
)

// Segment state constants.
var (
	// SegmentStateActive indicates the segment accepts new writes.
	SegmentStateActive SegmentState = 0x01

	// SegmentStateSealed indicates the segment is full, read-only.
	SegmentStateSealed SegmentState = 0x02

	// SegmentStateArchived indicates the segment is cold storage, may be compacted.
	SegmentStateArchived SegmentState = 0x04

	// SegmentStateCompactTarget indicates the segment is being rewritten.
	SegmentStateCompactTarget SegmentState = 0x08
)

// =============================================================================
// Errors
// =============================================================================

var (
	// ErrVAddrInvalid is returned when a VAddr operation fails due to invalid address.
	ErrVAddrInvalid = errors.New("vaddr: invalid address")

	// ErrSegmentNotFound is returned when a segment cannot be found.
	ErrSegmentNotFound = errors.New("vaddr: segment not found")

	// ErrSegmentFull is returned when a segment cannot accept more data.
	ErrSegmentFull = errors.New("vaddr: segment full")

	// ErrSegmentSealed is returned when attempting to write to a sealed segment.
	ErrSegmentSealed = errors.New("vaddr: segment sealed")

	// ErrPageNotFound is returned when a page cannot be found.
	ErrPageNotFound = errors.New("vaddr: page not found")

	// ErrStorageClosed is returned when storage is closed.
	ErrStorageClosed = errors.New("vaddr: storage closed")

	// ErrValueTooLarge is returned when a value exceeds maximum size.
	ErrValueTooLarge = errors.New("vaddr: value too large")

	// ErrChecksumMismatch is returned when data integrity check fails.
	ErrChecksumMismatch = errors.New("vaddr: checksum mismatch")

	// ErrInvalidSegmentFile is returned when a segment file is corrupted.
	ErrInvalidSegmentFile = errors.New("vaddr: invalid segment file")

	// ErrSegmentInUse is returned when attempting to delete a segment still in use.
	ErrSegmentInUse = errors.New("vaddr: segment in use")

	// ErrIO is returned for generic I/O errors.
	ErrIO = errors.New("vaddr: I/O error")
)

// =============================================================================
// Storage Interface
// =============================================================================

// Storage manages the append-only storage system with segments and pages.
// Invariant: All mutations are append-only; old data is never overwritten.
//
//go:generate mockgen -destination=internal/mocks/storage_mock.go . Storage
type Storage interface {
	// OpenSegment opens or creates a segment for writing.
	// Returns the segment ID and VAddr of the first page.
	// Why not return the Segment directly? Allows lazy loading of segment data.
	OpenSegment() (SegmentID, VAddr, error)

	// GetSegment returns a segment by ID.
	// Returns ErrSegmentNotFound if segment doesn't exist.
	GetSegment(id SegmentID) (Segment, error)

	// SealSegment marks a segment as sealed (read-only).
	// Invariant: Sealed segments never accept new writes.
	SealSegment(id SegmentID) error

	// ArchiveSegment marks a segment as archived (cold storage).
	// Invariant: Archived segments may be compacted in the future.
	ArchiveSegment(id SegmentID) error

	// AllocatePage allocates a new page and returns its ID and VAddr.
	// Invariant: Returned PageID is unique; Returned VAddr is unique and stable.
	AllocatePage() (PageID, VAddr, error)

	// ReadPage reads a page from the given VAddr.
	// Returns ErrPageNotFound if page doesn't exist.
	ReadPage(vaddr VAddr) (*Page, error)

	// WritePage writes a page to storage (appended, never in-place).
	// Invariant: Returns a new VAddr; old VAddr remains valid until reclaimed.
	WritePage(page *Page) (VAddr, error)

	// GetSegmentIDs returns all segment IDs.
	GetSegmentIDs() ([]SegmentID, error)

	// GetSegmentState returns the state of a segment.
	GetSegmentState(id SegmentID) (SegmentState, error)

	// PageCount returns the total number of allocated pages.
	PageCount() uint64

	// Manifest returns the manifest manager for recovery.
	// Why needed? Callers need access to manifest without additional coupling.
	// Per solution-b.md §5.4: manifest is required for crash recovery.
	Manifest() (ManifestManager, error)

	// Close releases resources held by storage.
	Close() error

	// Sync ensures durable storage of recent writes.
	Sync() error
}

// =============================================================================
// Segment Interface
// =============================================================================

// Segment represents a segment file containing data pages.
// Invariants:
//   - Segment is immutable once sealed
//   - Segment ID is monotonically increasing
//   - All VAddr.Offset values within segment are multiples of PageSize
//
//go:generate mockgen -destination=internal/mocks/segment_mock.go . Segment
type Segment interface {
	// ID returns the segment identifier.
	ID() SegmentID

	// State returns the current segment state.
	State() SegmentState

	// CreatedAt returns when the segment was created.
	CreatedAt() time.Time

	// PageCount returns the number of pages in this segment.
	PageCount() uint64

	// DataSize returns the total data bytes in this segment.
	DataSize() uint64

	// Page returns a page by its index within this segment.
	// Why index not offset? Simpler for sequential access.
	Page(index uint64) (*Page, error)

	// PageByOffset returns a page containing the given offset.
	PageByOffset(offset uint64) (*Page, error)

	// FirstVAddr returns the VAddr of the first data page.
	FirstVAddr() VAddr

	// LastVAddr returns the VAddr of the last data page.
	LastVAddr() VAddr

	// Checksum returns the segment integrity checksum.
	Checksum() uint64

	// IsSealed returns true if the segment is sealed.
	IsSealed() bool

	// IsArchived returns true if the segment is archived.
	IsArchived() bool

	// Close releases resources held by the segment.
	Close() error
}

// =============================================================================
// Page Interface
// =============================================================================

// Page represents a fixed-size data page.
// Invariant: Page is always PageSize bytes when serialized.
//
//go:generate mockgen -destination=internal/mocks/page_mock.go . Page
type Page interface {
	// ID returns the logical page identifier.
	ID() PageID

	// VAddr returns the physical address of this page.
	VAddr() VAddr

	// Checksum returns the page integrity checksum.
	Checksum() uint32

	// Flags returns the page flags.
	Flags() uint16

	// Data returns the page data (slice, not copy).
	// Why return slice not copy? Avoids allocation for read-only access.
	Data() []byte

	// SetChecksum sets the page checksum.
	SetChecksum(crc uint32)

	// SetFlags sets the page flags.
	SetFlags(flags uint16)

	// Clone returns a copy of the page.
	Clone() Page
}

// =============================================================================
// PageManager Interface
// =============================================================================

// PageManager maps PageID → VAddr and manages page allocation.
// Invariant: All mutations are append-only; old entries are tombstoned.
//
//go:generate mockgen -destination=internal/mocks/pagemanager_mock.go . PageManager
type PageManager interface {
	// GetVAddr returns the VAddr for a page_id, or VAddrInvalid if not allocated.
	GetVAddr(pageID PageID) VAddr

	// AllocatePage allocates a new page and returns its PageID and VAddr.
	// Invariant: Returned PageID is monotonically increasing.
	// Invariant: Returned VAddr is unique and never reused.
	AllocatePage() (PageID, VAddr, error)

	// FreePage marks a page as reclaimable.
	// Invariant: FreePage is idempotent.
	FreePage(pageID PageID)

	// UpdateMapping records that page_id now lives at vaddr.
	// Why update? Page may be compacted to new location.
	UpdateMapping(pageID PageID, vaddr VAddr)

	// PageCount returns the total number of allocated pages.
	PageCount() uint64

	// Iter calls fn for each page_id → vaddr mapping.
	// Why callback not iterator? Simpler for internal use; no interface pollution.
	Iter(fn func(pageID PageID, vaddr VAddr))

	// Flush ensures durable storage of the index.
	Flush() error

	// Close releases resources.
	Close() error
}

// =============================================================================
// FixedSizeKVIndex Interface
// =============================================================================

// IndexType identifies the underlying index structure.
type IndexType uint8

const (
	// IndexTypeDenseArray provides O(1) lookup for dense PageIDs (default).
	IndexTypeDenseArray IndexType = iota

	// IndexTypeRadixTree provides O(k) lookup for sparse PageIDs.
	IndexTypeRadixTree
)

// PageManagerIndexEntry is a fixed-size key-value pair.
// Invariant: Entry is always 24 bytes (8 + 16).
type PageManagerIndexEntry struct {
	PageID PageID // 8 bytes
	VAddr  VAddr  // 16 bytes
}

// FixedSizeKVIndex is the underlying index for page_id → vaddr mapping.
// Invariant: All entries are fixed-size (24 bytes).
//
//go:generate mockgen -destination=internal/mocks/fixedSizeKVIndex_mock.go . FixedSizeKVIndex
type FixedSizeKVIndex interface {
	// Get returns the VAddr for key, or VAddrInvalid if not found.
	Get(key PageID) VAddr

	// Put stores a key-value pair.
	Put(key PageID, value VAddr)

	// Len returns the number of entries.
	Len() uint64

	// RangeQuery returns entries in [start, end).
	RangeQuery(start, end PageID) []PageManagerIndexEntry

	// ByteSize returns the memory usage of the index.
	ByteSize() uint64
}

// =============================================================================
// FreeList Interface
// =============================================================================

// FreeList manages reclaimable pages for reuse.
// Design: Lock-free stack of PageIDs.
// Invariant: Freed pages are reused before allocating new PageIDs.
//
//go:generate mockgen -destination=internal/mocks/freelist_mock.go . FreeList
type FreeList interface {
	// Pop returns a freed PageID or false if empty.
	Pop() (PageID, bool)

	// Push adds a page ID to the free list.
	Push(pageID PageID)

	// Len returns the number of pages on the free list.
	Len() uint64

	// Clear removes all entries.
	Clear()
}

// =============================================================================
// Batch Interface
// =============================================================================

// Batch provides batched page operations.
// Invariant: Batch is atomic; either all operations succeed or none.
//
//go:generate mockgen -destination=internal/mocks/batch_mock.go . Batch
type Batch interface {
	// AllocatePages allocates multiple pages atomically.
	AllocatePages(count int) ([]PageID, []VAddr, error)

	// FreePages marks multiple pages as reclaimable atomically.
	FreePages(pageIDs []PageID)

	// UpdateMappings updates multiple page mappings atomically.
	UpdateMappings(mappings []PageManagerIndexEntry)

	// Commit applies all pending operations.
	Commit() error

	// Reset clears the batch without applying.
	Reset()
}

// =============================================================================
// EpochManager Interface
// =============================================================================

// EpochManager manages compaction epochs for MVCC.
// Used by compaction to determine when VAddrs can be reclaimed.
//
//go:generate mockgen -destination=internal/mocks/epochmanager_mock.go . EpochManager
type EpochManager interface {
	// RegisterEpoch creates a new epoch and returns its ID.
	RegisterEpoch() EpochID

	// UnregisterEpoch releases references to an epoch.
	UnregisterEpoch(epoch EpochID)

	// IsVisible returns true if vaddr is visible in the given epoch.
	IsVisible(vaddr VAddr, epoch EpochID) bool

	// IsSafeToReclaim returns true if vaddr can be safely reclaimed.
	// Requires: epoch >= vaddr.epoch + EpochGracePeriod
	IsSafeToReclaim(vaddr VAddr) bool

	// MarkCompactionComplete marks old segments as compacted.
	MarkCompactionComplete(oldSegments []SegmentID)

	// CurrentEpoch returns the current epoch ID.
	CurrentEpoch() EpochID
}

// =============================================================================
// Configuration
// =============================================================================

// Config holds storage configuration.
type Config struct {
	// Directory is the path to the storage directory.
	Directory string

	// MaxSegmentSize is the maximum size before segment rotation (default: 1GB).
	MaxSegmentSize uint64

	// IndexType selects the underlying index structure.
	IndexType IndexType

	// SyncWrites enables synchronous writes (default: true for durability).
	SyncWrites bool

	// CreateIfMissing creates the directory if it doesn't exist.
	CreateIfMissing bool

	// ReadOnly opens storage in read-only mode.
	ReadOnly bool
}

// DefaultConfig returns a configuration with sensible defaults.
func DefaultConfig(directory string) *Config {
	return &Config{
		Directory:       directory,
		MaxSegmentSize:  MaxSegmentSize,
		IndexType:       IndexTypeDenseArray,
		SyncWrites:      true,
		CreateIfMissing: true,
		ReadOnly:        false,
	}
}

// =============================================================================
// Factory Functions
// =============================================================================

// Open opens or creates a storage instance.
// Why factory function not constructor? Allows error handling without panics.
func Open(directory string, config *Config) (Storage, error) {
	panic("TODO: implement in internal/storage.go")
}

// OpenWithTransactions opens storage with transaction support.
// For vaddr layer, transactions manage page allocation atomicity.
func OpenWithTransactions(directory string, config *Config) (TransactionalStorage, error) {
	panic("TODO: implement in internal/storage.go")
}

// Destroy deletes the storage directory and all contents.
// Why not a method? Easier to call without holding a reference.
func Destroy(directory string) error {
	panic("TODO: implement in internal/storage.go")
}

// =============================================================================
// TransactionalStorage Interface
// =============================================================================

// TransactionalStorage extends Storage with transaction support.
// Used when atomic page operations across multiple segments are needed.
//
//go:generate mockgen -destination=internal/mocks/txstorage_mock.go . TransactionalStorage
type TransactionalStorage interface {
	Storage

	// BeginTx starts a new transaction.
	BeginTx() (StorageTransaction, error)

	// TxCount returns the number of active transactions.
	TxCount() int
}

// StorageTransaction represents an atomic set of storage operations.
//
//go:generate mockgen -destination=internal/mocks/storagetx_mock.go . StorageTransaction
type StorageTransaction interface {
	// AllocatePage allocates a page within this transaction.
	AllocatePage() (PageID, VAddr, error)

	// WritePage writes a page within this transaction.
	WritePage(page *Page) (VAddr, error)

	// Commit applies all operations in this transaction.
	Commit() error

	// Rollback aborts the transaction without applying changes.
	Rollback()

	// TxID returns the transaction identifier.
	TxID() uint64
}

// =============================================================================
// Manifest Interface
// =============================================================================

// Manifest records the current state of storage for recovery.
// Why separate from Storage? Allows recovery without loading all segments.
type Manifest struct {
	// Version is the manifest version number.
	Version uint64

	// ActiveSegmentID is the current active segment.
	ActiveSegmentID SegmentID

	// SealedSegments lists all sealed segment IDs.
	SealedSegments []SegmentID

	// ArchivedSegments lists all archived segment IDs.
	ArchivedSegments []SegmentID

	// LastCheckpointLSN is the LSN of the last checkpoint.
	LastCheckpointLSN uint64

	// PageManagerSnapshot captures the PageID → VAddr mapping.
	PageManagerSnapshot VAddr

	// CreatedAt is when the manifest was created.
	CreatedAt time.Time
}

// ManifestManager handles manifest persistence and recovery.
type ManifestManager interface {
	// Load reads the manifest from storage.
	Load() (*Manifest, error)

	// Save writes the manifest to storage.
	Save(m *Manifest) error

	// Update applies a manifest update atomically.
	Update(fn func(*Manifest)) error
}
