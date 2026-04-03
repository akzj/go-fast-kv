// Package cache provides the Cache API for the KV store.
// This file defines ONLY interfaces — no implementation code.
//
// Design invariants:
//   - OS Page Cache is the default caching strategy
//   - Each file type uses exactly one AccessPattern for its lifetime
//   - WAL must be synced before data pages for same transaction
//   - No file mixes mmap with write(2) or O_DIRECT with buffered I/O
//
// Module boundaries:
//   - This package is the public API for cache management
//   - Eviction policies are pluggable via Policy interface
//   - OS page cache integration via AccessPattern configuration
//   - All internal/ implementation details are private to internal/cache/
package cache

import (
	"errors"

	"github.com/akzj/go-fast-kv/internal/vaddr"
	"time"
)

// =============================================================================
// Type Aliases — Do NOT re-define, import from vaddr package
// =============================================================================

// VAddr is the physical address type (16 bytes: SegmentID[8] + Offset[8]).
// Defined in: vaddr package
type VAddr = vaddr.VAddr

// PageID is the logical identifier for a page (uint64).
// Defined in: vaddr package
type PageID = vaddr.PageID

// SegmentID identifies a segment file.
// Defined in: vaddr package
type SegmentID = vaddr.SegmentID

// =============================================================================
// Constants
// =============================================================================

const (
	// DefaultCacheSizeMB is the default cache size when using integrated cache.
	// OS Page Cache uses 0 (unbounded, kernel-managed).
	DefaultCacheSizeMB = 0

	// MinCacheSizeMB is the minimum cache size for integrated cache.
	MinCacheSizeMB = 16

	// MaxCacheSizeMB is the maximum cache size for integrated cache.
	MaxCacheSizeMB = 65536

	// PageSize is aligned with OS page size (4KB).
	// All VAddr.Offset values are multiples of PageSize.
	PageSize = 4096

	// CacheLineSize is the CPU cache line size for alignment.
	CacheLineSize = 64

	// DefaultGracePeriod is the default epoch grace period before reclamation.
	// Why 3? Absorbs slow readers without excessive memory retention.
	DefaultGracePeriod = 3
)

// =============================================================================
// Error Types
// =============================================================================

var (
	// ErrCacheDisabled is returned when cache operations are attempted
	// but no cache is configured.
	ErrCacheDisabled = errors.New("cache: cache disabled")

	// ErrCacheFull is returned when the integrated cache cannot accept more entries.
	ErrCacheFull = errors.New("cache: cache full")

	// ErrEntryNotFound is returned when the requested entry is not in cache.
	ErrEntryNotFound = errors.New("cache: entry not found")

	// ErrEvictionFailed is returned when eviction policy fails.
	ErrEvictionFailed = errors.New("cache: eviction failed")

	// ErrInvalidPolicy is returned when the eviction policy is invalid.
	ErrInvalidPolicy = errors.New("cache: invalid eviction policy")

	// ErrAccessPatternMismatch is returned when mixing access patterns on same file.
	ErrAccessPatternMismatch = errors.New("cache: access pattern mismatch")

	// ErrAlignmentRequired is returned when buffer alignment is required for DirectIO.
	ErrAlignmentRequired = errors.New("cache: buffer alignment required for DirectIO")

	// ErrSyncInProgress is returned when a sync operation is already running.
	ErrSyncInProgress = errors.New("cache: sync already in progress")
)

// =============================================================================
// Eviction Policy Types
// =============================================================================

// EvictionPolicy defines the algorithm for removing entries when cache is full.
// Invariant: Policy implementations must be goroutine-safe.
type EvictionPolicy interface {
	// Name returns the policy name for debugging and metrics.
	Name() string

	// OnAccess is called when an entry is accessed (hit or miss).
	// Allows policy to update access metadata.
	OnAccess(key CacheKey, value interface{})

	// OnInsert is called when a new entry is inserted.
	OnInsert(key CacheKey, value interface{})

	// OnEvict is called when an entry is about to be evicted.
	// Returns the key to evict.
	OnEvict() (CacheKey, bool)

	// OnRemove is called when an entry is explicitly removed.
	OnRemove(key CacheKey)

	// Reset clears all policy state.
	Reset()

	// Stats returns current policy statistics.
	Stats() EvictionStats
}

// EvictionStats holds statistics about eviction policy operations.
type EvictionStats struct {
	// Hits is the number of cache hits.
	Hits uint64

	// Misses is the number of cache misses.
	Misses uint64

	// Evictions is the total number of evictions performed.
	Evictions uint64

	// CurrentSize is the current number of entries in policy tracking.
	CurrentSize uint64
}

// CacheKey is a unique identifier for a cache entry.
// Invariant: CacheKey must be comparable (used as map key).
type CacheKey struct {
	// SegmentID identifies the segment.
	SegmentID SegmentID

	// PageID identifies the page within the segment.
	PageID PageID

	// Offset is the byte offset within the page (optional, for sub-page access).
	Offset uint32
}

// IsValid returns true if this cache key represents a valid entry.
func (k CacheKey) IsValid() bool {
	return k.SegmentID != 0 && k.PageID != 0
}

// =============================================================================
// Built-in Eviction Policies
// =============================================================================

// PolicyType identifies the type of eviction policy.
type PolicyType uint8

const (
	// PolicyLRU is Least Recently Used eviction.
	// Good for: general workloads with temporal locality.
	PolicyLRU PolicyType = iota

	// PolicyLFU is Least Frequently Used eviction.
	// Good for: workloads with popular items.
	PolicyLFU

	// PolicyARC is Adaptive Replacement Cache.
	// Good for: mixed workloads; self-tuning.
	PolicyARC

	// PolicyClock is Second-Chance (Clock) eviction.
	// Good for: large caches where LRU overhead is too high.
	PolicyClock

	// PolicyNoEviction disables eviction (infinite cache).
	// Use for: testing or when cache is disabled.
	PolicyNoEviction
)

// NewPolicy creates a new eviction policy of the given type.
// Returns ErrInvalidPolicy if type is unknown.
func NewPolicy(policyType PolicyType) (EvictionPolicy, error) {
	panic("TODO: implementation provided by branch")
}

// =============================================================================
// Cache Interface
// =============================================================================

// Cache is the main cache interface.
// Invariant: All cache operations are goroutine-safe.
// Invariant: Cache uses pluggable eviction policy.
type Cache interface {
	// Get retrieves a value from cache.
	// Returns (value, found, error).
	// Found is false if entry is not in cache (miss).
	Get(key CacheKey) (interface{}, bool, error)

	// Put stores a value in cache.
	// If cache is full, eviction policy determines what to remove.
	// Returns error if eviction fails or policy rejects.
	Put(key CacheKey, value interface{}) error

	// Delete removes an entry from cache.
	// Returns ErrEntryNotFound if key is not in cache.
	Delete(key CacheKey) error

	// Contains returns true if key is in cache.
	// Does not update access metadata.
	Contains(key CacheKey) bool

	// Len returns the number of entries in cache.
	Len() int

	// Capacity returns the maximum number of entries.
	Capacity() int

	// Clear removes all entries from cache.
	Clear() error

	// Close releases all resources held by the cache.
	Close() error
}

// =============================================================================
// Integrated Cache Interface (Alternative to OS Page Cache)
// =============================================================================

// IntegratedCache provides an in-process cache layer.
// Use when: write-heavy workloads (>50%), strict memory boundedness required,
// or P99 latency predictability needed.
//
// Why not always use IntegratedCache?
//   OS Page Cache is simpler, kernel-optimized, and reliable (64 vs 66 score).
//   IntegratedCache adds complexity without proportional benefit for read-heavy workloads.
//
// When to prefer IntegratedCache over OS Page Cache:
//   - Write-heavy workloads (>50% writes)
//   - Hard memory limit required
//   - P99 latency predictability critical
type IntegratedCache interface {
	Cache

	// Reserve pre-allocates memory for expected entry count.
	// Improves performance by reducing allocations.
	Reserve(n int) error

	// Stats returns cache statistics.
	Stats() CacheStats

	// SetCapacity changes the maximum cache size.
	// If new capacity < current size, triggers immediate eviction.
	SetCapacity(capacity int) error
}

// CacheStats holds cache performance statistics.
type CacheStats struct {
	// Hits is the number of cache hits.
	Hits uint64

	// Misses is the number of cache misses.
	Misses uint64

	// HitRate is the cache hit ratio (0.0 to 1.0).
	HitRate float64

	// Evictions is the total number of evictions.
	Evictions uint64

	// BytesAllocated is the current memory usage in bytes.
	BytesAllocated uint64

	// BytesCapacity is the maximum memory usage in bytes.
	BytesCapacity uint64
}

// =============================================================================
// OS Page Cache Integration
// =============================================================================

// AccessPattern defines how a file interacts with OS page cache.
// Invariant: Each file uses exactly one AccessPattern for its lifetime.
// Invariant: Mixing patterns on same file causes undefined behavior.
//
// Why three patterns?
//   - Buffered: Kernel manages caching; simplest, best for sequential access
//   - DirectIO: Bypass kernel; predictable latency, avoids double-buffering
//   - Mapped: mmap; pointer access, good for random access index files
type AccessPattern uint8

const (
	// AccessBuffered relies on kernel page cache for reads and writes.
	// Data may be in OS cache before reaching disk.
	// Use for: WAL segments, random-read data files.
	// Why Buffered for WAL? Sequential writes; kernel is efficient.
	AccessBuffered AccessPattern = iota

	// AccessDirectIO bypasses kernel page cache entirely.
	// Requires aligned buffers (typically 512-byte or 4096-byte aligned).
	// Use for: Large sequential reads, write-intensive workloads.
	// Why not default? No read caching; bad for random reads.
	AccessDirectIO

	// AccessMapped maps file into process address space.
	// OS handles paging; msync ensures durability.
	// Use for: Index files, B-link tree nodes.
	// Why Mapped for index? Random access; pointer dereference is fast.
	AccessMapped
)

// FileType identifies the purpose of a storage file.
// Invariant: Each file type has a fixed AccessPattern.
type FileType uint8

const (
	FileTypeWAL FileType = iota // Write-Ahead Log
	FileTypeSegment             // Data segment (B-link nodes)
	FileTypeExternalValue       // External large values
	FileTypeIndex               // Page manager index
	FileTypeCheckpoint          // Checkpoint metadata
)

// FileTypeAccessPattern maps file types to their access patterns.
// Why these assignments?
//   - WAL: Buffered (sequential writes, rely on kernel)
//   - Segments: Buffered (mixed reads/writes, kernel efficient)
//   - ExternalValue: Buffered (large sequential reads)
//   - Index: Mapped (random access, pointer access fast)
//   - Checkpoint: Mapped (read rarely, mmap for easy loading)
var FileTypeAccessPattern = map[FileType]AccessPattern{
	FileTypeWAL:           AccessBuffered,
	FileTypeSegment:       AccessBuffered,
	FileTypeExternalValue: AccessBuffered,
	FileTypeIndex:         AccessMapped,
	FileTypeCheckpoint:    AccessMapped,
}

// =============================================================================
// File Access Interfaces
// =============================================================================

// BufferedFile provides buffered I/O using kernel page cache.
// Invariant: All writes go through kernel page cache before disk.
// Invariant: fsync is required for durability.
//
// Why BufferedFile interface?
//   Abstracts OS-level buffered I/O for portability and testing.
type BufferedFile interface {
	// ReadAt reads len(b) bytes from offset into b.
	ReadAt(b []byte, offset int64) (n int, err error)

	// WriteAt writes len(b) bytes from b at offset.
	WriteAt(b []byte, offset int64) (n int, err error)

	// Sync ensures data is durable on disk.
	// Invariant: After Sync returns, all prior writes are durable.
	Sync() error

	// DataSync is like Sync but may skip metadata (faster).
	DataSync() error

	// Close releases the file handle.
	Close() error
}

// DirectIOFile provides O_DIRECT I/O bypassing kernel page cache.
// Invariant: All buffers passed to Read/Write are aligned to AlignmentBytes.
// Invariant: All read/write sizes are multiples of AlignmentBytes.
//
// Why DirectIO?
//   Avoids double-buffering when using integrated cache.
//   Predictable I/O latency without kernel cache interference.
//
// Why not default?
//   No read caching; terrible for random reads; alignment complexity.
type DirectIOFile interface {
	// ReadAt reads len(b) bytes from offset into b.
	// b must be aligned to AlignmentBytes.
	ReadAt(b []byte, offset int64) (n int, err error)

	// WriteAt writes len(b) bytes from b at offset.
	// b must be aligned to AlignmentBytes.
	WriteAt(b []byte, offset int64) (n int, err error)

	// Sync ensures data is durable on disk.
	Sync() error

	// Close releases the file handle.
	Close() error
}

// MmapFile maps a file into process address space.
// Invariant: File size must not change while mmap is active.
//
// Why MmapFile interface?
//   Abstracts memory-mapping for portability and testing.
//   Allows coordination with compaction truncation.
type MmapFile interface {
	// Data returns the entire mapped region.
	Data() []byte

	// Slice returns a sub-slice of the mapped region.
	Slice(start, end int) []byte

	// Msync synchronizes memory with disk.
	// mode: msync flags (MS_SYNC, MS_ASYNC)
	Msync(mode int) error

	// Advise hints to the kernel about usage pattern.
	// hint: madvise constants (MADV_RANDOM, MADV_SEQUENTIAL, etc.)
	Advise(hint int) error

	// Close unmaps the region and releases resources.
	Close() error
}

// =============================================================================
// Durability Coordination
// =============================================================================

// DurabilityManager ensures WAL-first ordering.
// Invariant: WAL must be synced before data pages for same transaction.
//
// Why WAL-first?
//   Crash recovery requires replaying WAL. If data pages sync before WAL,
//   a crash could replay partial data without corresponding WAL records.
type DurabilityManager interface {
	// SyncWAL ensures WAL is durable. Returns the LSN of synced position.
	SyncWAL() (lsn uint64, err error)

	// SyncData ensures all data files are durable.
	SyncData() error

	// SyncAll ensures both WAL and data are durable, in correct order.
	// Returns error if any sync fails.
	SyncAll() error

	// IsSyncing returns true if a sync operation is in progress.
	IsSyncing() bool
}

// =============================================================================
// Cache Coordinator
// =============================================================================

// CacheCoordinator manages cache interactions across the storage stack.
// Coordinates between integrated cache, OS page cache, and file access.
//
// Why separate coordinator?
//   Centralizes cache policy decisions; prevents double-buffering.
//   Ensures consistent AccessPattern per file type.
type CacheCoordinator interface {
	// FileForSegment returns the appropriate file interface for a segment.
	// Returns error if segment ID is invalid.
	// Invariant: Same segment always returns same file interface type.
	FileForSegment(segmentID SegmentID, pattern AccessPattern) (interface{}, error)

	// FileForIndex returns the appropriate file interface for the index.
	IndexFile() (MmapFile, error)

	// FileForWAL returns the appropriate file interface for WAL.
	WALFile() (BufferedFile, error)

	// RegisterCache registers an integrated cache for a file type.
	// Why register? Coordinator needs to invalidate cache on compaction.
	RegisterCache(fileType FileType, cache IntegratedCache)

	// InvalidateCacheForSegment removes all entries for a segment.
	// Called after segment compaction completes.
	InvalidateCacheForSegment(segmentID SegmentID) error

	// Durability returns the durability manager.
	Durability() DurabilityManager

	// Close releases all resources.
	Close() error
}

// =============================================================================
// Configuration
// =============================================================================

// CacheStrategy defines which caching strategy to use.
type CacheStrategy uint8

const (
	// StrategyOSPageCache uses the OS page cache (default).
	// Bounded by available memory; kernel-managed.
	// Best for: read-heavy workloads, simplicity, reliability.
	StrategyOSPageCache CacheStrategy = iota

	// StrategyIntegrated uses an in-process cache layer.
	// Explicitly bounded; user-managed.
	// Best for: write-heavy workloads, strict memory limits, P99 predictability.
	StrategyIntegrated
)

// CacheConfig holds configuration for the cache system.
type CacheConfig struct {
	// Strategy selects the caching strategy.
	Strategy CacheStrategy

	// IntegratedCacheSizeMB sets the integrated cache size (when StrategyIntegrated).
	// Default: 0 (uses OS Page Cache).
	// Ignored when Strategy is StrategyOSPageCache.
	IntegratedCacheSizeMB uint32

	// EvictionPolicy selects the eviction policy for integrated cache.
	EvictionPolicy PolicyType

	// DefaultAccessPattern sets the default AccessPattern for new segments.
	// If 0, uses FileTypeAccessPattern based on file type.
	DefaultAccessPattern AccessPattern

	// SyncMode controls when data is synced to disk.
	SyncMode SyncMode

	// CreateIfMissing creates cache directories if needed.
	CreateIfMissing bool
}

// DefaultCacheConfig returns a configuration optimized for read-heavy workloads.
// Uses OS Page Cache, Buffered access for segments, Mapped for index.
func DefaultCacheConfig() *CacheConfig {
	return &CacheConfig{
		Strategy:              StrategyOSPageCache,
		IntegratedCacheSizeMB: 0,
		EvictionPolicy:        PolicyLRU,
		DefaultAccessPattern:  0, // Use FileTypeAccessPattern defaults
		SyncMode:              SyncOnRotation,
		CreateIfMissing:       true,
	}
}

// SyncMode controls when data is synced to disk.
type SyncMode uint8

const (
	// SyncEveryRecord syncs after every write (WAL default).
	// Highest durability, lowest throughput.
	SyncEveryRecord SyncMode = iota

	// SyncOnRotation syncs when segment rotates (data default).
	// Balanced durability and throughput.
	SyncOnRotation

	// SyncOnClose syncs when file closes.
	// Lower overhead but relies on close being called.

	SyncOnClose

	// SyncOnDemand syncs only on explicit Sync() call.
	// Highest throughput, lowest durability.

	SyncOnDemand

	// SyncCooperative batches syncs and hints pages after access.
	// Good for large sequential scans.
	SyncCooperative
)

// =============================================================================
// Factory Functions
// =============================================================================

// NewCacheCoordinator creates a cache coordinator with the given configuration.
// Returns error if configuration is invalid.
func NewCacheCoordinator(config *CacheConfig) (CacheCoordinator, error) {
	panic("TODO: implementation provided by branch")
}

// NewIntegratedCache creates an integrated cache with the given policy and capacity.
// Capacity is the maximum number of entries.
//
// Why factory function?
//   Allows different cache implementations (LRU, ARC, etc.) to be created.
func NewIntegratedCache(policy EvictionPolicy, capacity int) (IntegratedCache, error) {
	panic("TODO: implementation provided by branch")
}

// NewDurabilityManager creates a durability manager for WAL-first sync ordering.
// Requires references to WAL and data files for coordinated syncs.
func NewDurabilityManager(walFile BufferedFile, dataFiles map[SegmentID]BufferedFile) DurabilityManager {
	panic("TODO: implementation provided by branch")
}

// =============================================================================
// Mmap Region Management
// =============================================================================

// MmapRegion represents a memory-mapped file region.
// Invariant: Region is not released until grace period expires.
type MmapRegion struct {
	SegmentID   SegmentID
	Data        []byte
	MappedAt    time.Time
	Reclaimable bool
}

// MmapManager tracks all mmap'd regions and coordinates truncation.
// Invariant: No region is unmapped while any pointer may reference it.
//
// Why MmapManager?
//   Compaction truncates segment files. Truncation before unmapping
//   causes undefined behavior. Manager coordinates the two operations.
type MmapManager interface {
	// Map creates a new memory-mapped region for a file.
	Map(segmentID SegmentID, data []byte) (*MmapRegion, error)

	// MarkCompacted marks a segment as compacted (pending release).
	MarkCompacted(segmentID SegmentID)

	// TryRelease attempts to release reclaimable regions.
	// Returns number of regions released.
	TryRelease() int

	// ActiveSegments returns segments with active mappings.
	ActiveSegments() []SegmentID

	// Close releases all regions.
	Close() error
}

// ReleasePolicy controls when mmap regions are released.
type ReleasePolicy struct {
	// GracePeriod is epochs before reclaimable regions can be released.
	// Default: 3 (matches EpochGracePeriod).
	GracePeriod int

	// MaxMappedRegions is the maximum mapped regions before forcing release.
	// Default: 0 (unbounded).
	MaxMappedRegions int64
}
