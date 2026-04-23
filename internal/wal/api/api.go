// Package walapi defines the interface for the shared Write-Ahead Log (WAL).
//
// The WAL provides crash-recovery guarantees for all mapping changes
// (PageStore, BlobStore, CLOG, root pointer). Records are written in
// atomic batches — either all records in a batch are durable, or none are.
//
// Design reference: docs/DESIGN.md §3.6
package walapi

import (
	"errors"
)

// ─── Errors ─────────────────────────────────────────────────────────

var (
	// ErrCorruptBatch is returned during Replay when a batch fails
	// CRC validation. The batch and all subsequent data are discarded.
	ErrCorruptBatch = errors.New("wal: corrupt batch (CRC mismatch)")

	// ErrClosed is returned when operating on a closed WAL.
	ErrClosed = errors.New("wal: closed")
)

// ─── Module Types ───────────────────────────────────────────────────

// ModuleType identifies which module owns this WAL record.
// Each module tracks its own checkpoint_lsn independently.
// WAL deletion uses the minimum checkpoint_lsn across all modules.
type ModuleType uint8

const (
	// ModuleTree represents B-link tree page records.
	ModuleTree ModuleType = 1
	// ModuleBlob represents BlobStore data records.
	ModuleBlob ModuleType = 2
	// ModuleLSM represents LSM Store (mapping) records.
	ModuleLSM ModuleType = 3
	// ModuleSegment represents segment lifecycle records (e.g., seal events).
	ModuleSegment ModuleType = 4
)

func (m ModuleType) String() string {
	switch m {
	case ModuleTree:
		return "Tree"
	case ModuleBlob:
		return "Blob"
	case ModuleLSM:
		return "LSM"
	case ModuleSegment:
		return "Segment"
	default:
		return "Unknown"
	}
}

// ─── Record Types ───────────────────────────────────────────────────

// RecordType identifies the kind of WAL record.
type RecordType uint8

const (
	// RecordPageMap records a PageStore mapping change: pageID → vaddr.
	RecordPageMap RecordType = 1

	// RecordBlobMap records a BlobStore mapping change: blobID → (vaddr, size).
	RecordBlobMap RecordType = 2

	// RecordBlobFree records a BlobStore deletion: blobID freed.
	RecordBlobFree RecordType = 3

	// RecordPageFree records a PageStore page release: pageID freed.
	RecordPageFree RecordType = 4

	// RecordSetRoot records a B-tree root pointer change: new rootPageID.
	RecordSetRoot RecordType = 5

	// RecordCheckpoint marks a checkpoint completion: checkpoint LSN.
	RecordCheckpoint RecordType = 6

	// RecordTxnCommit records a transaction commit: xid committed.
	RecordTxnCommit RecordType = 7

	// RecordTxnAbort records a transaction abort: xid aborted.
	RecordTxnAbort RecordType = 8

	// RecordSegmentSealed records a segment seal event: emitted by PageStore/BlobStore
	// after successful rotation. Used by GC to track segment boundaries.
	RecordSegmentSealed RecordType = 9
)

// String returns a human-readable name for the record type.
func (t RecordType) String() string {
	switch t {
	case RecordPageMap:
		return "PageMap"
	case RecordBlobMap:
		return "BlobMap"
	case RecordBlobFree:
		return "BlobFree"
	case RecordPageFree:
		return "PageFree"
	case RecordSetRoot:
		return "SetRoot"
	case RecordCheckpoint:
		return "Checkpoint"
	case RecordTxnCommit:
		return "TxnCommit"
	case RecordTxnAbort:
		return "TxnAbort"
	case RecordSegmentSealed:
		return "SegmentSealed"
	default:
		return "Unknown"
	}
}

// ─── Record ─────────────────────────────────────────────────────────

// RecordSize is the fixed size of a single WAL record in bytes.
const RecordSize = 34

// Record is a single WAL record. All records are fixed-size (34 bytes).
//
// Wire format:
//
//	[0:8]   uint64  LSN         — Log Sequence Number (assigned by WAL)
//	[8]     uint8   ModuleType   — ModuleType (0=Tree for backward compat)
//	[9]     uint8   Type        — RecordType
//	[10:18] uint64  ID          — pageID / blobID / xid / rootPageID (depends on Type)
//	[18:26] uint64  VAddr       — packed VAddr (segID<<32 | offset), or 0 if unused
//	[26:30] uint32  Size        — blob size (RecordBlobMap only), 0 otherwise
//	[30:34] uint32  CRC         — CRC32-C of bytes [0:30]
type Record struct {
	LSN        uint64
	ModuleType ModuleType // which module owns this record
	Type       RecordType
	ID         uint64 // pageID, blobID, xid, or rootPageID depending on Type
	VAddr      uint64 // packed VAddr (segmentID<<32 | offset), 0 if not applicable
	Size       uint32 // blob data size (RecordBlobMap only)
	CRC        uint32 // CRC32-C, computed over bytes [0:30] of the serialized record
}

// ─── Batch ──────────────────────────────────────────────────────────

// BatchHeaderSize is the fixed size of a batch header in bytes.
const BatchHeaderSize = 12

// Batch groups multiple records into an atomic unit.
//
// Wire format:
//
//	[0:4]   uint32  RecordCount
//	[4:8]   uint32  TotalSize   — BatchHeaderSize + RecordCount * RecordSize
//	[8:12]  uint32  BatchCRC    — CRC32-C of entire batch (this field zeroed during computation)
//	[12:..] records             — RecordCount × 34 bytes each
//
// Atomicity guarantee: during recovery, if BatchCRC validation fails,
// the entire batch (and all subsequent data) is discarded.
type Batch struct {
	Records []Record
}

// NewBatch creates an empty batch. Use Add() to append records.
func NewBatch() *Batch {
	return &Batch{}
}

// Add appends a record to the batch. The LSN and CRC fields are
// populated by the WAL when the batch is written — callers should
// only set ModuleType, Type, ID, VAddr, and Size.
//
// If moduleType is 0, it defaults to ModuleTree (backward compatibility).
func (b *Batch) Add(moduleType ModuleType, typ RecordType, id uint64, vaddr uint64, size uint32) {
	if moduleType == 0 {
		moduleType = ModuleTree // backward compatibility
	}
	b.Records = append(b.Records, Record{
		ModuleType: moduleType,
		Type:      typ,
		ID:        id,
		VAddr:     vaddr,
		Size:      size,
	})
}

// Len returns the number of records in the batch.
func (b *Batch) Len() int {
	return len(b.Records)
}

// Reset clears the batch for reuse, retaining the underlying slice capacity.
func (b *Batch) Reset() {
	b.Records = b.Records[:0]
}

// ─── Interface ──────────────────────────────────────────────────────

// WAL is the shared Write-Ahead Log.
//
// Thread safety: WAL must be safe for concurrent use. WriteBatch
// serializes writes internally (only one batch is written at a time).
// Replay is intended for single-threaded recovery at startup.
//
// Design reference: docs/DESIGN.md §3.6
type WAL interface {
	// WriteBatch atomically writes a batch of records to the WAL.
	//
	// The WAL assigns monotonically increasing LSNs to each record,
	// computes per-record CRC and batch CRC, writes the batch to the
	// active segment file, and calls fsync to ensure durability.
	//
	// After WriteBatch returns successfully, all records are durable.
	// The caller can then safely update in-memory state.
	//
	// Returns the LSN of the last record written.
	WriteBatch(batch *Batch) (lastLSN uint64, err error)

	// Replay reads all valid batches from WAL segment files starting after
	// the given LSN, calling fn for each record in order.
	//
	// If a batch fails CRC validation, it and all subsequent data are
	// discarded (the segment is truncated to the last valid batch).
	//
	// afterLSN = 0 means replay from the beginning.
	//
	// Used during crash recovery:
	//   wal.Replay(0, func(r Record) error {
	//       switch r.Type {
	//       case RecordPageMap: ...
	//       case RecordTxnCommit: ...
	//       }
	//   })
	Replay(afterLSN uint64, fn func(Record) error) error

	// CurrentLSN returns the LSN of the last successfully written record.
	// Returns 0 if no records have been written.
	CurrentLSN() uint64

	// Truncate removes all WAL data at or before the given LSN.
	//
	// Deprecated: This method now delegates to DeleteSegmentsBefore.
	// For backward compatibility, it deletes segments where end_lsn < upToLSN.
	Truncate(upToLSN uint64) error

	// Rotate closes the current active segment and starts a new one.
	// The old active segment is renamed to include its end LSN.
	// Call this to force a segment boundary before deleting old WAL data.
	Rotate() error

	// DeleteSegmentsBefore deletes all WAL segment files where the
	// end LSN is less than the given threshold.
	//
	// The active segment is never deleted.
	// This is the primary method for reclaiming WAL space after checkpoint.
	DeleteSegmentsBefore(lsn uint64) error

	// ListSegments returns the names of all WAL segment files in the
	// directory, sorted by begin LSN.
	ListSegments() []string

	// Close flushes and closes the WAL.
	// After Close, all operations return ErrClosed.
	Close() error
}

// ─── Config ─────────────────────────────────────────────────────────

// Config holds configuration for the WAL.
type Config struct {
	// Dir is the directory where WAL segment files are stored.
	// Files are named "wal.{begin_lsn}.{end_lsn}.log" or "wal.{begin_lsn}.active.log".
	// The directory is created if it does not exist.
	Dir string

	// SyncMode controls fsync behavior for WAL writes.
	// 0 (default) = SyncAlways: fsync after every write batch.
	// 1 = SyncNone: no per-write fsync (data in OS page cache only).
	// Close() always fsyncs regardless of this setting.
	SyncMode int

	// SegmentSize is the maximum size of a WAL segment file in bytes.
	// When a segment reaches this size, it is rotated (closed and renamed).
	// Default is 64MB (64 * 1024 * 1024).
	SegmentSize int64

	// MaxWALBatchSize controls the group commit channel capacity and the
	// maximum number of requests drained per batch.
	// Higher values improve throughput under high write concurrency.
	// Default is 1024.
	MaxWALBatchSize int
}