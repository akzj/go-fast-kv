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
	default:
		return "Unknown"
	}
}

// ─── Record ─────────────────────────────────────────────────────────

// RecordSize is the fixed size of a single WAL record in bytes.
const RecordSize = 33

// Record is a single WAL record. All records are fixed-size (33 bytes).
//
// Wire format:
//
//	[0:8]   uint64  LSN       — Log Sequence Number (assigned by WAL)
//	[8]     uint8   Type      — RecordType
//	[9:17]  uint64  ID        — pageID / blobID / xid / rootPageID (depends on Type)
//	[17:25] uint64  VAddr     — packed VAddr (segID<<32 | offset), or 0 if unused
//	[25:29] uint32  Size      — blob size (RecordBlobMap only), 0 otherwise
//	[29:33] uint32  CRC       — CRC32-C of bytes [0:29]
type Record struct {
	LSN   uint64
	Type  RecordType
	ID    uint64 // pageID, blobID, xid, or rootPageID depending on Type
	VAddr uint64 // packed VAddr (segmentID<<32 | offset), 0 if not applicable
	Size  uint32 // blob data size (RecordBlobMap only)
	CRC   uint32 // CRC32-C, computed over bytes [0:29] of the serialized record
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
//	[12:..] records             — RecordCount × 33 bytes each
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
// only set Type, ID, VAddr, and Size.
func (b *Batch) Add(typ RecordType, id uint64, vaddr uint64, size uint32) {
	b.Records = append(b.Records, Record{
		Type:  typ,
		ID:    id,
		VAddr: vaddr,
		Size:  size,
	})
}

// Len returns the number of records in the batch.
func (b *Batch) Len() int {
	return len(b.Records)
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
	// WAL file, and calls fsync to ensure durability.
	//
	// After WriteBatch returns successfully, all records are durable.
	// The caller can then safely update in-memory state.
	//
	// Returns the LSN of the last record written.
	WriteBatch(batch *Batch) (lastLSN uint64, err error)

	// Replay reads all valid batches from the WAL file starting after
	// the given LSN, calling fn for each record in order.
	//
	// If a batch fails CRC validation, it and all subsequent data are
	// discarded (the WAL is truncated to the last valid batch).
	//
	// afterLSN = 0 means replay from the beginning.
	//
	// Used during crash recovery:
	//   wal.Replay(checkpoint.LSN, func(r Record) error {
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
	// Called after a successful checkpoint to reclaim WAL space.
	// Records with LSN <= upToLSN are discarded.
	Truncate(upToLSN uint64) error

	// Close flushes and closes the WAL file.
	// After Close, all operations return ErrClosed.
	Close() error
}

// ─── Config ─────────────────────────────────────────────────────────

// Config holds configuration for the WAL.
type Config struct {
	// Dir is the directory where the WAL file is stored.
	// The WAL file is named "wal.log" within this directory.
	// The directory is created if it does not exist.
	Dir string

	// SyncMode controls fsync behavior for WAL writes.
	// 0 (default) = SyncAlways: fsync after every write batch.
	// 1 = SyncNone: no per-write fsync (data in OS page cache only).
	// Close() always fsyncs regardless of this setting.
	SyncMode int
}
