// Package btreeapi defines the interface for the B-link Tree index layer.
//
// The B-link tree provides ordered key-value storage with MVCC versioning.
// Each leaf entry carries TxnMin/TxnMax for multi-version concurrency control.
// Nodes use HighKey + right-link (Next) for the B-link concurrent access protocol.
//
// Key features:
//   - Variable-length []byte keys
//   - MVCC: LeafEntry with TxnMin/TxnMax, (Key ASC, TxnMin DESC) ordering
//   - B-link: HighKey + Next on both leaf and internal nodes
//   - Inline values (≤256 bytes) or BlobRef for large values
//   - Split threshold: serialized node size > PageSize (4096 bytes)
//
// Design reference: docs/DESIGN.md §3.4, §3.5
package btreeapi

import (
	"errors"
	"math"

	pagestoreapi "github.com/akzj/go-fast-kv/internal/pagestore/api"
)

// ─── Errors ─────────────────────────────────────────────────────────

var (
	// ErrKeyNotFound is returned when Get cannot find a visible version
	// of the requested key.
	ErrKeyNotFound = errors.New("btree: key not found")

	// ErrKeyTooLarge is returned when a key exceeds MaxKeySize.
	ErrKeyTooLarge = errors.New("btree: key too large")

	// ErrClosed is returned when operating on a closed BTree.
	ErrClosed = errors.New("btree: closed")
)

// ─── Constants ──────────────────────────────────────────────────────

const (
	// PageSize is the target page size for B-tree nodes (4KB).
	PageSize = 4096

	// MaxKeySize is the maximum allowed key size in bytes.
	// Keys larger than this are rejected to ensure nodes can hold
	// at least a few entries.
	MaxKeySize = 1024

	// InlineThreshold is the maximum value size for inline storage.
	// Values larger than this are stored in BlobStore.
	// Design reference: docs/DESIGN.md §7.3
	InlineThreshold = 256

	// TxnMaxInfinity represents "not deleted" — the maximum uint64 value.
	TxnMaxInfinity = math.MaxUint64
)

// ─── Value ──────────────────────────────────────────────────────────

// Value represents either an inline value or a BlobStore reference.
//
// Exactly one of Inline or BlobID is set:
//   - Inline: len(Inline) > 0, BlobID == 0
//   - BlobRef: len(Inline) == 0, BlobID > 0
type Value struct {
	Inline []byte // inline value data (empty if BlobRef)
	BlobID uint64 // BlobStore reference (0 if inline)
}

// IsInline returns true if this value is stored inline.
func (v Value) IsInline() bool {
	return len(v.Inline) > 0 || v.BlobID == 0
}

// ─── LeafEntry ──────────────────────────────────────────────────────

// LeafEntry is a single versioned key-value entry in a leaf node.
//
// Entries are sorted by (Key ASC, TxnMin DESC) — for the same key,
// the newest version (highest TxnMin) comes first.
//
// MVCC semantics:
//   - TxnMin: the transaction ID that created this version
//   - TxnMax: the transaction ID that deleted/superseded this version
//     (TxnMaxInfinity means "not yet deleted")
//
// Design reference: docs/DESIGN.md §3.4, §3.9.5
type LeafEntry struct {
	Key    []byte
	TxnMin uint64
	TxnMax uint64
	Value  Value
}

// ─── Node ───────────────────────────────────────────────────────────

// Node represents a B-link tree node (leaf or internal).
//
// Both leaf and internal nodes have HighKey and Next (right-link),
// which is required for the B-link concurrent access protocol.
//
// Design reference: docs/DESIGN.md §3.4, §3.5
type Node struct {
	IsLeaf bool
	Count  uint16 // number of entries (leaf) or keys (internal)

	// B-link fields (both leaf and internal):
	HighKey []byte             // key range upper bound (exclusive), nil = +∞ (rightmost node)
	Next    pagestoreapi.PageID // right sibling PageID (0 = no sibling)

	// Leaf node fields:
	Entries []LeafEntry // sorted by (Key ASC, TxnMin DESC)

	// Internal node fields:
	Keys     [][]byte               // separator keys
	Children []pagestoreapi.PageID  // child PageIDs, len(Children) == len(Keys) + 1
}

// ─── Serialization ──────────────────────────────────────────────────

// Serialization wire format (docs/DESIGN.md §3.5):
//
//	Header (variable length):
//	  [0]       uint8   flags (bit0=isLeaf)
//	  [1]       uint8   reserved
//	  [2:4]     uint16  count
//	  [4:12]    uint64  next (PageID)
//	  [12:16]   uint32  checksum (CRC32-C, zeroed during computation)
//	  [16:18]   uint16  highKeyLen (0 = nil = +∞)
//	  [18:18+hkl]       highKey bytes
//
//	Leaf entry (variable length):
//	  [0:2]     uint16  keyLen
//	  [2:2+kl]          key
//	  [next 8]  uint64  txnMin
//	  [next 8]  uint64  txnMax
//	  [next 1]  uint8   valueType (0=inline, 1=blobRef)
//	  if inline:
//	    [next 4] uint32  valueLen
//	    [next vl]        value bytes
//	  if blobRef:
//	    [next 8] uint64  blobID
//
//	Internal entry (variable length):
//	  [0:2]     uint16  keyLen
//	  [2:2+kl]          key
//	  [next 8]  uint64  childPageID
//
// Checksum: CRC32-C (Castagnoli) over entire serialized page
// with the checksum field zeroed.

// NodeSerializer handles serialization and deserialization of Nodes.
type NodeSerializer interface {
	// Serialize encodes a Node into bytes (≤ PageSize).
	// Returns the serialized bytes with CRC32-C checksum.
	Serialize(node *Node) ([]byte, error)

	// Deserialize decodes bytes into a Node.
	// Validates the CRC32-C checksum.
	Deserialize(data []byte) (*Node, error)

	// SerializedSize returns the byte size of a node if serialized.
	// Used to check split threshold without actually serializing.
	SerializedSize(node *Node) int
}

// ─── Iterator ───────────────────────────────────────────────────────

// Iterator provides forward iteration over key-value pairs.
//
// Usage:
//
//	iter := tree.Scan(startKey, endKey)
//	defer iter.Close()
//	for iter.Next() {
//	    key := iter.Key()
//	    value := iter.Value()
//	    // ...
//	}
//	if err := iter.Err(); err != nil { ... }
type Iterator interface {
	// Next advances to the next key-value pair.
	// Returns false when iteration is complete or an error occurred.
	Next() bool

	// Key returns the current key. Valid only after Next() returns true.
	Key() []byte

	// Value returns the current value. Valid only after Next() returns true.
	// For BlobRef entries, the value is resolved from BlobStore automatically.
	Value() []byte

	// Err returns any error encountered during iteration.
	Err() error

	// Close releases resources held by the iterator.
	Close()
}

// ─── BTree Interface ────────────────────────────────────────────────

// BTree provides ordered key-value storage with MVCC versioning.
//
// Phase 5 implementation uses a mock/real PageStore for node storage.
// Phase 6 integrates with the real PageStore + SegmentManager.
//
// Thread safety: BTree is NOT thread-safe in Phase 5.
// Thread safety via per-page RwLock is added in Phase 6 (§3.8).
//
// Design reference: docs/DESIGN.md §3.4
type BTree interface {
	// Put inserts or updates a key-value pair.
	//
	// MVCC behavior:
	//   - Creates a new LeafEntry with TxnMin=txnID, TxnMax=MaxUint64
	//   - If a visible version exists, marks it with TxnMax=txnID
	//   - Large values (> InlineThreshold) are stored in BlobStore
	//
	// May trigger node splits if the leaf exceeds PageSize after insertion.
	Put(key, value []byte, txnID uint64) error

	// Get retrieves the value for a key visible to the given transaction.
	//
	// Returns the first visible version (highest TxnMin where IsVisible).
	// Returns ErrKeyNotFound if no visible version exists.
	//
	// For Phase 5 (simplified visibility): returns the latest version
	// where TxnMin <= txnID and TxnMax > txnID.
	Get(key []byte, txnID uint64) ([]byte, error)

	// Delete marks a key as deleted for the given transaction.
	//
	// MVCC behavior: sets TxnMax=txnID on the visible version.
	// Does NOT physically remove the entry — that's done by Vacuum.
	//
	// Returns ErrKeyNotFound if no visible version exists.
	Delete(key []byte, txnID uint64) error

	// Scan returns an iterator over keys in [start, end).
	//
	// Only returns entries visible to the given transaction.
	// Each key appears at most once (latest visible version).
	Scan(start, end []byte, txnID uint64) Iterator

	// RootPageID returns the current root node's PageID.
	RootPageID() pagestoreapi.PageID

	// SetRootPageID sets the root node's PageID (used during recovery).
	SetRootPageID(pagestoreapi.PageID)

	// Close releases resources held by the BTree.
	Close() error
}

// ─── PageProvider ───────────────────────────────────────────────────

// PageProvider abstracts page read/write for the B-tree.
// This allows Phase 5 to use a mock (in-memory) page store,
// and Phase 6 to plug in the real PageStore.
type PageProvider interface {
	// AllocPage allocates a new page and returns its PageID.
	AllocPage() pagestoreapi.PageID

	// ReadPage reads and deserializes a node from the given PageID.
	ReadPage(pageID pagestoreapi.PageID) (*Node, error)

	// WritePage serializes and writes a node to the given PageID.
	WritePage(pageID pagestoreapi.PageID, node *Node) error
}

// ─── Config ─────────────────────────────────────────────────────────

// Config holds configuration for the BTree.
type Config struct {
	// InlineThreshold is the max value size for inline storage.
	// Values larger than this are stored via BlobWriter.
	// Defaults to InlineThreshold (256) if zero.
	InlineThreshold int

	// VisibilityChecker determines if a version (txnMin, txnMax) is visible
	// to a reader with the given readTxnID. If nil, the default range check
	// is used: txnMin <= readTxnID && txnMax > readTxnID.
	// KVStore sets this to check CLOG + snapshot boundary (readTxnID),
	// ensuring uncommitted, aborted, and future entries are never visible.
	VisibilityChecker func(txnMin, txnMax, readTxnID uint64) bool
}

// ─── BlobWriter ─────────────────────────────────────────────────────

// BlobWriter abstracts blob write/read for the B-tree.
// Allows the B-tree to store large values without directly
// depending on the BlobStore implementation.
type BlobWriter interface {
	// WriteBlob stores a blob and returns its BlobID.
	WriteBlob(data []byte) (uint64, error)

	// ReadBlob reads a blob by its BlobID.
	ReadBlob(blobID uint64) ([]byte, error)
}
