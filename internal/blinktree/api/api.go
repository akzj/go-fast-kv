// Package blinktree provides the B-link-tree index API.
// This file defines ONLY interfaces — no implementation code.
//
// Design invariants:
//   - B-link-tree uses right-biased splits: new node always receives keys >= splitKey
//   - Latch crabbing: acquire parent before child, release in reverse order
//   - Single-writer/multi-reader: exclusive write access required for mutations
//   - Node immutability: once written, a node is never modified in place
//   - B-tree uses PageID (logical, stable) for child/sibling pointers
//   - VAddr (physical, append-only) is only used inside storage layer
//   - Keys are raw []byte, compared with bytes.Compare (lexicographic order)
//   - MaxKeySize = 64 bytes; keys are stored in fixed 66-byte slots (64 data + 2 length)
package blinktree

import (
	"errors"

	vaddr "github.com/akzj/go-fast-kv/internal/vaddr"
)

// =============================================================================
// Type Aliases
// =============================================================================

type VAddr = vaddr.VAddr
type PageID = vaddr.PageID

// =============================================================================
// Constants
// =============================================================================

const (
	ExternalThreshold = 48
	MaxKeySize        = 64                   // Maximum key size in bytes
	KeySlotSize       = MaxKeySize + 2       // 64 bytes data + 2 bytes length = 66
	NodeHeaderSize    = 98                   // 1+1+1+1+2+2+8+8+66+4+4 = 98
	MaxNodeCapacity   = 255
	LeafEntrySize     = KeySlotSize + 64     // 66 (key slot) + 64 (InlineValue) = 130
	InternalEntrySize = KeySlotSize + 8      // 66 (key slot) + 8 (Child PageID) = 74
	NodeTypeLeaf      = uint8(0)
	NodeTypeInternal  = uint8(1)
	OpPut             = OperationType(0)
	OpDelete          = OperationType(1)
)

// =============================================================================
// Errors
// =============================================================================

var (
	ErrKeyNotFound   = errors.New("blinktree: key not found")
	ErrStoreClosed   = errors.New("blinktree: store is closed")
	ErrWriteLocked   = errors.New("blinktree: write operation in progress")
	ErrNodeNotFound  = errors.New("blinktree: node not found at address")
	ErrInvalidNode   = errors.New("blinktree: invalid node format")
	ErrNodeFull      = errors.New("blinktree: node is full")
	ErrKeyTooLarge   = errors.New("blinktree: key too large")
	ErrValueTooLarge = errors.New("blinktree: value too large for inline storage")
)

// =============================================================================
// Data Structures
// =============================================================================

type NodeFormat struct {
	NodeType    uint8
	IsDeleted   uint8
	Level       uint8
	Count       uint8
	Capacity    uint16
	_           uint16
	HighSibling PageID // Sibling pointer: stable PageID, not VAddr
	LowSibling  PageID // Sibling pointer: stable PageID, not VAddr
	HighKey     []byte // Highest key in this node (variable length, max 64 bytes)
	Checksum    uint32
	_           [4]byte
	RawData     []byte
}

type LeafEntry struct {
	Key   []byte
	Value InlineValue
}

type InternalEntry struct {
	Key   []byte
	Child PageID // Stable logical PageID, not physical VAddr
}

type InlineValue struct {
	Length [8]byte
	Data   [56]byte
}

func (v InlineValue) IsExternal() bool {
	return v.Length[0]&0x80 != 0
}

func (v InlineValue) IsValid() bool {
	return v.Length[0] != 0 || v.Length[1] != 0 || v.Length[2] != 0 ||
		v.Length[3] != 0 || v.Length[4] != 0 || v.Length[5] != 0 ||
		v.Length[6] != 0 || v.Length[7] != 0
}

// =============================================================================
// Interfaces
// =============================================================================

type NodeOperations interface {
	Search(node *NodeFormat, key []byte) int
	Insert(node *NodeFormat, key []byte, value InlineValue) (newNode *NodeFormat, splitKey []byte, err error)
	Split(node *NodeFormat) (left, right *NodeFormat, splitKey []byte)
	UpdateHighKey(node *NodeFormat) []byte
	Serialize(node *NodeFormat) []byte
	Deserialize(data []byte) (*NodeFormat, error)
}

// NodeManager manages node persistence using stable PageIDs.
// The underlying storage maps PageID → VAddr internally.
// B-tree code never sees VAddr.
type NodeManager interface {
	// CreateLeaf creates a new empty leaf node and returns it with its PageID.
	CreateLeaf() (*NodeFormat, PageID)

	// CreateInternal creates a new internal node at given level.
	CreateInternal(level uint8) (*NodeFormat, PageID)

	// Persist writes the node data for the given PageID.
	// PageID is stable — the same PageID is used for the lifetime of the node.
	Persist(node *NodeFormat, pageID PageID) error

	// Load reads the node for the given PageID.
	Load(pageID PageID) (*NodeFormat, error)
}

type TreeOperation struct {
	Type  OperationType
	Key   []byte
	Value InlineValue
}

type OperationType uint8

type TreeIterator interface {
	Next() bool
	Key() []byte
	Value() InlineValue
	Error() error
	Close()
}

type Tree interface {
	Open(path string) error
	Close() error
	IsClosed() bool
	Get(key []byte) (InlineValue, error)
	Write(op TreeOperation) error
	Scan(start, end []byte) (TreeIterator, error)
	Batch(ops []TreeOperation) error
}

type TreeMutator interface {
	Tree
	Put(key []byte, value InlineValue) error
	Delete(key []byte) error
	GetRootPageID() PageID
	RestoreRootPageID(pageID PageID)
}
