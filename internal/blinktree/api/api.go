// Package blinktree provides the B-link-tree index API.
// This file defines ONLY interfaces — no implementation code.
//
// Design invariants:
//   - B-link-tree uses right-biased splits: new node always receives keys >= splitKey
//   - Latch crabbing: acquire parent before child, release in reverse order
//   - Single-writer/multi-reader: exclusive write access required for mutations
//   - Node immutability: once written, a node is never modified in place
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
	ExternalThreshold  = 48
	NodeHeaderSize     = 64
	MaxNodeCapacity    = 255
	LeafEntrySize      = 72  // 8 (Key) + 8 (Length) + 56 (Data)
	InternalEntrySize  = 24  // 8 (Key) + 16 (Child VAddr)
	NodeTypeLeaf       = uint8(0)
	NodeTypeInternal   = uint8(1)
	OpPut              = OperationType(0)
	OpDelete           = OperationType(1)
)

// =============================================================================
// Errors
// =============================================================================

var (
	ErrKeyNotFound = errors.New("blinktree: key not found")
	ErrStoreClosed = errors.New("blinktree: store is closed")
	ErrWriteLocked = errors.New("blinktree: write operation in progress")
	ErrNodeNotFound = errors.New("blinktree: node not found at address")
	ErrInvalidNode = errors.New("blinktree: invalid node format")
	ErrNodeFull     = errors.New("blinktree: node is full")
	ErrKeyTooLarge  = errors.New("blinktree: key too large")
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
	HighSibling VAddr
	LowSibling  VAddr
	HighKey     PageID
	Checksum    uint32
	_           [4]byte
	RawData     []byte
}

type LeafEntry struct {
	Key   PageID
	Value InlineValue
}

type InternalEntry struct {
	Key   PageID
	Child VAddr
}

type InlineValue struct {
	Length [8]byte
	Data   [56]byte
}

func (v InlineValue) IsExternal() bool {
	return v.Length[0]&0x80 != 0
}

func (v InlineValue) IsValid() bool {
	// For external values: top bit of Length[0] is set
	// For inline values: length > 0 means valid
	// Check if any length byte is non-zero
	return v.Length[0] != 0 || v.Length[1] != 0 || v.Length[2] != 0 ||
		v.Length[3] != 0 || v.Length[4] != 0 || v.Length[5] != 0 ||
		v.Length[6] != 0 || v.Length[7] != 0
}

// =============================================================================
// Interfaces
// =============================================================================

type NodeOperations interface {
	Search(node *NodeFormat, key PageID) int
	Insert(node *NodeFormat, key PageID, value InlineValue) (newNode *NodeFormat, splitKey PageID, err error)
	Split(node *NodeFormat) (left, right *NodeFormat, splitKey PageID)
	UpdateHighKey(node *NodeFormat) PageID
	Serialize(node *NodeFormat) []byte
	Deserialize(data []byte) (*NodeFormat, error)
}

type NodeManager interface {
	CreateLeaf() (*NodeFormat, VAddr)
	CreateInternal(level uint8) (*NodeFormat, VAddr)
	Persist(node *NodeFormat) (VAddr, error)
	Load(addr VAddr) (*NodeFormat, error)
	UpdateParent(parentVAddr VAddr, oldChild, newChild VAddr, splitKey PageID) error
}

type TreeOperation struct {
	Type  OperationType
	Key   PageID
	Value InlineValue
}

type OperationType uint8

type TreeIterator interface {
	Next() bool
	Key() PageID
	Value() InlineValue
	Error() error
	Close()
}

type Tree interface {
	Open(path string) error
	Close() error
	IsClosed() bool
	Get(key PageID) (InlineValue, error)
	Write(op TreeOperation) error
	Scan(start, end PageID) (TreeIterator, error)
	Batch(ops []TreeOperation) error
}

type TreeMutator interface {
	Tree
	Put(key PageID, value InlineValue) error
	Delete(key PageID) error
	GetRootAddress() []byte
	RestoreRoot(data []byte)
}
