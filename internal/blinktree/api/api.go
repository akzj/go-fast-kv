// Package blinktree provides the B-link-tree index API.
// This file defines ONLY interfaces — no implementation code.
//
// Design invariants:
//   - B-link-tree uses right-biased splits: new node always receives keys >= splitKey
//   - Latch crabbing: acquire parent before child, release in reverse order
//   - Single-writer/multi-reader: exclusive write access required for mutations
//   - Node immutability: once written, a node is never modified in place
//
// Why VAddr in entries (not PageID)?
//   Direct pointer avoids double-indirection through PageManager index.
//
// Why right-biased splits?
//   Simpler boundary condition: left contains <= splitKey, right contains > splitKey.
package blinktree

import (
	"errors"

	"github.com/akzj/go-fast-kv/internal/vaddr"
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

// =============================================================================
// Constants
// =============================================================================

const (
	// ExternalThreshold is the maximum size for inline values.
	// Values > 48 bytes are stored externally with VAddr reference.
	// Why 48? InlineValue.Data is 56 bytes; external values need 16 bytes for VAddr.
	ExternalThreshold = 48

	// NodeHeaderSize is the fixed-size header at the start of every node.
	// Why 64 bytes? Cache-line aligned for performance.
	NodeHeaderSize = 64

	// MaxNodeCapacity is the maximum entries per node.
	// Node capacity is (PageSize - NodeHeaderSize) / EntrySize.
	MaxNodeCapacity = 255 // ~4080 bytes / 16 bytes per entry

	// LeafEntrySize is the size of a leaf entry (key + inline value pointer).
	LeafEntrySize = 64 // 8 bytes key + 56 bytes inline value

	// InternalEntrySize is the size of an internal entry (key + child pointer).
	InternalEntrySize = 24 // 8 bytes key + 16 bytes VAddr
)

// Node type constants.
const (
	NodeTypeLeaf     uint8 = 0
	NodeTypeInternal uint8 = 1
)

// =============================================================================
// Error Types
// =============================================================================

var (
	// ErrKeyNotFound is returned when the requested key does not exist.
	ErrKeyNotFound = errors.New("blinktree: key not found")

	// ErrStoreClosed is returned when operations are attempted on a closed tree.
	ErrStoreClosed = errors.New("blinktree: store is closed")

	// ErrWriteLocked is returned when a write operation is attempted
	// while another write is in progress (single-writer violation).
	ErrWriteLocked = errors.New("blinktree: write operation in progress")

	// ErrNodeNotFound is returned when a node cannot be loaded from storage.
	ErrNodeNotFound = errors.New("blinktree: node not found at address")

	// ErrInvalidNode is returned when deserialized node fails validation.
	ErrInvalidNode = errors.New("blinktree: invalid node format")

	// ErrNodeFull is returned when a node cannot accept more entries.
	ErrNodeFull = errors.New("blinktree: node is full")

	// ErrKeyTooLarge is returned when key exceeds maximum size.
	ErrKeyTooLarge = errors.New("blinktree: key too large")

	// ErrValueTooLarge is returned when value exceeds maximum inline size.
	// For values > ExternalThreshold, use external value store directly.
	ErrValueTooLarge = errors.New("blinktree: value too large for inline storage")
)

// =============================================================================
// Node Data Structures
// =============================================================================

// NodeFormat is the binary layout for both internal and leaf nodes.
// Invariant: Node is always (NodeHeaderSize + entries) bytes.
// Invariant: Count <= Capacity; Count monotonically increases until node is sealed.
type NodeFormat struct {
	// Common header (64 bytes, cache-line aligned)
	NodeType  uint8  // 0=Leaf, 1=Internal
	IsDeleted uint8  // Soft delete flag (1=deleted)
	Level     uint8  // 0=leaf, 1+=internal levels
	Count     uint8  // Number of entries in use
	Capacity  uint16 // Total entry slots
	_         uint16 // Reserved for alignment

	HighSibling VAddr  // Pointer to next node at same level (right link)
	LowSibling  VAddr  // Pointer to previous node at same level (left link)
	HighKey     PageID // Maximum key in this subtree (internal nodes only)

	Checksum uint32 // CRC32c of node data
	_        [4]byte // Padding to 64-byte header alignment
}

// LeafEntry stores key-value pairs in leaf nodes.
// Invariant: Key is strictly increasing within a node.
// Invariant: For external values, Value.Data contains VAddr; for inline, Value.Data contains value.
type LeafEntry struct {
	Key   PageID      // 8 bytes
	Value InlineValue // 56 bytes
}

// InternalEntry stores separator keys and child pointers.
// Invariant: Key is separator between Child and next sibling's keys.
// Invariant: HighKey of child subtree is < this Key.
type InternalEntry struct {
	Key   PageID // 8 bytes: separator key
	Child VAddr  // 16 bytes: pointer to child node
}

// InlineValue encodes both inline values and external references.
// Why 56 bytes for data? LeafEntry is 64 bytes total; 8 bytes for key = 56 for value.
//
// Encoding:
//   - Top bit of Length[0] = 0: inline value, length = actual bytes
//   - Top bit of Length[0] = 1: external value, data contains VAddr
type InlineValue struct {
	Length [8]byte // Big-endian length (top bit = is_external flag)
	Data  [56]byte // Inline data (≤48 bytes) or VAddr (16 bytes)
}

// IsExternal returns true if this value is stored externally.
func (v InlineValue) IsExternal() bool {
	return v.Length[0]&0x80 != 0
}

// IsValid returns true if this InlineValue contains a non-empty value.
func (v InlineValue) IsValid() bool {
	return v.Length[0] != 0 || v.Length[1] != 0 || v.Length[2] != 0
}

// =============================================================================
// Interface: NodeOperations
// =============================================================================

// NodeOperations defines low-level node manipulation primitives.
// Implementations must be goroutine-safe when used with appropriate latching.
//
// Why not merge Search into a higher-level Find?
//   Separate Search allows the caller to implement custom traversal (e.g., lock-free reads).
//   Search is O(log n), callers may want to inject retry logic on concurrent splits.
//
// Why return (newNode, splitKey) from Insert?
//   Split is triggered by overflow; caller must decide whether to propagate up.
//   Returning newNode avoids internal coupling to NodeManager.
type NodeOperations interface {
	// Search finds the child index for key K in an internal node,
	// or returns the leaf entry index for a key in a leaf node.
	// For internal nodes: returns smallest i where entries[i].Key > K, or Count if none.
	// For leaf nodes: returns smallest i where entries[i].Key >= K, or Count if none.
	Search(node *NodeFormat, key PageID) int

	// Insert adds (key, value) to leaf node. Returns (newNode, splitKey, err).
	// If node has capacity, returns (nil, 0, nil) — no split needed.
	// If node overflows, returns (newRightNode, splitKey, nil) where splitKey is median key.
	// Why splitKey as PageID? Used to update parent's HighKey or create parent entry.
	//
	// Invariant: Insert never modifies original node; returns new node for split.
	Insert(node *NodeFormat, key PageID, value InlineValue) (newNode *NodeFormat, splitKey PageID, err error)

	// Split divides node at median key. Returns (left, right, splitKey).
	// Left receives first (Capacity/2) entries; right receives rest.
	// Why Split separate from Insert? Allows custom split policies.
	//
	// Invariant: splitKey is the first key in right node.
	// Invariant: Left.HighSibling = right address after split.
	Split(node *NodeFormat) (left, right *NodeFormat, splitKey PageID)

	// UpdateHighKey recomputes HighKey from rightmost child.
	// For leaf nodes: HighKey = last entry's key.
	// For internal nodes: HighKey = rightmost child's HighKey.
	UpdateHighKey(node *NodeFormat) PageID

	// Serialize returns binary representation for append-only storage.
	// Invariant: Output length = NodeHeaderSize + (entryCount * entrySize).
	Serialize(node *NodeFormat) []byte

	// Deserialize parses binary representation from storage.
	// Returns error if checksum fails or format is invalid.
	// Invariant: Deserialized node is immutable; never modify returned node.
	Deserialize(data []byte) (*NodeFormat, error)
}

// =============================================================================
// Interface: NodeManager
// =============================================================================

// NodeManager handles node lifecycle: allocation, persistence, and loading.
// All nodes are append-only; once persisted, a VAddr never changes.
//
// Why separate NodeManager from NodeOperations?
//   NodeOperations is stateless transformation; NodeManager handles storage.
//   This separation allows NodeOperations to be tested without storage.
//
// Why Persist returns VAddr?
//   Caller needs address to store in parent or return to caller.
type NodeManager interface {
	// CreateLeaf initializes a new empty leaf node with zero entries.
	// Returns (node, vaddr) where vaddr is the persistable address.
	// Invariant: Node.Count = 0, Node.Capacity = MaxNodeCapacity.
	CreateLeaf() (*NodeFormat, VAddr)

	// CreateInternal initializes a new internal node at given level.
	// Level 0 is invalid for internal nodes (use CreateLeaf).
	// Returns (node, vaddr) where vaddr is the persistable address.
	// Invariant: Node.Count = 0, Node.Level = level.
	CreateInternal(level uint8) (*NodeFormat, VAddr)

	// Persist appends node to append-only storage and returns its VAddr.
	// Invariant: Returned VAddr is unique and never reused.
	// Invariant: Persist is atomic; either succeeds completely or not at all.
	Persist(node *NodeFormat) (VAddr, error)

	// Load reads node from storage by VAddr.
	// Returns error if VAddr is invalid or node cannot be read.
	// Invariant: Returned node is immutable; caller must not modify.
	Load(vaddr VAddr) (*NodeFormat, error)

	// UpdateParent updates parent node's child pointer after a split.
	// Called when child split requires parent to reference new right child.
	// oldChild: original child VAddr (now contains keys <= splitKey)
	// newChild: new right child VAddr (contains keys > splitKey)
	// splitKey: median key that separates the two children
	//
	// Why UpdateParent in NodeManager?
	//   Parent node must be persisted to new address; NodeManager handles storage.
	//   This avoids exposing storage details to caller.
	UpdateParent(parentVAddr VAddr, oldChild, newChild VAddr, splitKey PageID) error
}

// =============================================================================
// Interface: Tree
// =============================================================================

// Tree is the high-level B-link-tree interface.
// All mutations require exclusive write access (enforced by implementation).
//
// Why single Write() method instead of Put/Delete?
//   Write unifies mutation semantics and allows future transaction support.
//   Separate Put/Delete available via TreeMutator interface.
type Tree interface {
	// Open initializes or opens an existing tree at the given path.
	// If path is empty, creates an in-memory tree.
	// Returns error if tree cannot be opened or initialized.
	Open(path string) error

	// Close releases all resources held by the tree.
	// Invariant: Close is idempotent; subsequent calls return nil.
	Close() error

	// IsClosed returns true if the tree has been closed.
	IsClosed() bool

	// Get retrieves the value for key.
	// Returns ErrKeyNotFound if key does not exist.
	// Why not return (value, found bool)? Consistent with KV store semantics.
	Get(key PageID) (InlineValue, error)

	// Write performs a mutation on the tree.
	// op determines whether this is a Put (insert/update) or Delete.
	// Returns error if operation fails.
	//
	// Invariant: Write is exclusive; only one Write may execute at a time.
	// Invariant: Write is atomic; partial writes are impossible.
	Write(op TreeOperation) error

	// Scan returns an iterator over keys in range [start, end).
	// If end is nil, scans to end of tree.
	// Iterator must be closed by caller.
	//
	// Why Scan instead of RangeQuery?
	//   Iterator pattern allows streaming results without loading all into memory.
	Scan(start, end PageID) (TreeIterator, error)

	// Batch performs multiple mutations atomically.
	// All operations succeed or none do.
	// Invariant: Batch acquires exclusive write lock for duration.
	Batch(ops []TreeOperation) error
}

// =============================================================================
// Interface: TreeMutator
// =============================================================================

// TreeMutator provides explicit Put/Delete methods.
// Use when caller prefers method names over operation constants.
//
// Why not merge with Tree?
//   Separate interface allows read-only trees to implement only Tree.
//   TreeMutator embeds Tree; callers can check interface assertion.
type TreeMutator interface {
	Tree

	// Put inserts or updates a key-value pair.
	// If value exceeds ExternalThreshold, caller must store externally
	// and pass InlineValue with VAddr encoding.
	//
	// Invariant: Put acquires exclusive write access.
	// Invariant: Put is atomic.
	Put(key PageID, value InlineValue) error

	// Delete removes a key-value pair.
	// Returns ErrKeyNotFound if key does not exist.
	// Deletion is soft; tombstone is written to maintain consistency.
	//
	// Invariant: Delete acquires exclusive write access.
	// Invariant: Delete is atomic.
	Delete(key PageID) error
}

// =============================================================================
// Supporting Types
// =============================================================================

// TreeOperation represents a mutation on the tree.
type TreeOperation struct {
	Type  OperationType
	Key   PageID
	Value InlineValue
}

// OperationType determines the kind of mutation.
type OperationType uint8

const (
	OpPut    OperationType = 0
	OpDelete OperationType = 1
)

// TreeIterator provides sequential access to a range of key-value pairs.
// Invariant: Iterator is not goroutine-safe; caller must ensure single access.
type TreeIterator interface {
	// Next advances the iterator and returns true if a valid entry exists.
	// Returns false when iteration is complete or on error.
	Next() bool

	// Key returns the current key.
	// Undefined if Next() has not returned true.
	Key() PageID

	// Value returns the current value.
	// Undefined if Next() has not returned true.
	Value() InlineValue

	// Error returns any error encountered during iteration.
	Error() error

	// Close releases resources held by the iterator.
	// Close is idempotent; subsequent calls are no-ops.
	Close()
}

// =============================================================================
// Factory Functions
// =============================================================================

// NewTree creates a new empty B-link-tree.
// Returns Tree that must be opened before use.
//
// Why a factory function instead of constructor?
//   Allows implementation flexibility (e.g., mock trees, different node managers).
func NewTree(nodeOps NodeOperations, nodeMgr NodeManager) Tree {
	panic("TODO: implementation provided by branch")
}

// NewTreeMutator creates a mutable B-link-tree.
func NewTreeMutator(nodeOps NodeOperations, nodeMgr NodeManager) TreeMutator {
	panic("TODO: implementation provided by branch")
}
