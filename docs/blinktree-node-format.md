# B-link-tree Node Format Specification

## 1. Overview

Defines the on-disk format for B-link-tree nodes and the core operations interface. All nodes are immutable after creation (append-only constraint).

## 2. Invariants

1. **NodeImmutability**: Once written to append-only storage, a node is never modified.
2. **SiblingChain**: Leaf nodes form a doubly-linked list via `HighSibling`. Internal nodes form a singly-linked list.
3. **KeyOrdering**: Keys within a node are strictly increasing (no duplicates at same level).
4. **SplitBoundary**: For a split at key K, left node contains keys ≤ K, right node contains keys > K.
5. **HighKeyInvariant**: Every internal node's `HighKey` equals the maximum key in its rightmost child subtree.

## 3. Node Structure

### 3.1 Binary Layout

```go
// NodeFormat is the binary layout for both internal and leaf nodes.
type NodeFormat struct {
    // Common header (32 bytes)
    NodeType     uint8   // 0=Leaf, 1=Internal
    IsDeleted    uint8   // Soft delete flag
    Level        uint8   // 0=leaf, 1+=internal levels
    Count        uint8   // Number of entries in use
    Capacity     uint16  // Total entry slots
    Reserved     uint16
    
    HighSibling  VAddr   // Pointer to next node at same level
    LowSibling   VAddr   // Pointer to previous node at same level
    HighKey      PageID  // Maximum key in this subtree (internal only)
    Checksum     uint32  // CRC32c
    
    _            [4]byte // Padding to 64-byte header alignment
}
```

### 3.2 Entry Formats

```go
// LeafNode body: Count entries of this format
type LeafEntry struct {
    Key     PageID        // Fixed-size key (8 bytes)
    Value   InlineValue   // Inline value or pointer
}

// InternalNode body: Count entries of this format
type InternalEntry struct {
    Key     PageID        // Separator key (8 bytes)
    Child   VAddr         // Pointer to child node
}

// InlineValue encodes both inline values and external references.
type InlineValue struct {
    Length [8]byte        // Big-endian length (top bit = is_external flag)
    Data   [56]byte       // Inline data or VAddr of external value
}

// Constants
const (
    MaxInlineValueSize = 55    // 56 - 1 length byte
    ExternalThreshold  = 48    // Values > 48 bytes get externalized
)

// InlineValue helper methods
func (v *InlineValue) IsExternal() bool {
    return v.Length[0]&0x80 != 0
}

func (v *InlineValue) GetSize() uint64 {
    return binary.BigEndian.Uint64(v.Length[:]) & 0x7F
}
```

## 4. Node Operations Interface

```go
type NodeOperations interface {
    // Search finds the child index for key K.
    Search(node *NodeFormat, key PageID) int

    // Insert adds (key, value) to leaf node. Returns (newNode, splitKey).
    Insert(node *NodeFormat, key PageID, value InlineValue) (newNode *NodeFormat, splitKey PageID)

    // Split divides node at median key. Returns (left, right, splitKey).
    Split(node *NodeFormat) (left *NodeFormat, right *NodeFormat, splitKey PageID)

    // UpdateHighKey recomputes HighKey from rightmost child.
    UpdateHighKey(node *NodeFormat) PageID

    // Serialize returns binary representation for append-only storage.
    Serialize(node *NodeFormat) []byte

    // Deserialize parses binary representation from storage.
    Deserialize(data []byte) (*NodeFormat, error)
}
```

## 5. Append-Only Node Management

```go
type NodeManager interface {
    // CreateLeaf initializes a new empty leaf node.
    CreateLeaf() (*NodeFormat, VAddr)

    // CreateInternal initializes a new internal node with given level.
    CreateInternal(level uint8) (*NodeFormat, VAddr)

    // Persist appends node to append-only storage.
    Persist(node *NodeFormat) VAddr

    // Load reads node from storage by VAddr.
    Load(vaddr VAddr) (*NodeFormat, error)

    // UpdateParent updates parent node's child pointer after split.
    UpdateParent(parentVAddr VAddr, oldChild VAddr, newChild VAddr, splitKey PageID) error
}
```

## 6. Concurrency Patterns

### Write Path
1. Acquire write latch on node
2. Perform operation
3. If split: create new node, update parent (append)
4. Release latch

### Read Path (optimistic, no latches)
1. Start at root
2. Search for key
3. If HighSibling exists for target key: load sibling, retry
4. If node was split during read: retry from root

## 7. Design Decisions

| Decision | Alternative | Why Not |
|----------|-------------|---------|
| VAddr in entries | PageID | Direct lookup avoids double-indirection |
| HighKey in internal nodes | None | O(1) routing; no child load needed |
| Right-biased split | Left-biased | Simpler boundary condition |
| InlineValue in 16-byte slot | Separate pointer | Cache-friendly for small values |
| Sibling pointers on internal | None | Lock-free internal node traversal |

---

*Document Status: Contract Definition*
*Last Updated: 2024*
