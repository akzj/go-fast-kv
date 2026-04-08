package internal

import (
	"encoding/binary"
	"hash/crc32"

	vaddr "github.com/akzj/go-fast-kv/internal/vaddr"
)

// =============================================================================
// NodeOperations Implementation
// =============================================================================

// nodeOperations implements NodeOperations interface.
type nodeOperations struct{}

// NewNodeOperations creates a new NodeOperations instance.
func NewNodeOperations() NodeOperations {
	return &nodeOperations{}
}

// Search finds the child index for key K in an internal node,
// or returns the leaf entry index for a key in a leaf node.
func (ops *nodeOperations) Search(node *NodeFormat, key PageID) int {
	if node.NodeType == NodeTypeLeaf {
		return ops.searchLeaf(node, key)
	}
	return ops.searchInternal(node, key)
}

func (ops *nodeOperations) searchLeaf(node *NodeFormat, key PageID) int {
	entries := ExtractLeafEntries(node)
	lo, hi := 0, int(node.Count)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if entries[mid].Key < key {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func (ops *nodeOperations) searchInternal(node *NodeFormat, key PageID) int {
	entries := ExtractInternalEntries(node)
	lo, hi := 0, int(node.Count)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if entries[mid].Key < key {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// Insert adds (key, value) to leaf node.
func (ops *nodeOperations) Insert(node *NodeFormat, key PageID, value InlineValue) (*NodeFormat, PageID, error) {
	if node.NodeType != NodeTypeLeaf {
		return nil, 0, ErrInvalidNode
	}

	entries := ExtractLeafEntries(node)

	// If node has capacity, just insert
	if int(node.Count) < int(node.Capacity) {
		pos := ops.searchLeaf(node, key)
		if node.Count == 0 {
			entries = []LeafEntry{{Key: key, Value: value}}
			node.Count++
			StoreLeafEntries(node, entries)
		} else {
			if pos < len(entries) && entries[pos].Key == key {
				entries[pos].Value = value
				StoreLeafEntries(node, entries)
			} else {
				newEntries := make([]LeafEntry, len(entries)+1)
				copy(newEntries, entries[:pos])
				newEntries[pos] = LeafEntry{Key: key, Value: value}
				copy(newEntries[pos+1:], entries[pos:])
				entries = newEntries
				node.Count++
				StoreLeafEntries(node, entries)
			}
		}
		return nil, 0, nil
	}

	// Need to split
	left, right, splitKey := ops.Split(node)

	// Determine which node gets the new key
	if key > splitKey {
		rightEntries := ExtractLeafEntries(right)
		pos := ops.searchLeaf(right, key)
		if pos < len(rightEntries) && rightEntries[pos].Key == key {
			rightEntries[pos].Value = value
			StoreLeafEntries(right, rightEntries)
		} else {
			newEntries := make([]LeafEntry, len(rightEntries)+1)
			copy(newEntries, rightEntries[:pos])
			newEntries[pos] = LeafEntry{Key: key, Value: value}
			copy(newEntries[pos+1:], rightEntries[pos:])
			right.Count++
			StoreLeafEntries(right, newEntries)
		}
	} else {
		entries := ExtractLeafEntries(left)
		pos := ops.searchLeaf(left, key)
		if pos < len(entries) && entries[pos].Key == key {
			entries[pos].Value = value
			StoreLeafEntries(left, entries)
		} else {
			newEntries := make([]LeafEntry, len(entries)+1)
			copy(newEntries, entries[:pos])
			newEntries[pos] = LeafEntry{Key: key, Value: value}
			copy(newEntries[pos+1:], entries[pos:])
			left.Count++
			StoreLeafEntries(left, newEntries)
		}
	}

	return right, splitKey, nil
}

// Split divides node at median key. Returns (left, right, splitKey).
func (ops *nodeOperations) Split(node *NodeFormat) (*NodeFormat, *NodeFormat, PageID) {
	if node.NodeType == NodeTypeLeaf {
		return ops.splitLeaf(node)
	}
	return ops.splitInternal(node)
}

func (ops *nodeOperations) splitLeaf(node *NodeFormat) (*NodeFormat, *NodeFormat, PageID) {
	entries := ExtractLeafEntries(node)
	median := int(node.Count) / 2
	splitKey := entries[median].Key // First key of right (separator)

	// Create right node
	right := &NodeFormat{
		NodeType:    NodeTypeLeaf,
		IsDeleted:   node.IsDeleted,
		Level:       node.Level,
		Count:       node.Count - uint8(median),
		Capacity:    node.Capacity,
		HighSibling: node.HighSibling, // Will be set by caller
		LowSibling:  0,               // Will be set to left's PageID by caller
		HighKey:     node.HighKey,
	}

	rightEntries := make([]LeafEntry, right.Count)
	copy(rightEntries, entries[median:])
	StoreLeafEntries(right, rightEntries)

	// Update left node
	leftEntries := make([]LeafEntry, median)
	copy(leftEntries, entries[:median])
	node.Count = uint8(median)
	StoreLeafEntries(node, leftEntries)

	return node, right, splitKey
}

func (ops *nodeOperations) splitInternal(node *NodeFormat) (*NodeFormat, *NodeFormat, PageID) {
	entries := ExtractInternalEntries(node)
	median := int(node.Count) / 2
	splitKey := entries[median].Key

	// Create right node with entries from median onwards
	right := &NodeFormat{
		NodeType:    NodeTypeInternal,
		IsDeleted:   node.IsDeleted,
		Level:       node.Level,
		Count:       node.Count - uint8(median),
		Capacity:    node.Capacity,
		HighSibling: node.HighSibling,
		LowSibling:  0, // Will be set by caller
		HighKey:     node.HighKey,
	}

	rightEntries := make([]InternalEntry, right.Count)
	copy(rightEntries, entries[median:])
	StoreInternalEntries(right, rightEntries)

	// Update left node with entries before median
	leftEntries := make([]InternalEntry, median)
	copy(leftEntries, entries[:median])
	node.Count = uint8(median)
	StoreInternalEntries(node, leftEntries)

	return node, right, splitKey
}

// UpdateHighKey recomputes HighKey from rightmost child.
func (ops *nodeOperations) UpdateHighKey(node *NodeFormat) PageID {
	if node.Count == 0 {
		return 0
	}
	if node.NodeType == NodeTypeLeaf {
		entries := ExtractLeafEntries(node)
		return entries[node.Count-1].Key
	}
	entries := ExtractInternalEntries(node)
	return entries[node.Count-1].Key
}

// Serialize returns binary representation.
// Layout (40-byte header + entries):
//   0: NodeType(1) + IsDeleted(1) + Level(1) + Count(1) = 4
//   4: Capacity(2) + Reserved(2) = 8
//   8: HighSibling(8) = 16
//  16: LowSibling(8) = 24
//  24: HighKey(8) = 32
//  32: Checksum(4) = 36
//  36: Padding(4) = 40
// 40+: LeafEntry(72) or InternalEntry(16)
func (ops *nodeOperations) Serialize(node *NodeFormat) []byte {
	buf := make([]byte, vaddr.PageSize)
	offset := 0

	// Write header fields
	buf[offset] = node.NodeType
	offset++
	buf[offset] = node.IsDeleted
	offset++
	buf[offset] = node.Level
	offset++

	// Defensive: cap count to what fits in page
	maxCount := uint8(255)
	if node.NodeType == NodeTypeLeaf {
		maxCount = uint8((vaddr.PageSize - 40) / LeafEntrySize)
	} else {
		maxCount = uint8((vaddr.PageSize - 40) / InternalEntrySize)
	}
	if node.Count > maxCount {
		node.Count = maxCount
	}
	buf[offset] = node.Count
	offset++

	binary.BigEndian.PutUint16(buf[offset:], node.Capacity)
	offset += 2
	binary.BigEndian.PutUint16(buf[offset:], 0) // Reserved
	offset += 2

	// HighSibling (PageID, 8 bytes)
	binary.BigEndian.PutUint64(buf[offset:], uint64(node.HighSibling))
	offset += 8

	// LowSibling (PageID, 8 bytes)
	binary.BigEndian.PutUint64(buf[offset:], uint64(node.LowSibling))
	offset += 8

	// HighKey (8 bytes)
	binary.BigEndian.PutUint64(buf[offset:], uint64(node.HighKey))
	offset += 8

	// Checksum (4 bytes) + Padding (4 bytes)
	offset += 8

	// Write entries
	if node.NodeType == NodeTypeLeaf {
		entries := ExtractLeafEntries(node)
		for i := 0; i < int(node.Count); i++ {
			binary.BigEndian.PutUint64(buf[offset:], uint64(entries[i].Key))
			offset += 8
			copy(buf[offset:], entries[i].Value.Length[:])
			offset += 8
			copy(buf[offset:], entries[i].Value.Data[:])
			offset += 56
		}
	} else {
		entries := ExtractInternalEntries(node)
		for i := 0; i < int(node.Count); i++ {
			binary.BigEndian.PutUint64(buf[offset:], uint64(entries[i].Key))
			offset += 8
			binary.BigEndian.PutUint64(buf[offset:], uint64(entries[i].Child))
			offset += 8
		}
	}

	// Compute checksum on full buffer, zeroing checksum field first
	binary.BigEndian.PutUint32(buf[32:36], 0)
	cs := crc32.ChecksumIEEE(buf)
	binary.BigEndian.PutUint32(buf[32:36], cs)
	node.Checksum = cs

	return buf
}

// Deserialize parses binary representation from storage.
func (ops *nodeOperations) Deserialize(data []byte) (*NodeFormat, error) {
	if len(data) < 40 {
		return nil, ErrInvalidNode
	}

	offset := 0
	node := &NodeFormat{}

	node.NodeType = data[offset]
	offset++
	node.IsDeleted = data[offset]
	offset++
	node.Level = data[offset]
	offset++
	node.Count = data[offset]
	offset++

	node.Capacity = binary.BigEndian.Uint16(data[offset:])
	offset += 2
	offset += 2 // Reserved

	node.HighSibling = PageID(binary.BigEndian.Uint64(data[offset:]))
	offset += 8

	node.LowSibling = PageID(binary.BigEndian.Uint64(data[offset:]))
	offset += 8

	node.HighKey = PageID(binary.BigEndian.Uint64(data[offset:]))
	offset += 8

	storedCS := binary.BigEndian.Uint32(data[offset:])
	offset += 4
	offset += 4 // Padding

	// Verify checksum
	data[32] = 0
	data[33] = 0
	data[34] = 0
	data[35] = 0
	cs := crc32.ChecksumIEEE(data)
	if storedCS != cs {
		return nil, ErrInvalidNode
	}
	node.Checksum = storedCS

	// Copy entry data (starts at offset 40)
	var entrySize int
	if node.NodeType == NodeTypeLeaf {
		entrySize = LeafEntrySize
	} else {
		entrySize = InternalEntrySize
	}
	entryDataLen := int(node.Count) * entrySize
	node.RawData = make([]byte, entryDataLen)
	copy(node.RawData, data[40:40+entryDataLen])

	return node, nil
}

// =============================================================================
// Entry Storage (using RawData field in NodeFormat)
// =============================================================================

// NodeFormatWithEntries is a helper for accessing parsed entries.
type NodeFormatWithEntries struct {
	*NodeFormat
	leafEntries     []LeafEntry
	internalEntries []InternalEntry
}

// ExtractLeafEntries extracts leaf entries from node's RawData or returns empty.
func ExtractLeafEntries(node *NodeFormat) []LeafEntry {
	if node == nil {
		return nil
	}
	entries := make([]LeafEntry, node.Count)
	if len(node.RawData) == 0 || len(node.RawData) < int(node.Count)*LeafEntrySize {
		return entries
	}

	offset := 0
	for i := 0; i < int(node.Count); i++ {
		entries[i].Key = PageID(binary.BigEndian.Uint64(node.RawData[offset:]))
		offset += 8
		copy(entries[i].Value.Length[:], node.RawData[offset:])
		offset += 8
		copy(entries[i].Value.Data[:], node.RawData[offset:offset+56])
		offset += 56
	}
	return entries
}

// StoreLeafEntries serializes entries back into node.RawData.
func StoreLeafEntries(node *NodeFormat, entries []LeafEntry) {
	size := len(entries) * LeafEntrySize
	node.RawData = make([]byte, size)
	offset := 0
	for i := 0; i < len(entries); i++ {
		binary.BigEndian.PutUint64(node.RawData[offset:], uint64(entries[i].Key))
		offset += 8
		copy(node.RawData[offset:], entries[i].Value.Length[:])
		offset += 8
		copy(node.RawData[offset:], entries[i].Value.Data[:])
		offset += 56
	}
}

// ExtractInternalEntries extracts internal entries from node's RawData.
// Child is PageID (stable logical address).
func ExtractInternalEntries(node *NodeFormat) []InternalEntry {
	if node == nil {
		return nil
	}
	entries := make([]InternalEntry, node.Count)
	if len(node.RawData) == 0 {
		return entries
	}

	offset := 0
	for i := 0; i < int(node.Count); i++ {
		entries[i].Key = PageID(binary.BigEndian.Uint64(node.RawData[offset:]))
		offset += 8
		entries[i].Child = PageID(binary.BigEndian.Uint64(node.RawData[offset:]))
		offset += 8
	}
	return entries
}

// StoreInternalEntries serializes entries back into node.RawData.
// Child is PageID (stable logical address).
func StoreInternalEntries(node *NodeFormat, entries []InternalEntry) {
	size := len(entries) * InternalEntrySize
	node.RawData = make([]byte, size)
	offset := 0
	for i := 0; i < len(entries); i++ {
		binary.BigEndian.PutUint64(node.RawData[offset:], uint64(entries[i].Key))
		offset += 8
		binary.BigEndian.PutUint64(node.RawData[offset:], uint64(entries[i].Child))
		offset += 8
	}
}

// MakeInlineValue creates an InlineValue from a byte slice.
func MakeInlineValue(data []byte) InlineValue {
	var iv InlineValue
	binary.BigEndian.PutUint64(iv.Length[:], uint64(len(data)))
	copy(iv.Data[:], data)
	return iv
}
