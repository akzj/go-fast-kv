package internal

import (
	"encoding/binary"
	"hash/crc32"

	"github.com/akzj/go-fast-kv/internal/vaddr"
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
		// Handle empty node case (Count=0)
		if node.Count == 0 {
			entries = []LeafEntry{{Key: key, Value: value}}
		} else {
			// Create new slice with room for one more entry
			newEntries := make([]LeafEntry, 0, node.Count+1)
			newEntries = append(newEntries, entries[:pos]...)
			newEntries = append(newEntries, LeafEntry{Key: key, Value: value})
			if pos < len(entries) {
				newEntries = append(newEntries, entries[pos:]...)
			}
			entries = newEntries
		}
		node.Count++
		StoreLeafEntries(node, entries)
		return nil, 0, nil
	}

	// Need to split
	left, right, splitKey := ops.Split(node)

	// Determine which node gets the new key
	if key >= splitKey {
		rightEntries := ExtractLeafEntries(right)
		pos := ops.searchLeaf(right, key)
		copy(rightEntries[pos+1:], rightEntries[pos:])
		rightEntries[pos] = LeafEntry{Key: key, Value: value}
		right.Count++
		StoreLeafEntries(right, rightEntries)
	} else {
		entries := ExtractLeafEntries(left)
		pos := ops.searchLeaf(left, key)
		copy(entries[pos+1:], entries[pos:])
		entries[pos] = LeafEntry{Key: key, Value: value}
		left.Count++
		StoreLeafEntries(left, entries)
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
	// Split so that splitKey goes to LEFT as the last entry.
	// Left has keys <= splitKey, right has keys > splitKey.
	// Use ceil(n/2) for left to ensure splitKey is the last key of left.
	median := (int(node.Count) + 1) / 2
	splitKey := entries[median-1].Key // Last key of left

	// Create right node - must NOT share RawData with left
	right := &NodeFormat{
		NodeType:     NodeTypeLeaf,
		IsDeleted:    node.IsDeleted,
		Level:        node.Level,
		Count:        node.Count - uint8(median),
		Capacity:     node.Capacity,
		HighSibling:  node.HighSibling,
		LowSibling:   vaddr.VAddr{},
		HighKey:      node.HighKey,
	}

	// Copy entries to right's own buffer
	rightEntries := make([]LeafEntry, right.Count)
	copy(rightEntries, entries[median:])
	StoreLeafEntries(right, rightEntries)

	// Update left node - use entries from left's own buffer
	leftEntries := make([]LeafEntry, median)
	copy(leftEntries, entries[:median])
	node.Count = uint8(median)
	node.HighSibling = vaddr.VAddr{} // Will be set by caller
	StoreLeafEntries(node, leftEntries)

	return node, right, splitKey
}

func (ops *nodeOperations) splitInternal(node *NodeFormat) (*NodeFormat, *NodeFormat, PageID) {
	entries := ExtractInternalEntries(node)
	median := int(node.Count) / 2
	splitKey := entries[median].Key

	// Create right node - must NOT share RawData with left
	right := &NodeFormat{
		NodeType:     NodeTypeInternal,
		IsDeleted:    node.IsDeleted,
		Level:        node.Level,
		Count:        node.Count - uint8(median) - 1, // Exclude median separator
		Capacity:     node.Capacity,
		HighSibling:  node.HighSibling,
		LowSibling:   vaddr.VAddr{},
		HighKey:      node.HighKey,
	}

	// Copy entries to right's own buffer (skip the median entry)
	rightEntries := make([]InternalEntry, right.Count)
	copy(rightEntries, entries[median+1:])
	StoreInternalEntries(right, rightEntries)

	// Update left node - use entries from left's own buffer (exclude median)
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
// Always outputs PageSize bytes for consistent checksum and storage alignment.
func (ops *nodeOperations) Serialize(node *NodeFormat) []byte {
	// Always output PageSize for storage alignment
	buf := make([]byte, vaddr.PageSize)
	offset := 0

	// Write header fields
	buf[offset] = node.NodeType
	offset++
	buf[offset] = node.IsDeleted
	offset++
	buf[offset] = node.Level
	offset++
	buf[offset] = node.Count
	offset++

	binary.BigEndian.PutUint16(buf[offset:], node.Capacity)
	offset += 2
	binary.BigEndian.PutUint16(buf[offset:], 0)
	offset += 2

	// HighSibling
	hs := node.HighSibling.ToBytes()
	copy(buf[offset:], hs[:])
	offset += 16

	// LowSibling
	ls := node.LowSibling.ToBytes()
	copy(buf[offset:], ls[:])
	offset += 16

	// HighKey
	binary.BigEndian.PutUint64(buf[offset:], uint64(node.HighKey))
	offset += 8

	// Skip checksum field (will compute below)
	offset += 4
	offset += 4 // Padding

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
			child := entries[i].Child.ToBytes()
			copy(buf[offset:], child[:])
			offset += 16
		}
	}

	// Compute checksum on full PageSize buffer
	// Zero checksum field first, then compute
	binary.BigEndian.PutUint32(buf[48:52], 0)
	cs := crc32.ChecksumIEEE(buf)
	binary.BigEndian.PutUint32(buf[48:52], cs)
	node.Checksum = cs

	return buf
}

// Deserialize parses binary representation from storage.
func (ops *nodeOperations) Deserialize(data []byte) (*NodeFormat, error) {
	if len(data) < NodeHeaderSize {
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

	node.HighSibling = vaddr.VAddrFromBytes(read16(data, &offset))
	node.LowSibling = vaddr.VAddrFromBytes(read16(data, &offset))
	node.HighKey = PageID(binary.BigEndian.Uint64(data[offset:]))
	offset += 8

	storedCS := binary.BigEndian.Uint32(data[offset:])
	offset += 4
	offset += 4 // Padding

	// Verify checksum
	// Zero checksum field at bytes 48-51 before computing checksum
	// This must match how Serialize computes it (writes to buf[48:52])
	data[48] = 0
	data[49] = 0
	data[50] = 0
	data[51] = 0
	cs := crc32.ChecksumIEEE(data)
	if storedCS != cs {
		return nil, ErrInvalidNode
	}

	node.Checksum = storedCS

	// Store raw data for entry extraction
	// Entries start at offset 56 in serialized format
	node.RawData = data[56:]

	return node, nil
}

func read16(data []byte, offset *int) [16]byte {
	var b [16]byte
	copy(b[:], data[*offset:*offset+16])
	*offset += 16
	return b
}

// =============================================================================
// Entry Storage (using RawData field in NodeFormat)
// =============================================================================

// RawData stores the serialized entry bytes for use after deserialization.
// This is stored as a []byte slice in the NodeFormat for in-memory operations.
type NodeFormatWithEntries struct {
	*NodeFormat
	leafEntries    []LeafEntry
	internalEntries []InternalEntry
}

// ExtractLeafEntries extracts leaf entries from node's RawData or returns empty.
func ExtractLeafEntries(node *NodeFormat) []LeafEntry {
	if node == nil {
		return nil
	}
	entries := make([]LeafEntry, node.Count)

	// If RawData is empty or too short for the count, return zeroed entries
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
		entries[i].Child = vaddr.VAddrFromBytes(read16(node.RawData, &offset))
	}
	return entries
}

// StoreInternalEntries serializes entries back into node.RawData.
func StoreInternalEntries(node *NodeFormat, entries []InternalEntry) {
	size := len(entries) * InternalEntrySize
	node.RawData = make([]byte, size)
	offset := 0
	for i := 0; i < len(entries); i++ {
		binary.BigEndian.PutUint64(node.RawData[offset:], uint64(entries[i].Key))
		offset += 8
		child := entries[i].Child.ToBytes()
		copy(node.RawData[offset:], child[:])
		offset += 16
	}
}

// MakeInlineValue creates an InlineValue from a byte slice.
func MakeInlineValue(data []byte) InlineValue {
	var iv InlineValue
	binary.BigEndian.PutUint64(iv.Length[:], uint64(len(data)))
	copy(iv.Data[:], data)
	return iv
}
