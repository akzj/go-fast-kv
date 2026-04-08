package internal

import (
	"bytes"
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

// compareKeys compares two byte-slice keys lexicographically.
func compareKeys(a, b []byte) int {
	return bytes.Compare(a, b)
}

// Search finds the child index for key K in an internal node,
// or returns the leaf entry index for a key in a leaf node.
// Uses lower-bound binary search: returns first index where entry.Key >= key.
func (ops *nodeOperations) Search(node *NodeFormat, key []byte) int {
	if node.NodeType == NodeTypeLeaf {
		return ops.searchLeaf(node, key)
	}
	return ops.searchInternal(node, key)
}

func (ops *nodeOperations) searchLeaf(node *NodeFormat, key []byte) int {
	entries := ExtractLeafEntries(node)
	lo, hi := 0, int(node.Count)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if compareKeys(entries[mid].Key, key) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func (ops *nodeOperations) searchInternal(node *NodeFormat, key []byte) int {
	entries := ExtractInternalEntries(node)
	lo, hi := 0, int(node.Count)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if compareKeys(entries[mid].Key, key) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// Insert adds (key, value) to leaf node.
func (ops *nodeOperations) Insert(node *NodeFormat, key []byte, value InlineValue) (*NodeFormat, []byte, error) {
	if node.NodeType != NodeTypeLeaf {
		return nil, nil, ErrInvalidNode
	}

	entries := ExtractLeafEntries(node)

	// If node has capacity, just insert
	if int(node.Count) < int(node.Capacity) {
		pos := ops.searchLeaf(node, key)
		if node.Count == 0 {
			entries = []LeafEntry{{Key: copyKey(key), Value: value}}
			node.Count++
			StoreLeafEntries(node, entries)
		} else {
			if pos < len(entries) && compareKeys(entries[pos].Key, key) == 0 {
				entries[pos].Value = value
				StoreLeafEntries(node, entries)
			} else {
				newEntries := make([]LeafEntry, len(entries)+1)
				copy(newEntries, entries[:pos])
				newEntries[pos] = LeafEntry{Key: copyKey(key), Value: value}
				copy(newEntries[pos+1:], entries[pos:])
				entries = newEntries
				node.Count++
				StoreLeafEntries(node, entries)
			}
		}
		return nil, nil, nil
	}

	// Need to split
	left, right, splitKey := ops.Split(node)

	// Determine which node gets the new key
	if compareKeys(key, splitKey) > 0 {
		rightEntries := ExtractLeafEntries(right)
		pos := ops.searchLeaf(right, key)
		if pos < len(rightEntries) && compareKeys(rightEntries[pos].Key, key) == 0 {
			rightEntries[pos].Value = value
			StoreLeafEntries(right, rightEntries)
		} else {
			newEntries := make([]LeafEntry, len(rightEntries)+1)
			copy(newEntries, rightEntries[:pos])
			newEntries[pos] = LeafEntry{Key: copyKey(key), Value: value}
			copy(newEntries[pos+1:], rightEntries[pos:])
			right.Count++
			StoreLeafEntries(right, newEntries)
		}
	} else {
		entries := ExtractLeafEntries(left)
		pos := ops.searchLeaf(left, key)
		if pos < len(entries) && compareKeys(entries[pos].Key, key) == 0 {
			entries[pos].Value = value
			StoreLeafEntries(left, entries)
		} else {
			newEntries := make([]LeafEntry, len(entries)+1)
			copy(newEntries, entries[:pos])
			newEntries[pos] = LeafEntry{Key: copyKey(key), Value: value}
			copy(newEntries[pos+1:], entries[pos:])
			left.Count++
			StoreLeafEntries(left, newEntries)
		}
	}

	return right, splitKey, nil
}

// Split divides node at median key. Returns (left, right, splitKey).
func (ops *nodeOperations) Split(node *NodeFormat) (*NodeFormat, *NodeFormat, []byte) {
	if node.NodeType == NodeTypeLeaf {
		return ops.splitLeaf(node)
	}
	return ops.splitInternal(node)
}

func (ops *nodeOperations) splitLeaf(node *NodeFormat) (*NodeFormat, *NodeFormat, []byte) {
	entries := ExtractLeafEntries(node)
	median := int(node.Count) / 2
	splitKey := copyKey(entries[median].Key) // First key of right (separator)

	// Create right node
	right := &NodeFormat{
		NodeType:    NodeTypeLeaf,
		IsDeleted:   node.IsDeleted,
		Level:       node.Level,
		Count:       node.Count - uint8(median),
		Capacity:    node.Capacity,
		HighSibling: node.HighSibling, // Will be set by caller
		LowSibling:  0,               // Will be set to left's PageID by caller
		HighKey:     copyKey(node.HighKey),
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

func (ops *nodeOperations) splitInternal(node *NodeFormat) (*NodeFormat, *NodeFormat, []byte) {
	entries := ExtractInternalEntries(node)
	median := int(node.Count) / 2
	splitKey := copyKey(entries[median].Key)

	// Create right node with entries from median onwards
	right := &NodeFormat{
		NodeType:    NodeTypeInternal,
		IsDeleted:   node.IsDeleted,
		Level:       node.Level,
		Count:       node.Count - uint8(median),
		Capacity:    node.Capacity,
		HighSibling: node.HighSibling,
		LowSibling:  0, // Will be set by caller
		HighKey:     copyKey(node.HighKey),
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
func (ops *nodeOperations) UpdateHighKey(node *NodeFormat) []byte {
	if node.Count == 0 {
		return nil
	}
	if node.NodeType == NodeTypeLeaf {
		entries := ExtractLeafEntries(node)
		return copyKey(entries[node.Count-1].Key)
	}
	entries := ExtractInternalEntries(node)
	return copyKey(entries[node.Count-1].Key)
}

// Serialize returns binary representation.
// Layout (98-byte header + entries):
//   0: NodeType(1) + IsDeleted(1) + Level(1) + Count(1) = 4
//   4: Capacity(2) + Reserved(2) = 8
//   8: HighSibling(8) = 16
//  16: LowSibling(8) = 24
//  24: HighKey(66) = 90  [64 bytes data + 2 bytes length]
//  90: Checksum(4) = 94
//  94: Padding(4) = 98
//  98+: LeafEntry(130) or InternalEntry(74)
//
// Key slot format (66 bytes): [keyData:64][keyLen:2]
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
		maxCount = uint8((vaddr.PageSize - NodeHeaderSize) / LeafEntrySize)
	} else {
		maxCount = uint8((vaddr.PageSize - NodeHeaderSize) / InternalEntrySize)
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

	// HighKey (66 bytes: 64 data + 2 length)
	writeKeySlot(buf[offset:], node.HighKey)
	offset += KeySlotSize

	// Checksum (4 bytes) + Padding (4 bytes)
	offset += 8

	// Write entries
	if node.NodeType == NodeTypeLeaf {
		entries := ExtractLeafEntries(node)
		for i := 0; i < int(node.Count); i++ {
			writeKeySlot(buf[offset:], entries[i].Key)
			offset += KeySlotSize
			copy(buf[offset:], entries[i].Value.Length[:])
			offset += 8
			copy(buf[offset:], entries[i].Value.Data[:])
			offset += 56
		}
	} else {
		entries := ExtractInternalEntries(node)
		for i := 0; i < int(node.Count); i++ {
			writeKeySlot(buf[offset:], entries[i].Key)
			offset += KeySlotSize
			binary.BigEndian.PutUint64(buf[offset:], uint64(entries[i].Child))
			offset += 8
		}
	}

	// Compute checksum on full buffer, zeroing checksum field first
	binary.BigEndian.PutUint32(buf[90:94], 0)
	cs := crc32.ChecksumIEEE(buf)
	binary.BigEndian.PutUint32(buf[90:94], cs)
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

	node.HighSibling = PageID(binary.BigEndian.Uint64(data[offset:]))
	offset += 8

	node.LowSibling = PageID(binary.BigEndian.Uint64(data[offset:]))
	offset += 8

	// HighKey (66 bytes)
	node.HighKey = readKeySlot(data[offset:])
	offset += KeySlotSize

	storedCS := binary.BigEndian.Uint32(data[offset:])
	offset += 4
	offset += 4 // Padding

	// Verify checksum
	data[90] = 0
	data[91] = 0
	data[92] = 0
	data[93] = 0
	cs := crc32.ChecksumIEEE(data)
	if storedCS != cs {
		return nil, ErrInvalidNode
	}
	node.Checksum = storedCS

	// Copy entry data (starts at offset NodeHeaderSize=98)
	var entrySize int
	if node.NodeType == NodeTypeLeaf {
		entrySize = LeafEntrySize
	} else {
		entrySize = InternalEntrySize
	}
	entryDataLen := int(node.Count) * entrySize
	node.RawData = make([]byte, entryDataLen)
	copy(node.RawData, data[NodeHeaderSize:NodeHeaderSize+entryDataLen])

	return node, nil
}

// =============================================================================
// Key Slot Helpers
// =============================================================================

// writeKeySlot writes a key into a fixed 66-byte slot: [keyData:64][keyLen:2]
func writeKeySlot(buf []byte, key []byte) {
	// Zero the slot first
	for i := 0; i < KeySlotSize; i++ {
		buf[i] = 0
	}
	kLen := len(key)
	if kLen > MaxKeySize {
		kLen = MaxKeySize
	}
	copy(buf, key[:kLen])
	binary.BigEndian.PutUint16(buf[MaxKeySize:], uint16(kLen))
}

// readKeySlot reads a key from a fixed 66-byte slot: [keyData:64][keyLen:2]
func readKeySlot(buf []byte) []byte {
	kLen := int(binary.BigEndian.Uint16(buf[MaxKeySize:]))
	if kLen == 0 {
		return nil
	}
	if kLen > MaxKeySize {
		kLen = MaxKeySize
	}
	key := make([]byte, kLen)
	copy(key, buf[:kLen])
	return key
}

// copyKey makes a copy of a key byte slice.
func copyKey(key []byte) []byte {
	if key == nil {
		return nil
	}
	cp := make([]byte, len(key))
	copy(cp, key)
	return cp
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
// Each leaf entry in RawData: [keyData:64][keyLen:2][valueLength:8][valueData:56] = 130 bytes
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
		entries[i].Key = readKeySlot(node.RawData[offset:])
		offset += KeySlotSize
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
		writeKeySlot(node.RawData[offset:], entries[i].Key)
		offset += KeySlotSize
		copy(node.RawData[offset:], entries[i].Value.Length[:])
		offset += 8
		copy(node.RawData[offset:], entries[i].Value.Data[:])
		offset += 56
	}
}

// ExtractInternalEntries extracts internal entries from node's RawData.
// Each internal entry in RawData: [keyData:64][keyLen:2][child:8] = 74 bytes
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
		entries[i].Key = readKeySlot(node.RawData[offset:])
		offset += KeySlotSize
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
		writeKeySlot(node.RawData[offset:], entries[i].Key)
		offset += KeySlotSize
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
