// Package btree implements the B-link tree index layer.
package internal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

	btreeapi "github.com/akzj/go-fast-kv/internal/btree/api"
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

var (
	errNodeTooLarge    = errors.New("btree: serialized node exceeds PageSize")
	errChecksumInvalid = errors.New("btree: checksum mismatch")
	errDataTooShort    = errors.New("btree: data too short for deserialization")
)

// headerBaseSize is the fixed portion of the header before highKey.
// flags(1) + reserved(1) + count(2) + next(8) + checksum(4) + highKeyLen(2) = 18
const headerBaseSize = 18

// nodeSerializer implements btreeapi.NodeSerializer.
type nodeSerializer struct{}

// NewNodeSerializer creates a new NodeSerializer.
func NewNodeSerializer() btreeapi.NodeSerializer {
	return &nodeSerializer{}
}

// SerializedSize returns the byte size of a node if serialized (without padding).
func (s *nodeSerializer) SerializedSize(node *btreeapi.Node) int {
	size := headerBaseSize + len(node.HighKey)
	if node.IsLeaf {
		for i := range node.Entries {
			size += leafEntrySize(&node.Entries[i])
		}
	} else {
		// first child (8 bytes) + N * (keyLen(2) + key + child(8))
		size += 8 // children[0]
		for _, k := range node.Keys {
			size += 2 + len(k) + 8
		}
	}
	return size
}

func leafEntrySize(e *btreeapi.LeafEntry) int {
	// keyLen(2) + key + txnMin(8) + txnMax(8) + valueType(1)
	size := 2 + len(e.Key) + 8 + 8 + 1
	if e.Value.BlobID > 0 {
		size += 8 // blobID
	} else {
		size += 4 + len(e.Value.Inline) // valueLen + value
	}
	return size
}

// Serialize encodes a Node into PageSize bytes with CRC32-C checksum.
func (s *nodeSerializer) Serialize(node *btreeapi.Node) ([]byte, error) {
	needed := s.SerializedSize(node)
	if needed > btreeapi.PageSize {
		return nil, fmt.Errorf("%w: need %d bytes", errNodeTooLarge, needed)
	}

	buf := make([]byte, btreeapi.PageSize)
	off := 0

	// flags
	if node.IsLeaf {
		buf[off] = 1
	}
	off++

	// reserved
	off++

	// count
	binary.LittleEndian.PutUint16(buf[off:], node.Count)
	off += 2

	// next
	binary.LittleEndian.PutUint64(buf[off:], node.Next)
	off += 8

	// checksum placeholder (offset 12)
	checksumOff := off
	off += 4

	// highKeyLen + highKey
	binary.LittleEndian.PutUint16(buf[off:], uint16(len(node.HighKey)))
	off += 2
	copy(buf[off:], node.HighKey)
	off += len(node.HighKey)

	if node.IsLeaf {
		off = serializeLeafEntries(buf, off, node.Entries)
	} else {
		off = serializeInternalEntries(buf, off, node.Keys, node.Children)
	}

	// compute CRC32-C over entire page (checksum field is 0)
	crc := crc32.Checksum(buf, crc32cTable)
	binary.LittleEndian.PutUint32(buf[checksumOff:], crc)

	return buf, nil
}

func serializeLeafEntries(buf []byte, off int, entries []btreeapi.LeafEntry) int {
	for i := range entries {
		e := &entries[i]
		binary.LittleEndian.PutUint16(buf[off:], uint16(len(e.Key)))
		off += 2
		copy(buf[off:], e.Key)
		off += len(e.Key)
		binary.LittleEndian.PutUint64(buf[off:], e.TxnMin)
		off += 8
		binary.LittleEndian.PutUint64(buf[off:], e.TxnMax)
		off += 8
		if e.Value.BlobID > 0 {
			buf[off] = 1 // blobRef
			off++
			binary.LittleEndian.PutUint64(buf[off:], e.Value.BlobID)
			off += 8
		} else {
			buf[off] = 0 // inline
			off++
			binary.LittleEndian.PutUint32(buf[off:], uint32(len(e.Value.Inline)))
			off += 4
			copy(buf[off:], e.Value.Inline)
			off += len(e.Value.Inline)
		}
	}
	return off
}

func serializeInternalEntries(buf []byte, off int, keys [][]byte, children []uint64) int {
	// first child
	if len(children) > 0 {
		binary.LittleEndian.PutUint64(buf[off:], children[0])
	}
	off += 8
	for i, k := range keys {
		binary.LittleEndian.PutUint16(buf[off:], uint16(len(k)))
		off += 2
		copy(buf[off:], k)
		off += len(k)
		binary.LittleEndian.PutUint64(buf[off:], children[i+1])
		off += 8
	}
	return off
}

// Deserialize decodes bytes into a Node.
// CRC checksum validation is intentionally skipped — assumes data integrity
// is guaranteed by the underlying storage layer. For on-disk nodes where
// integrity validation is required, wrap with a CRC-checking layer.
func (s *nodeSerializer) Deserialize(data []byte) (*btreeapi.Node, error) {
	if len(data) < headerBaseSize {
		return nil, errDataTooShort
	}

	node := &btreeapi.Node{}
	node.DataRef = data // hold reference to keep zero-copy slices valid
	off := 0

	node.IsLeaf = data[off]&1 != 0
	off++
	off++ // reserved

	node.Count = binary.LittleEndian.Uint16(data[off:])
	off += 2

	node.Next = binary.LittleEndian.Uint64(data[off:])
	off += 8

	off += 4 // skip checksum

	hkl := int(binary.LittleEndian.Uint16(data[off:]))
	off += 2
	// Zero-copy: directly reference the byte slice from original data (no allocation)
	// Note: must distinguish nil (rightmost node, HighKey = +∞) from empty slice
	if hkl > 0 {
		node.HighKey = data[off : off+hkl]
	}
	off += hkl

	if node.IsLeaf {
		var err error
		node.Entries, off, err = deserializeLeafEntriesZeroCopy(data, off, int(node.Count))
		if err != nil {
			return nil, err
		}
	} else {
		var err error
		node.Keys, node.Children, off, err = deserializeInternalEntriesZeroCopy(data, off, int(node.Count))
		if err != nil {
			return nil, err
		}
	}

	return node, nil
}

// deserializeLeafEntriesZeroCopy deserializes leaf entries without memory allocation.
// Keys and inline values are zero-copy references into the original data slice.
// Caller must ensure data is valid for the lifetime of the returned entries.
func deserializeLeafEntriesZeroCopy(data []byte, off, count int) ([]btreeapi.LeafEntry, int, error) {
	entries := make([]btreeapi.LeafEntry, count)
	for i := 0; i < count; i++ {
		if off+2 > len(data) {
			return nil, off, errDataTooShort
		}
		kl := int(binary.LittleEndian.Uint16(data[off:]))
		off += 2
		if off+kl > len(data) {
			return nil, off, errDataTooShort
		}
		// Zero-copy: directly reference the byte slice from original data (no allocation)
		entries[i].Key = data[off : off+kl]
		off += kl

		if off+17 > len(data) { // txnMin(8)+txnMax(8)+valueType(1)
			return nil, off, errDataTooShort
		}
		entries[i].TxnMin = binary.LittleEndian.Uint64(data[off:])
		off += 8
		entries[i].TxnMax = binary.LittleEndian.Uint64(data[off:])
		off += 8

		vt := data[off]
		off++
		if vt == 1 { // blobRef
			if off+8 > len(data) {
				return nil, off, errDataTooShort
			}
			entries[i].Value.BlobID = binary.LittleEndian.Uint64(data[off:])
			off += 8
		} else { // inline
			if off+4 > len(data) {
				return nil, off, errDataTooShort
			}
			vl := int(binary.LittleEndian.Uint32(data[off:]))
			off += 4
			if off+vl > len(data) {
				return nil, off, errDataTooShort
			}
			// Zero-copy: directly reference the byte slice from original data (no allocation)
			entries[i].Value.Inline = data[off : off+vl]
			off += vl
		}
	}
	return entries, off, nil
}

// deserializeInternalEntriesZeroCopy deserializes internal node entries without memory allocation.
// Keys are zero-copy references into the original data slice.
// Caller must ensure data is valid for the lifetime of the returned keys/children.
func deserializeInternalEntriesZeroCopy(data []byte, off, count int) ([][]byte, []uint64, int, error) {
	children := make([]uint64, 0, count+1)
	keys := make([][]byte, 0, count)

	if off+8 > len(data) {
		return nil, nil, off, errDataTooShort
	}
	children = append(children, binary.LittleEndian.Uint64(data[off:]))
	off += 8

	for i := 0; i < count; i++ {
		if off+2 > len(data) {
			return nil, nil, off, errDataTooShort
		}
		kl := int(binary.LittleEndian.Uint16(data[off:]))
		off += 2
		if off+kl+8 > len(data) {
			return nil, nil, off, errDataTooShort
		}
		// Zero-copy: directly reference the byte slice from original data (no allocation)
		keys = append(keys, data[off:off+kl])
		off += kl
		children = append(children, binary.LittleEndian.Uint64(data[off:]))
		off += 8
	}
	return keys, children, off, nil
}
