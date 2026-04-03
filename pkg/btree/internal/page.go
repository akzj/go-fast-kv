// Package internal provides the B+Tree implementation.
package internal

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/akzj/go-fast-kv/pkg/btree/api"
)

const (
	// Page header size for leaf: flags(1) + num_keys(2) + parent_hint(8) + next_page_id(8) = 19 bytes
	pageHeaderSizeLeaf = 19
	// Page header size for internal: flags(1) + num_keys(2) + parent_hint(8) = 11 bytes
	pageHeaderSizeInternal = 11
)

// Page flag bits
const (
	pageFlagLeaf byte = 0x01
	pageFlagDirty byte = 0x02
)

// Page represents a B+Tree page (either leaf or internal).
type page struct {
	// Header
	flags      byte       // leaf/internal flags
	numKeys    uint16     // number of keys stored
	parentHint api.PageID // parent page ID (for navigation)

	// For leaf pages: keys, values
	// For internal pages: keys, children
	keys         [][]byte
	values       [][]byte      // leaf only: raw values (may include blob refs)
	childPageIDs []api.PageID  // internal only: child page IDs (len = numKeys + 1)

	// Sibling pointer for leaf pages (B-link tree)
	nextPageID api.PageID

	// Dirty flag (not persisted)
	dirty bool
}

// newPage creates a new page.
func newPage(leaf bool) *page {
	p := &page{
		keys:         make([][]byte, 0, 256),
		childPageIDs: make([]api.PageID, 0, 257),
		values:       make([][]byte, 0, 256),
		dirty:        true,
	}
	if leaf {
		p.flags |= pageFlagLeaf
	}
	return p
}

// isLeaf returns true if this is a leaf page.
func (p *page) isLeaf() bool {
	return p.flags&pageFlagLeaf != 0
}

// isDirty returns true if the page has unsaved changes.
func (p *page) isDirty() bool {
	return p.dirty
}

// setDirty marks the page as dirty.
func (p *page) setDirty() {
	p.dirty = true
}

// clearDirty marks the page as clean.
func (p *page) clearDirty() {
	p.dirty = false
}

// search finds the index where key should be inserted or already exists.
func (p *page) search(key []byte) int {
	low, high := 0, int(p.numKeys)
	for low < high {
		mid := (low + high) / 2
		cmp := bytes.Compare(key, p.keys[mid])
		if cmp <= 0 {
			high = mid
		} else {
			low = mid + 1
		}
	}
	return low
}

// get retrieves a value by key from a leaf page.
func (p *page) get(key []byte) ([]byte, bool) {
	if !p.isLeaf() {
		return nil, false
	}

	idx := p.search(key)
	if idx < int(p.numKeys) && bytes.Equal(key, p.keys[idx]) {
		return p.values[idx], true
	}
	return nil, false
}

// insert inserts a key-value pair into a leaf page.
// Returns true if split is needed (page would exceed capacity).
func (p *page) insert(key, value []byte) bool {
	if !p.isLeaf() {
		panic("insert called on non-leaf page")
	}

	idx := p.search(key)

	// Key already exists - update
	if idx < int(p.numKeys) && bytes.Equal(key, p.keys[idx]) {
		p.values[idx] = value
		p.setDirty()
		return false
	}

	// Check if we're about to exceed max capacity
	// Max keys per page is 254 (to leave room for split)
	if int(p.numKeys) >= 254 {
		return true // Signal split needed
	}

	// Insert at position idx
	if cap(p.keys) > len(p.keys) {
		p.keys = p.keys[:len(p.keys)+1]
		p.values = p.values[:len(p.values)+1]
		copy(p.keys[idx+1:], p.keys[idx:len(p.keys)-1])
		copy(p.values[idx+1:], p.values[idx:len(p.values)-1])
	} else {
		newKeys := make([][]byte, len(p.keys)+1)
		newValues := make([][]byte, len(p.values)+1)
		copy(newKeys, p.keys[:idx])
		copy(newValues, p.values[:idx])
		copy(newKeys[idx+1:], p.keys[idx:])
		copy(newValues[idx+1:], p.values[idx:])
		p.keys = newKeys
		p.values = newValues
	}
	p.keys[idx] = key
	p.values[idx] = value
	p.numKeys++
	p.setDirty()

	// Check if we need to split now (data size based)
	if int(p.numKeys) >= 127 {
		return true
	}
	return false
}

// delete removes a key from a leaf page.
func (p *page) delete(key []byte) bool {
	if !p.isLeaf() {
		panic("delete called on non-leaf page")
	}

	idx := p.search(key)
	if idx >= int(p.numKeys) || !bytes.Equal(key, p.keys[idx]) {
		return false
	}

	// Remove at position idx
	copy(p.keys[idx:], p.keys[idx+1:])
	copy(p.values[idx:], p.values[idx+1:])
	p.keys = p.keys[:p.numKeys-1]
	p.values = p.values[:p.numKeys-1]
	p.numKeys--
	p.setDirty()

	return int(p.numKeys) < 128 // min keys = ceil(order/2) - 1
}

// insertChild inserts a key and child pointer into an internal page.
func (p *page) insertChild(key []byte, childID api.PageID) bool {
	if p.isLeaf() {
		panic("insertChild called on leaf page")
	}

	idx := p.search(key)

	// Key already exists - update child pointer
	if idx < int(p.numKeys) && bytes.Equal(key, p.keys[idx]) {
		if cap(p.childPageIDs) > len(p.childPageIDs) {
			p.childPageIDs = p.childPageIDs[:len(p.childPageIDs)+1]
			copy(p.childPageIDs[idx+2:], p.childPageIDs[idx+1:len(p.childPageIDs)-1])
		} else {
			newChildren := make([]api.PageID, len(p.childPageIDs)+1)
			copy(newChildren, p.childPageIDs[:idx+1])
			copy(newChildren[idx+2:], p.childPageIDs[idx+1:])
			p.childPageIDs = newChildren
		}
		p.childPageIDs[idx+1] = childID
		p.setDirty()
		return int(p.numKeys) >= 255
	}

	// Insert new key and child
	if cap(p.keys) > len(p.keys) {
		p.keys = p.keys[:len(p.keys)+1]
		p.childPageIDs = p.childPageIDs[:len(p.childPageIDs)+1]
		copy(p.keys[idx+1:], p.keys[idx:len(p.keys)-1])
		copy(p.childPageIDs[idx+2:], p.childPageIDs[idx+1:len(p.childPageIDs)-1])
	} else {
		newKeys := make([][]byte, len(p.keys)+1)
		newChildren := make([]api.PageID, len(p.childPageIDs)+1)
		copy(newKeys, p.keys[:idx])
		copy(newChildren, p.childPageIDs[:idx+1])
		copy(newKeys[idx+1:], p.keys[idx:])
		copy(newChildren[idx+2:], p.childPageIDs[idx+1:])
		p.keys = newKeys
		p.childPageIDs = newChildren
	}
	p.keys[idx] = key
	p.childPageIDs[idx+1] = childID
	p.numKeys++
	p.setDirty()

	return int(p.numKeys) >= 255
}

// split splits this page and returns the middle key and new page.
func (p *page) split() ([]byte, *page, int) {
	splitPoint := 127 // floor(255/2)

	if p.isLeaf() {
		return p.splitLeaf(splitPoint)
	}
	return p.splitInternal(splitPoint)
}

// splitLeaf splits a leaf page.
func (p *page) splitLeaf(splitPoint int) ([]byte, *page, int) {
	midKey := p.keys[splitPoint]

	newPage := newPage(true)
	newPage.parentHint = p.parentHint
	newPage.nextPageID = p.nextPageID
	p.nextPageID = 0

	// Copy second half to new page
	newPage.keys = append(newPage.keys, p.keys[splitPoint:]...)
	newPage.values = append(newPage.values, p.values[splitPoint:]...)
	newPage.numKeys = uint16(len(newPage.keys))

	// Truncate original page
	p.keys = p.keys[:splitPoint]
	p.values = p.values[:splitPoint]
	p.numKeys = uint16(len(p.keys))
	p.setDirty()
	newPage.setDirty()

	return midKey, newPage, splitPoint
}

// splitInternal splits an internal page.
func (p *page) splitInternal(splitPoint int) ([]byte, *page, int) {
	midKey := p.keys[splitPoint]

	newPage := newPage(false)
	newPage.parentHint = p.parentHint

	// Copy second half (excluding the separator key) to new page
	newPage.keys = append(newPage.keys, p.keys[splitPoint+1:]...)
	newPage.numKeys = uint16(len(newPage.keys))

	// childPageIDs[0..splitPoint] stay in original, childPageIDs[splitPoint+1..end] go to new
	newPage.childPageIDs = append(newPage.childPageIDs, p.childPageIDs[splitPoint+1:]...)

	// Truncate original page (remove separator key)
	p.keys = p.keys[:splitPoint]
	p.numKeys = uint16(len(p.keys))

	p.setDirty()
	newPage.setDirty()

	return midKey, newPage, splitPoint
}

// merge merges another page into this page (for underflow handling).
func (p *page) merge(other *page) {
	if p.isLeaf() && other.isLeaf() {
		p.mergeLeaf(other)
	} else if !p.isLeaf() && !other.isLeaf() {
		panic("mergeInternal requires separator key - use mergeWithSeparator")
	}
}

// mergeWithSeparator merges a sibling internal page with a separator key.
func (p *page) mergeWithSeparator(other *page, separatorKey []byte) {
	if p.isLeaf() || other.isLeaf() {
		panic("mergeWithSeparator requires internal pages")
	}

	// Move separator key down
	p.keys = append(p.keys, separatorKey)
	p.keys = append(p.keys, other.keys...)
	p.numKeys = uint16(len(p.keys))

	// Append child pointers from other
	p.childPageIDs = append(p.childPageIDs, other.childPageIDs...)
	p.setDirty()
}

// mergeLeaf merges a sibling leaf page.
func (p *page) mergeLeaf(other *page) {
	p.keys = append(p.keys, other.keys...)
	p.values = append(p.values, other.values...)
	p.numKeys = uint16(len(p.keys))
	p.nextPageID = other.nextPageID
	p.setDirty()
}

// scan iterates over keys in range [start, end) and calls iterator.
func (p *page) scan(start, end []byte, iterator func(key, value []byte) bool) bool {
	if !p.isLeaf() {
		panic("scan called on non-leaf page")
	}

	startIdx := 0
	if start != nil {
		startIdx = p.search(start)
	}

	endIdx := int(p.numKeys)
	if end != nil {
		endIdx = p.search(end)
	}

	for i := startIdx; i < endIdx; i++ {
		if !iterator(p.keys[i], p.values[i]) {
			return false
		}
	}
	return true
}

// MarshalBinary serializes the page to a byte slice.
// Layout: [header][key_len_2bytes...][key_data...][value_len_2bytes...][value_data...]
func (p *page) MarshalBinary() ([]byte, error) {
	var headerSize int
	if p.isLeaf() {
		headerSize = pageHeaderSizeLeaf
	} else {
		headerSize = pageHeaderSizeInternal
	}

	// Calculate sizes: 2 bytes per key len, 2 bytes per value len, actual data
	keyLenSize := int(p.numKeys) * 2
	valueLenSize := 0
	keyDataSize := 0
	valueDataSize := 0

	for i := 0; i < int(p.numKeys); i++ {
		keyDataSize += len(p.keys[i])
		if p.isLeaf() {
			valueLenSize += 2
			valueDataSize += len(p.values[i])
		}
	}

	totalSize := headerSize + keyLenSize + keyDataSize + valueLenSize + valueDataSize
	if totalSize > 4096 {
		return nil, fmt.Errorf("page data exceeds 4KB limit: %d bytes", totalSize)
	}

	data := make([]byte, 4096)

	// Write header
	data[0] = p.flags
	binary.LittleEndian.PutUint16(data[1:3], p.numKeys)
	binary.LittleEndian.PutUint64(data[3:11], uint64(p.parentHint))

	if p.isLeaf() {
		binary.LittleEndian.PutUint64(data[11:19], uint64(p.nextPageID))
	}

	// Write key lengths and data
	keyLenOffset := headerSize
	keyDataOffset := headerSize + keyLenSize
	valueLenOffset := keyDataOffset + keyDataSize
	valueDataOffset := valueLenOffset + valueLenSize

	keyPos := keyDataOffset
	valuePos := valueDataOffset

	for i := 0; i < int(p.numKeys); i++ {
		// Write key length
		binary.LittleEndian.PutUint16(data[keyLenOffset+i*2:keyLenOffset+(i+1)*2], uint16(len(p.keys[i])))
		// Write key data
		copy(data[keyPos:], p.keys[i])
		keyPos += len(p.keys[i])
	}

	// Write value lengths and data (leaf only)
	if p.isLeaf() {
		for i := 0; i < int(p.numKeys); i++ {
			// Write value length
			binary.LittleEndian.PutUint16(data[valueLenOffset+i*2:valueLenOffset+(i+1)*2], uint16(len(p.values[i])))
			// Write value data
			copy(data[valuePos:], p.values[i])
			valuePos += len(p.values[i])
		}
	}

	return data[:totalSize], nil
}

// UnmarshalBinary deserializes a page from a byte slice.
func (p *page) UnmarshalBinary(data []byte) error {
	if len(data) < pageHeaderSizeInternal {
		return fmt.Errorf("data too short for page header: need %d, got %d", pageHeaderSizeInternal, len(data))
	}

	// Read header
	p.flags = data[0]
	p.numKeys = binary.LittleEndian.Uint16(data[1:3])
	p.parentHint = api.PageID(binary.LittleEndian.Uint64(data[3:11]))

	var headerSize int
	if p.isLeaf() {
		if len(data) < pageHeaderSizeLeaf {
			return fmt.Errorf("data too short for leaf page header: need %d, got %d", pageHeaderSizeLeaf, len(data))
		}
		p.nextPageID = api.PageID(binary.LittleEndian.Uint64(data[11:19]))
		headerSize = pageHeaderSizeLeaf
	} else {
		headerSize = pageHeaderSizeInternal
	}

	keyLenSize := int(p.numKeys) * 2
	keyDataOffset := headerSize + keyLenSize

	// Read key lengths
	keyLengths := make([]int, p.numKeys)
	for i := 0; i < int(p.numKeys); i++ {
		keyLengths[i] = int(binary.LittleEndian.Uint16(data[headerSize+i*2 : headerSize+(i+1)*2]))
	}

	// Read keys
	p.keys = make([][]byte, p.numKeys)
	keyPos := keyDataOffset
	for i := 0; i < int(p.numKeys); i++ {
		if keyPos+keyLengths[i] > len(data) {
			return fmt.Errorf("key %d data out of bounds", i)
		}
		p.keys[i] = make([]byte, keyLengths[i])
		copy(p.keys[i], data[keyPos:keyPos+keyLengths[i]])
		keyPos += keyLengths[i]
	}

	// Read values for leaf pages
	if p.isLeaf() {
		valueLenOffset := keyDataOffset + keyPos - keyDataOffset
		valueDataOffset := valueLenOffset + int(p.numKeys)*2

		p.values = make([][]byte, p.numKeys)
		valuePos := valueDataOffset
		for i := 0; i < int(p.numKeys); i++ {
			valueLen := int(binary.LittleEndian.Uint16(data[valueLenOffset+i*2 : valueLenOffset+(i+1)*2]))
			if valuePos+valueLen > len(data) {
				p.values[i] = []byte{}
				continue
			}
			p.values[i] = make([]byte, valueLen)
			copy(p.values[i], data[valuePos:valuePos+valueLen])
			valuePos += valueLen
		}
	}

	p.dirty = false
	return nil
}
