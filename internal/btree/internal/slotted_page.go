// Package internal implements the B-link tree index layer.
//
// Page is a []byte view of a 4096-byte B-tree page using a slotted page layout.
// All operations directly read/write the underlying byte buffer, eliminating
// serialize/deserialize overhead entirely.
//
// Page Layout:
//
//	Offset  Size  Field
//	──────  ────  ─────
//	0       1     flags (bit 0 = isLeaf)
//	1       1     reserved
//	2       2     count (number of entries / keys)
//	4       8     next (right sibling PageID, B-link)
//	12      2     freeEnd (offset where cell content area starts, grows ←)
//	14      2     highKeyLen
//	16      N     highKey bytes
//	16+N    ...   Slot Array: count × 2 bytes (uint16 offsets into page)
//	...           Free Space
//	freeEnd ...   Cell Content Area (cells packed from end of page, growing ←)
//
// Leaf Cell:
//
//	[keyLen:2] [key:keyLen] [txnMin:8] [txnMax:8] [valueType:1]
//	  inline:  [valueLen:4] [value:valueLen]
//	  blobRef: [blobID:8]
//
// Internal Cell:
//
//	[keyLen:2] [key:keyLen] [rightChild:8]
//
// Internal pages also store child0 (leftmost child) at offset 16+highKeyLen,
// and the slot array starts at 16+highKeyLen+8.
package internal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sort"

	btreeapi "github.com/akzj/go-fast-kv/internal/btree/api"
)

// Header field offsets.
const (
	offFlags      = 0
	offReserved   = 1
	offCount      = 2
	offNext       = 4
	offFreeEnd    = 12
	offHighKeyLen = 14
	offHighKey    = 16
)

// Flag bits.
const (
	flagLeaf byte = 1 << 0
)

var (
	// ErrPageFull is returned when an insert cannot fit in the page.
	ErrPageFull = errors.New("btree: page full")
)

// Page is a []byte view of a 4096-byte B-tree page.
// All operations directly read/write the underlying byte buffer.
type Page struct {
	data []byte // exactly btreeapi.PageSize bytes
}

// ─── Construction ───────────────────────────────────────────────────

// NewLeafPage allocates a zeroed 4096-byte leaf page.
func NewLeafPage() *Page {
	data := make([]byte, btreeapi.PageSize)
	data[offFlags] = flagLeaf
	binary.LittleEndian.PutUint16(data[offFreeEnd:], uint16(btreeapi.PageSize))
	return &Page{data: data}
}

// NewInternalPage allocates a zeroed 4096-byte internal page.
func NewInternalPage() *Page {
	data := make([]byte, btreeapi.PageSize)
	// flags = 0 (not leaf)
	binary.LittleEndian.PutUint16(data[offFreeEnd:], uint16(btreeapi.PageSize))
	return &Page{data: data}
}

// PageFromBytes wraps an existing []byte buffer as a Page.
// The buffer must be exactly PageSize bytes. No copy is made.
func PageFromBytes(data []byte) *Page {
	return &Page{data: data}
}

// Clone returns a deep copy of this page.
func (p *Page) Clone() *Page {
	cp := make([]byte, btreeapi.PageSize)
	copy(cp, p.data)
	return &Page{data: cp}
}

// ─── Header Accessors ───────────────────────────────────────────────

// IsLeaf returns true if this is a leaf page.
func (p *Page) IsLeaf() bool {
	return p.data[offFlags]&flagLeaf != 0
}

// Count returns the number of entries (leaf) or keys (internal).
func (p *Page) Count() int {
	return int(binary.LittleEndian.Uint16(p.data[offCount:]))
}

func (p *Page) setCount(n int) {
	binary.LittleEndian.PutUint16(p.data[offCount:], uint16(n))
}

// Next returns the right sibling PageID (B-link).
func (p *Page) Next() uint64 {
	return binary.LittleEndian.Uint64(p.data[offNext:])
}

// SetNext sets the right sibling PageID.
func (p *Page) SetNext(pid uint64) {
	binary.LittleEndian.PutUint64(p.data[offNext:], pid)
}

// freeEnd returns the offset where the cell content area starts.
func (p *Page) freeEnd() int {
	return int(binary.LittleEndian.Uint16(p.data[offFreeEnd:]))
}

func (p *Page) setFreeEnd(off int) {
	binary.LittleEndian.PutUint16(p.data[offFreeEnd:], uint16(off))
}

// highKeyLen returns the length of the highKey.
func (p *Page) highKeyLen() int {
	return int(binary.LittleEndian.Uint16(p.data[offHighKeyLen:]))
}

// HighKey returns the high key bytes (zero-copy slice into p.data).
// Returns nil if highKeyLen == 0 (rightmost node, +∞).
func (p *Page) HighKey() []byte {
	hkl := p.highKeyLen()
	if hkl == 0 {
		return nil
	}
	return p.data[offHighKey : offHighKey+hkl]
}

// SetHighKey writes the high key into the page header.
// This must be called before any entries are inserted (it affects slot array position).
// Passing nil sets highKeyLen to 0 (rightmost node).
func (p *Page) SetHighKey(key []byte) {
	hkl := len(key)
	binary.LittleEndian.PutUint16(p.data[offHighKeyLen:], uint16(hkl))
	if hkl > 0 {
		copy(p.data[offHighKey:], key)
	}
}

// slotArrayStart returns the byte offset where the slot array begins.
// For leaf pages: 16 + highKeyLen
// For internal pages: 16 + highKeyLen + 8 (child0)
func (p *Page) slotArrayStart() int {
	base := offHighKey + p.highKeyLen()
	if !p.IsLeaf() {
		base += 8 // child0
	}
	return base
}

// slotArrayEnd returns the byte offset just past the last slot.
func (p *Page) slotArrayEnd() int {
	return p.slotArrayStart() + p.Count()*2
}

// FreeSpace returns the number of bytes available for new entries.
// This accounts for both the new slot (2 bytes) and the cell data.
func (p *Page) FreeSpace() int {
	free := p.freeEnd() - p.slotArrayEnd()
	if free < 0 {
		return 0
	}
	return free
}

// Data returns the underlying byte buffer.
// For WritePage, this IS the serialized form — zero serialize.
func (p *Page) Data() []byte {
	return p.data
}

// ─── Slot Array ─────────────────────────────────────────────────────

// slotOffset returns the cell offset stored in slot[i].
func (p *Page) slotOffset(i int) int {
	off := p.slotArrayStart() + i*2
	return int(binary.LittleEndian.Uint16(p.data[off:]))
}

// setSlot writes a cell offset into slot[i].
func (p *Page) setSlot(i int, cellOff int) {
	off := p.slotArrayStart() + i*2
	binary.LittleEndian.PutUint16(p.data[off:], uint16(cellOff))
}

// ─── Leaf Entry Accessors (zero-copy reads) ─────────────────────────

// leafCellKeyLen returns the key length from a leaf cell at the given offset.
func (p *Page) leafCellKeyLen(cellOff int) int {
	return int(binary.LittleEndian.Uint16(p.data[cellOff:]))
}

// EntryKey returns the key of leaf entry i (zero-copy slice into p.data).
func (p *Page) EntryKey(i int) []byte {
	cellOff := p.slotOffset(i)
	kl := p.leafCellKeyLen(cellOff)
	start := cellOff + 2
	return p.data[start : start+kl]
}

// EntryTxnMin returns the TxnMin of leaf entry i.
func (p *Page) EntryTxnMin(i int) uint64 {
	cellOff := p.slotOffset(i)
	kl := p.leafCellKeyLen(cellOff)
	off := cellOff + 2 + kl
	return binary.LittleEndian.Uint64(p.data[off:])
}

// EntryTxnMax returns the TxnMax of leaf entry i.
func (p *Page) EntryTxnMax(i int) uint64 {
	cellOff := p.slotOffset(i)
	kl := p.leafCellKeyLen(cellOff)
	off := cellOff + 2 + kl + 8
	return binary.LittleEndian.Uint64(p.data[off:])
}

// SetEntryTxnMax modifies TxnMax of leaf entry i in-place.
func (p *Page) SetEntryTxnMax(i int, val uint64) {
	cellOff := p.slotOffset(i)
	kl := p.leafCellKeyLen(cellOff)
	off := cellOff + 2 + kl + 8
	binary.LittleEndian.PutUint64(p.data[off:], val)
}

// entryValueTypeOffset returns the offset of the valueType byte for entry i.
func (p *Page) entryValueTypeOffset(i int) int {
	cellOff := p.slotOffset(i)
	kl := p.leafCellKeyLen(cellOff)
	return cellOff + 2 + kl + 8 + 8
}

// EntryValueType returns the value type of leaf entry i (0=inline, 1=blobRef).
func (p *Page) EntryValueType(i int) byte {
	return p.data[p.entryValueTypeOffset(i)]
}

// EntryInlineValue returns the inline value of leaf entry i (zero-copy).
func (p *Page) EntryInlineValue(i int) []byte {
	vtOff := p.entryValueTypeOffset(i)
	off := vtOff + 1
	vl := int(binary.LittleEndian.Uint32(p.data[off:]))
	off += 4
	if vl == 0 {
		return nil
	}
	return p.data[off : off+vl]
}

// EntryBlobID returns the blob ID of leaf entry i.
func (p *Page) EntryBlobID(i int) uint64 {
	vtOff := p.entryValueTypeOffset(i)
	off := vtOff + 1
	return binary.LittleEndian.Uint64(p.data[off:])
}

// EntryValue returns the Value struct for leaf entry i.
func (p *Page) EntryValue(i int) btreeapi.Value {
	if p.EntryValueType(i) == 1 {
		return btreeapi.Value{BlobID: p.EntryBlobID(i)}
	}
	return btreeapi.Value{Inline: p.EntryInlineValue(i)}
}

// ─── Leaf Mutations ─────────────────────────────────────────────────

// LeafCellSize returns the cell size for a leaf entry with the given key/value.
func LeafCellSize(keyLen, valueLen int, isBlobRef bool) int {
	// keyLen(2) + key + txnMin(8) + txnMax(8) + valueType(1)
	size := 2 + keyLen + 8 + 8 + 1
	if isBlobRef {
		size += 8 // blobID
	} else {
		size += 4 + valueLen // valueLen(4) + value
	}
	return size
}

// InsertLeafEntry inserts a new leaf entry at position pos.
// blobID > 0 means blob reference; otherwise value is stored inline.
// Returns ErrPageFull if there isn't enough space.
func (p *Page) InsertLeafEntry(pos int, key []byte, txnMin, txnMax uint64, value []byte, blobID uint64) error {
	isBlobRef := blobID > 0
	cellSize := LeafCellSize(len(key), len(value), isBlobRef)
	count := p.Count()

	// Check space: need cellSize bytes for the cell + 2 bytes for the new slot
	if p.freeEnd()-(p.slotArrayEnd()+2) < cellSize {
		return ErrPageFull
	}

	// 1. Write cell at freeEnd - cellSize
	newFreeEnd := p.freeEnd() - cellSize
	off := newFreeEnd

	binary.LittleEndian.PutUint16(p.data[off:], uint16(len(key)))
	off += 2
	copy(p.data[off:], key)
	off += len(key)
	binary.LittleEndian.PutUint64(p.data[off:], txnMin)
	off += 8
	binary.LittleEndian.PutUint64(p.data[off:], txnMax)
	off += 8
	if isBlobRef {
		p.data[off] = 1
		off++
		binary.LittleEndian.PutUint64(p.data[off:], blobID)
	} else {
		p.data[off] = 0
		off++
		binary.LittleEndian.PutUint32(p.data[off:], uint32(len(value)))
		off += 4
		copy(p.data[off:], value)
	}

	// 2. Shift slots [pos:count] right by 2 bytes to make room for new slot
	slotBase := p.slotArrayStart()
	if pos < count {
		src := slotBase + pos*2
		dst := src + 2
		copy(p.data[dst:dst+(count-pos)*2], p.data[src:src+(count-pos)*2])
	}

	// 3. Write slot[pos] = new cell offset
	binary.LittleEndian.PutUint16(p.data[slotBase+pos*2:], uint16(newFreeEnd))

	// 4. Update freeEnd and count
	p.setFreeEnd(newFreeEnd)
	p.setCount(count + 1)

	return nil
}

// DeleteLeafEntry removes the slot for entry i.
// The cell data becomes garbage (fragmented). Use Compact() to reclaim.
func (p *Page) DeleteLeafEntry(i int) {
	count := p.Count()
	if i < 0 || i >= count {
		return
	}

	// Shift slots [i+1:count] left by 2 bytes
	slotBase := p.slotArrayStart()
	if i < count-1 {
		src := slotBase + (i+1)*2
		dst := slotBase + i*2
		copy(p.data[dst:dst+(count-1-i)*2], p.data[src:src+(count-1-i)*2])
	}

	// Clear the last slot (now unused)
	lastSlot := slotBase + (count-1)*2
	p.data[lastSlot] = 0
	p.data[lastSlot+1] = 0

	p.setCount(count - 1)
}

// ─── Internal Node Accessors ────────────────────────────────────────

// child0Offset returns the byte offset of child0 in the page.
func (p *Page) child0Offset() int {
	return offHighKey + p.highKeyLen()
}

// Child0 returns the leftmost child PageID (internal nodes only).
func (p *Page) Child0() uint64 {
	return binary.LittleEndian.Uint64(p.data[p.child0Offset():])
}

// SetChild0 sets the leftmost child PageID.
func (p *Page) SetChild0(pid uint64) {
	binary.LittleEndian.PutUint64(p.data[p.child0Offset():], pid)
}

// InternalKey returns the key at index i (zero-copy).
func (p *Page) InternalKey(i int) []byte {
	cellOff := p.slotOffset(i)
	kl := int(binary.LittleEndian.Uint16(p.data[cellOff:]))
	return p.data[cellOff+2 : cellOff+2+kl]
}

// InternalChild returns the right child of key[i] (i.e., children[i+1]).
func (p *Page) InternalChild(i int) uint64 {
	cellOff := p.slotOffset(i)
	kl := int(binary.LittleEndian.Uint16(p.data[cellOff:]))
	off := cellOff + 2 + kl
	return binary.LittleEndian.Uint64(p.data[off:])
}

// SetInternalChild updates the right child pointer of key[i] in-place.
func (p *Page) SetInternalChild(i int, pid uint64) {
	cellOff := p.slotOffset(i)
	kl := int(binary.LittleEndian.Uint16(p.data[cellOff:]))
	off := cellOff + 2 + kl
	binary.LittleEndian.PutUint64(p.data[off:], pid)
}

// InternalCellSize returns the cell size for an internal entry.
func InternalCellSize(keyLen int) int {
	return 2 + keyLen + 8 // keyLen(2) + key + rightChild(8)
}

// InsertInternalEntry inserts a new key + right child at position pos.
func (p *Page) InsertInternalEntry(pos int, key []byte, rightChild uint64) error {
	cellSize := InternalCellSize(len(key))
	count := p.Count()

	// Check space: cellSize + 2 bytes for new slot
	if p.freeEnd()-(p.slotArrayEnd()+2) < cellSize {
		return ErrPageFull
	}

	// 1. Write cell
	newFreeEnd := p.freeEnd() - cellSize
	off := newFreeEnd
	binary.LittleEndian.PutUint16(p.data[off:], uint16(len(key)))
	off += 2
	copy(p.data[off:], key)
	off += len(key)
	binary.LittleEndian.PutUint64(p.data[off:], rightChild)

	// 2. Shift slots right
	slotBase := p.slotArrayStart()
	if pos < count {
		src := slotBase + pos*2
		dst := src + 2
		copy(p.data[dst:dst+(count-pos)*2], p.data[src:src+(count-pos)*2])
	}

	// 3. Write slot
	binary.LittleEndian.PutUint16(p.data[slotBase+pos*2:], uint16(newFreeEnd))

	// 4. Update
	p.setFreeEnd(newFreeEnd)
	p.setCount(count + 1)

	return nil
}

// FindChild returns the child PageID for the given search key.
// Uses binary search: finds first i where key < keys[i], returns children[i].
func (p *Page) FindChild(key []byte) uint64 {
	count := p.Count()
	i := sort.Search(count, func(i int) bool {
		return bytes.Compare(key, p.InternalKey(i)) < 0
	})
	if i == 0 {
		return p.Child0()
	}
	return p.InternalChild(i - 1)
}

// ─── Search ─────────────────────────────────────────────────────────

// SearchLeaf returns the first index where EntryKey(i) >= key.
// Uses binary search on the slot array.
func (p *Page) SearchLeaf(key []byte) int {
	return sort.Search(p.Count(), func(i int) bool {
		return bytes.Compare(p.EntryKey(i), key) >= 0
	})
}

// FindInsertPos returns the position for inserting (key, txnMin) maintaining
// (Key ASC, TxnMin DESC) order.
func (p *Page) FindInsertPos(key []byte, txnMin uint64) int {
	return sort.Search(p.Count(), func(i int) bool {
		cmp := bytes.Compare(key, p.EntryKey(i))
		if cmp != 0 {
			return cmp < 0
		}
		return txnMin > p.EntryTxnMin(i)
	})
}

// ─── Split Operations ───────────────────────────────────────────────

// SplitLeaf splits a leaf page at position mid.
// Entries [0:mid) stay in p, entries [mid:count) go to the new right page.
// Returns the split key (key of entry mid) and the new right page.
// The caller is responsible for setting HighKey and Next on both pages.
func (p *Page) SplitLeaf(mid int) (splitKey []byte, right *Page) {
	count := p.Count()
	if mid <= 0 || mid >= count {
		mid = count / 2
	}

	right = NewLeafPage()

	// Copy entries [mid:count) to right page
	for i := mid; i < count; i++ {
		key := p.EntryKey(i)
		txnMin := p.EntryTxnMin(i)
		txnMax := p.EntryTxnMax(i)
		vt := p.EntryValueType(i)
		pos := i - mid
		if vt == 1 {
			right.InsertLeafEntry(pos, key, txnMin, txnMax, nil, p.EntryBlobID(i))
		} else {
			right.InsertLeafEntry(pos, key, txnMin, txnMax, p.EntryInlineValue(i), 0)
		}
	}

	// The split key is the first key in the right page
	splitKey = make([]byte, len(right.EntryKey(0)))
	copy(splitKey, right.EntryKey(0))

	// Truncate left page: remove slots [mid:count)
	// We just reduce the count — the cell data becomes garbage but that's OK.
	// If we need the space, Compact() can be called.
	p.setCount(mid)

	return splitKey, right
}

// SplitInternal splits an internal page at position mid.
// The mid-th key is pushed up (not copied to either side).
// Left gets keys[0:mid), right gets keys[mid+1:count).
// child0 of right = InternalChild(mid) (the right child of the pushed-up key).
func (p *Page) SplitInternal(mid int) (splitKey []byte, right *Page) {
	count := p.Count()
	if mid <= 0 || mid >= count {
		mid = count / 2
	}

	// The split key is pushed up
	sk := p.InternalKey(mid)
	splitKey = make([]byte, len(sk))
	copy(splitKey, sk)

	right = NewInternalPage()

	// Right page's child0 = the right child of the split key
	right.SetChild0(p.InternalChild(mid))

	// Copy keys [mid+1:count) to right page
	for i := mid + 1; i < count; i++ {
		key := p.InternalKey(i)
		child := p.InternalChild(i)
		right.InsertInternalEntry(i-mid-1, key, child)
	}

	// Truncate left page
	p.setCount(mid)

	return splitKey, right
}

// ─── Compaction ─────────────────────────────────────────────────────

// Compact rebuilds the page by re-packing all live cells contiguously.
// This reclaims fragmented space from deleted entries.
func (p *Page) Compact() {
	count := p.Count()
	if count == 0 {
		p.setFreeEnd(btreeapi.PageSize)
		return
	}

	// Collect all live cell data with their slot indices
	type cellInfo struct {
		slotIdx int
		data    []byte
	}
	cells := make([]cellInfo, count)

	for i := 0; i < count; i++ {
		cellOff := p.slotOffset(i)
		cellEnd := p.cellEnd(i)
		cellData := make([]byte, cellEnd-cellOff)
		copy(cellData, p.data[cellOff:cellEnd])
		cells[i] = cellInfo{slotIdx: i, data: cellData}
	}

	// Re-pack cells from the end of the page
	newFreeEnd := btreeapi.PageSize
	for i := 0; i < count; i++ {
		cellSize := len(cells[i].data)
		newFreeEnd -= cellSize
		copy(p.data[newFreeEnd:], cells[i].data)
		p.setSlot(cells[i].slotIdx, newFreeEnd)
	}

	// Clear the reclaimed space between slot array end and new freeEnd
	slotEnd := p.slotArrayEnd()
	for i := slotEnd; i < newFreeEnd; i++ {
		p.data[i] = 0
	}

	p.setFreeEnd(newFreeEnd)
}

// cellEnd returns the byte offset just past the end of cell i.
func (p *Page) cellEnd(i int) int {
	cellOff := p.slotOffset(i)
	if p.IsLeaf() {
		return p.leafCellEnd(cellOff)
	}
	return p.internalCellEnd(cellOff)
}

// leafCellEnd returns the byte offset just past a leaf cell at cellOff.
func (p *Page) leafCellEnd(cellOff int) int {
	kl := int(binary.LittleEndian.Uint16(p.data[cellOff:]))
	off := cellOff + 2 + kl + 8 + 8 // keyLen + key + txnMin + txnMax
	vt := p.data[off]
	off++
	if vt == 1 { // blobRef
		off += 8
	} else { // inline
		vl := int(binary.LittleEndian.Uint32(p.data[off:]))
		off += 4 + vl
	}
	return off
}

// internalCellEnd returns the byte offset just past an internal cell at cellOff.
func (p *Page) internalCellEnd(cellOff int) int {
	kl := int(binary.LittleEndian.Uint16(p.data[cellOff:]))
	return cellOff + 2 + kl + 8 // keyLen + key + rightChild
}

// ─── UsedBytes ──────────────────────────────────────────────────────

// UsedBytes returns the total bytes used by header + slot array + cells.
// Useful for split threshold checks.
func (p *Page) UsedBytes() int {
	return p.slotArrayEnd() + (btreeapi.PageSize - p.freeEnd())
}
