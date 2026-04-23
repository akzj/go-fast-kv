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
//	14      2     highKeyOff (offset of highKey cell in page, 0 = nil/+∞)
//
//	For leaf pages:
//	  16      ...   Slot Array: count × 2 bytes (uint16 offsets into page)
//
//	For internal pages:
//	  16      8     child0 (leftmost child PageID)
//	  24      ...   Slot Array: count × 2 bytes
//
//	...           Free Space
//	freeEnd ...   Cell Content Area (cells packed from end of page, growing ←)
//
// The highKey is stored as a cell in the content area: [highKeyLen:2][highKeyBytes:N]
// This ensures that changing the highKey does NOT shift the slot array.
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
package internal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"sync"

	btreeapi "github.com/akzj/go-fast-kv/internal/btree/api"
)

// Header field offsets.
const (
	offFlags      = 0
	offReserved   = 1
	offCount      = 2
	offNext       = 4
	offFreeEnd    = 12
	offHighKeyOff = 14
	offData       = 16 // start of slot array (leaf) or child0 (internal)
)

// Flag bits.
const (
	flagLeaf byte = 1 << 0
)

var (
	// ErrPageFull is returned when an insert cannot fit in the page.
	ErrPageFull = errors.New("btree: page full")
)

// pageDataPool amortizes 4096-byte buffer allocations for page clones.
// Buffers are returned to the pool when evicted from hotPages or LRU cache.
var pageDataPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, btreeapi.PageSize)
		return &buf
	},
}

// Page is a []byte view of a 4096-byte B-tree page.
// All operations directly read/write the underlying byte buffer.
type Page struct {
	data    []byte  // exactly btreeapi.PageSize bytes
	poolBuf *[]byte // non-nil if data came from pageDataPool
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

// ClonePooled returns a deep copy using a buffer from pageDataPool.
// The returned page's buffer can be returned to the pool via ReleaseToPool().
func (p *Page) ClonePooled() *Page {
	bp := pageDataPool.Get().(*[]byte)
	buf := *bp
	copy(buf, p.data)
	return &Page{data: buf, poolBuf: bp}
}

// ReleaseToPool returns the page's data buffer to the pool (if it was pooled).
// After ReleaseToPool, the page must not be used.
func (p *Page) ReleaseToPool() {
	if p.poolBuf != nil {
		pageDataPool.Put(p.poolBuf)
		p.poolBuf = nil
		p.data = nil
	}
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

// highKeyOff returns the offset of the highKey cell in the page (0 = nil).
func (p *Page) highKeyOff() int {
	return int(binary.LittleEndian.Uint16(p.data[offHighKeyOff:]))
}

func (p *Page) setHighKeyOff(off int) {
	binary.LittleEndian.PutUint16(p.data[offHighKeyOff:], uint16(off))
}

// HighKey returns the high key bytes (zero-copy slice into p.data).
// Returns nil if highKeyOff == 0 (rightmost node, +∞).
func (p *Page) HighKey() []byte {
	hkOff := p.highKeyOff()
	if hkOff == 0 {
		return nil
	}
	hkl := int(binary.LittleEndian.Uint16(p.data[hkOff:]))
	if hkl == 0 {
		return nil
	}
	return p.data[hkOff+2 : hkOff+2+hkl]
}

// SetHighKey writes the high key into the cell content area.
// Can be called at any time — does NOT affect slot array position.
// Passing nil clears the highKey (sets highKeyOff to 0).
//
// If a highKey already exists and the new key fits in the same space,
// it is overwritten in-place (no new allocation). Otherwise, the old
// highKey cell becomes garbage and a new cell is allocated.
func (p *Page) SetHighKey(key []byte) {
	if len(key) == 0 {
		p.setHighKeyOff(0)
		return
	}

	// Check if we can reuse the existing highKey cell
	hkOff := p.highKeyOff()
	if hkOff != 0 {
		oldLen := int(binary.LittleEndian.Uint16(p.data[hkOff:]))
		if len(key) <= oldLen {
			// Reuse in-place: update length and copy key (pad with zeros if shorter)
			binary.LittleEndian.PutUint16(p.data[hkOff:], uint16(len(key)))
			copy(p.data[hkOff+2:hkOff+2+len(key)], key)
			// Zero out remaining bytes if shorter
			for i := hkOff + 2 + len(key); i < hkOff+2+oldLen; i++ {
				p.data[i] = 0
			}
			return
		}
		// New key is larger — old cell becomes garbage, allocate new
	}

	// Allocate space in cell content area for [highKeyLen:2][highKey:N]
	cellSize := 2 + len(key)
	newFreeEnd := p.freeEnd() - cellSize
	binary.LittleEndian.PutUint16(p.data[newFreeEnd:], uint16(len(key)))
	copy(p.data[newFreeEnd+2:], key)
	p.setFreeEnd(newFreeEnd)
	p.setHighKeyOff(newFreeEnd)
}

// ClearHighKey sets highKey to nil (+∞) without allocating new space.
func (p *Page) ClearHighKey() {
	p.setHighKeyOff(0)
}

// slotArrayStart returns the byte offset where the slot array begins.
// For leaf pages: 16
// For internal pages: 24 (after child0)
func (p *Page) slotArrayStart() int {
	if !p.IsLeaf() {
		return offData + 8 // child0
	}
	return offData
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
	return offData
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
	if count < 2 {
		panic(fmt.Sprintf("SplitLeaf: cannot split page with count=%d", count))
	}
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

	// Truncate left page: zero stale slots and update count
	oldCount := count
	p.setCount(mid)

	// Clear stale slot entries to prevent corruption on future inserts
	slotBase := p.slotArrayStart()
	for i := mid; i < oldCount; i++ {
		off := slotBase + i*2
		p.data[off] = 0
		p.data[off+1] = 0
	}

	// Reclaim cell space: compact the left page to recover space from
	// the entries that were moved to the right page.
	// This is critical — without it, repeated splits of the same page
	// cause freeEnd to keep decreasing until the page can't hold entries.
	p.Compact()

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

	// Truncate left page: zero stale slots and compact
	oldCount := count
	p.setCount(mid)
	slotBase := p.slotArrayStart()
	for i := mid; i < oldCount; i++ {
		off := slotBase + i*2
		p.data[off] = 0
		p.data[off+1] = 0
	}
	p.Compact()

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
		if cellOff < p.slotArrayStart() || cellOff >= btreeapi.PageSize {
			panic(fmt.Sprintf("Compact: corrupt slot[%d]=%d, count=%d, freeEnd=%d, isLeaf=%v",
				i, cellOff, count, p.freeEnd(), p.IsLeaf()))
		}
		cellEnd := p.cellEnd(i)
		cellData := make([]byte, cellEnd-cellOff)
		copy(cellData, p.data[cellOff:cellEnd])
		cells[i] = cellInfo{slotIdx: i, data: cellData}
	}

	// Also preserve the highKey cell if present
	var highKeyData []byte
	hkOff := p.highKeyOff()
	if hkOff != 0 {
		hkl := int(binary.LittleEndian.Uint16(p.data[hkOff:]))
		highKeyData = make([]byte, 2+hkl)
		copy(highKeyData, p.data[hkOff:hkOff+2+hkl])
	}

	// Re-pack cells from the end of the page
	newFreeEnd := btreeapi.PageSize

	// Re-pack highKey first (if present)
	if highKeyData != nil {
		newFreeEnd -= len(highKeyData)
		copy(p.data[newFreeEnd:], highKeyData)
		p.setHighKeyOff(newFreeEnd)
	}

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

// ─── Page ↔ Node Conversion ────────────────────────────────────────

// PageToNode converts a slotted *Page to a *btreeapi.Node for backward
// compatibility with vacuum and other external consumers.
func PageToNode(p *Page) *btreeapi.Node {
	count := p.Count()
	node := &btreeapi.Node{
		IsLeaf: p.IsLeaf(),
		Count:  uint16(count),
		Next:   p.Next(),
	}
	if hk := p.HighKey(); hk != nil {
		node.HighKey = cloneBytes(hk)
	}

	if p.IsLeaf() {
		node.Entries = make([]btreeapi.LeafEntry, count)
		for i := 0; i < count; i++ {
			node.Entries[i] = btreeapi.LeafEntry{
				Key:    cloneBytes(p.EntryKey(i)),
				TxnMin: p.EntryTxnMin(i),
				TxnMax: p.EntryTxnMax(i),
			}
			v := p.EntryValue(i)
			node.Entries[i].Value = btreeapi.Value{
				Inline: cloneBytes(v.Inline),
				BlobID: v.BlobID,
			}
		}
	} else {
		node.Keys = make([][]byte, count)
		node.Children = make([]uint64, count+1)
		node.Children[0] = p.Child0()
		for i := 0; i < count; i++ {
			node.Keys[i] = cloneBytes(p.InternalKey(i))
			node.Children[i+1] = p.InternalChild(i)
		}
	}
	return node
}

// NodeToPage converts a *btreeapi.Node to a slotted *Page for backward
// compatibility with WritePageNode.
func NodeToPage(node *btreeapi.Node) *Page {
	var p *Page
	if node.IsLeaf {
		p = NewLeafPage()
		if node.HighKey != nil {
			p.SetHighKey(node.HighKey)
		}
		p.SetNext(node.Next)
		for i, e := range node.Entries {
			blobID := e.Value.BlobID
			p.InsertLeafEntry(i, e.Key, e.TxnMin, e.TxnMax, e.Value.Inline, blobID)
		}
	} else {
		p = NewInternalPage()
		if node.HighKey != nil {
			p.SetHighKey(node.HighKey)
		}
		p.SetNext(node.Next)
		if len(node.Children) > 0 {
			p.SetChild0(node.Children[0])
		}
		for i, key := range node.Keys {
			childPID := uint64(0)
			if i+1 < len(node.Children) {
				childPID = node.Children[i+1]
			}
			p.InsertInternalEntry(i, key, childPID)
		}
	}
	return p
}
