package internal

// Offset constants for page header (matching node.go)
const (
	directOffsetFlags      = 0
	directOffsetReserved   = 1
	directOffsetCount      = 2
	directOffsetNext       = 4
	directOffsetChecksum   = 12
	directOffsetHighKeyLen = 16
)

// PageAccessor provides zero-copy access to page bytes without deserializing.
// It reads directly from the raw page data using slice views.
type PageAccessor struct {
	data    []byte
	isLeaf  bool
	count   uint16
	next    uint64
	highKey []byte
}

// NewPageAccessor creates a zero-copy accessor for page data.
func NewPageAccessor(data []byte) (*PageAccessor, error) {
	if len(data) < headerBaseSize {
		return nil, errDataTooShort
	}
	acc := &PageAccessor{data: data}
	acc.isLeaf = data[directOffsetFlags]&1 != 0
	acc.count = readUint16(data[directOffsetCount:])
	acc.next = readUint64(data[directOffsetNext:])
	hkl := readUint16(data[directOffsetHighKeyLen:])
	if hkl > 0 {
		acc.highKey = data[headerBaseSize : headerBaseSize+int(hkl)]
	}
	return acc, nil
}

func readUint16(b []byte) uint16 {
	return uint16(b[0]) | uint16(b[1])<<8
}

func readUint64(b []byte) uint64 {
	return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
		uint64(b[4])<<32 | uint64(b[5])<<40 | uint64(b[6])<<48 | uint64(b[7])<<56
}

// IsLeaf returns true if this is a leaf node.
func (a *PageAccessor) IsLeaf() bool { return a.isLeaf }

// Count returns the number of entries.
func (a *PageAccessor) Count() uint16 { return a.count }

// Next returns the right sibling PageID.
func (a *PageAccessor) Next() uint64 { return a.next }

// HighKey returns the high key (nil means +∞).
func (a *PageAccessor) HighKey() []byte { return a.highKey }

// LeafEntryAt returns a zero-copy view of the i-th leaf entry.
func (a *PageAccessor) LeafEntryAt(i int) (key, value []byte, txnMin, txnMax uint64, err error) {
	if !a.isLeaf || i < 0 || i >= int(a.count) {
		return nil, nil, 0, 0, errDataTooShort
	}
	off := a.getLeafEntryOffset(i)
	return a.parseLeafEntry(off)
}

// getLeafEntryOffset returns the absolute offset in data where entry i starts.
func (a *PageAccessor) getLeafEntryOffset(i int) int {
	hkl := len(a.highKey)
	offsetArrayStart := headerBaseSize + hkl
	relOff := readUint16(a.data[offsetArrayStart+2*i:])
	return offsetArrayStart + 2*int(a.count) + int(relOff)
}

// parseLeafEntry parses a leaf entry at the given offset.
func (a *PageAccessor) parseLeafEntry(off int) (key, value []byte, txnMin, txnMax uint64, err error) {
	if off+2 > len(a.data) {
		return nil, nil, 0, 0, errDataTooShort
	}
	kl := readUint16(a.data[off:])
	off += 2
	if off+int(kl)+17 > len(a.data) {
		return nil, nil, 0, 0, errDataTooShort
	}
	key = a.data[off : off+int(kl)]
	off += int(kl)
	txnMin = readUint64(a.data[off:])
	off += 8
	txnMax = readUint64(a.data[off:])
	off += 8
	vt := a.data[off]
	off++
	if vt == 1 { // blobRef
		off += 8
	} else { // inline
		vl := uint32(a.data[off]) | uint32(a.data[off+1])<<8 | uint32(a.data[off+2])<<16 | uint32(a.data[off+3])<<24
		off += 4
		value = a.data[off : off+int(vl)]
	}
	return key, value, txnMin, txnMax, nil
}

// FirstChild returns the first child PageID of an internal node.
func (a *PageAccessor) FirstChild() uint64 {
	if a.isLeaf {
		return 0
	}
	hkl := len(a.highKey)
	return readUint64(a.data[headerBaseSize+hkl:])
}
