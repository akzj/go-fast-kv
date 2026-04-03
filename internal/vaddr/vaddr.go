package vaddr

// DefaultMaxValueSize is the maximum value size (64 MB).
const DefaultMaxValueSize = 64 * 1024 * 1024

// VAddr encodes a physical address in the append-only address space.
type VAddr struct {
	SegmentID uint64
	Offset    uint64
}

// IsValid returns true if this VAddr represents a valid address.
func (v VAddr) IsValid() bool {
	return v.SegmentID != 0
}

// VAddrInvalid is the null/invalid address (reserved).
var VAddrInvalid = VAddr{SegmentID: 0, Offset: 0}

// PageID is a logical identifier for a page.
type PageID uint64
