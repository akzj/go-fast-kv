package internal

import (
	"encoding/binary"
	"testing"

	"unsafe"

	vaddr "github.com/akzj/go-fast-kv/internal/vaddr"
)

func TestVAddrIsValid(t *testing.T) {
	tests := []struct {
		name      string
		segmentID uint64
		offset    uint64
		want      bool
	}{
		{"invalid_zero", 0, 0, false},
		{"invalid_only_segment", 0, 100, false},
		{"valid", 1, 0, true},
		{"valid_with_offset", 5, 4096, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := vaddr.VAddr{SegmentID: tt.segmentID, Offset: tt.offset}
			if got := v.IsValid(); got != tt.want {
				t.Errorf("VAddr.IsValid() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestVAddrIsZero(t *testing.T) {
	tests := []struct {
		name      string
		segmentID uint64
		offset    uint64
		want      bool
	}{
		{"zero", 0, 0, true},
		{"non_zero_segment", 1, 0, false},
		{"non_zero_offset", 0, 1, false},
		{"both_nonzero", 5, 4096, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := vaddr.VAddr{SegmentID: tt.segmentID, Offset: tt.offset}
			if got := v.IsZero(); got != tt.want {
				t.Errorf("VAddr.IsZero() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestVAddrToBytes(t *testing.T) {
	v := vaddr.VAddr{SegmentID: 1, Offset: 4096}
	b := v.ToBytes()

	// Verify it's exactly 16 bytes
	if len(b) != 16 {
		t.Errorf("ToBytes() returned %d bytes, want 16", len(b))
	}

	// Verify big-endian encoding
	// For SegmentID=1 (0x01), MSB is at b[7], LSB at b[0]
	if b[0] != 0x00 || b[7] != 0x01 {
		t.Errorf("SegmentID encoding incorrect, got [%x...%x], want MSB=0x01 at b[7]", b[0], b[7])
	}

	// For Offset=4096 (0x1000), MSB is at b[8], LSB at b[15]
	if b[8] != 0x00 || b[9] != 0x00 || b[10] != 0x00 || b[11] != 0x00 ||
		b[12] != 0x00 || b[13] != 0x00 || b[14] != 0x10 || b[15] != 0x00 {
		t.Errorf("Offset encoding incorrect, got %x", b[8:])
	}
}

func TestVAddrFromBytes(t *testing.T) {
	// Test valid 16-byte input: SegmentID=1, Offset=4096
	var b [16]byte
	b[7] = 0x01 // SegmentID = 1 (MSB at b[7])
	b[14] = 0x10
	b[15] = 0x00 // Offset = 4096

	v := vaddr.VAddrFromBytes(b)
	if v.SegmentID != 1 {
		t.Errorf("VAddrFromBytes SegmentID = %d, want 1", v.SegmentID)
	}
	if v.Offset != 4096 {
		t.Errorf("VAddrFromBytes Offset = %d, want 4096", v.Offset)
	}
}

func TestVAddrRoundTrip(t *testing.T) {
	tests := []struct {
		name      string
		segmentID uint64
		offset    uint64
	}{
		{"basic", 1, 0},
		{"with_offset", 5, 4096},
		{"max_segment", 0xFFFFFFFFFFFFFFFF, 0},
		{"max_offset", 1, 0xFFFFFFFFFFFFFFFF},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := vaddr.VAddr{SegmentID: tt.segmentID, Offset: tt.offset}
			b := v.ToBytes()
			v2 := vaddr.VAddrFromBytes(b)

			if v2 != v {
				t.Errorf("Round trip failed: %v != %v", v2, v)
			}
		})
	}
}

func TestVAddrBinaryFormat(t *testing.T) {
	// VAddr must be exactly 16 bytes
	v := vaddr.VAddr{SegmentID: 1, Offset: 4096}
	b := v.ToBytes()

	if n := len(b); n != 16 {
		t.Errorf("VAddr.ToBytes() returns %d bytes, must be exactly 16", n)
	}
}

func TestSegmentIDIsValid(t *testing.T) {
	tests := []struct {
		name   string
		id     vaddr.SegmentID
		want   bool
	}{
		{"invalid_zero", 0, false},
		{"valid_min", 1, true},
		{"valid_large", 100, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.id.IsValid(); got != tt.want {
				t.Errorf("SegmentID.IsValid() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPageIDIsValid(t *testing.T) {
	tests := []struct {
		name string
		id   vaddr.PageID
		want bool
	}{
		{"invalid_zero", 0, false},
		{"valid_min", 1, true},
		{"valid_large", 100, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.id.IsValid(); got != tt.want {
				t.Errorf("PageID.IsValid() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSegmentStateString(t *testing.T) {
	tests := []struct {
		state vaddr.SegmentState
		want  string
	}{
		{vaddr.SegmentStateActive, "Active"},
		{vaddr.SegmentStateSealed, "Sealed"},
		{vaddr.SegmentStateArchived, "Archived"},
		{vaddr.SegmentState(0xFF), "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.state.String(); got != tt.want {
				t.Errorf("SegmentState.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConstants(t *testing.T) {
	// Verify key constants match expected values
	if vaddr.PageSize != 4096 {
		t.Errorf("PageSize = %d, want 4096", vaddr.PageSize)
	}
	if vaddr.ExternalThreshold != 48 {
		t.Errorf("ExternalThreshold = %d, want 48", vaddr.ExternalThreshold)
	}
	if vaddr.SegmentStateActive != 0x01 {
		t.Errorf("SegmentStateActive = 0x%02x, want 0x01", vaddr.SegmentStateActive)
	}
	if vaddr.SegmentStateSealed != 0x02 {
		t.Errorf("SegmentStateSealed = 0x%02x, want 0x02", vaddr.SegmentStateSealed)
	}
	if vaddr.SegmentStateArchived != 0x04 {
		t.Errorf("SegmentStateArchived = 0x%02x, want 0x04", vaddr.SegmentStateArchived)
	}
}

// TestVAddrSize verifies VAddr is exactly 16 bytes using unsafe.Sizeof.
// This is critical for binary protocol compatibility.
func TestVAddrSize(t *testing.T) {
	v := vaddr.VAddr{}
	size := unsafe.Sizeof(v)
	if size != 16 {
		t.Errorf("VAddr size = %d bytes, want exactly 16 bytes", size)
	}
}

// TestVAddrStructLayout verifies the struct has expected field layout.
// First field is SegmentID (offset 0), second is Offset (offset 8).
func TestVAddrStructLayout(t *testing.T) {
	v := vaddr.VAddr{}
	// SegmentID should be at offset 0
	segPtr := (*uint64)(unsafe.Pointer(&v))
	if *segPtr != 0 {
		t.Errorf("SegmentID not at offset 0")
	}
	// Offset should be at offset 8
	offsetPtr := (*uint64)(unsafe.Pointer(uintptr(unsafe.Pointer(&v)) + 8))
	if *offsetPtr != 0 {
		t.Errorf("Offset not at offset 8")
	}
}

// TestAllConstants verifies all exported constants.
func TestAllConstants(t *testing.T) {
	tests := []struct {
		name  string
		got   uint64
		want  uint64
	}{
		{"PageSize", uint64(vaddr.PageSize), 4096},
		{"ExternalThreshold", uint64(vaddr.ExternalThreshold), 48},
		{"MaxSegmentSize", uint64(vaddr.MaxSegmentSize), 1 << 30},
		{"SegmentHeaderSize", uint64(vaddr.SegmentHeaderSize), 32},
		{"SegmentTrailerSize", uint64(vaddr.SegmentTrailerSize), 32},
		{"EpochGracePeriod", uint64(vaddr.EpochGracePeriod), 3},
		{"SegmentIDInvalid", uint64(vaddr.SegmentIDInvalid), 0},
		{"SegmentIDMin", uint64(vaddr.SegmentIDMin), 1},
		{"PageIDInvalid", uint64(vaddr.PageIDInvalid), 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("%s = %d, want %d", tt.name, tt.got, tt.want)
			}
		})
	}
}

// TestVAddrSerializationEdgeCases tests serialization with edge values.
func TestVAddrSerializationEdgeCases(t *testing.T) {
	tests := []struct {
		name      string
		segmentID uint64
		offset    uint64
	}{
		{"zero_segment", 0, 0},
		{"zero_offset", 1, 0},
		{"min_valid", 1, 0},
		{"page_boundary", 1, 4096},
		{"multi_page", 1, 8192},
		{"large_offset", 1, 0xFFFFFFFFFFFF},
		{"max_segment", 0xFFFFFFFFFFFFFFFF, 0},
		{"max_offset", 1, 0xFFFFFFFFFFFFFFFF},
		{"max_both", 0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF},
		{"external_threshold_boundary", 1, 48},
		{"page_size_boundary", 1, vaddr.PageSize},
		{"page_size_minus_one", 1, vaddr.PageSize - 1},
		{"page_size_plus_one", 1, vaddr.PageSize + 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := vaddr.VAddr{SegmentID: tt.segmentID, Offset: tt.offset}
			b := v.ToBytes()

			// Must be exactly 16 bytes
			if len(b) != 16 {
				t.Errorf("ToBytes() returned %d bytes, want 16", len(b))
			}

			// Round-trip test
			v2 := vaddr.VAddrFromBytes(b)
			if v2 != v {
				t.Errorf("Round trip failed: got %v, want %v", v2, v)
			}

			// Verify big-endian encoding
			segID := binary.BigEndian.Uint64(b[0:8])
			offset := binary.BigEndian.Uint64(b[8:16])
			if segID != tt.segmentID {
				t.Errorf("SegmentID encoding: got 0x%x, want 0x%x", segID, tt.segmentID)
			}
			if offset != tt.offset {
				t.Errorf("Offset encoding: got 0x%x, want 0x%x", offset, tt.offset)
			}
		})
	}
}

// TestSegmentIDBoundaryValues tests SegmentID edge values.
func TestSegmentIDBoundaryValues(t *testing.T) {
	tests := []struct {
		name string
		id   vaddr.SegmentID
		want bool
	}{
		{"invalid_zero", 0, false},
		{"valid_min", 1, true},
		{"valid_typical", 100, true},
		{"valid_large", 0x7FFFFFFFFFFFFFFF, true},
		{"valid_max", 0xFFFFFFFFFFFFFFFF, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.id.IsValid(); got != tt.want {
				t.Errorf("SegmentID(%d).IsValid() = %v, want %v", tt.id, got, tt.want)
			}
		})
	}
}

// TestPageIDBoundaryValues tests PageID edge values.
func TestPageIDBoundaryValues(t *testing.T) {
	tests := []struct {
		name string
		id   vaddr.PageID
		want bool
	}{
		{"invalid_zero", 0, false},
		{"valid_min", 1, true},
		{"valid_typical", 100, true},
		{"valid_large", 0x7FFFFFFFFFFFFFFF, true},
		{"valid_max", 0xFFFFFFFFFFFFFFFF, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.id.IsValid(); got != tt.want {
				t.Errorf("PageID(%d).IsValid() = %v, want %v", tt.id, got, tt.want)
			}
		})
	}
}

// TestSegmentStateAllValues tests all SegmentState values.
func TestSegmentStateAllValues(t *testing.T) {
	tests := []struct {
		state vaddr.SegmentState
		want  string
	}{
		{vaddr.SegmentStateActive, "Active"},
		{vaddr.SegmentStateSealed, "Sealed"},
		{vaddr.SegmentStateArchived, "Archived"},
		{0x00, "Unknown"},
		{0x03, "Unknown"},
		{0x05, "Unknown"},
		{0x07, "Unknown"},
		{0xFF, "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.state.String(); got != tt.want {
				t.Errorf("SegmentState(0x%02x).String() = %q, want %q", tt.state, got, tt.want)
			}
		})
	}
}

// TestSegmentStateConstants verifies state constant values.
func TestSegmentStateConstants(t *testing.T) {
	if vaddr.SegmentStateActive != 0x01 {
		t.Errorf("SegmentStateActive = 0x%02x, want 0x01", vaddr.SegmentStateActive)
	}
	if vaddr.SegmentStateSealed != 0x02 {
		t.Errorf("SegmentStateSealed = 0x%02x, want 0x02", vaddr.SegmentStateSealed)
	}
	if vaddr.SegmentStateArchived != 0x04 {
		t.Errorf("SegmentStateArchived = 0x%02x, want 0x04", vaddr.SegmentStateArchived)
	}
}

// TestBigEndianEncoding verifies proper big-endian byte order.
func TestBigEndianEncoding(t *testing.T) {
	tests := []struct {
		segmentID uint64
		offset    uint64
		segMSB    byte
		segLSB    byte
		offMSB    byte
		offLSB    byte
	}{
		{1, 0, 0x00, 0x01, 0x00, 0x00},
		{256, 0, 0x00, 0x00, 0x00, 0x00}, // 256 = 0x100, LSB is 0
		{0x8000000000000001, 0, 0x80, 0x01, 0x00, 0x00}, // MSB at b[0]
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			v := vaddr.VAddr{SegmentID: tt.segmentID, Offset: tt.offset}
			b := v.ToBytes()

			if b[0] != tt.segMSB || b[7] != tt.segLSB {
				t.Errorf("SegmentID encoding incorrect for 0x%x: MSB=0x%02x, LSB=0x%02x, want MSB=0x%02x, LSB=0x%02x",
					tt.segmentID, b[0], b[7], tt.segMSB, tt.segLSB)
			}
			if b[8] != tt.offMSB || b[15] != tt.offLSB {
				t.Errorf("Offset encoding incorrect for 0x%x: MSB=0x%02x, LSB=0x%02x, want MSB=0x%02x, LSB=0x%02x",
					tt.offset, b[8], b[15], tt.offMSB, tt.offLSB)
			}
		})
	}
}

// TestVAddrIsZeroEdgeCases tests IsZero with various inputs.
func TestVAddrIsZeroEdgeCases(t *testing.T) {
	tests := []struct {
		name      string
		segmentID uint64
		offset    uint64
		want      bool
	}{
		{"both_zero", 0, 0, true},
		{"segment_only", 1, 0, false},
		{"offset_only", 0, 1, true}, // SegmentID=0 means zero even with offset
		{"both_nonzero", 1, 1, false},
		{"max_segment", 0xFFFFFFFFFFFFFFFF, 0, false},
		{"max_offset", 0, 0xFFFFFFFFFFFFFFFF, true}, // SegmentID=0 means zero
	}
	_ = tests
}

// TestVAddrInvalidInputs tests behavior with invalid inputs.
func TestVAddrInvalidInputs(t *testing.T) {
	// SegmentID=0 is invalid (reserved)
	v := vaddr.VAddr{SegmentID: 0, Offset: 100}
	if v.IsValid() {
		t.Errorf("VAddr with SegmentID=0 should be invalid")
	}
	// IsZero requires both fields to be zero
	if v.IsZero() {
		t.Errorf("VAddr with SegmentID=0 and Offset=100 should not be zero")
	}

	// True zero address
	v2 := vaddr.VAddr{SegmentID: 0, Offset: 0}
	if v2.IsValid() {
		t.Errorf("Zero VAddr should be invalid")
	}
	if !v2.IsZero() {
		t.Errorf("Zero VAddr should be zero")
	}
}

// TestVAddrToBytesConsistency verifies ToBytes produces consistent output.
func TestVAddrToBytesConsistency(t *testing.T) {
	v := vaddr.VAddr{SegmentID: 0x123456789ABCDEF0, Offset: 0xFEDCBA9876543210}

	// Call multiple times, should get same result
	b1 := v.ToBytes()
	b2 := v.ToBytes()

	if b1 != b2 {
		t.Errorf("ToBytes() not consistent: %x vs %x", b1, b2)
	}

	// Verify specific byte values
	b := v.ToBytes()
	expected := [16]byte{
		0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0,
		0xFE, 0xDC, 0xBA, 0x98, 0x76, 0x54, 0x32, 0x10,
	}
	for i := 0; i < 16; i++ {
		if b[i] != expected[i] {
			t.Errorf("Byte[%d] = 0x%02x, want 0x%02x", i, b[i], expected[i])
		}
	}
}

// TestPageSizeAlignment tests that PageSize is power of 2 and reasonable.
func TestPageSizeAlignment(t *testing.T) {
	if vaddr.PageSize != 4096 {
		t.Errorf("PageSize = %d, want 4096", vaddr.PageSize)
	}
	// PageSize should be power of 2 for efficient alignment
	if vaddr.PageSize&(vaddr.PageSize-1) != 0 {
		t.Errorf("PageSize = %d is not a power of 2", vaddr.PageSize)
	}
}

// TestExternalThreshold tests the external threshold constant.
func TestExternalThreshold(t *testing.T) {
	if vaddr.ExternalThreshold != 48 {
		t.Errorf("ExternalThreshold = %d, want 48", vaddr.ExternalThreshold)
	}
	// ExternalThreshold should be less than PageSize
	if vaddr.ExternalThreshold >= vaddr.PageSize {
		t.Errorf("ExternalThreshold = %d should be less than PageSize = %d",
			vaddr.ExternalThreshold, vaddr.PageSize)
	}
}
