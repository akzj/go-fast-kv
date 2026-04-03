package internal

import (
	"testing"

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
