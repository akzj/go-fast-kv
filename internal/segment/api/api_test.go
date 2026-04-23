package segmentapi

import (
	"testing"
)

func TestPackPageVAddr_RoundTrip(t *testing.T) {
	tests := []struct {
		segID     uint32
		offset    uint32
		recordLen uint16
	}{
		{0, 0, 0},
		{1, 0, 14},                    // minimal record (empty page)
		{1, 4108, 500},                // typical after one fixed-size record
		{100, 1024, 4108},             // full 4096-byte page record
		{1000, 512*1024*1024, 2000},   // half-full page after split
		{(1 << 20) - 1, (1 << 30) - 1, (1 << 14) - 1}, // max values
	}

	for _, tt := range tests {
		packed := PackPageVAddr(tt.segID, tt.offset, tt.recordLen)
		gotSeg, gotOff, gotLen := UnpackPageVAddr(packed)

		if gotSeg != tt.segID {
			t.Errorf("segID: packed(%d,%d,%d) → got %d, want %d",
				tt.segID, tt.offset, tt.recordLen, gotSeg, tt.segID)
		}
		if gotOff != tt.offset {
			t.Errorf("offset: packed(%d,%d,%d) → got %d, want %d",
				tt.segID, tt.offset, tt.recordLen, gotOff, tt.offset)
		}
		if gotLen != tt.recordLen {
			t.Errorf("recordLen: packed(%d,%d,%d) → got %d, want %d",
				tt.segID, tt.offset, tt.recordLen, gotLen, tt.recordLen)
		}
	}
}

func TestSegmentIDFromPageVAddr(t *testing.T) {
	tests := []struct {
		segID     uint32
		offset    uint32
		recordLen uint16
	}{
		{0, 0, 0},
		{42, 1000, 500},
		{(1 << 20) - 1, (1 << 30) - 1, (1 << 14) - 1},
	}

	for _, tt := range tests {
		packed := PackPageVAddr(tt.segID, tt.offset, tt.recordLen)
		got := SegmentIDFromPageVAddr(packed)
		if got != tt.segID {
			t.Errorf("SegmentIDFromPageVAddr: packed(%d,%d,%d) → got %d, want %d",
				tt.segID, tt.offset, tt.recordLen, got, tt.segID)
		}
	}
}

func TestPackPageVAddr_ZeroIsSafe(t *testing.T) {
	// Zero packed value should unpack to all zeros
	segID, offset, recordLen := UnpackPageVAddr(0)
	if segID != 0 || offset != 0 || recordLen != 0 {
		t.Errorf("UnpackPageVAddr(0) = (%d, %d, %d), want (0, 0, 0)",
			segID, offset, recordLen)
	}
}

func TestPackPageVAddr_IndependentFields(t *testing.T) {
	// Setting one field should not affect others
	p1 := PackPageVAddr(1, 0, 0)
	s1, o1, l1 := UnpackPageVAddr(p1)
	if s1 != 1 || o1 != 0 || l1 != 0 {
		t.Errorf("only segID=1: got (%d, %d, %d)", s1, o1, l1)
	}

	p2 := PackPageVAddr(0, 1, 0)
	s2, o2, l2 := UnpackPageVAddr(p2)
	if s2 != 0 || o2 != 1 || l2 != 0 {
		t.Errorf("only offset=1: got (%d, %d, %d)", s2, o2, l2)
	}

	p3 := PackPageVAddr(0, 0, 1)
	s3, o3, l3 := UnpackPageVAddr(p3)
	if s3 != 0 || o3 != 0 || l3 != 1 {
		t.Errorf("only recordLen=1: got (%d, %d, %d)", s3, o3, l3)
	}
}

// TestOldPackUnpack_Unchanged verifies the existing Pack/Unpack is not broken.
func TestOldPackUnpack_Unchanged(t *testing.T) {
	v := VAddr{SegmentID: 42, Offset: 12345}
	packed := v.Pack()
	restored := UnpackVAddr(packed)
	if restored.SegmentID != 42 || restored.Offset != 12345 {
		t.Errorf("old Pack/Unpack broken: got %+v", restored)
	}
}
