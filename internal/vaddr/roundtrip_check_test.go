package vaddr

import (
	"encoding/binary"
	
	"testing"
)

func TestVAddrRoundtrip(t *testing.T) {
	testCases := []struct {
		name      string
		segID     uint64
		offset    uint64
	}{
		{"Small offset", 1, 32}, // headerSize
		{"Page-aligned", 1, 32 + PageSize},
		{"Multiple pages", 1, 32 + PageSize*10},
		{"Large offset", 1, 32 + PageSize*56},
		{"Segment 2", 2, 32 + PageSize*100},
	}
	
	for _, tc := range testCases {
		orig := VAddr{SegmentID: tc.segID, Offset: tc.offset}
		bytes := orig.ToBytes()
		restored := VAddrFromBytes(bytes)
		
		if orig != restored {
			t.Errorf("%s: MISMATCH! Original SegID=%d, Offset=%d vs Restored SegID=%d, Offset=%d",
				tc.name, orig.SegmentID, orig.Offset, restored.SegmentID, restored.Offset)
		}
	}
}

func TestPageIDRoundtrip(t *testing.T) {
	testCases := []struct {
		name string
		key  []byte
	}{
		{"Empty", []byte{}},
		{"1 byte", []byte("a")},
		{"7 bytes", []byte("abcdefg")},
		{"8 bytes", []byte("abcdefgh")},
		{"16 bytes", []byte("abcdefghijklmnop")},
		{"100 bytes", make([]byte, 100)},
	}
	
	for _, tc := range testCases {
		pageID := bytesToPageIDTest(tc.key)
		result := pageIDToBytesTest(pageID)
		
		if string(result) != string(tc.key) {
			t.Errorf("%s: MISMATCH! Original=%q (len=%d), Result=%q (len=%d), PageID=0x%x",
				tc.name, string(tc.key), len(tc.key), string(result), len(result), uint64(pageID))
		}
	}
}

func bytesToPageIDTest(key []byte) uint64 {
	if len(key) == 0 {
		return 0
	}
	
	if len(key) <= 7 {
		var data uint64
		for i := 0; i < len(key); i++ {
			data = (data << 8) | uint64(key[i])
		}
		data = data << 32
		data |= uint64(len(key))
		return data
	}
	
	hash := fnvHash64Test(key)
	hash |= 0x8000000000000000
	return hash
}

func pageIDToBytesTest(pageID uint64) []byte {
	if pageID == 0 {
		return nil
	}
	
	if pageID&0x8000000000000000 != 0 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], pageID)
		return buf[:]
	}
	
	length := int(uint64(pageID) & 0x000000000000000F)
	if length == 0 || length > 7 {
		return nil
	}
	
	data := uint64(pageID) >> 32
	
	result := make([]byte, length)
	for i := length - 1; i >= 0; i-- {
		result[i] = byte(data & 0xFF)
		data >>= 8
	}
	return result
}

func fnvHash64Test(data []byte) uint64 {
	hash := uint64(14695981039346656037)
	for _, b := range data {
		hash ^= uint64(b)
		hash *= 1099511628211
	}
	return hash
}
