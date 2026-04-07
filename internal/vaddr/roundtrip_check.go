//go:build ignore

package vaddr

import (
	"encoding/binary"
	"fmt"
)

// Test VAddr and PageID roundtrip
func main() {
	// Test various VAddrs
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
	
	fmt.Println("=== VAddr Roundtrip Test ===")
	for _, tc := range testCases {
		orig := VAddr{SegmentID: tc.segID, Offset: tc.offset}
		bytes := orig.ToBytes()
		restored := VAddrFromBytes(bytes)
		
		fmt.Printf("\n%s:\n", tc.name)
		fmt.Printf("  Original:   SegID=%d, Offset=%d\n", orig.SegmentID, orig.Offset)
		fmt.Printf("  Bytes:     %x\n", bytes)
		fmt.Printf("  Restored:  SegID=%d, Offset=%d\n", restored.SegmentID, restored.Offset)
		
		if orig != restored {
			fmt.Printf("  ❌ MISMATCH!\n")
		} else {
			fmt.Printf("  ✓ OK\n")
		}
	}
	
	// Test PageID encoding
	fmt.Println("\n=== PageID Roundtrip Test ===")
	testPageIDs := []struct {
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
	
	for _, tc := range testPageIDs {
		pageID := bytesToPageIDTest(tc.key)
		result := pageIDToBytesTest(pageID)
		
		match := string(result) == string(tc.key)
		status := "✓"
		if !match {
			status = "❌"
		}
		
		fmt.Printf("\n%s: %s\n", tc.name, status)
		fmt.Printf("  Original: %q (len=%d)\n", string(tc.key), len(tc.key))
		fmt.Printf("  PageID:   0x%x\n", uint64(pageID))
		fmt.Printf("  Result:   %q (len=%d)\n", string(result), len(result))
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
	
	// For keys > 7 bytes: use FNV-1a hash with bit 63 set
	hash := fnvHash64Test(key)
	hash |= 0x8000000000000000
	return hash
}

func pageIDToBytesTest(pageID uint64) []byte {
	if pageID == 0 {
		return nil
	}
	
	// Check if this is a hash-based key (bit 63 set)
	if pageID&0x8000000000000000 != 0 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], pageID)
		return buf[:]
	}
	
	// Extract length from bits 60-63
	length := int(uint64(pageID) & 0x000000000000000F)
	if length == 0 || length > 7 {
		return nil
	}
	
	// Extract data from upper bits
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
