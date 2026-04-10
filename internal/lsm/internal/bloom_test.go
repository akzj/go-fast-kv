package internal

import (
	"testing"
)

func TestBloomFilterBasic(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)
	
	// Add some keys
	bf.Add(1)
	bf.Add(100)
	bf.Add(1000)
	
	// These should definitely be present
	if !bf.Contains(1) {
		t.Error("key 1 should be present")
	}
	if !bf.Contains(100) {
		t.Error("key 100 should be present")
	}
	if !bf.Contains(1000) {
		t.Error("key 1000 should be present")
	}
	
	// This should definitely NOT be present (not added)
	if bf.Contains(999999) {
		t.Error("key 999999 should NOT be present")
	}
}

func TestBloomFilterFalsePositives(t *testing.T) {
	bf := NewBloomFilter(100, 0.01)
	
	// Add 100 keys
	for i := uint64(0); i < 100; i++ {
		bf.Add(i)
	}
	
	// Check for keys that were not added
	// With 1% false positive rate, we expect some false positives
	falsePositives := 0
	for i := uint64(1000); i < 2000; i++ {
		if bf.Contains(i) {
			falsePositives++
		}
	}
	
	// We added 100 keys to 1000 slots, so false positive rate should be low
	// With 1% rate, we expect ~10 false positives out of 1000 checks
	t.Logf("False positives: %d / 1000", falsePositives)
	
	// Allow up to 50 false positives (5% is generous for testing)
	if falsePositives > 50 {
		t.Errorf("Too many false positives: %d", falsePositives)
	}
}

func TestBloomFilterSerialization(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)
	
	// Add some keys
	for i := uint64(0); i < 100; i++ {
		bf.Add(i)
	}
	
	// Serialize
	data := bf.Serialize()
	
	// Deserialize
	bf2 := DeserializeBloomFilter(data)
	
	// Check that all added keys are still found
	for i := uint64(0); i < 100; i++ {
		if !bf2.Contains(i) {
			t.Errorf("key %d should be present after deserialization", i)
		}
	}
	
	// Check that non-added keys are not found (or false positive)
	if bf2.Contains(999999) {
		// This might be a false positive, which is acceptable
		t.Log("key 999999 might be a false positive (acceptable)")
	}
}

func TestBloomFilterEmpty(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)
	
	// Empty filter should return false for any key
	if bf.Contains(1) {
		t.Error("empty filter should return false for any key")
	}
	if bf.Contains(1000) {
		t.Error("empty filter should return false for any key")
	}
}

func TestBloomFilterSize(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)
	
	// Check that size is reasonable
	size := bf.Size()
	if size < 9000 {
		t.Errorf("Bloom filter size too small: %d", size)
	}
	
	// Check that numHash is reasonable
	numHash := bf.NumHash()
	if numHash < 1 || numHash > 20 {
		t.Errorf("Bloom filter numHash out of range: %d", numHash)
	}
}

func TestBloomFilterWithManyKeys(t *testing.T) {
	bf := NewBloomFilter(100000, 0.01)
	
	// Add 100000 keys
	for i := uint64(0); i < 100000; i++ {
		bf.Add(i)
	}
	
	// All added keys should be found
	for i := uint64(0); i < 100000; i++ {
		if !bf.Contains(i) {
			t.Errorf("key %d should be present", i)
		}
	}
}

func TestSSTableWithBloomFilter(t *testing.T) {
	// Create temp dir
	dir := t.TempDir()
	
	// Create entries
	pageMappings := []sstEntry{
		{key: 1, value: 100},
		{key: 2, value: 200},
		{key: 3, value: 300},
	}
	blobMappings := []sstEntry{
		{key: 100, value: 1000, size: 500},
		{key: 200, value: 2000, size: 600},
	}
	
	// Write SSTable
	path := dir + "/test.sst"
	err := writeSSTable(path, pageMappings, blobMappings)
	if err != nil {
		t.Fatalf("writeSSTable failed: %v", err)
	}
	
	// Read Bloom Filter
	bloom := readBloomFilter(path)
	if bloom == nil {
		t.Fatal("Bloom Filter should not be nil")
	}
	
	// Check that added keys are reported as possibly present
	if !bloom.Contains(1) {
		t.Error("key 1 should be reported as possibly present")
	}
	if !bloom.Contains(100) {
		t.Error("key 100 should be reported as possibly present")
	}
	
	// Check that non-existent keys might be reported as not present
	// (They should return false - no false positive)
	if bloom.Contains(999999) {
		t.Log("key 999999 is a false positive (acceptable)")
	}
	
	// Read SSTable
	pages, blobs, err := readSSTable(path)
	if err != nil {
		t.Fatalf("readSSTable failed: %v", err)
	}
	
	// Verify data
	if len(pages) != 3 {
		t.Errorf("expected 3 page mappings, got %d", len(pages))
	}
	if len(blobs) != 2 {
		t.Errorf("expected 2 blob mappings, got %d", len(blobs))
	}
}
