package internal

import (
	"hash/fnv"
	"math"
)

// ─── Bloom Filter ─────────────────────────────────────────────────

// BloomFilter is a probabilistic data structure for fast membership testing.
// It can tell you if an element is possibly in a set (with false positives)
// but never says an element is definitely NOT in a set when it is.
type BloomFilter struct {
	bits    []bool
	size    uint64
	numHash uint64
}

// NewBloomFilter creates a new Bloom Filter with the given expected number of items
// and desired false positive rate.
func NewBloomFilter(expectedItems uint64, falsePositiveRate float64) *BloomFilter {
	// Calculate optimal size: m = -n * ln(p) / (ln(2)^2)
	size := optimalSize(expectedItems, falsePositiveRate)

	// Calculate optimal number of hash functions: k = (m/n) * ln(2)
	numHash := optimalNumHash(size, expectedItems)

	return &BloomFilter{
		bits:    make([]bool, size),
		size:    size,
		numHash: numHash,
	}
}

// NewBloomFilterWithSize creates a Bloom Filter with the given bit array size
// and number of hash functions.
func NewBloomFilterWithSize(size uint64, numHash uint64) *BloomFilter {
	return &BloomFilter{
		bits:    make([]bool, size),
		size:    size,
		numHash: numHash,
	}
}

// constants for optimal parameter calculation
const ln2 = 0.6931471805599453 // ln(2)

// optimalSize calculates the optimal bit array size for given expected items and false positive rate.
func optimalSize(n uint64, p float64) uint64 {
	// m = -n * ln(p) / (ln(2)^2)
	m := float64(n) * -math.Log(p) / (ln2 * ln2)
	return uint64(m)
}

// optimalNumHash calculates the optimal number of hash functions.
func optimalNumHash(m, n uint64) uint64 {
	// k = (m/n) * ln(2)
	k := (float64(m) / float64(n)) * ln2
	if k < 1 {
		k = 1
	}
	return uint64(k)
}

// Add adds a key to the Bloom Filter.
func (bf *BloomFilter) Add(key uint64) {
	h1, h2 := bf.hash(key)
	for i := uint64(0); i < bf.numHash; i++ {
		idx := (h1 + i*h2) % bf.size
		bf.bits[idx] = true
	}
}

// Contains checks if a key might be in the Bloom Filter.
// Returns true if possibly present (may be false positive),
// false if definitely not present.
func (bf *BloomFilter) Contains(key uint64) bool {
	h1, h2 := bf.hash(key)
	for i := uint64(0); i < bf.numHash; i++ {
		idx := (h1 + i*h2) % bf.size
		if !bf.bits[idx] {
			return false // definitely not present
		}
	}
	return true // possibly present
}

// Size returns the number of bits in the Bloom Filter.
func (bf *BloomFilter) Size() uint64 {
	return bf.size
}

// NumHash returns the number of hash functions.
func (bf *BloomFilter) NumHash() uint64 {
	return bf.numHash
}

// hash returns two independent hash values for double hashing.
func (bf *BloomFilter) hash(key uint64) (uint64, uint64) {
	// Double hashing: h(i) = h1 + i * h2
	h1 := fnvHash(key)
	h2 := (key * 0x9e3779b97f4a7c15) % bf.size // secondary hash
	if h2 == 0 {
		h2 = 1
	}
	return h1 % bf.size, h2
}

// fnvHash returns a FNV-1a hash of the key.
func fnvHash(key uint64) uint64 {
	h := fnv.New64a()
	h.Write([]byte{
		byte(key >> 56), byte(key >> 48), byte(key >> 40), byte(key >> 32),
		byte(key >> 24), byte(key >> 16), byte(key >> 8), byte(key),
	})
	return h.Sum64()
}

// ─── Bloom Filter Serialization ──────────────────────────────────

// Serialize encodes the Bloom Filter to bytes.
func (bf *BloomFilter) Serialize() []byte {
	// Format: [size:8][numHash:8][bits:var]
	// bits are packed as one bit per entry
	size := 16 + int((bf.size+7)/8) // 8 bytes size + 8 bytes numHash + bits
	buf := make([]byte, size)

	pos := 0
	// Write size
	putUint64(buf[pos:], bf.size)
	pos += 8

	// Write numHash
	putUint64(buf[pos:], bf.numHash)
	pos += 8

	// Write bits (packed)
	for i := uint64(0); i < bf.size; i++ {
		if bf.bits[i] {
			buf[pos+int(i/8)] |= 1 << (i % 8)
		}
	}

	return buf
}

// DeserializeBloomFilter decodes a Bloom Filter from bytes.
func DeserializeBloomFilter(data []byte) *BloomFilter {
	if len(data) < 16 {
		return nil
	}

	pos := 0
	size := getUint64(data[pos:])
	pos += 8

	numHash := getUint64(data[pos:])
	pos += 8

	bf := &BloomFilter{
		bits:    make([]bool, size),
		size:    size,
		numHash: numHash,
	}

	// Read bits
	for i := uint64(0); i < size; i++ {
		byteIdx := int(i / 8)
		bitIdx := i % 8
		if pos+byteIdx < len(data) {
			bf.bits[i] = (data[pos+byteIdx] & (1 << bitIdx)) != 0
		}
	}

	return bf
}

func putUint64(buf []byte, v uint64) {
	buf[0] = byte(v)
	buf[1] = byte(v >> 8)
	buf[2] = byte(v >> 16)
	buf[3] = byte(v >> 24)
	buf[4] = byte(v >> 32)
	buf[5] = byte(v >> 40)
	buf[6] = byte(v >> 48)
	buf[7] = byte(v >> 56)
}

func getUint64(buf []byte) uint64 {
	return uint64(buf[0]) |
		uint64(buf[1])<<8 |
		uint64(buf[2])<<16 |
		uint64(buf[3])<<24 |
		uint64(buf[4])<<32 |
		uint64(buf[5])<<40 |
		uint64(buf[6])<<48 |
		uint64(buf[7])<<56
}
