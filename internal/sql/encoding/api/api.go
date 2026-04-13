// Package api defines the public interfaces for the encoding module.
//
// To understand the encoding module, read only this file.
//
// The encoding module provides:
//   - KeyEncoder: order-preserving key encoding for table rows and index entries
//   - RowCodec: row data serialization/deserialization
//   - CompareValues (in parent package): typed value comparison with NULL semantics
package api

import (
	"errors"

	catalogapi "github.com/akzj/go-fast-kv/internal/sql/catalog/api"
)

// ─── Errors ─────────────────────────────────────────────────────────

var (
	// ErrInvalidKey is returned when a key cannot be decoded.
	ErrInvalidKey = errors.New("encoding: invalid key format")

	// ErrInvalidRow is returned when row data cannot be decoded.
	ErrInvalidRow = errors.New("encoding: invalid row format")

	// ErrTypeMismatch is returned when comparing values of different types.
	ErrTypeMismatch = errors.New("encoding: type mismatch")
)

// ─── CompareOp ──────────────────────────────────────────────────────

// CompareOp represents a comparison operator for index scans.
type CompareOp int

const (
	OpEQ CompareOp = 0 // =
	OpNE CompareOp = 1 // !=
	OpLT CompareOp = 2 // <
	OpLE CompareOp = 3 // <=
	OpGT CompareOp = 4 // >
	OpGE CompareOp = 5 // >=
)

// ─── Key Encoding ───────────────────────────────────────────────────

// Key layout:
//
//	Row key:      t{tableID:4B BE}r{rowID:8B BE}           — 14 bytes fixed
//	Index key:    t{tableID:4B BE}i{indexID:4B BE}{encodedValue}{rowID:8B BE}
//	Metadata key: t{tableID:4B BE}m
//
// Value encoding (order-preserving, used in index keys):
//
//	NULL:   [0x00]
//	Int:    [0x02][8B: int64 XOR 0x8000000000000000, big-endian]
//	Float:  [0x03][8B: IEEE754 transform, big-endian]
//	Text:   [0x04][escaped bytes: 0x00→0x00 0xFF][0x00 0x00]
//	Blob:   [0x05][escaped bytes: 0x00→0x00 0xFF][0x00 0x00]

// KeyEncoder encodes/decodes KV keys for table rows and index entries.
type KeyEncoder interface {
	// EncodeRowKey encodes a table row key: t{tableID}r{rowID}
	EncodeRowKey(tableID uint32, rowID uint64) []byte

	// DecodeRowKey extracts tableID and rowID from a row key.
	// Returns ErrInvalidKey if the key format is wrong.
	DecodeRowKey(key []byte) (tableID uint32, rowID uint64, err error)

	// EncodeIndexKey encodes an index entry key.
	// Layout: t{tableID}i{indexID}{encodedValue}{rowID}
	EncodeIndexKey(tableID uint32, indexID uint32, value catalogapi.Value, rowID uint64) []byte

	// DecodeIndexKey extracts components from an index key.
	//
	// Decode algorithm:
	//   1. Read type tag at offset [10]
	//   2. NULL (0x00): value is 1 byte, rowID starts at [11]
	//   3. Int/Float (0x02/0x03): value is 9 bytes, rowID starts at [19]
	//   4. Text/Blob (0x04/0x05): scan for unescaped 0x00 0x00, rowID is last 8 bytes
	DecodeIndexKey(key []byte) (tableID uint32, indexID uint32, value catalogapi.Value, rowID uint64, err error)

	// EncodeValue encodes a single value in order-preserving format (for index keys).
	EncodeValue(v catalogapi.Value) []byte

	// DecodeValue decodes a single order-preserving encoded value.
	// Returns the decoded value and the number of bytes consumed.
	DecodeValue(data []byte) (catalogapi.Value, int, error)

	// EncodeRowPrefix returns the key prefix for all rows of a table: t{tableID}r
	EncodeRowPrefix(tableID uint32) []byte

	// EncodeRowPrefixEnd returns the exclusive end key for row prefix scan.
	// Computed by incrementing last byte: 'r' (0x72) → 's' (0x73).
	EncodeRowPrefixEnd(tableID uint32) []byte

	// EncodeIndexPrefix returns the key prefix for all entries of an index: t{tableID}i{indexID}
	EncodeIndexPrefix(tableID uint32, indexID uint32) []byte

	// EncodeIndexPrefixEnd returns the exclusive end key for index prefix scan.
	// Computed by incrementing last byte of the prefix.
	EncodeIndexPrefixEnd(tableID uint32, indexID uint32) []byte
}

// ─── Row Codec ──────────────────────────────────────────────────────

// Row value wire format:
//
//	[0:2]    uint16 column count (big-endian)
//	[2:2+N]  null bitmap (ceil(count/8) bytes, bit=1 means NULL)
//	[...]    column values concatenated:
//	         NULL:  skipped (indicated by null bitmap)
//	         Int:   8B int64 big-endian
//	         Float: 8B float64 IEEE754
//	         Text:  4B uint32 length + UTF-8 bytes
//	         Blob:  4B uint32 length + raw bytes

// RowCodec encodes/decodes row data (KV values, not keys).
type RowCodec interface {
	// EncodeRow encodes column values into a byte slice.
	// values[i] corresponds to columns[i].
	// A value is NULL if and only if IsNull == true (canonical rule).
	EncodeRow(values []catalogapi.Value) []byte

	// DecodeRow decodes a byte slice back into column values.
	// Returns values aligned with the column definitions.
	DecodeRow(data []byte, columns []catalogapi.ColumnDef) ([]catalogapi.Value, error)
}
