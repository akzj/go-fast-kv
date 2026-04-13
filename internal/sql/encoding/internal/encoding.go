// Package internal implements the encoding module.
package internal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/akzj/go-fast-kv/internal/sql/encoding/api"

	catalogapi "github.com/akzj/go-fast-kv/internal/sql/catalog/api"
)

// Compile-time interface checks.
var _ api.KeyEncoder = (*keyEncoder)(nil)
var _ api.RowCodec = (*rowCodec)(nil)

// ─── Constants ──────────────────────────────────────────────────────

const (
	prefixTable byte = 't' // 0x74
	tagRow      byte = 'r' // 0x72
	tagIndex    byte = 'i' // 0x69
	tagMeta     byte = 'm' // 0x6D

	// Value type tags (order-preserving: NULL < Int < Float < Text < Blob).
	tagNull  byte = 0x00
	tagInt   byte = 0x02
	tagFloat byte = 0x03
	tagText  byte = 0x04
	tagBlob  byte = 0x05

	rowKeyLen = 14 // 1 + 4 + 1 + 8
)

// ─── KeyEncoder ─────────────────────────────────────────────────────

type keyEncoder struct{}

// NewKeyEncoder creates a new KeyEncoder.
func NewKeyEncoder() api.KeyEncoder { return &keyEncoder{} }

func (e *keyEncoder) EncodeRowKey(tableID uint32, rowID uint64) []byte {
	buf := make([]byte, rowKeyLen)
	buf[0] = prefixTable
	binary.BigEndian.PutUint32(buf[1:5], tableID)
	buf[5] = tagRow
	binary.BigEndian.PutUint64(buf[6:14], rowID)
	return buf
}

func (e *keyEncoder) DecodeRowKey(key []byte) (uint32, uint64, error) {
	if len(key) != rowKeyLen || key[0] != prefixTable || key[5] != tagRow {
		return 0, 0, fmt.Errorf("%w: expected row key (len=%d, got %d)", api.ErrInvalidKey, rowKeyLen, len(key))
	}
	tableID := binary.BigEndian.Uint32(key[1:5])
	rowID := binary.BigEndian.Uint64(key[6:14])
	return tableID, rowID, nil
}

func (e *keyEncoder) EncodeIndexKey(tableID uint32, indexID uint32, value catalogapi.Value, rowID uint64) []byte {
	// Header: t{tableID}i{indexID} = 10 bytes
	header := make([]byte, 10)
	header[0] = prefixTable
	binary.BigEndian.PutUint32(header[1:5], tableID)
	header[5] = tagIndex
	binary.BigEndian.PutUint32(header[6:10], indexID)

	encoded := e.EncodeValue(value)

	// Suffix: rowID = 8 bytes
	suffix := make([]byte, 8)
	binary.BigEndian.PutUint64(suffix, rowID)

	result := make([]byte, 0, len(header)+len(encoded)+len(suffix))
	result = append(result, header...)
	result = append(result, encoded...)
	result = append(result, suffix...)
	return result
}

func (e *keyEncoder) DecodeIndexKey(key []byte) (uint32, uint32, catalogapi.Value, uint64, error) {
	if len(key) < 11 || key[0] != prefixTable || key[5] != tagIndex {
		return 0, 0, catalogapi.Value{}, 0, fmt.Errorf("%w: too short or wrong prefix for index key", api.ErrInvalidKey)
	}

	tableID := binary.BigEndian.Uint32(key[1:5])
	indexID := binary.BigEndian.Uint32(key[6:10])

	// Decode value starting at offset 10.
	value, consumed, err := e.DecodeValue(key[10:])
	if err != nil {
		return 0, 0, catalogapi.Value{}, 0, fmt.Errorf("%w: decoding index value: %v", api.ErrInvalidKey, err)
	}

	// rowID is the last 8 bytes after the value.
	rowIDStart := 10 + consumed
	if rowIDStart+8 != len(key) {
		return 0, 0, catalogapi.Value{}, 0, fmt.Errorf("%w: index key has %d trailing bytes, expected 8", api.ErrInvalidKey, len(key)-rowIDStart)
	}
	rowID := binary.BigEndian.Uint64(key[rowIDStart : rowIDStart+8])

	return tableID, indexID, value, rowID, nil
}

func (e *keyEncoder) EncodeValue(v catalogapi.Value) []byte {
	// Canonical NULL rule: IsNull == true means NULL.
	if v.IsNull {
		return []byte{tagNull}
	}

	switch v.Type {
	case catalogapi.TypeNull:
		return []byte{tagNull}

	case catalogapi.TypeInt:
		buf := make([]byte, 9)
		buf[0] = tagInt
		// XOR with sign bit to make signed comparison work as unsigned.
		binary.BigEndian.PutUint64(buf[1:], uint64(v.Int)^(1<<63))
		return buf

	case catalogapi.TypeFloat:
		buf := make([]byte, 9)
		buf[0] = tagFloat
		f := v.Float
		// Normalize: -0.0 → +0.0 (IEEE 754 -0.0 == +0.0 in SQL semantics)
		if f == 0 {
			f = 0
		}
		// Normalize: all NaN → canonical positive NaN
		// This ensures consistent encoding regardless of NaN payload/sign.
		if math.IsNaN(f) {
			f = math.NaN() // canonical: 0x7FF8000000000001
		}
		bits := math.Float64bits(f)
		if math.Signbit(f) {
			// Negative (including -Inf): flip all bits.
			bits = ^bits
		} else {
			// Positive, +0.0, +Inf, NaN: flip sign bit only.
			bits ^= 1 << 63
		}
		binary.BigEndian.PutUint64(buf[1:], bits)
		return buf

	case catalogapi.TypeText:
		return encodeBytes(tagText, []byte(v.Text))

	case catalogapi.TypeBlob:
		return encodeBytes(tagBlob, v.Blob)

	default:
		return []byte{tagNull}
	}
}

func (e *keyEncoder) DecodeValue(data []byte) (catalogapi.Value, int, error) {
	if len(data) == 0 {
		return catalogapi.Value{}, 0, fmt.Errorf("%w: empty value data", api.ErrInvalidKey)
	}

	tag := data[0]
	switch tag {
	case tagNull:
		return catalogapi.Value{IsNull: true, Type: catalogapi.TypeNull}, 1, nil

	case tagInt:
		if len(data) < 9 {
			return catalogapi.Value{}, 0, fmt.Errorf("%w: int value too short", api.ErrInvalidKey)
		}
		u := binary.BigEndian.Uint64(data[1:9])
		v := int64(u ^ (1 << 63))
		return catalogapi.Value{Type: catalogapi.TypeInt, Int: v}, 9, nil

	case tagFloat:
		if len(data) < 9 {
			return catalogapi.Value{}, 0, fmt.Errorf("%w: float value too short", api.ErrInvalidKey)
		}
		bits := binary.BigEndian.Uint64(data[1:9])
		// Reverse the transform: check if sign bit is set in encoded form.
		if bits&(1<<63) != 0 {
			// Was positive or zero: flip sign bit back.
			bits ^= 1 << 63
		} else {
			// Was negative or NaN: flip all bits back.
			bits = ^bits
		}
		f := math.Float64frombits(bits)
		return catalogapi.Value{Type: catalogapi.TypeFloat, Float: f}, 9, nil

	case tagText:
		raw, consumed, err := decodeBytes(data[1:])
		if err != nil {
			return catalogapi.Value{}, 0, err
		}
		return catalogapi.Value{Type: catalogapi.TypeText, Text: string(raw)}, 1 + consumed, nil

	case tagBlob:
		raw, consumed, err := decodeBytes(data[1:])
		if err != nil {
			return catalogapi.Value{}, 0, err
		}
		return catalogapi.Value{Type: catalogapi.TypeBlob, Blob: raw}, 1 + consumed, nil

	default:
		return catalogapi.Value{}, 0, fmt.Errorf("%w: unknown value type tag 0x%02x", api.ErrInvalidKey, tag)
	}
}

func (e *keyEncoder) EncodeRowPrefix(tableID uint32) []byte {
	buf := make([]byte, 6)
	buf[0] = prefixTable
	binary.BigEndian.PutUint32(buf[1:5], tableID)
	buf[5] = tagRow
	return buf
}

func (e *keyEncoder) EncodeRowPrefixEnd(tableID uint32) []byte {
	buf := make([]byte, 6)
	buf[0] = prefixTable
	binary.BigEndian.PutUint32(buf[1:5], tableID)
	buf[5] = tagRow + 1 // 'r' → 's'
	return buf
}

func (e *keyEncoder) EncodeIndexPrefix(tableID uint32, indexID uint32) []byte {
	buf := make([]byte, 10)
	buf[0] = prefixTable
	binary.BigEndian.PutUint32(buf[1:5], tableID)
	buf[5] = tagIndex
	binary.BigEndian.PutUint32(buf[6:10], indexID)
	return buf
}

func (e *keyEncoder) EncodeIndexPrefixEnd(tableID uint32, indexID uint32) []byte {
	buf := make([]byte, 10)
	buf[0] = prefixTable
	binary.BigEndian.PutUint32(buf[1:5], tableID)
	buf[5] = tagIndex
	// Increment indexID by 1 for exclusive end.
	binary.BigEndian.PutUint32(buf[6:10], indexID+1)
	return buf
}

// ─── Byte Escape Helpers ────────────────────────────────────────────

// encodeBytes encodes variable-length data with escape scheme:
//
//	0x00 → 0x00 0xFF
//	terminated by 0x00 0x00
func encodeBytes(tag byte, data []byte) []byte {
	// Worst case: every byte is 0x00 → doubles, plus tag + terminator.
	buf := make([]byte, 0, 1+len(data)*2+2)
	buf = append(buf, tag)
	for _, b := range data {
		if b == 0x00 {
			buf = append(buf, 0x00, 0xFF)
		} else {
			buf = append(buf, b)
		}
	}
	buf = append(buf, 0x00, 0x00) // terminator
	return buf
}

// decodeBytes decodes escaped bytes, returning raw data and bytes consumed.
func decodeBytes(data []byte) ([]byte, int, error) {
	var result []byte
	i := 0
	for i < len(data) {
		if data[i] == 0x00 {
			if i+1 >= len(data) {
				return nil, 0, fmt.Errorf("%w: truncated escape sequence", api.ErrInvalidKey)
			}
			if data[i+1] == 0x00 {
				// Terminator found.
				return result, i + 2, nil
			}
			if data[i+1] == 0xFF {
				// Escaped null byte.
				result = append(result, 0x00)
				i += 2
				continue
			}
			return nil, 0, fmt.Errorf("%w: invalid escape byte 0x%02x", api.ErrInvalidKey, data[i+1])
		}
		result = append(result, data[i])
		i++
	}
	return nil, 0, fmt.Errorf("%w: unterminated byte sequence", api.ErrInvalidKey)
}

// ─── RowCodec ───────────────────────────────────────────────────────

type rowCodec struct{}

// NewRowCodec creates a new RowCodec.
func NewRowCodec() api.RowCodec { return &rowCodec{} }

func (c *rowCodec) EncodeRow(values []catalogapi.Value) []byte {
	count := len(values)
	bitmapLen := (count + 7) / 8

	// Header: 2 bytes count + bitmap.
	buf := make([]byte, 2+bitmapLen)
	binary.BigEndian.PutUint16(buf[0:2], uint16(count))

	// Build null bitmap and estimate data size.
	for i, v := range values {
		if v.IsNull {
			byteIdx := i / 8
			bitIdx := uint(i % 8)
			buf[2+byteIdx] |= 1 << bitIdx
		}
	}

	// Encode non-null column values.
	for _, v := range values {
		if v.IsNull {
			continue
		}
		switch v.Type {
		case catalogapi.TypeInt:
			tmp := make([]byte, 8)
			binary.BigEndian.PutUint64(tmp, uint64(v.Int))
			buf = append(buf, tmp...)

		case catalogapi.TypeFloat:
			tmp := make([]byte, 8)
			binary.BigEndian.PutUint64(tmp, math.Float64bits(v.Float))
			buf = append(buf, tmp...)

		case catalogapi.TypeText:
			tmp := make([]byte, 4)
			binary.BigEndian.PutUint32(tmp, uint32(len(v.Text)))
			buf = append(buf, tmp...)
			buf = append(buf, []byte(v.Text)...)

		case catalogapi.TypeBlob:
			tmp := make([]byte, 4)
			binary.BigEndian.PutUint32(tmp, uint32(len(v.Blob)))
			buf = append(buf, tmp...)
			buf = append(buf, v.Blob...)

		case catalogapi.TypeNull:
			// Should not happen (covered by IsNull), but skip.
		}
	}

	return buf
}

func (c *rowCodec) DecodeRow(data []byte, columns []catalogapi.ColumnDef) ([]catalogapi.Value, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("%w: data too short for header", api.ErrInvalidRow)
	}

	count := int(binary.BigEndian.Uint16(data[0:2]))
	if count != len(columns) {
		return nil, fmt.Errorf("%w: column count mismatch: encoded %d, schema %d", api.ErrInvalidRow, count, len(columns))
	}

	bitmapLen := (count + 7) / 8
	if len(data) < 2+bitmapLen {
		return nil, fmt.Errorf("%w: data too short for null bitmap", api.ErrInvalidRow)
	}

	bitmap := data[2 : 2+bitmapLen]
	offset := 2 + bitmapLen

	values := make([]catalogapi.Value, count)
	for i := 0; i < count; i++ {
		byteIdx := i / 8
		bitIdx := uint(i % 8)
		if bitmap[byteIdx]&(1<<bitIdx) != 0 {
			// NULL value.
			values[i] = catalogapi.Value{Type: columns[i].Type, IsNull: true}
			continue
		}

		switch columns[i].Type {
		case catalogapi.TypeInt:
			if offset+8 > len(data) {
				return nil, fmt.Errorf("%w: truncated int at column %d", api.ErrInvalidRow, i)
			}
			values[i] = catalogapi.Value{
				Type: catalogapi.TypeInt,
				Int:  int64(binary.BigEndian.Uint64(data[offset : offset+8])),
			}
			offset += 8

		case catalogapi.TypeFloat:
			if offset+8 > len(data) {
				return nil, fmt.Errorf("%w: truncated float at column %d", api.ErrInvalidRow, i)
			}
			bits := binary.BigEndian.Uint64(data[offset : offset+8])
			values[i] = catalogapi.Value{
				Type:  catalogapi.TypeFloat,
				Float: math.Float64frombits(bits),
			}
			offset += 8

		case catalogapi.TypeText:
			if offset+4 > len(data) {
				return nil, fmt.Errorf("%w: truncated text length at column %d", api.ErrInvalidRow, i)
			}
			strLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
			offset += 4
			if offset+strLen > len(data) {
				return nil, fmt.Errorf("%w: truncated text data at column %d", api.ErrInvalidRow, i)
			}
			values[i] = catalogapi.Value{
				Type: catalogapi.TypeText,
				Text: string(data[offset : offset+strLen]),
			}
			offset += strLen

		case catalogapi.TypeBlob:
			if offset+4 > len(data) {
				return nil, fmt.Errorf("%w: truncated blob length at column %d", api.ErrInvalidRow, i)
			}
			blobLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
			offset += 4
			if offset+blobLen > len(data) {
				return nil, fmt.Errorf("%w: truncated blob data at column %d", api.ErrInvalidRow, i)
			}
			blobCopy := make([]byte, blobLen)
			copy(blobCopy, data[offset:offset+blobLen])
			values[i] = catalogapi.Value{
				Type: catalogapi.TypeBlob,
				Blob: blobCopy,
			}
			offset += blobLen

		case catalogapi.TypeNull:
			values[i] = catalogapi.Value{Type: catalogapi.TypeNull, IsNull: true}
		}
	}

	return values, nil
}

// ─── CompareValues ──────────────────────────────────────────────────

// CompareValues compares two Values with SQL NULL semantics.
//
//	NULL == NULL → 0
//	NULL < non-NULL → -1
//	non-NULL > NULL → +1
//	Same-type: standard comparison
//	Cross-type: ErrTypeMismatch
func CompareValues(a, b catalogapi.Value) (int, error) {
	aNull := a.IsNull || a.Type == catalogapi.TypeNull
	bNull := b.IsNull || b.Type == catalogapi.TypeNull

	if aNull && bNull {
		return 0, nil
	}
	if aNull {
		return -1, nil
	}
	if bNull {
		return 1, nil
	}

	if a.Type != b.Type {
		return 0, fmt.Errorf("%w: cannot compare %d with %d", api.ErrTypeMismatch, a.Type, b.Type)
	}

	switch a.Type {
	case catalogapi.TypeInt:
		switch {
		case a.Int < b.Int:
			return -1, nil
		case a.Int > b.Int:
			return 1, nil
		default:
			return 0, nil
		}

	case catalogapi.TypeFloat:
		switch {
		case a.Float < b.Float:
			return -1, nil
		case a.Float > b.Float:
			return 1, nil
		case a.Float == b.Float:
			return 0, nil
		default:
			// NaN cases: NaN is not equal to anything including itself.
			// For ordering: NaN sorts AFTER all non-NaN values (consistent
			// with EncodeValue where NaN encodes to 0xFFF8... which is the
			// highest encoded float value).
			aNaN := math.IsNaN(a.Float)
			bNaN := math.IsNaN(b.Float)
			if aNaN && bNaN {
				return 0, nil
			}
			if aNaN {
				return 1, nil // NaN sorts after non-NaN
			}
			return -1, nil
		}

	case catalogapi.TypeText:
		switch {
		case a.Text < b.Text:
			return -1, nil
		case a.Text > b.Text:
			return 1, nil
		default:
			return 0, nil
		}

	case catalogapi.TypeBlob:
		return bytes.Compare(a.Blob, b.Blob), nil

	default:
		return 0, fmt.Errorf("%w: unknown type %d", api.ErrTypeMismatch, a.Type)
	}
}
