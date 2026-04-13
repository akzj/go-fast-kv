package internal

import (
	"bytes"
	"math"
	"testing"

	catalogapi "github.com/akzj/go-fast-kv/internal/sql/catalog/api"
)

// ─── Row Key Tests ──────────────────────────────────────────────────

func TestEncodeDecodeRowKey(t *testing.T) {
	enc := NewKeyEncoder()

	tests := []struct {
		name    string
		tableID uint32
		rowID   uint64
	}{
		{"zero", 0, 0},
		{"simple", 1, 42},
		{"max_table", 0xFFFFFFFF, 1},
		{"max_row", 1, 0xFFFFFFFFFFFFFFFF},
		{"both_max", 0xFFFFFFFF, 0xFFFFFFFFFFFFFFFF},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := enc.EncodeRowKey(tt.tableID, tt.rowID)
			if len(key) != 14 {
				t.Fatalf("expected 14 bytes, got %d", len(key))
			}

			gotTable, gotRow, err := enc.DecodeRowKey(key)
			if err != nil {
				t.Fatalf("decode error: %v", err)
			}
			if gotTable != tt.tableID || gotRow != tt.rowID {
				t.Errorf("roundtrip failed: got (%d, %d), want (%d, %d)",
					gotTable, gotRow, tt.tableID, tt.rowID)
			}
		})
	}

	// Invalid keys.
	t.Run("invalid_short", func(t *testing.T) {
		_, _, err := enc.DecodeRowKey([]byte{0x74, 0x00})
		if err == nil {
			t.Error("expected error for short key")
		}
	})

	t.Run("invalid_prefix", func(t *testing.T) {
		key := make([]byte, 14)
		key[0] = 'x' // wrong prefix
		_, _, err := enc.DecodeRowKey(key)
		if err == nil {
			t.Error("expected error for wrong prefix")
		}
	})
}

// ─── Index Key Tests ────────────────────────────────────────────────

func TestEncodeDecodeIndexKey(t *testing.T) {
	enc := NewKeyEncoder()

	tests := []struct {
		name    string
		tableID uint32
		indexID uint32
		value   catalogapi.Value
		rowID   uint64
	}{
		{
			"null_value",
			1, 1,
			catalogapi.Value{IsNull: true},
			100,
		},
		{
			"int_positive",
			1, 2,
			catalogapi.Value{Type: catalogapi.TypeInt, Int: 42},
			200,
		},
		{
			"int_negative",
			1, 2,
			catalogapi.Value{Type: catalogapi.TypeInt, Int: -100},
			201,
		},
		{
			"int_zero",
			1, 2,
			catalogapi.Value{Type: catalogapi.TypeInt, Int: 0},
			202,
		},
		{
			"int_min",
			1, 2,
			catalogapi.Value{Type: catalogapi.TypeInt, Int: math.MinInt64},
			203,
		},
		{
			"int_max",
			1, 2,
			catalogapi.Value{Type: catalogapi.TypeInt, Int: math.MaxInt64},
			204,
		},
		{
			"float_positive",
			2, 3,
			catalogapi.Value{Type: catalogapi.TypeFloat, Float: 3.14},
			300,
		},
		{
			"float_negative",
			2, 3,
			catalogapi.Value{Type: catalogapi.TypeFloat, Float: -2.71},
			301,
		},
		{
			"float_zero",
			2, 3,
			catalogapi.Value{Type: catalogapi.TypeFloat, Float: 0.0},
			302,
		},
		{
			"text_simple",
			3, 4,
			catalogapi.Value{Type: catalogapi.TypeText, Text: "hello"},
			400,
		},
		{
			"text_empty",
			3, 4,
			catalogapi.Value{Type: catalogapi.TypeText, Text: ""},
			401,
		},
		{
			"text_with_null_bytes",
			3, 4,
			catalogapi.Value{Type: catalogapi.TypeText, Text: "a\x00b\x00c"},
			402,
		},
		{
			"blob_simple",
			4, 5,
			catalogapi.Value{Type: catalogapi.TypeBlob, Blob: []byte{0x01, 0x02, 0x03}},
			500,
		},
		{
			"blob_with_nulls",
			4, 5,
			catalogapi.Value{Type: catalogapi.TypeBlob, Blob: []byte{0x00, 0x00, 0xFF, 0x00}},
			501,
		},
		{
			"blob_empty",
			4, 5,
			catalogapi.Value{Type: catalogapi.TypeBlob, Blob: []byte{}},
			502,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := enc.EncodeIndexKey(tt.tableID, tt.indexID, tt.value, tt.rowID)

			gotTable, gotIndex, gotValue, gotRow, err := enc.DecodeIndexKey(key)
			if err != nil {
				t.Fatalf("decode error: %v", err)
			}
			if gotTable != tt.tableID {
				t.Errorf("tableID: got %d, want %d", gotTable, tt.tableID)
			}
			if gotIndex != tt.indexID {
				t.Errorf("indexID: got %d, want %d", gotIndex, tt.indexID)
			}
			if gotRow != tt.rowID {
				t.Errorf("rowID: got %d, want %d", gotRow, tt.rowID)
			}

			// Compare values.
			cmp, err := CompareValues(gotValue, tt.value)
			if err != nil {
				t.Fatalf("compare error: %v", err)
			}
			if cmp != 0 {
				t.Errorf("value mismatch: got %+v, want %+v", gotValue, tt.value)
			}
		})
	}
}

// ─── Value Encoding Order Tests ─────────────────────────────────────

func TestValueEncoding_IntOrder(t *testing.T) {
	enc := NewKeyEncoder()

	values := []int64{
		math.MinInt64,
		-1000,
		-1,
		0,
		1,
		1000,
		math.MaxInt64,
	}

	var prev []byte
	for i, v := range values {
		encoded := enc.EncodeValue(catalogapi.Value{Type: catalogapi.TypeInt, Int: v})
		if prev != nil && bytes.Compare(prev, encoded) >= 0 {
			t.Errorf("order violation at index %d: encoded(%d) >= encoded(%d)", i, values[i-1], v)
		}
		prev = encoded
	}
}

func TestValueEncoding_FloatOrder(t *testing.T) {
	enc := NewKeyEncoder()

	values := []float64{
		math.Inf(-1),
		-1000.0,
		-1.0,
		-math.SmallestNonzeroFloat64,
		0.0,
		math.SmallestNonzeroFloat64,
		1.0,
		1000.0,
		math.Inf(1),
	}

	var prev []byte
	for i, v := range values {
		encoded := enc.EncodeValue(catalogapi.Value{Type: catalogapi.TypeFloat, Float: v})
		if prev != nil && bytes.Compare(prev, encoded) >= 0 {
			t.Errorf("order violation at index %d: encoded(%v) >= encoded(%v)", i, values[i-1], v)
		}
		prev = encoded
	}
}

func TestValueEncoding_TextOrder(t *testing.T) {
	enc := NewKeyEncoder()

	values := []string{
		"",
		"\x00",
		"\x00\x00",
		"\x00\x01",
		"\x01",
		"A",
		"B",
		"a",
		"aa",
		"ab",
		"b",
		"hello",
		"world",
	}

	var prev []byte
	for i, v := range values {
		encoded := enc.EncodeValue(catalogapi.Value{Type: catalogapi.TypeText, Text: v})
		if prev != nil && bytes.Compare(prev, encoded) >= 0 {
			t.Errorf("order violation at index %d: encoded(%q) >= encoded(%q)", i, values[i-1], v)
		}
		prev = encoded
	}
}

func TestValueEncoding_BlobOrder(t *testing.T) {
	enc := NewKeyEncoder()

	values := [][]byte{
		{},
		{0x00},
		{0x00, 0x00},
		{0x00, 0x01},
		{0x01},
		{0x01, 0x00},
		{0xFF},
		{0xFF, 0xFF},
	}

	var prev []byte
	for i, v := range values {
		encoded := enc.EncodeValue(catalogapi.Value{Type: catalogapi.TypeBlob, Blob: v})
		if prev != nil && bytes.Compare(prev, encoded) >= 0 {
			t.Errorf("order violation at index %d: encoded(%x) >= encoded(%x)", i, values[i-1], v)
		}
		prev = encoded
	}
}

func TestValueEncoding_NullFirst(t *testing.T) {
	enc := NewKeyEncoder()

	nullEnc := enc.EncodeValue(catalogapi.Value{IsNull: true})
	intEnc := enc.EncodeValue(catalogapi.Value{Type: catalogapi.TypeInt, Int: math.MinInt64})
	floatEnc := enc.EncodeValue(catalogapi.Value{Type: catalogapi.TypeFloat, Float: math.Inf(-1)})
	textEnc := enc.EncodeValue(catalogapi.Value{Type: catalogapi.TypeText, Text: ""})
	blobEnc := enc.EncodeValue(catalogapi.Value{Type: catalogapi.TypeBlob, Blob: []byte{}})

	if bytes.Compare(nullEnc, intEnc) >= 0 {
		t.Error("NULL should sort before Int")
	}
	if bytes.Compare(nullEnc, floatEnc) >= 0 {
		t.Error("NULL should sort before Float")
	}
	if bytes.Compare(nullEnc, textEnc) >= 0 {
		t.Error("NULL should sort before Text")
	}
	if bytes.Compare(nullEnc, blobEnc) >= 0 {
		t.Error("NULL should sort before Blob")
	}
}

func TestValueEncoding_CrossType(t *testing.T) {
	enc := NewKeyEncoder()

	// NULL < Int < Float < Text < Blob (by type tag).
	nullEnc := enc.EncodeValue(catalogapi.Value{IsNull: true})
	intEnc := enc.EncodeValue(catalogapi.Value{Type: catalogapi.TypeInt, Int: 0})
	floatEnc := enc.EncodeValue(catalogapi.Value{Type: catalogapi.TypeFloat, Float: 0})
	textEnc := enc.EncodeValue(catalogapi.Value{Type: catalogapi.TypeText, Text: ""})
	blobEnc := enc.EncodeValue(catalogapi.Value{Type: catalogapi.TypeBlob, Blob: []byte{}})

	ordered := [][]byte{nullEnc, intEnc, floatEnc, textEnc, blobEnc}
	names := []string{"NULL", "Int", "Float", "Text", "Blob"}

	for i := 1; i < len(ordered); i++ {
		if bytes.Compare(ordered[i-1], ordered[i]) >= 0 {
			t.Errorf("%s should sort before %s", names[i-1], names[i])
		}
	}
}

// ─── Row Codec Tests ────────────────────────────────────────────────

func TestRowCodec_Roundtrip(t *testing.T) {
	codec := NewRowCodec()

	columns := []catalogapi.ColumnDef{
		{Name: "id", Type: catalogapi.TypeInt},
		{Name: "name", Type: catalogapi.TypeText},
		{Name: "score", Type: catalogapi.TypeFloat},
		{Name: "data", Type: catalogapi.TypeBlob},
		{Name: "nullable", Type: catalogapi.TypeInt},
	}

	values := []catalogapi.Value{
		{Type: catalogapi.TypeInt, Int: 42},
		{Type: catalogapi.TypeText, Text: "hello world"},
		{Type: catalogapi.TypeFloat, Float: 3.14},
		{Type: catalogapi.TypeBlob, Blob: []byte{0x01, 0x02, 0x03}},
		{Type: catalogapi.TypeInt, IsNull: true},
	}

	data := codec.EncodeRow(values)
	decoded, err := codec.DecodeRow(data, columns)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if len(decoded) != len(values) {
		t.Fatalf("expected %d values, got %d", len(values), len(decoded))
	}

	// Check each value.
	if decoded[0].Int != 42 {
		t.Errorf("column 0: got %d, want 42", decoded[0].Int)
	}
	if decoded[1].Text != "hello world" {
		t.Errorf("column 1: got %q, want %q", decoded[1].Text, "hello world")
	}
	if decoded[2].Float != 3.14 {
		t.Errorf("column 2: got %f, want 3.14", decoded[2].Float)
	}
	if !bytes.Equal(decoded[3].Blob, []byte{0x01, 0x02, 0x03}) {
		t.Errorf("column 3: got %x, want 010203", decoded[3].Blob)
	}
	if !decoded[4].IsNull {
		t.Error("column 4: expected NULL")
	}
}

func TestRowCodec_AllNull(t *testing.T) {
	codec := NewRowCodec()

	columns := []catalogapi.ColumnDef{
		{Name: "a", Type: catalogapi.TypeInt},
		{Name: "b", Type: catalogapi.TypeText},
		{Name: "c", Type: catalogapi.TypeFloat},
		{Name: "d", Type: catalogapi.TypeBlob},
	}

	values := []catalogapi.Value{
		{Type: catalogapi.TypeInt, IsNull: true},
		{Type: catalogapi.TypeText, IsNull: true},
		{Type: catalogapi.TypeFloat, IsNull: true},
		{Type: catalogapi.TypeBlob, IsNull: true},
	}

	data := codec.EncodeRow(values)
	decoded, err := codec.DecodeRow(data, columns)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	for i, v := range decoded {
		if !v.IsNull {
			t.Errorf("column %d: expected NULL, got %+v", i, v)
		}
	}
}

func TestRowCodec_EmptyStringsAndBlobs(t *testing.T) {
	codec := NewRowCodec()

	columns := []catalogapi.ColumnDef{
		{Name: "text", Type: catalogapi.TypeText},
		{Name: "blob", Type: catalogapi.TypeBlob},
	}

	values := []catalogapi.Value{
		{Type: catalogapi.TypeText, Text: ""},
		{Type: catalogapi.TypeBlob, Blob: []byte{}},
	}

	data := codec.EncodeRow(values)
	decoded, err := codec.DecodeRow(data, columns)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if decoded[0].Text != "" {
		t.Errorf("expected empty text, got %q", decoded[0].Text)
	}
	if len(decoded[1].Blob) != 0 {
		t.Errorf("expected empty blob, got %x", decoded[1].Blob)
	}
}

// ─── CompareValues Tests ────────────────────────────────────────────

func TestCompareValues(t *testing.T) {
	tests := []struct {
		name    string
		a, b    catalogapi.Value
		want    int
		wantErr bool
	}{
		// NULL semantics.
		{"null_null", val(true, 0, 0, "", nil), val(true, 0, 0, "", nil), 0, false},
		{"null_lt_int", val(true, 0, 0, "", nil), catalogapi.Value{Type: catalogapi.TypeInt, Int: 0}, -1, false},
		{"int_gt_null", catalogapi.Value{Type: catalogapi.TypeInt, Int: 0}, val(true, 0, 0, "", nil), 1, false},

		// Int comparisons.
		{"int_eq", catalogapi.Value{Type: catalogapi.TypeInt, Int: 42}, catalogapi.Value{Type: catalogapi.TypeInt, Int: 42}, 0, false},
		{"int_lt", catalogapi.Value{Type: catalogapi.TypeInt, Int: -1}, catalogapi.Value{Type: catalogapi.TypeInt, Int: 1}, -1, false},
		{"int_gt", catalogapi.Value{Type: catalogapi.TypeInt, Int: 100}, catalogapi.Value{Type: catalogapi.TypeInt, Int: 1}, 1, false},

		// Float comparisons.
		{"float_eq", catalogapi.Value{Type: catalogapi.TypeFloat, Float: 3.14}, catalogapi.Value{Type: catalogapi.TypeFloat, Float: 3.14}, 0, false},
		{"float_lt", catalogapi.Value{Type: catalogapi.TypeFloat, Float: -1.0}, catalogapi.Value{Type: catalogapi.TypeFloat, Float: 1.0}, -1, false},
		{"float_gt", catalogapi.Value{Type: catalogapi.TypeFloat, Float: 100.0}, catalogapi.Value{Type: catalogapi.TypeFloat, Float: 1.0}, 1, false},
		{"float_nan_nan", catalogapi.Value{Type: catalogapi.TypeFloat, Float: math.NaN()}, catalogapi.Value{Type: catalogapi.TypeFloat, Float: math.NaN()}, 0, false},
		{"float_nan_gt_num", catalogapi.Value{Type: catalogapi.TypeFloat, Float: math.NaN()}, catalogapi.Value{Type: catalogapi.TypeFloat, Float: 0}, 1, false},

		// Text comparisons.
		{"text_eq", catalogapi.Value{Type: catalogapi.TypeText, Text: "abc"}, catalogapi.Value{Type: catalogapi.TypeText, Text: "abc"}, 0, false},
		{"text_lt", catalogapi.Value{Type: catalogapi.TypeText, Text: "abc"}, catalogapi.Value{Type: catalogapi.TypeText, Text: "abd"}, -1, false},
		{"text_gt", catalogapi.Value{Type: catalogapi.TypeText, Text: "abd"}, catalogapi.Value{Type: catalogapi.TypeText, Text: "abc"}, 1, false},

		// Blob comparisons.
		{"blob_eq", catalogapi.Value{Type: catalogapi.TypeBlob, Blob: []byte{1, 2}}, catalogapi.Value{Type: catalogapi.TypeBlob, Blob: []byte{1, 2}}, 0, false},
		{"blob_lt", catalogapi.Value{Type: catalogapi.TypeBlob, Blob: []byte{1}}, catalogapi.Value{Type: catalogapi.TypeBlob, Blob: []byte{2}}, -1, false},

		// Cross-type error.
		{"cross_type", catalogapi.Value{Type: catalogapi.TypeInt, Int: 1}, catalogapi.Value{Type: catalogapi.TypeText, Text: "1"}, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CompareValues(tt.a, tt.b)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("CompareValues(%+v, %+v) = %d, want %d", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

// val creates a Value with IsNull flag.
func val(isNull bool, i int64, f float64, text string, blob []byte) catalogapi.Value {
	return catalogapi.Value{IsNull: isNull, Int: i, Float: f, Text: text, Blob: blob}
}

// ─── Prefix Tests ───────────────────────────────────────────────────

func TestPrefixEnd(t *testing.T) {
	enc := NewKeyEncoder()

	t.Run("row_prefix_end", func(t *testing.T) {
		prefix := enc.EncodeRowPrefix(1)
		prefixEnd := enc.EncodeRowPrefixEnd(1)

		if len(prefix) != 6 || len(prefixEnd) != 6 {
			t.Fatalf("prefix lengths: %d, %d", len(prefix), len(prefixEnd))
		}

		// prefixEnd should be > prefix.
		if bytes.Compare(prefix, prefixEnd) >= 0 {
			t.Error("prefixEnd should be > prefix")
		}

		// A row key should be >= prefix and < prefixEnd.
		rowKey := enc.EncodeRowKey(1, 42)
		if bytes.Compare(rowKey, prefix) < 0 {
			t.Error("row key should be >= prefix")
		}
		if bytes.Compare(rowKey, prefixEnd) >= 0 {
			t.Error("row key should be < prefixEnd")
		}
	})

	t.Run("index_prefix_end", func(t *testing.T) {
		prefix := enc.EncodeIndexPrefix(1, 1)
		prefixEnd := enc.EncodeIndexPrefixEnd(1, 1)

		if bytes.Compare(prefix, prefixEnd) >= 0 {
			t.Error("prefixEnd should be > prefix")
		}

		// An index key should be >= prefix and < prefixEnd.
		idxKey := enc.EncodeIndexKey(1, 1, catalogapi.Value{Type: catalogapi.TypeInt, Int: 42}, 100)
		if bytes.Compare(idxKey, prefix) < 0 {
			t.Error("index key should be >= prefix")
		}
		if bytes.Compare(idxKey, prefixEnd) >= 0 {
			t.Error("index key should be < prefixEnd")
		}
	})

	t.Run("different_tables_dont_overlap", func(t *testing.T) {
		prefixEnd1 := enc.EncodeRowPrefixEnd(1)
		prefix2 := enc.EncodeRowPrefix(2)

		// Table 1's end should be <= table 2's start.
		if bytes.Compare(prefixEnd1, prefix2) > 0 {
			t.Error("table 1 row prefix end should be <= table 2 row prefix start")
		}
	})
}

// ─── Canonical NULL Rule Test ───────────────────────────────────────

func TestCanonicalNull(t *testing.T) {
	enc := NewKeyEncoder()

	// A value with IsNull=true but Type=TypeInt should encode as NULL.
	v := catalogapi.Value{Type: catalogapi.TypeInt, Int: 42, IsNull: true}
	encoded := enc.EncodeValue(v)

	nullEncoded := enc.EncodeValue(catalogapi.Value{IsNull: true})
	if !bytes.Equal(encoded, nullEncoded) {
		t.Errorf("IsNull=true should encode as NULL regardless of Type: got %x, want %x", encoded, nullEncoded)
	}
}
