// Package value provides the value type system for SQL on go-fast-kv.
package value

import (
	"fmt"
	"strconv"
)

// Type represents a SQL data type.
type Type int

const (
	TypeNull Type = iota
	TypeInt
	TypeFloat
	TypeText
	TypeBlob
)

func (t Type) String() string {
	switch t {
	case TypeNull:
		return "NULL"
	case TypeInt:
		return "INT"
	case TypeFloat:
		return "FLOAT"
	case TypeText:
		return "TEXT"
	case TypeBlob:
		return "BLOB"
	default:
		return "UNKNOWN"
	}
}

// Value represents a typed SQL value.
type Value struct {
	Type  Type
	Int   int64
	Float float64
	Text  string
	Blob  []byte
}

// NewInt creates an INT value.
func NewInt(v int64) Value {
	return Value{Type: TypeInt, Int: v}
}

// NewFloat creates a FLOAT value.
func NewFloat(v float64) Value {
	return Value{Type: TypeFloat, Float: v}
}

// NewText creates a TEXT value.
func NewText(v string) Value {
	return Value{Type: TypeText, Text: v}
}

// NewBlob creates a BLOB value.
func NewBlob(v []byte) Value {
	return Value{Type: TypeBlob, Blob: v}
}

// AsInt converts the value to int64.
func (v Value) AsInt() int64 {
	switch v.Type {
	case TypeInt:
		return v.Int
	case TypeFloat:
		return int64(v.Float)
	case TypeText:
		i, _ := strconv.ParseInt(v.Text, 10, 64)
		return i
	default:
		return 0
	}
}

// AsFloat converts the value to float64.
func (v Value) AsFloat() float64 {
	switch v.Type {
	case TypeInt:
		return float64(v.Int)
	case TypeFloat:
		return v.Float
	case TypeText:
		f, _ := strconv.ParseFloat(v.Text, 64)
		return f
	default:
		return 0
	}
}

// AsText converts the value to string.
func (v Value) AsText() string {
	switch v.Type {
	case TypeInt:
		return strconv.FormatInt(v.Int, 10)
	case TypeFloat:
		return strconv.FormatFloat(v.Float, 'f', -1, 64)
	case TypeText:
		return v.Text
	default:
		return ""
	}
}

// AsBytes converts the value to []byte.
func (v Value) AsBytes() []byte {
	switch v.Type {
	case TypeBlob:
		return v.Blob
	default:
		return []byte(v.AsText())
	}
}

// Compare compares two values.
// Returns -1 if v < other, 0 if v == other, 1 if v > other.
func (v Value) Compare(other Value) int {
	// NULL compares less than everything
	if v.Type == TypeNull && other.Type == TypeNull {
		return 0
	}
	if v.Type == TypeNull {
		return -1
	}
	if other.Type == TypeNull {
		return 1
	}

	// Compare by type
	switch v.Type {
	case TypeInt:
		if other.Type == TypeInt {
			if v.Int < other.Int {
				return -1
			} else if v.Int > other.Int {
				return 1
			}
			return 0
		}
		// Compare as float
		vf, of := float64(v.Int), other.AsFloat()
		if vf < of {
			return -1
		} else if vf > of {
			return 1
		}
		return 0

	case TypeFloat:
		vf, of := v.Float, other.AsFloat()
		if vf < of {
			return -1
		} else if vf > of {
			return 1
		}
		return 0

	case TypeText:
		if other.Type == TypeText {
			if v.Text < other.Text {
				return -1
			} else if v.Text > other.Text {
				return 1
			}
			return 0
		}
		// Compare as text
		vt, ot := v.Text, other.AsText()
		if vt < ot {
			return -1
		} else if vt > ot {
			return 1
		}
		return 0

	case TypeBlob:
		if other.Type == TypeBlob {
			for i := 0; i < len(v.Blob) && i < len(other.Blob); i++ {
				if v.Blob[i] < other.Blob[i] {
					return -1
				} else if v.Blob[i] > other.Blob[i] {
					return 1
				}
			}
			if len(v.Blob) < len(other.Blob) {
				return -1
			} else if len(v.Blob) > len(other.Blob) {
				return 1
			}
			return 0
		}
		return 0

	default:
		return 0
	}
}

func (v Value) String() string {
	switch v.Type {
	case TypeNull:
		return "NULL"
	case TypeInt:
		return strconv.FormatInt(v.Int, 10)
	case TypeFloat:
		return strconv.FormatFloat(v.Float, 'f', -1, 64)
	case TypeText:
		return fmt.Sprintf("%q", v.Text)
	case TypeBlob:
		return fmt.Sprintf("BLOB(%d)", len(v.Blob))
	default:
		return "?"
	}
}

// ParseValue parses a string into a typed value based on the target type.
func ParseValue(s string, targetType Type) Value {
	switch targetType {
	case TypeInt:
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return Value{Type: TypeNull}
		}
		return NewInt(i)
	case TypeFloat:
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return Value{Type: TypeNull}
		}
		return NewFloat(f)
	case TypeText:
		return NewText(s)
	case TypeBlob:
		return NewBlob([]byte(s))
	default:
		return Value{Type: TypeNull}
	}
}
