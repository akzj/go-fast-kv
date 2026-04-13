// Package encoding provides key encoding, row codec, and value comparison
// for the SQL layer.
package encoding

import (
	catalogapi "github.com/akzj/go-fast-kv/internal/sql/catalog/api"

	"github.com/akzj/go-fast-kv/internal/sql/encoding/api"
	"github.com/akzj/go-fast-kv/internal/sql/encoding/internal"
)

// Re-export types for convenience.
type CompareOp = api.CompareOp

const (
	OpEQ = api.OpEQ
	OpNE = api.OpNE
	OpLT = api.OpLT
	OpLE = api.OpLE
	OpGT = api.OpGT
	OpGE = api.OpGE
)

// Re-export errors.
var (
	ErrInvalidKey  = api.ErrInvalidKey
	ErrInvalidRow  = api.ErrInvalidRow
	ErrTypeMismatch = api.ErrTypeMismatch
)

// NewKeyEncoder creates a new KeyEncoder.
func NewKeyEncoder() api.KeyEncoder {
	return internal.NewKeyEncoder()
}

// NewRowCodec creates a new RowCodec.
func NewRowCodec() api.RowCodec {
	return internal.NewRowCodec()
}

// CompareValues compares two Values with SQL NULL semantics.
// See api package for full documentation.
func CompareValues(a, b catalogapi.Value) (int, error) {
	return internal.CompareValues(a, b)
}
