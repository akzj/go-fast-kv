// Package api provides the public interface for batchwriter.
// Implementation details are in the internal package.
package api

import "github.com/akzj/go-fast-kv/internal/batchwriter/internal"

// Type aliases for public API.
// These point to internal implementation types.
type (
	WriteRequest = internal.WriteRequest
	BatchWriter  = internal.BatchWriter
)

// Re-export errors.
var (
	ErrClosed     = internal.ErrClosed
	ErrNoWriteFunc = internal.ErrNoWriteFunc
)

// New creates a new BatchWriter with the given buffer size.
var New = internal.NewBatchWriter
