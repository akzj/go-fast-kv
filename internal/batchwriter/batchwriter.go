// Package batchwriter provides buffered batch writing with event-driven flushing.
//
// Usage:
//
//	import "github.com/akzj/go-fast-kv/internal/batchwriter"
//
// This package re-exports all public interfaces from the api package.
package batchwriter

import batchwriterapi "github.com/akzj/go-fast-kv/internal/batchwriter/api"

// Re-export all types.
type (
	WriteRequest = batchwriterapi.WriteRequest
	BatchWriter  = batchwriterapi.BatchWriter
)

// Re-export errors.
var (
	ErrClosed     = batchwriterapi.ErrClosed
	ErrNoWriteFunc = batchwriterapi.ErrNoWriteFunc
)

// Re-export functions.
var (
	New = batchwriterapi.New
)
