// Package batchwriter provides buffered batch writing with event-driven flushing.
//
// Usage:
//
//	import "github.com/akzj/go-fast-kv/internal/batchwriter"
//
// This package re-exports all public interfaces from the api package.
package batchwriter

import "github.com/akzj/go-fast-kv/internal/batchwriter/internal"

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

// Re-export New function.
var New = batchwriterapi.New

// Drain waits for pending writes to complete without closing the batchwriter.
var Drain = (*internal.BatchWriter).Drain
