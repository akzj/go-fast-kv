// Package wal provides the Write-Ahead Log for crash recovery.
//
// Design reference: docs/DESIGN.md §3.2
package wal

import (
	walapi "github.com/akzj/go-fast-kv/internal/wal/api"
	"github.com/akzj/go-fast-kv/internal/wal/internal"
)

// New creates a new WAL instance.
func New(cfg walapi.Config) (walapi.WAL, error) {
	return internal.New(cfg)
}
