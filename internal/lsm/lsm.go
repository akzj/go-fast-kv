// Package lsm provides the LSM-based mapping store for PageStore page→VAddr mappings.
package lsm

import (
	lsmapi "github.com/akzj/go-fast-kv/internal/lsm/api"
	"github.com/akzj/go-fast-kv/internal/lsm/internal"
)

// New creates a new LSM MappingStore backed by SSTables in the given directory.
func New(cfg lsmapi.Config) (lsmapi.MappingStore, error) {
	return internal.New(cfg)
}

// NewRecoveryStore creates a recovery store for replaying WAL entries into the LSM.
func NewRecoveryStore(dir string) (lsmapi.RecoveryStore, error) {
	return internal.NewRecoveryStore(dir)
}
