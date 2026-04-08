// Package txn provides the MVCC transaction manager.
//
// Design reference: docs/DESIGN.md §3.9
package txn

import (
	txnapi "github.com/akzj/go-fast-kv/internal/txn/api"
	"github.com/akzj/go-fast-kv/internal/txn/internal"
)

// New creates a new TxnManager.
func New() txnapi.TxnManager {
	return internal.New()
}
