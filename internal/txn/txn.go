// Package txn provides the MVCC transaction manager.
//
// Design reference: docs/DESIGN.md §3.9
package txn

import (
	api "github.com/akzj/go-fast-kv/internal/txn/api"
	"github.com/akzj/go-fast-kv/internal/txn/internal"
)

// TxnManager is the concrete transaction manager type.
// Exported so kvstore can call BeginSSITxn().
type TxnManager = internal.TxnManager

// New creates a new TxnManager.
func New() api.TxnManager {
	return internal.New()
}
