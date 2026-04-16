// Package engine provides table and index CRUD operations mapped to KV storage.
package engine

import (
	"fmt"

	txnapi "github.com/akzj/go-fast-kv/internal/txn/api"
)

// Tx represents an active SQL transaction with BEGIN/COMMIT/ROLLBACK semantics.
//
// Tx wraps a TxnContext and provides:
//   - Execute(plan): Execute a planned statement within the transaction
//   - Commit(): Commit all changes made during the transaction
//   - Rollback(): Abort the transaction, discarding all changes
//
// Usage:
//
//	tx, _ := engine.BeginTx(txnManager, executor)
//	tx.Execute(plan)
//	tx.Commit()
type Tx struct {
	txnCtx    txnapi.TxnContext
	committed bool
}

// BeginTx starts a new SQL transaction using the provided TxnContext factory.
func BeginTx(txnMgr txnapi.TxnContextFactory) (*Tx, error) {
	txnCtx := txnMgr.BeginTxnContext()
	if txnCtx == nil {
		return nil, fmt.Errorf("engine: failed to begin transaction")
	}

	return &Tx{
		txnCtx:    txnCtx,
		committed: false,
	}, nil
}

// Commit commits all changes made during the transaction.
// After Commit, the transaction is no longer active.
func (tx *Tx) Commit() error {
	if tx.committed {
		return fmt.Errorf("engine: transaction already committed")
	}
	if !tx.txnCtx.IsActive() {
		return fmt.Errorf("engine: transaction not active")
	}

	tx.committed = true
	return tx.txnCtx.Commit()
}

// Rollback aborts the transaction, discarding all changes.
// After Rollback, the transaction is no longer active.
func (tx *Tx) Rollback() {
	if tx.committed {
		return
	}
	tx.txnCtx.Rollback()
	tx.committed = true
}

// XID returns the transaction's ID.
func (tx *Tx) XID() uint64 {
	return tx.txnCtx.XID()
}

// Snapshot returns the transaction's MVCC snapshot.
func (tx *Tx) Snapshot() *txnapi.Snapshot {
	return tx.txnCtx.Snapshot()
}

// LockManager returns the row lock manager for the transaction.
func (tx *Tx) LockManager() txnapi.LockManager {
	return tx.txnCtx.LockManager()
}

// TxnContext returns the underlying TxnContext.
func (tx *Tx) TxnContext() txnapi.TxnContext {
	return tx.txnCtx
}
