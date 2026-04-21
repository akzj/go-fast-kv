package internal

import (
	"fmt"
	"testing"

	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
	kvstore "github.com/akzj/go-fast-kv/internal/kvstore"
	txn "github.com/akzj/go-fast-kv/internal/txn"
	txnapi "github.com/akzj/go-fast-kv/internal/txn/api"
)

// ─── Parser Tests ─────────────────────────────────────────────────

func TestSavepointParser(t *testing.T) {
	env := newTestEnv(t)

	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		// SAVEPOINT
		{"simple savepoint", "SAVEPOINT sp1", false},
		{"savepoint with underscore", "SAVEPOINT my_savepoint", false},
		{"savepoint with numbers", "SAVEPOINT sp123", false},
		{"savepoint name too short", "SAVEPOINT", true},

		// ROLLBACK TO SAVEPOINT
		{"rollback to savepoint", "ROLLBACK TO SAVEPOINT sp1", false},
		{"rollback to savepoint via ident", "ROLLBACK TO SAVEPOINT my_sp", false},
		{"rollback to missing SAVEPOINT keyword", "ROLLBACK TO sp1", true}, // TO followed by ident, not SAVEPOINT
		{"rollback to savepoint 2", "ROLLBACK TO SAVEPOINT sp2", false},

		// RELEASE SAVEPOINT
		{"release savepoint", "RELEASE SAVEPOINT sp1", false},
		{"release savepoint via ident", "RELEASE SAVEPOINT my_sp", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := env.parser.Parse(tt.sql)
			if err != nil {
				if !tt.wantErr {
					t.Errorf("parse error: %v", err)
				}
				return
			}
			if tt.wantErr {
				t.Errorf("expected parse error, got nil")
				return
			}
			if stmt == nil {
				t.Errorf("expected statement, got nil")
			}
		})
	}
}

// inactiveTxnContext is a minimal implementation of txnapi.TxnContext
// for testing error paths on inactive transactions.
type inactiveTxnContext struct{}

func (i *inactiveTxnContext) XID() uint64                          { return 0 }
func (i *inactiveTxnContext) Snapshot() *txnapi.Snapshot          { return nil }
func (i *inactiveTxnContext) LockManager() txnapi.LockManager     { return nil }
func (i *inactiveTxnContext) AddLock(string, txnapi.LockMode) bool { return false }
func (i *inactiveTxnContext) Commit() error                        { return nil }
func (i *inactiveTxnContext) Rollback()                            {}
func (i *inactiveTxnContext) IsActive() bool                       { return false }
func (i *inactiveTxnContext) AddPendingWrite([]byte, []byte)    {}
func (i *inactiveTxnContext) GetPendingWrites() [][]byte          { return nil }
func (i *inactiveTxnContext) CreateSavepoint(string) error {
	return fmt.Errorf("txn: cannot create savepoint on inactive transaction")
}
func (i *inactiveTxnContext) RollbackToSavepoint(string, interface {
	DeleteWithXID(key []byte, xid uint64) error
	PutWithXID(key, value []byte, xid uint64) error
}) error {
	return fmt.Errorf("txn: cannot rollback to savepoint on inactive transaction")
}
func (i *inactiveTxnContext) ReleaseSavepoint(string) error {
	return fmt.Errorf("txn: cannot release savepoint on inactive transaction")
}
func (i *inactiveTxnContext) GetSavepoints() []string { return nil }

func newInactiveTxnContext() txnapi.TxnContext {
	return &inactiveTxnContext{}
}

// ─── TxnContext Savepoint Tests ───────────────────────────────────

func TestSavepoint_TxnContext(t *testing.T) {
	store, err := kvstore.Open(kvstoreapi.Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	txnMgr := txn.New()
	txnCtx := txnMgr.BeginTxnContext()

	// Helper to get store interface for RollbackToSavepoint
	storeDelete := store

	// Test: CreateSavepoint on inactive transaction should fail
	t.Run("create savepoint on inactive", func(t *testing.T) {
		inactive := newInactiveTxnContext()
		err := inactive.CreateSavepoint("sp1")
		if err == nil {
			t.Errorf("expected error creating savepoint on inactive txn")
		}
	})

	// Test: Create savepoint
	t.Run("create savepoint", func(t *testing.T) {
		err := txnCtx.CreateSavepoint("sp1")
		if err != nil {
			t.Fatalf("CreateSavepoint: %v", err)
		}
		sps := txnCtx.GetSavepoints()
		if len(sps) != 1 || sps[0] != "sp1" {
			t.Errorf("expected [sp1], got %v", sps)
		}
	})

	// Test: Create nested savepoints
	t.Run("nested savepoints", func(t *testing.T) {
		err := txnCtx.CreateSavepoint("sp2")
		if err != nil {
			t.Fatalf("CreateSavepoint sp2: %v", err)
		}
		sps := txnCtx.GetSavepoints()
		if len(sps) != 2 {
			t.Errorf("expected 2 savepoints, got %d: %v", len(sps), sps)
		}
		if sps[0] != "sp1" || sps[1] != "sp2" {
			t.Errorf("expected [sp1, sp2], got %v", sps)
		}
	})

	// Test: Release savepoint
	t.Run("release savepoint", func(t *testing.T) {
		err := txnCtx.ReleaseSavepoint("sp1")
		if err != nil {
			t.Fatalf("ReleaseSavepoint: %v", err)
		}
		sps := txnCtx.GetSavepoints()
		if len(sps) != 1 || sps[0] != "sp2" {
			t.Errorf("expected [sp2], got %v", sps)
		}
	})

	// Test: Release non-existent savepoint
	t.Run("release non-existent", func(t *testing.T) {
		err := txnCtx.ReleaseSavepoint("nonexistent")
		if err == nil {
			t.Errorf("expected error releasing non-existent savepoint")
		}
	})

	// Test: Rollback to savepoint
	t.Run("rollback to savepoint", func(t *testing.T) {
		// Add a pending write
		txnCtx.AddPendingWrite([]byte("test_key_1"), nil)

		// Create another savepoint after the write
		err := txnCtx.CreateSavepoint("sp3")
		if err != nil {
			t.Fatalf("CreateSavepoint sp3: %v", err)
		}

		// Add another pending write
		txnCtx.AddPendingWrite([]byte("test_key_2"), nil)

		if len(txnCtx.GetPendingWrites()) != 2 {
			t.Fatalf("expected 2 pending writes, got %d", len(txnCtx.GetPendingWrites()))
		}

		// Rollback to sp3
		err = txnCtx.RollbackToSavepoint("sp3", storeDelete)
		if err != nil {
			t.Fatalf("RollbackToSavepoint: %v", err)
		}

		// Should be back to 1 pending write
		pws := txnCtx.GetPendingWrites()
		if len(pws) != 1 {
			t.Errorf("expected 1 pending write after rollback, got %d", len(pws))
		}

		// Savepoint should still exist (can rollback again)
		sps := txnCtx.GetSavepoints()
		if len(sps) != 2 || sps[0] != "sp2" || sps[1] != "sp3" {
			t.Errorf("expected [sp2, sp3], got %v", sps)
		}
	})

	// Test: Rollback non-existent savepoint
	t.Run("rollback non-existent", func(t *testing.T) {
		err := txnCtx.RollbackToSavepoint("nonexistent", storeDelete)
		if err == nil {
			t.Errorf("expected error rolling back to non-existent savepoint")
		}
	})

	// Test: Rollback to inactive transaction
	t.Run("rollback to inactive", func(t *testing.T) {
		inactive := newInactiveTxnContext()
		err := inactive.RollbackToSavepoint("sp1", storeDelete)
		if err == nil {
			t.Errorf("expected error rolling back to savepoint on inactive txn")
		}
	})

	// Test: Release on inactive
	t.Run("release on inactive", func(t *testing.T) {
		inactive := newInactiveTxnContext()
		err := inactive.ReleaseSavepoint("sp1")
		if err == nil {
			t.Errorf("expected error releasing savepoint on inactive txn")
		}
	})

	// Test: Release all and commit
	t.Run("release all and commit", func(t *testing.T) {
		err := txnCtx.ReleaseSavepoint("sp2")
		if err != nil {
			t.Fatalf("ReleaseSavepoint sp2: %v", err)
		}
		err = txnCtx.ReleaseSavepoint("sp3")
		if err != nil {
			t.Fatalf("ReleaseSavepoint sp3: %v", err)
		}
		sps := txnCtx.GetSavepoints()
		if len(sps) != 0 {
			t.Errorf("expected 0 savepoints after release, got %d", len(sps))
		}
		err = txnCtx.Commit()
		if err != nil {
			t.Fatalf("Commit: %v", err)
		}
	})
}
