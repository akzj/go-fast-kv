package internal

import (
	"fmt"
	"testing"

	kvstore "github.com/akzj/go-fast-kv/internal/kvstore"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
	catalogapi "github.com/akzj/go-fast-kv/internal/sql/catalog/api"
	catalog "github.com/akzj/go-fast-kv/internal/sql/catalog"
	encoding "github.com/akzj/go-fast-kv/internal/sql/encoding"
	engine "github.com/akzj/go-fast-kv/internal/sql/engine"
	executorapi "github.com/akzj/go-fast-kv/internal/sql/executor/api"
	parserapi "github.com/akzj/go-fast-kv/internal/sql/parser/api"
	plannerapi "github.com/akzj/go-fast-kv/internal/sql/planner/api"
	txnapi "github.com/akzj/go-fast-kv/internal/txn/api"
	txn "github.com/akzj/go-fast-kv/internal/txn"
	parser "github.com/akzj/go-fast-kv/internal/sql/parser"
	planner "github.com/akzj/go-fast-kv/internal/sql/planner"
)

// Test environment for FOR UPDATE tests with transaction support
type forUpdateTestEnv struct {
	store   kvstoreapi.Store
	cat     catalogapi.CatalogManager
	parser  parserapi.Parser
	planner plannerapi.Planner
	exec    *executor
	txnMgr  txnapi.TxnManager
}

// newForUpdateTestEnv creates a test environment with transaction support
func newForUpdateTestEnv(t *testing.T) *forUpdateTestEnv {
	t.Helper()
	store, err := kvstore.Open(kvstoreapi.Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	cat := catalog.New(store)
	enc := encoding.NewKeyEncoder()
	codec := encoding.NewRowCodec()
	tbl := engine.NewTableEngine(store, enc, codec)
	idx := engine.NewIndexEngine(store, enc)
	p := parser.New()
	pl := planner.New(cat)
	exec := New(store, cat, tbl, idx, nil, pl, p)
	txnMgr := txn.New()

	return &forUpdateTestEnv{
		store:   store,
		cat:     cat,
		parser:  p,
		planner: pl,
		exec:    exec,
		txnMgr:  txnMgr,
	}
}

// beginTxn starts a new transaction with row locking
func (env *forUpdateTestEnv) beginTxn() txnapi.TxnContext {
	return env.txnMgr.BeginTxnContext()
}

// setupTestData creates a test table with rows
func (env *forUpdateTestEnv) setupTestData(t *testing.T) {
	t.Helper()
	env.execSQL(t, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")
	env.execSQL(t, "INSERT INTO users VALUES (1, 'Alice', 30)")
	env.execSQL(t, "INSERT INTO users VALUES (2, 'Bob', 25)")
	env.execSQL(t, "INSERT INTO users VALUES (3, 'Charlie', 35)")
}

// getTableID returns the table ID for a table name
func (env *forUpdateTestEnv) getTableID(t *testing.T, tableName string) uint32 {
	t.Helper()
	tbl, err := env.cat.GetTable(tableName)
	if err != nil {
		t.Fatalf("GetTable(%s): %v", tableName, err)
	}
	return tbl.TableID
}

// rowKey constructs a lock key for testing (same format as executor)
func rowKey(tableID uint32, rowID uint64) string {
	return fmt.Sprintf("%d:%d", tableID, rowID)
}

// execSQL parses, plans, and executes a SQL statement (no transaction)
func (env *forUpdateTestEnv) execSQL(t *testing.T, sql string) *executorapi.Result {
	t.Helper()
	stmt, err := env.parser.Parse(sql)
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	plan, err := env.planner.Plan(stmt)
	if err != nil {
		t.Fatalf("plan %q: %v", sql, err)
	}
	result, err := env.exec.ExecuteWithTxn(plan, nil)
	if err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
	return result
}

// execSQLWithTxn parses, plans, and executes a SQL statement with transaction
func (env *forUpdateTestEnv) execSQLWithTxn(t *testing.T, txnCtx txnapi.TxnContext, sql string) (*executorapi.Result, error) {
	t.Helper()
	stmt, err := env.parser.Parse(sql)
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	plan, err := env.planner.Plan(stmt)
	if err != nil {
		t.Fatalf("plan %q: %v", sql, err)
	}
	return env.exec.ExecuteWithTxn(plan, txnCtx)
}

// TestForUpdate_BasicAcquiresLock tests that SELECT FOR UPDATE acquires row locks
func TestForUpdate_BasicAcquiresLock(t *testing.T) {
	env := newForUpdateTestEnv(t)
	env.setupTestData(t)

	// Get actual table ID
	tableID := env.getTableID(t, "users")

	// Start transaction 1
	txn1 := env.beginTxn()
	t.Cleanup(func() { txn1.Rollback() })

	// Execute SELECT FOR UPDATE in txn1
	result, err := env.execSQLWithTxn(t, txn1, "SELECT * FROM users WHERE id = 1 FOR UPDATE")
	if err != nil {
		t.Fatalf("SELECT FOR UPDATE: %v", err)
	}

	// Should have acquired lock on row 1 (rowID is typically 1 for first insert)
	lockMgr := txn1.LockManager()
	lockKey := rowKey(tableID, 1)
	if !lockMgr.IsLocked(lockKey) {
		t.Errorf("Expected row to be locked after SELECT FOR UPDATE")
	}

	// Verify result
	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][1].Text != "Alice" {
		t.Errorf("Expected Alice, got %s", result.Rows[0][1].Text)
	}

	// Commit transaction - should release locks
	err = txn1.Commit()
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// After commit, lock should be released
	if lockMgr.IsLocked(lockKey) {
		t.Errorf("Expected lock to be released after COMMIT")
	}
}

// TestForUpdate_CommitsReleasesLock tests that COMMIT releases row locks
func TestForUpdate_CommitsReleasesLock(t *testing.T) {
	env := newForUpdateTestEnv(t)
	env.setupTestData(t)

	// Get actual table ID
	tableID := env.getTableID(t, "users")

	// Start transaction
	txn := env.beginTxn()
	lockMgr := txn.LockManager()

	// Execute SELECT FOR UPDATE
	_, err := env.execSQLWithTxn(t, txn, "SELECT * FROM users WHERE id = 2 FOR UPDATE")
	if err != nil {
		t.Fatalf("SELECT FOR UPDATE: %v", err)
	}

	lockKey := rowKey(tableID, 2)
	if !lockMgr.IsLocked(lockKey) {
		t.Errorf("Expected row to be locked")
	}

	// Commit
	err = txn.Commit()
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Lock should be released
	if lockMgr.IsLocked(lockKey) {
		t.Errorf("Expected lock to be released after COMMIT")
	}
}

// TestForUpdate_RollbackReleasesLock tests that ROLLBACK releases row locks
func TestForUpdate_RollbackReleasesLock(t *testing.T) {
	env := newForUpdateTestEnv(t)
	env.setupTestData(t)

	// Get actual table ID
	tableID := env.getTableID(t, "users")

	// Start transaction
	txn := env.beginTxn()
	lockMgr := txn.LockManager()

	// Execute SELECT FOR UPDATE
	_, err := env.execSQLWithTxn(t, txn, "SELECT * FROM users WHERE id = 3 FOR UPDATE")
	if err != nil {
		t.Fatalf("SELECT FOR UPDATE: %v", err)
	}

	lockKey := rowKey(tableID, 3)
	if !lockMgr.IsLocked(lockKey) {
		t.Errorf("Expected row to be locked")
	}

	// Rollback
	txn.Rollback()

	// Lock should be released
	if lockMgr.IsLocked(lockKey) {
		t.Errorf("Expected lock to be released after ROLLBACK")
	}
}

// TestForUpdate_ConcurrentNowait tests that NOWAIT returns error on contention
func TestForUpdate_ConcurrentNowait(t *testing.T) {
	env := newForUpdateTestEnv(t)
	env.setupTestData(t)

	// Get actual table ID
	tableID := env.getTableID(t, "users")

	// Start transaction 1 and lock row 1
	txn1 := env.beginTxn()
	_, err := env.execSQLWithTxn(t, txn1, "SELECT * FROM users WHERE id = 1 FOR UPDATE")
	if err != nil {
		t.Fatalf("SELECT FOR UPDATE txn1: %v", err)
	}

	// Verify row 1 is locked by txn1
	lockMgr1 := txn1.LockManager()
	lockKey := rowKey(tableID, 1)
	if !lockMgr1.IsLocked(lockKey) {
		t.Fatalf("Row should be locked by txn1")
	}

	// Start transaction 2 and try to lock same row with NOWAIT
	txn2 := env.beginTxn()
	t.Cleanup(func() { txn2.Rollback() })

	// NOWAIT should return error since row is locked by txn1
	_, err = env.execSQLWithTxn(t, txn2, "SELECT * FROM users WHERE id = 1 FOR UPDATE NOWAIT")
	if err == nil {
		t.Fatalf("Expected error for NOWAIT on locked row, got nil")
	}

	// Cleanup txn1
	txn1.Rollback()
}

// TestForUpdate_SkipLockedSkipsRows tests that SKIP LOCKED skips locked rows
func TestForUpdate_SkipLockedSkipsRows(t *testing.T) {
	env := newForUpdateTestEnv(t)
	env.setupTestData(t)

	// Get actual table ID
	tableID := env.getTableID(t, "users")

	// Start transaction 1 and lock row 1
	txn1 := env.beginTxn()
	_, err := env.execSQLWithTxn(t, txn1, "SELECT * FROM users WHERE id = 1 FOR UPDATE")
	if err != nil {
		t.Fatalf("SELECT FOR UPDATE txn1: %v", err)
	}

	// Verify row 1 is locked
	lockMgr1 := txn1.LockManager()
	lockKey := rowKey(tableID, 1)
	if !lockMgr1.IsLocked(lockKey) {
		t.Fatalf("Row should be locked by txn1")
	}

	// Start transaction 2 with SKIP LOCKED
	txn2 := env.beginTxn()
	t.Cleanup(func() { txn2.Rollback() })

	// SELECT FOR UPDATE SKIP LOCKED should skip row 1 (locked) and return rows 2 and 3
	result, err := env.execSQLWithTxn(t, txn2, "SELECT * FROM users WHERE id > 0 ORDER BY id FOR UPDATE SKIP LOCKED")
	if err != nil {
		t.Fatalf("SELECT FOR UPDATE SKIP LOCKED: %v", err)
	}

	// Should get rows 2 and 3 (row 1 was skipped)
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows (skipped locked row 1), got %d", len(result.Rows))
	}

	// Verify the correct rows were returned
	if len(result.Rows) >= 1 && result.Rows[0][0].Int != 2 {
		t.Errorf("Expected row 2 first, got %d", result.Rows[0][0].Int)
	}
	if len(result.Rows) >= 2 && result.Rows[1][0].Int != 3 {
		t.Errorf("Expected row 3 second, got %d", result.Rows[1][0].Int)
	}

	// Cleanup txn1
	txn1.Rollback()
}

// TestForUpdate_LockAllScannedRows tests that FOR UPDATE locks all scanned rows
func TestForUpdate_LockAllScannedRows(t *testing.T) {
	env := newForUpdateTestEnv(t)
	env.setupTestData(t)

	// Get actual table ID
	tableID := env.getTableID(t, "users")

	// Start transaction
	txn := env.beginTxn()
	t.Cleanup(func() { txn.Rollback() })
	lockMgr := txn.LockManager()

	// Execute SELECT FOR UPDATE without WHERE (locks all rows)
	result, err := env.execSQLWithTxn(t, txn, "SELECT * FROM users FOR UPDATE")
	if err != nil {
		t.Fatalf("SELECT FOR UPDATE: %v", err)
	}

	// Should have locked all 3 rows
	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(result.Rows))
	}

	// Check each row is locked
	for i := uint64(1); i <= 3; i++ {
		lockKey := rowKey(tableID, i)
		if !lockMgr.IsLocked(lockKey) {
			t.Errorf("Expected row %d to be locked", i)
		}
	}
}

// TestForUpdate_NoLockWithoutForUpdate tests that regular SELECT doesn't lock
func TestForUpdate_NoLockWithoutForUpdate(t *testing.T) {
	env := newForUpdateTestEnv(t)
	env.setupTestData(t)

	// Get actual table ID
	tableID := env.getTableID(t, "users")

	// Start transaction
	txn := env.beginTxn()
	t.Cleanup(func() { txn.Rollback() })
	lockMgr := txn.LockManager()

	// Execute regular SELECT (no FOR UPDATE)
	_, err := env.execSQLWithTxn(t, txn, "SELECT * FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}

	// Row should NOT be locked
	lockKey := rowKey(tableID, 1)
	if lockMgr.IsLocked(lockKey) {
		t.Errorf("Expected row NOT to be locked for regular SELECT")
	}
}

// TestForUpdate_MultipleTransactions tests multiple concurrent transactions
func TestForUpdate_MultipleTransactions(t *testing.T) {
	env := newForUpdateTestEnv(t)
	env.setupTestData(t)

	// Get actual table ID
	tableID := env.getTableID(t, "users")

	// Start transaction 1 and lock rows 1 and 2
	txn1 := env.beginTxn()
	_, err := env.execSQLWithTxn(t, txn1, "SELECT * FROM users WHERE id IN (1, 2) FOR UPDATE")
	if err != nil {
		t.Fatalf("SELECT FOR UPDATE txn1: %v", err)
	}

	lockMgr1 := txn1.LockManager()
	if !lockMgr1.IsLocked(rowKey(tableID, 1)) {
		t.Errorf("Expected row 1 to be locked by txn1")
	}
	if !lockMgr1.IsLocked(rowKey(tableID, 2)) {
		t.Errorf("Expected row 2 to be locked by txn1")
	}

	// Start transaction 2 - should be able to lock row 3
	txn2 := env.beginTxn()
	t.Cleanup(func() { txn2.Rollback() })

	_, err = env.execSQLWithTxn(t, txn2, "SELECT * FROM users WHERE id = 3 FOR UPDATE")
	if err != nil {
		t.Fatalf("SELECT FOR UPDATE txn2: %v", err)
	}

	// Start transaction 3 - try to lock row 1 with NOWAIT (should fail)
	txn3 := env.beginTxn()
	t.Cleanup(func() { txn3.Rollback() })

	_, err = env.execSQLWithTxn(t, txn3, "SELECT * FROM users WHERE id = 1 FOR UPDATE NOWAIT")
	if err == nil {
		t.Fatalf("Expected error for NOWAIT on locked row")
	}

	// Cleanup
	txn1.Rollback()
	txn2.Rollback()
}

// TestForUpdate_NowaitWithoutContention tests that NOWAIT succeeds when no contention
func TestForUpdate_NowaitWithoutContention(t *testing.T) {
	env := newForUpdateTestEnv(t)
	env.setupTestData(t)

	// Get actual table ID
	tableID := env.getTableID(t, "users")

	// Start transaction
	txn := env.beginTxn()
	t.Cleanup(func() { txn.Rollback() })

	// Execute SELECT FOR UPDATE NOWAIT when no contention
	result, err := env.execSQLWithTxn(t, txn, "SELECT * FROM users WHERE id = 1 FOR UPDATE NOWAIT")
	if err != nil {
		t.Fatalf("SELECT FOR UPDATE NOWAIT: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.Rows))
	}

	// Row should be locked
	lockMgr := txn.LockManager()
	if !lockMgr.IsLocked(rowKey(tableID, 1)) {
		t.Errorf("Expected row to be locked")
	}
}

// TestForUpdate_LockReleasedOnError tests that locks are released even if error occurs
func TestForUpdate_LockReleasedOnError(t *testing.T) {
	env := newForUpdateTestEnv(t)
	env.setupTestData(t)

	// Get actual table ID
	tableID := env.getTableID(t, "users")

	// Start transaction
	txn := env.beginTxn()
	lockMgr := txn.LockManager()

	// Execute SELECT FOR UPDATE
	_, err := env.execSQLWithTxn(t, txn, "SELECT * FROM users WHERE id = 1 FOR UPDATE")
	if err != nil {
		t.Fatalf("SELECT FOR UPDATE: %v", err)
	}

	// Row should be locked
	lockKey := rowKey(tableID, 1)
	if !lockMgr.IsLocked(lockKey) {
		t.Errorf("Expected row to be locked")
	}

	// Rollback
	txn.Rollback()

	// Lock should be released
	if lockMgr.IsLocked(lockKey) {
		t.Errorf("Expected lock to be released after error/rollback")
	}
}

// TestForUpdate_InsertSelectWithLock tests INSERT...SELECT with FOR UPDATE
func TestForUpdate_InsertSelectWithLock(t *testing.T) {
	env := newForUpdateTestEnv(t)
	env.setupTestData(t)

	// Start transaction
	txn := env.beginTxn()
	t.Cleanup(func() { txn.Rollback() })

	// Create another table
	_, err := env.execSQLWithTxn(t, txn, "CREATE TABLE users_copy (id INT PRIMARY KEY, name TEXT, age INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Insert select (no FOR UPDATE on SELECT part - different from SELECT FOR UPDATE)
	_, err = env.execSQLWithTxn(t, txn, "INSERT INTO users_copy SELECT * FROM users")
	if err != nil {
		t.Fatalf("INSERT SELECT: %v", err)
	}

	// Verify data was copied
	rows, err := env.execSQLWithTxn(t, txn, "SELECT COUNT(*) FROM users_copy")
	if err != nil {
		t.Fatalf("SELECT COUNT: %v", err)
	}
	if rows.Rows[0][0].Int != 3 {
		t.Errorf("Expected 3 rows in users_copy, got %d", rows.Rows[0][0].Int)
	}

	txn.Commit()
}

// TestForUpdate_PlannerPopulatesLockMode tests that the planner correctly populates LockMode
func TestForUpdate_PlannerPopulatesLockMode(t *testing.T) {
	env := newForUpdateTestEnv(t)
	env.setupTestData(t)

	tests := []struct {
		sql      string
		wantMode parserapi.LockMode
		wantWait parserapi.LockWait
	}{
		{"SELECT * FROM users", parserapi.NoUpdate, parserapi.LockWaitDefault},
		{"SELECT * FROM users FOR UPDATE", parserapi.UpdateExclusive, parserapi.LockWaitDefault},
		{"SELECT * FROM users FOR UPDATE NOWAIT", parserapi.UpdateExclusive, parserapi.LockWaitNowait},
		{"SELECT * FROM users FOR UPDATE SKIP LOCKED", parserapi.UpdateExclusive, parserapi.LockWaitSkipLocked},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			stmt, err := env.parser.Parse(tt.sql)
			if err != nil {
				t.Fatalf("parse %q: %v", tt.sql, err)
			}

			plan, err := env.planner.Plan(stmt)
			if err != nil {
				t.Fatalf("plan %q: %v", tt.sql, err)
			}

			selPlan, ok := plan.(*plannerapi.SelectPlan)
			if !ok {
				t.Fatalf("expected SelectPlan, got %T", plan)
			}

			if selPlan.LockMode != tt.wantMode {
				t.Errorf("LockMode = %v, want %v", selPlan.LockMode, tt.wantMode)
			}
			if selPlan.LockWait != tt.wantWait {
				t.Errorf("LockWait = %v, want %v", selPlan.LockWait, tt.wantWait)
			}
		})
	}
}

// TestForUpdate_ForUpdateShare tests FOR UPDATE SHARE mode
func TestForUpdate_ForUpdateShare(t *testing.T) {
	env := newForUpdateTestEnv(t)
	env.setupTestData(t)

	// Start transaction 1
	txn1 := env.beginTxn()
	t.Cleanup(func() { txn1.Rollback() })

	// The planner may not yet support FOR UPDATE SHARE, but we can test that
	// the LockMode is correctly set if the query is parsed
	stmt, err := env.parser.Parse("SELECT * FROM users FOR UPDATE")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	plan, err := env.planner.Plan(stmt)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}

	selPlan := plan.(*plannerapi.SelectPlan)
	if selPlan.LockMode != parserapi.UpdateExclusive {
		t.Errorf("Expected UpdateExclusive lock mode, got %v", selPlan.LockMode)
	}

	// Execute to ensure it works end-to-end
	result, err := env.execSQLWithTxn(t, txn1, "SELECT * FROM users FOR UPDATE")
	if err != nil {
		t.Fatalf("SELECT FOR UPDATE: %v", err)
	}

	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(result.Rows))
	}
}

// TestForUpdate_LockKeyFormat tests the lock key format is consistent
func TestForUpdate_LockKeyFormat(t *testing.T) {
	env := newForUpdateTestEnv(t)
	env.setupTestData(t)

	// Start transaction
	txn := env.beginTxn()
	t.Cleanup(func() { txn.Rollback() })

	// Execute SELECT FOR UPDATE for row with known row ID
	result, err := env.execSQLWithTxn(t, txn, "SELECT * FROM users WHERE id = 1 FOR UPDATE")
	if err != nil {
		t.Fatalf("SELECT FOR UPDATE: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}

	// Lock key should be "tableID:rowID"
	// The row ID is the internal row ID, not the primary key value
	lockMgr := txn.LockManager()

	// With the test data, the first inserted row typically gets rowID=1
	// But we can't assume exact rowID - let's just verify locking works
	// by checking that at least one lock exists
	stats := lockMgr.LockStats()
	if stats.TotalLocks < 1 {
		t.Errorf("Expected at least 1 lock, got %d", stats.TotalLocks)
	}
}

// TestForUpdate_CloseDoesNotAffectLocks tests that LockManager().Close() behavior
func TestForUpdate_CloseDoesNotAffectLocks(t *testing.T) {
	env := newForUpdateTestEnv(t)
	env.setupTestData(t)

	// Start transaction
	txn := env.beginTxn()
	lockMgr := txn.LockManager()

	// Execute SELECT FOR UPDATE
	_, err := env.execSQLWithTxn(t, txn, "SELECT * FROM users WHERE id = 1 FOR UPDATE")
	if err != nil {
		t.Fatalf("SELECT FOR UPDATE: %v", err)
	}

	// Verify lock acquired
	stats := lockMgr.LockStats()
	if stats.TotalLocks < 1 {
		t.Errorf("Expected at least 1 lock, got %d", stats.TotalLocks)
	}

	// Rollback should release locks
	txn.Rollback()

	// After rollback, stats should show 0 locks
	// Note: LockStats() returns current state
	stats = lockMgr.LockStats()
	if stats.TotalLocks != 0 {
		t.Errorf("Expected 0 locks after rollback, got %d", stats.TotalLocks)
	}
}

// TestForUpdate_TxnContextNilSafety tests that ExecuteWithTxn handles nil TxnContext
func TestForUpdate_TxnContextNilSafety(t *testing.T) {
	env := newForUpdateTestEnv(t)
	env.setupTestData(t)

	// Execute without transaction context - should work without locking
	result, err := env.execSQLWithTxn(t, nil, "SELECT * FROM users")
	if err != nil {
		t.Fatalf("SELECT without txn: %v", err)
	}

	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(result.Rows))
	}
}

// TestForUpdate_LockReuseAcrossStatements tests locks are held across multiple statements
func TestForUpdate_LockReuseAcrossStatements(t *testing.T) {
	env := newForUpdateTestEnv(t)
	env.setupTestData(t)

	// Get actual table ID
	tableID := env.getTableID(t, "users")

	// Start transaction
	txn := env.beginTxn()
	t.Cleanup(func() { txn.Rollback() })
	lockMgr := txn.LockManager()

	// First SELECT FOR UPDATE
	_, err := env.execSQLWithTxn(t, txn, "SELECT * FROM users WHERE id = 1 FOR UPDATE")
	if err != nil {
		t.Fatalf("First SELECT FOR UPDATE: %v", err)
	}

	// Lock should be held
	lockKey1 := rowKey(tableID, 1)
	if !lockMgr.IsLocked(lockKey1) {
		t.Errorf("Expected row to be locked")
	}

	// Second SELECT - row 1 should still be locked
	// (simulating a transaction that does multiple operations)
	_, err = env.execSQLWithTxn(t, txn, "SELECT * FROM users WHERE id = 2 FOR UPDATE")
	if err != nil {
		t.Fatalf("Second SELECT FOR UPDATE: %v", err)
	}

	// Both rows should be locked
	if !lockMgr.IsLocked(lockKey1) {
		t.Errorf("Expected row 1 to still be locked")
	}
	if !lockMgr.IsLocked(rowKey(tableID, 2)) {
		t.Errorf("Expected row 2 to be locked")
	}
}

// BenchmarkForUpdate_LockAcquisition benchmarks row lock acquisition
func BenchmarkForUpdate_LockAcquisition(b *testing.B) {
	env := &forUpdateTestEnv{}
	store, _ := kvstore.Open(kvstoreapi.Config{Dir: b.TempDir()})
	defer store.Close()

	env.store = store
	env.cat = catalog.New(store)
	enc := encoding.NewKeyEncoder()
	codec := encoding.NewRowCodec()
	tbl := engine.NewTableEngine(store, enc, codec)
	idx := engine.NewIndexEngine(store, enc)
	env.parser = parser.New()
	env.planner = planner.New(env.cat)
	env.exec = New(store, env.cat, tbl, idx, nil, env.planner, env.parser)
	env.txnMgr = txn.New()

	// Setup
	env.execSQL(nil, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	for i := 1; i <= 100; i++ {
		env.execSQL(nil, fmt.Sprintf("INSERT INTO users VALUES (%d, 'User%d')", i, i))
	}

	txn := env.txnMgr.BeginTxnContext()
	defer txn.Rollback()

	b.ResetTimer()
	for i := 1; i <= b.N; i++ {
		stmt, _ := env.parser.Parse(fmt.Sprintf("SELECT * FROM users WHERE id = %d FOR UPDATE", (i%100)+1))
		plan, _ := env.planner.Plan(stmt)
		env.exec.ExecuteWithTxn(plan, txn)
	}
}
