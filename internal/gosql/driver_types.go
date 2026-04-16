package gosql

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"

	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
	txnapi "github.com/akzj/go-fast-kv/internal/txn/api"
	gosql "github.com/akzj/go-fast-kv/internal/sql"
)

// Compile-time interface checks.
var _ driver.Conn = (*Conn)(nil)
var _ driver.Stmt = (*Stmt)(nil)
var _ driver.Tx = (*Tx)(nil)
var _ driver.Rows = (*RowsWithDB)(nil)
var _ driver.Result = (*Result)(nil)
var _ driver.Connector = (*driverConnector)(nil)

// db wraps the internal SQL DB with a method to create driver.Conn.
type db struct {
	store  kvstoreapi.Store
	txnMgr txnapi.TxnContextFactory // for creating transaction contexts
}

// newDB creates a new SQL DB wrapper.
func newDB(store kvstoreapi.Store) *db {
	return &db{
		store:  store,
		txnMgr: store.TxnManager(),
	}
}

// conn returns a driver.Conn for this database.
func (d *db) conn() (driver.Conn, error) {
	return &Conn{
		db:        d,
		txnDB:     gosql.Open(d.store),
		closed:    false,
	}, nil
}

// Conn implements driver.Conn.
type Conn struct {
	db        *db
	tx        *Tx      // current transaction, if any
	txnDB     *gosql.DB // SQL DB instance (owned by Conn)
	closed    bool
}

// Prepare implements driver.Conn.Prepare.
// Prepares a SQL statement for execution.
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}
	return &Stmt{
		conn:  c,
		query: query,
	}, nil
}

// Begin implements driver.Conn.Begin.
// Starts a new transaction with a real TxnContext.
func (c *Conn) Begin() (driver.Tx, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}
	if c.tx != nil {
		return nil, fmt.Errorf("gosql: transaction already active")
	}

	// Create a real transaction context using the txnMgr.
	txnCtx := c.db.txnMgr.BeginTxnContext()
	if txnCtx == nil {
		return nil, fmt.Errorf("gosql: failed to begin transaction")
	}

	c.tx = &Tx{
		conn:       c,
		txnCtx:     txnCtx,
		committed:  false,
		rollbacked: false,
	}

	return c.tx, nil
}

// Close implements driver.Conn.Close.
func (c *Conn) Close() error {
	c.closed = true
	if c.txnDB != nil {
		c.txnDB.Close()
		c.txnDB = nil
	}
	return nil
}

// ResetSession implements driver.Conn.ResetSession.
// Not supported - returns nil.
func (c *Conn) ResetSession() error {
	return nil
}

// CheckNamedValue implements driver.Conn.CheckNamedValue.
// Named parameters are not supported.
func (c *Conn) CheckNamedValue(nv *driver.NamedValue) error {
	return driver.ErrSkip
}

// getDB returns the SQL DB for executing queries.
func (c *Conn) getDB() *gosql.DB {
	return c.txnDB
}

// Stmt implements driver.Stmt.
type Stmt struct {
	conn  *Conn
	query string
}

// Close implements driver.Stmt.Close.
func (s *Stmt) Close() error {
	return nil
}

// NumInput implements driver.Stmt.NumInput.
// Returns -1 to indicate unknown argument count.
func (s *Stmt) NumInput() int {
	return -1 // Unknown until we parse the query
}

// Exec implements driver.Stmt.Exec.
// Executes a statement that doesn't return rows.
func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	if s.conn.closed {
		return nil, driver.ErrBadConn
	}

	// Substitute positional placeholders ($1, $2, etc.)
	query, err := substitutePlaceholders(s.query, argsToInterface(args))
	if err != nil {
		return nil, err
	}

	// Execute via the internal SQL layer.
	db := s.conn.getDB()
	result, err := db.Exec(query)
	if err != nil {
		return nil, err
	}

	return &Result{
		rowsAffected: result.RowsAffected,
	}, nil
}

// Query implements driver.Stmt.Query.
// Executes a query that returns rows.
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	if s.conn.closed {
		return nil, driver.ErrBadConn
	}

	// Substitute positional placeholders ($1, $2, etc.)
	query, err := substitutePlaceholders(s.query, argsToInterface(args))
	if err != nil {
		return nil, err
	}

	// Execute via the internal SQL layer.
	db := s.conn.getDB()
	result, err := db.Query(query)
	if err != nil {
		return nil, err
	}

	// Convert Result to driver.Rows.
	rows, err := newRowsFromResult(result)
	if err != nil {
		return nil, err
	}

	return &RowsWithDB{
		rows: rows,
		db:   db,
		idx:  0,
	}, nil
}

// LastInsertId implements driver.Stmt.LastInsertId.
// KNOWN TRAP: KV stores don't support LastInsertId - always returns 0, false.
func (s *Stmt) LastInsertId() (int64, bool) {
	return 0, false
}

// CheckNamedValue implements driver.Stmt.CheckNamedValue.
// Named parameters are not supported.
func (s *Stmt) CheckNamedValue(nv *driver.NamedValue) error {
	return driver.ErrSkip
}

// Tx implements driver.Tx.
// Uses a real TxnContext for transaction lifecycle management.
type Tx struct {
	conn       *Conn
	txnCtx     txnapi.TxnContext // the active transaction context
	committed  bool
	rollbacked bool
}

// Commit implements driver.Tx.Commit.
// Commits the transaction via txnCtx.Commit().
func (t *Tx) Commit() error {
	if t.committed {
		return fmt.Errorf("gosql: transaction already committed")
	}
	if t.rollbacked {
		return fmt.Errorf("gosql: transaction already rolled back")
	}
	if !t.txnCtx.IsActive() {
		return fmt.Errorf("gosql: transaction not active")
	}
	t.committed = true
	err := t.txnCtx.Commit()
	t.conn.tx = nil // clear the transaction
	return err
}

// Rollback implements driver.Tx.Rollback.
func (t *Tx) Rollback() error {
	if t.committed {
		return nil // already committed: no-op (per MySQL/Postgres)
	}
	if t.rollbacked {
		return nil // already rolled back: no-op
	}
	if !t.txnCtx.IsActive() {
		return nil // not active: no-op
	}
	t.rollbacked = true
	t.txnCtx.Rollback()
	t.conn.tx = nil // clear the transaction
	return nil
}

// Stmt implements driver.Tx.Stmt.
// Creates a statement from this transaction.
func (t *Tx) Stmt(stmt *Stmt) driver.Stmt {
	return &TxStmt{
		tx:    t,
		query: stmt.query,
	}
}

// TxStmt is a statement within a transaction.
type TxStmt struct {
	tx    *Tx
	query string
}

func (s *TxStmt) Close() error { return nil }
func (s *TxStmt) NumInput() int { return -1 }

func (s *TxStmt) Exec(args []driver.Value) (driver.Result, error) {
	query, err := substitutePlaceholders(s.query, argsToInterface(args))
	if err != nil {
		return nil, err
	}
	result, err := s.tx.conn.txnDB.Exec(query)
	if err != nil {
		return nil, err
	}
	return &Result{rowsAffected: result.RowsAffected}, nil
}

func (s *TxStmt) Query(args []driver.Value) (driver.Rows, error) {
	query, err := substitutePlaceholders(s.query, argsToInterface(args))
	if err != nil {
		return nil, err
	}
	result, err := s.tx.conn.txnDB.Query(query)
	if err != nil {
		return nil, err
	}
	rows, err := newRowsFromResult(result)
	if err != nil {
		return nil, err
	}
	return &RowsWithDB{rows: rows, db: s.tx.conn.txnDB, idx: 0}, nil
}

func (s *TxStmt) LastInsertId() (int64, bool) { return 0, false }
func (s *TxStmt) CheckNamedValue(nv *driver.NamedValue) error { return driver.ErrSkip }

// Result implements driver.Result.
type Result struct {
	rowsAffected int64
}

// LastInsertId implements driver.Result.LastInsertId.
// KV stores don't support LastInsertId - always returns 0, nil (success).
func (r *Result) LastInsertId() (int64, error) {
	return 0, nil
}

// RowsAffected implements driver.Result.RowsAffected.
func (r *Result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

// RowsWithDB implements driver.Rows and holds reference to sql.DB for cleanup.
type RowsWithDB struct {
	rows *Rows
	db   *gosql.DB
	idx  int
}

// Close implements driver.Rows.Close.
// KNOWN TRAP: Must close the sql.DB to prevent goroutine leaks.
func (r *RowsWithDB) Close() error {
	if r.db != nil {
		r.db.Close()
		r.db = nil
	}
	return nil
}

// Columns implements driver.Rows.Columns.
// Returns lowercase column names to match sqlx struct tag expectations.
func (r *RowsWithDB) Columns() []string {
	cols := make([]string, len(r.rows.columns))
	for i, c := range r.rows.columns {
		cols[i] = strings.ToLower(c)
	}
	return cols
}

// Next implements driver.Rows.Next.
// Returns nil while rows available, io.EOF when done.
func (r *RowsWithDB) Next(dest []driver.Value) error {
	if r.idx >= len(r.rows.rows) {
		return io.EOF
	}
	r.idx++
	if r.idx > len(r.rows.rows) {
		return io.EOF
	}
	row := r.rows.rows[r.idx-1]
	for i, val := range row {
		if i >= len(dest) {
			break
		}
		v, err := convertValue(val)
		if err != nil {
			return err
		}
		dest[i] = v
	}
	return nil
}

// Err implements driver.Rows.Err.
// Returns any error encountered during iteration.
func (r *RowsWithDB) Err() error {
	return nil
}

// driverConnector implements driver.Connector for sql.OpenDB.
type driverConnector struct {
	store kvstoreapi.Store
}

// Connect implements driver.Connector.Connect.
func (dc *driverConnector) Connect(ctx context.Context) (driver.Conn, error) {
	db := newDB(dc.store)
	return db.conn()
}

// Driver implements driver.Connector.Driver.
func (dc *driverConnector) Driver() driver.Driver {
	return &Driver{}
}

// Rows holds the row data for driver.Rows implementation.
type Rows struct {
	columns []string
	rows    [][]interface{}
}

// newRowsFromResult converts an executor.Result to driver.Rows.
func newRowsFromResult(result *gosql.Result) (*Rows, error) {
	rows := &Rows{
		columns: result.Columns,
		rows:    make([][]interface{}, len(result.Rows)),
	}

	for i, row := range result.Rows {
		converted := make([]interface{}, len(row))
		for j, val := range row {
			v, err := convertValue(val)
			if err != nil {
				return nil, err
			}
			converted[j] = v
		}
		rows.rows[i] = converted
	}

	return rows, nil
}

// argsToInterface converts []driver.Value to []interface{}.
func argsToInterface(args []driver.Value) []interface{} {
	result := make([]interface{}, len(args))
	for i, v := range args {
		result[i] = v
	}
	return result
}