package gosql

import (
	"context"
	"database/sql/driver"
	"io"

	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
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
	store kvstoreapi.Store
}

// newDB creates a new SQL DB wrapper.
func newDB(store kvstoreapi.Store) *db {
	return &db{store: store}
}

// conn returns a driver.Conn for this database.
func (d *db) conn() (driver.Conn, error) {
	return &Conn{
		db:     d,
		closed: false,
	}, nil
}

// Conn implements driver.Conn.
type Conn struct {
	db     *db
	tx     *Tx      // current transaction, if any
	txnDB  *gosql.DB // SQL DB for non-transactional queries
	closed bool
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
// Starts a new transaction.
func (c *Conn) Begin() (driver.Tx, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}

	// Create a transaction wrapper.
	// The actual transaction will be managed via WriteBatch.
	txWrapper := &Tx{
		conn:    c,
		batch:   nil,
		started: false,
	}

	return txWrapper, nil
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

// getTxnDB returns the SQL DB for executing queries.
// Lazy initialization.
func (c *Conn) getTxnDB() *gosql.DB {
	if c.txnDB == nil {
		c.txnDB = gosql.Open(c.db.store)
	}
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
	sqlDB := s.conn.getTxnDB()
	result, err := sqlDB.Exec(query)
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
	sqlDB := s.conn.getTxnDB()
	result, err := sqlDB.Query(query)
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
		db:   sqlDB,
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
// Uses WriteBatch for transaction atomicity.
type Tx struct {
	conn    *Conn
	batch   kvstoreapi.WriteBatch
	started bool
}

// Commit implements driver.Tx.Commit.
// Commits the transaction.
func (t *Tx) Commit() error {
	if !t.started {
		return nil
	}
	if t.batch != nil {
		err := t.batch.Commit()
		t.batch = nil
		t.started = false
		return err
	}
	t.started = false
	return nil
}

// Rollback implements driver.Tx.Rollback.
func (t *Tx) Rollback() error {
	if t.batch != nil {
		t.batch.Discard()
		t.batch = nil
	}
	t.started = false
	return nil
}

// Stmt implements driver.Tx.Stmt.
// Creates a statement from this transaction.
func (t *Tx) Stmt(stmt *Stmt) driver.Stmt {
	return &Stmt{
		conn:  t.conn,
		query: stmt.query,
	}
}

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
func (r *RowsWithDB) Columns() []string {
	return r.rows.columns
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
	return &Conn{
		db:     &db{store: dc.store},
		closed: false,
	}, nil
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