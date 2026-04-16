package internal

import (
	"testing"

	kvstore "github.com/akzj/go-fast-kv/internal/kvstore"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
	catalogapi "github.com/akzj/go-fast-kv/internal/sql/catalog/api"
	"github.com/akzj/go-fast-kv/internal/sql/catalog"
	"github.com/akzj/go-fast-kv/internal/sql/encoding"
	"github.com/akzj/go-fast-kv/internal/sql/engine"
	"github.com/akzj/go-fast-kv/internal/sql/parser"
	"github.com/akzj/go-fast-kv/internal/sql/parser/api"
	"github.com/akzj/go-fast-kv/internal/sql/planner"
	plannerapi "github.com/akzj/go-fast-kv/internal/sql/planner/api"
	executorapi "github.com/akzj/go-fast-kv/internal/sql/executor/api"
)

// newBenchEnv creates a test environment for benchmarks.
func newBenchEnv(b *testing.B) *benchEnv {
	b.Helper()
	store, err := kvstore.Open(kvstoreapi.Config{Dir: b.TempDir()})
	if err != nil {
		b.Fatalf("open store: %v", err)
	}

	cat := catalog.New(store)
	enc := encoding.NewKeyEncoder()
	codec := encoding.NewRowCodec()
	tbl := engine.NewTableEngine(store, enc, codec)
	idx := engine.NewIndexEngine(store, enc)
	p := parser.New()
	pl := planner.New(cat)
	ex := New(store, cat, tbl, idx, pl)

	return &benchEnv{
		store:   store,
		cat:     cat,
		parser:  p,
		planner: pl,
		exec:    ex,
	}
}

type benchEnv struct {
	store   kvstoreapi.Store
	cat     catalogapi.CatalogManager
	parser  api.Parser
	planner plannerapi.Planner
	exec    executorapi.Executor
}

func (env *benchEnv) execSQL(sql string) {
	stmt, err := env.parser.Parse(sql)
	if err != nil {
		panic(err)
	}
	plan, err := env.planner.Plan(stmt)
	if err != nil {
		panic(err)
	}
	_, err = env.exec.Execute(plan)
	if err != nil {
		panic(err)
	}
}

func (env *benchEnv) prepare(sql string) (plannerapi.Plan, error) {
	stmt, err := env.parser.Parse(sql)
	if err != nil {
		return nil, err
	}
	return env.planner.Plan(stmt)
}

// BenchmarkNestedLoopJoinAlloc measures allocations in nested loop join (INNER JOIN).
func BenchmarkNestedLoopJoinAlloc(b *testing.B) {
	env := newBenchEnv(b)
	defer env.store.Close()

	// Create tables with enough rows to make join meaningful
	env.execSQL("CREATE TABLE t1 (id INT, val TEXT)")
	env.execSQL("CREATE TABLE t2 (id INT, val TEXT)")

	// Insert 50 rows into each table (50*50 = 2500 comparisons)
	for i := 1; i <= 50; i++ {
		env.execSQL("INSERT INTO t1 VALUES (" + itoa(i) + ", 'val')")
		env.execSQL("INSERT INTO t2 VALUES (" + itoa(i) + ", 'val')")
	}

	plan, err := env.prepare("SELECT t1.id, t2.id FROM t1 JOIN t2 ON t1.id = t2.id")
	if err != nil {
		b.Fatalf("prepare: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := env.exec.Execute(plan)
		if err != nil {
			b.Fatalf("execute: %v", err)
		}
	}
}

// BenchmarkHashJoinAlloc specifically tests hash join allocation patterns.
func BenchmarkHashJoinAlloc(b *testing.B) {
	env := newBenchEnv(b)
	defer env.store.Close()

	// Create tables for hash join
	env.execSQL("CREATE TABLE users (id INT, name TEXT)")
	env.execSQL("CREATE TABLE orders (user_id INT, amount INT)")

	// Insert 100 users, 500 orders (some users have multiple orders)
	for i := 1; i <= 100; i++ {
		env.execSQL("INSERT INTO users VALUES (" + itoa(i) + ", 'user')")
	}
	for i := 1; i <= 500; i++ {
		userID := (i % 100) + 1
		env.execSQL("INSERT INTO orders VALUES (" + itoa(userID) + ", " + itoa(i*10) + ")")
	}

	plan, err := env.prepare("SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id")
	if err != nil {
		b.Fatalf("prepare: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := env.exec.Execute(plan)
		if err != nil {
			b.Fatalf("execute: %v", err)
		}
	}
}

// BenchmarkHashJoin measures hash join performance with multiple rows and multiple matches.
// This is the primary benchmark for the acceptance criteria.
func BenchmarkHashJoin(b *testing.B) {
	env := newBenchEnv(b)
	defer env.store.Close()

	// Create tables for hash join with realistic data
	env.execSQL("CREATE TABLE customers (id INT, name TEXT)")
	env.execSQL("CREATE TABLE orders (customer_id INT, amount INT)")

	// Insert 100 customers, 500 orders (~5 orders per customer on average)
	// This creates multiple matches per customer, testing the hot path
	for i := 1; i <= 100; i++ {
		env.execSQL("INSERT INTO customers VALUES (" + itoa(i) + ", 'customer')")
	}
	for i := 1; i <= 500; i++ {
		customerID := (i % 100) + 1
		env.execSQL("INSERT INTO orders VALUES (" + itoa(customerID) + ", " + itoa(i*10) + ")")
	}

	plan, err := env.prepare("SELECT customers.name, orders.amount FROM customers JOIN orders ON customers.id = orders.customer_id")
	if err != nil {
		b.Fatalf("prepare: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := env.exec.Execute(plan)
		if err != nil {
			b.Fatalf("execute: %v", err)
		}
	}
}

// BenchmarkNestedLoopJoin measures nested loop join with ON condition.
// This tests the combinedVals pre-allocation optimization in execInnerJoin.
func BenchmarkNestedLoopJoin(b *testing.B) {
	env := newBenchEnv(b)
	defer env.store.Close()

	// Create tables with more columns to increase combinedVals size
	env.execSQL("CREATE TABLE t1 (id INT, a INT, b INT, c INT)")
	env.execSQL("CREATE TABLE t2 (id INT, x INT, y INT, z INT)")

	// Insert 30 rows each (30*30 = 900 comparisons, with multiple ON checks)
	for i := 1; i <= 30; i++ {
		env.execSQL("INSERT INTO t1 VALUES (" + itoa(i) + ", " + itoa(i) + ", " + itoa(i*2) + ", " + itoa(i*3) + ")")
		env.execSQL("INSERT INTO t2 VALUES (" + itoa(i) + ", " + itoa(i) + ", " + itoa(i*2) + ", " + itoa(i*3) + ")")
	}

	// Use a query that forces nested loop with ON condition
	// The extra conditions (t1.a = t2.x AND t1.b > 5) exercise the ON evaluation path
	plan, err := env.prepare("SELECT t1.id FROM t1 JOIN t2 ON t1.id = t2.id AND t1.a = t2.x AND t1.b > 5")
	if err != nil {
		b.Fatalf("prepare: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := env.exec.Execute(plan)
		if err != nil {
			b.Fatalf("execute: %v", err)
		}
	}
}

// itoa converts int to string without importing strconv.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	n := i
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[pos:])
}
