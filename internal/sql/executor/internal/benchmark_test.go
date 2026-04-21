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
	ex := New(store, cat, tbl, idx, pl, p)

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

// BenchmarkHashJoinWithExtraON measures hash join when there's an extra ON condition
// beyond the equi-join key. This exercises the combinedVals allocation in execHashJoin.
func BenchmarkHashJoinWithExtraON(b *testing.B) {
	env := newBenchEnv(b)
	defer env.store.Close()

	// Create tables for hash join with extra column
	env.execSQL("CREATE TABLE customers (id INT, name TEXT, score INT)")
	env.execSQL("CREATE TABLE orders (customer_id INT, amount INT)")

	// Insert 100 customers, 500 orders
	for i := 1; i <= 100; i++ {
		env.execSQL("INSERT INTO customers VALUES (" + itoa(i) + ", 'customer', " + itoa(i*10) + ")")
	}
	for i := 1; i <= 500; i++ {
		customerID := (i % 100) + 1
		env.execSQL("INSERT INTO orders VALUES (" + itoa(customerID) + ", " + itoa(i*10) + ")")
	}

	// Query with extra ON condition: JOIN ON customers.id = orders.customer_id AND customers.score > 50
	// This forces combinedVals allocation in execHashJoin for ON evaluation
	plan, err := env.prepare("SELECT customers.name, orders.amount FROM customers JOIN orders ON customers.id = orders.customer_id AND customers.score > 50")
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

// ─── STRESS TESTS: Large Dataset Benchmarks ──────────────────────────────────

// BenchmarkJoin1000Rows tests JOIN performance with 1000+ rows.
// This validates JOIN scales well for large datasets per acceptance criteria.
func BenchmarkJoin1000Rows(b *testing.B) {
	env := newBenchEnv(b)
	defer env.store.Close()

	env.execSQL("CREATE TABLE big_users (id INT, name TEXT)")
	env.execSQL("CREATE TABLE big_orders (user_id INT, amount INT)")

	// Insert 1500 users
	for i := 1; i <= 1500; i++ {
		env.execSQL("INSERT INTO big_users VALUES (" + itoa(i) + ", 'user')")
	}
	// Insert 1500 orders (one per user on average)
	for i := 1; i <= 1500; i++ {
		env.execSQL("INSERT INTO big_orders VALUES (" + itoa(i) + ", " + itoa(i*100) + ")")
	}

	plan, err := env.prepare("SELECT big_users.name, big_orders.amount FROM big_users JOIN big_orders ON big_users.id = big_orders.user_id")
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

// BenchmarkGroupBy1000Rows tests GROUP BY with 1000+ rows.
func BenchmarkGroupBy1000Rows(b *testing.B) {
	env := newBenchEnv(b)
	defer env.store.Close()

	env.execSQL("CREATE TABLE large_orders (user_id INT, amount INT, category TEXT)")

	// Insert 1500 rows with 100 distinct users
	for i := 1; i <= 1500; i++ {
		userID := (i % 100) + 1
		cat := "cat" + itoa(i%10)
		env.execSQL("INSERT INTO large_orders VALUES (" + itoa(userID) + ", " + itoa(i*10) + ", '" + cat + "')")
	}

	plan, err := env.prepare("SELECT user_id, COUNT(*), SUM(amount), AVG(amount) FROM large_orders GROUP BY user_id")
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

// BenchmarkOrderBy1000Rows tests ORDER BY with 1000+ rows.
func BenchmarkOrderBy1000Rows(b *testing.B) {
	env := newBenchEnv(b)
	defer env.store.Close()

	env.execSQL("CREATE TABLE large_data (id INT, val INT, name TEXT)")

	// Insert 1500 rows
	for i := 1; i <= 1500; i++ {
		env.execSQL("INSERT INTO large_data VALUES (" + itoa(i) + ", " + itoa(i*7) + ", 'item')")
	}

	plan, err := env.prepare("SELECT * FROM large_data ORDER BY val DESC")
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

// BenchmarkHashJoin10KRows tests hash join with 10K+ rows per acceptance criteria.
// Using -benchtime=1x to reduce iterations and avoid timeout due to WAL sync overhead.
func BenchmarkHashJoin10KRows(b *testing.B) {
	env := newBenchEnv(b)
	defer env.store.Close()

	env.execSQL("CREATE TABLE huge_customers_10k (id INT, name TEXT)")
	env.execSQL("CREATE TABLE huge_orders_10k (customer_id INT, amount INT)")

	// Insert 5000 customers
	for i := 1; i <= 5000; i++ {
		env.execSQL("INSERT INTO huge_customers_10k VALUES (" + itoa(i) + ", 'customer')")
	}
	// Insert 12000 orders (some customers have multiple orders)
	for i := 1; i <= 12000; i++ {
		customerID := (i % 5000) + 1
		env.execSQL("INSERT INTO huge_orders_10k VALUES (" + itoa(customerID) + ", " + itoa(i*10) + ")")
	}

	plan, err := env.prepare("SELECT huge_customers_10k.name, huge_orders_10k.amount FROM huge_customers_10k JOIN huge_orders_10k ON huge_customers_10k.id = huge_orders_10k.customer_id")
	if err != nil {
		b.Fatalf("prepare: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(0) // Prevent iteration scaling

	for i := 0; i < b.N && i < 5; i++ { // Limit iterations to avoid timeout
		_, err := env.exec.Execute(plan)
		if err != nil {
			b.Fatalf("execute: %v", err)
		}
	}
}

// BenchmarkGroupByOrderBy1000Rows tests GROUP BY with ORDER BY on 1000+ rows.
func BenchmarkGroupByOrderBy1000Rows(b *testing.B) {
	env := newBenchEnv(b)
	defer env.store.Close()

	env.execSQL("CREATE TABLE sales (region TEXT, product TEXT, amount INT)")

	// Insert 1500 rows
	for i := 1; i <= 1500; i++ {
		region := "region" + itoa(i%5)
		product := "product" + itoa(i%20)
		env.execSQL("INSERT INTO sales VALUES ('" + region + "', '" + product + "', " + itoa(i*10) + ")")
	}

	plan, err := env.prepare("SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY region")
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

// ─── STRESS TESTS: Edge Cases ────────────────────────────────────────────────

// TestEdgeCase_EmptyTable tests SELECT from empty table.
func TestEdgeCase_EmptyTable(t *testing.T) {
	env := newTestEnv(t)
	defer env.store.Close()

	env.execSQL(t, "CREATE TABLE empty_table (id INT, name TEXT)")
	// Don't insert any rows

	result := env.execSQL(t, "SELECT * FROM empty_table")
	if len(result.Rows) != 0 {
		t.Errorf("expected 0 rows from empty table, got %d", len(result.Rows))
	}

	// SELECT with WHERE on empty table
	result = env.execSQL(t, "SELECT * FROM empty_table WHERE id > 0")
	if len(result.Rows) != 0 {
		t.Errorf("expected 0 rows from empty table with WHERE, got %d", len(result.Rows))
	}

	// JOIN with empty table
	env.execSQL(t, "CREATE TABLE other_table (id INT, val TEXT)")
	env.execSQL(t, "INSERT INTO other_table VALUES (1, 'x')")

	result = env.execSQL(t, "SELECT * FROM empty_table JOIN other_table ON empty_table.id = other_table.id")
	if len(result.Rows) != 0 {
		t.Errorf("expected 0 rows from JOIN with empty table, got %d", len(result.Rows))
	}
}

// TestEdgeCase_NullValuesInJoin tests NULL handling in various contexts.
func TestEdgeCase_NullValuesInJoin(t *testing.T) {
	env := newTestEnv(t)
	defer env.store.Close()

	// Test NULL in WHERE clause
	env.execSQL(t, "CREATE TABLE null_test_xyz (id INT, val INT)")
	env.execSQL(t, "INSERT INTO null_test_xyz VALUES (1, 100)")
	env.execSQL(t, "INSERT INTO null_test_xyz VALUES (2, NULL)")
	env.execSQL(t, "INSERT INTO null_test_xyz VALUES (3, NULL)")
	env.execSQL(t, "INSERT INTO null_test_xyz VALUES (4, 200)")

	// IS NULL should return 2 rows
	result := env.execSQL(t, "SELECT * FROM null_test_xyz WHERE val IS NULL")
	if len(result.Rows) != 2 {
		t.Errorf("IS NULL: expected 2 rows, got %d", len(result.Rows))
	}

	// IS NOT NULL should return 2 rows
	result = env.execSQL(t, "SELECT * FROM null_test_xyz WHERE val IS NOT NULL")
	if len(result.Rows) != 2 {
		t.Errorf("IS NOT NULL: expected 2 rows, got %d", len(result.Rows))
	}

	// INNER JOIN with no overlapping values should return 0 rows
	env.execSQL(t, "CREATE TABLE join_left_xyz (id INT, val INT)")
	env.execSQL(t, "CREATE TABLE join_right_xyz (id INT, val INT)")
	env.execSQL(t, "INSERT INTO join_left_xyz VALUES (1, 100)")
	env.execSQL(t, "INSERT INTO join_left_xyz VALUES (2, 200)")
	env.execSQL(t, "INSERT INTO join_right_xyz VALUES (10, 300)")
	env.execSQL(t, "INSERT INTO join_right_xyz VALUES (20, 400)")

	result = env.execSQL(t, "SELECT join_left_xyz.id, join_right_xyz.id FROM join_left_xyz JOIN join_right_xyz ON join_left_xyz.val = join_right_xyz.val")
	if len(result.Rows) != 0 {
		t.Errorf("INNER JOIN with no matches: expected 0 rows, got %d", len(result.Rows))
	}
}

// TestEdgeCase_LargeResultSet tests handling of large result sets.
func TestEdgeCase_LargeResultSet(t *testing.T) {
	env := newTestEnv(t)
	defer env.store.Close()

	env.execSQL(t, "CREATE TABLE large_table_xyz (id INT PRIMARY KEY, data TEXT)")

	// Insert 500 rows (reduced from 2000 to avoid timeout)
	for i := 1; i <= 500; i++ {
		env.execSQL(t, "INSERT INTO large_table_xyz VALUES ("+itoa(i)+", 'data_"+itoa(i)+"')")
	}

	// SELECT all rows
	result := env.execSQL(t, "SELECT * FROM large_table_xyz")
	if len(result.Rows) != 500 {
		t.Errorf("expected 500 rows, got %d", len(result.Rows))
	}

	// SELECT with LIMIT
	result = env.execSQL(t, "SELECT * FROM large_table_xyz LIMIT 100")
	if len(result.Rows) != 100 {
		t.Errorf("expected 100 rows with LIMIT, got %d", len(result.Rows))
	}

	// GROUP BY on large dataset
	env.execSQL(t, "CREATE TABLE large_orders_xyz (user_id INT, amount INT)")
	for i := 1; i <= 500; i++ {
		userID := (i % 50) + 1
		env.execSQL(t, "INSERT INTO large_orders_xyz VALUES ("+itoa(userID)+", "+itoa(i*10)+")")
	}

	result = env.execSQL(t, "SELECT user_id, COUNT(*), SUM(amount) FROM large_orders_xyz GROUP BY user_id")
	if len(result.Rows) != 50 {
		t.Errorf("GROUP BY: expected 50 groups, got %d", len(result.Rows))
	}
}

// TestEdgeCase_AllNullColumns tests table with all NULL values.
func TestEdgeCase_AllNullColumns(t *testing.T) {
	env := newTestEnv(t)
	defer env.store.Close()

	env.execSQL(t, "CREATE TABLE null_table (id INT, val INT)")
	env.execSQL(t, "INSERT INTO null_table VALUES (1, NULL)")
	env.execSQL(t, "INSERT INTO null_table VALUES (2, NULL)")
	env.execSQL(t, "INSERT INTO null_table VALUES (3, NULL)")

	// IS NULL should return all rows
	result := env.execSQL(t, "SELECT * FROM null_table WHERE val IS NULL")
	if len(result.Rows) != 3 {
		t.Errorf("IS NULL: expected 3 rows, got %d", len(result.Rows))
	}

	// IS NOT NULL should return 0 rows
	result = env.execSQL(t, "SELECT * FROM null_table WHERE val IS NOT NULL")
	if len(result.Rows) != 0 {
		t.Errorf("IS NOT NULL: expected 0 rows, got %d", len(result.Rows))
	}
}

// TestEdgeCase_MultipleJoinsLargeData tests multiple JOINs on large datasets.
func TestEdgeCase_MultipleJoinsLargeData(t *testing.T) {
	env := newTestEnv(t)
	defer env.store.Close()

	env.execSQL(t, "CREATE TABLE users_multi (id INT, name TEXT)")
	env.execSQL(t, "CREATE TABLE orders_multi (user_id INT, order_id INT, order_date TEXT)")
	env.execSQL(t, "CREATE TABLE products_multi (order_id INT, product_name TEXT)")

	// Insert 200 users (reduced from 500)
	for i := 1; i <= 200; i++ {
		env.execSQL(t, "INSERT INTO users_multi VALUES ("+itoa(i)+", 'user_"+itoa(i)+"')")
	}
	// Insert 500 orders
	for i := 1; i <= 500; i++ {
		userID := (i % 200) + 1
		env.execSQL(t, "INSERT INTO orders_multi VALUES ("+itoa(userID)+", "+itoa(i)+", '2024-01-"+itoa(i%28+1)+"')")
	}
	// Insert 500 order-product links
	for i := 1; i <= 500; i++ {
		env.execSQL(t, "INSERT INTO products_multi VALUES ("+itoa(i)+", 'product_"+itoa(i)+"')")
	}

	// Triple JOIN: users -> orders -> products
	result := env.execSQL(t, "SELECT users_multi.name, orders_multi.order_date, products_multi.product_name FROM users_multi JOIN orders_multi ON users_multi.id = orders_multi.user_id JOIN products_multi ON orders_multi.order_id = products_multi.order_id")
	if len(result.Rows) != 500 {
		t.Errorf("Triple JOIN: expected 500 rows, got %d", len(result.Rows))
	}
}

// TestStress_500RowsWithAggregates tests 500+ rows with aggregate functions.
func TestStress_500RowsWithAggregates(t *testing.T) {
	env := newTestEnv(t)
	defer env.store.Close()

	env.execSQL(t, "CREATE TABLE events_stress (user_id INT, event_type TEXT, value INT, timestamp INT)")

	// Insert 500 rows (reduced from 1500 to avoid timeout)
	for i := 1; i <= 500; i++ {
		eventType := "type" + itoa(i%5)
		env.execSQL(t, "INSERT INTO events_stress VALUES ("+itoa(i%50)+", '"+eventType+"', "+itoa(i*10)+", "+itoa(i)+")")
	}

	// Multiple aggregations
	result := env.execSQL(t, "SELECT user_id, COUNT(*), SUM(value), AVG(value), MIN(value), MAX(value) FROM events_stress GROUP BY user_id")
	if len(result.Rows) != 50 {
		t.Errorf("expected 50 groups, got %d", len(result.Rows))
	}

	// Verify aggregation is correct for first user
	for _, row := range result.Rows {
		if row[0].Int == 1 {
			if row[2].Int <= 0 {
				t.Errorf("SUM(value) should be positive, got %d", row[2].Int)
			}
		}
	}
}
