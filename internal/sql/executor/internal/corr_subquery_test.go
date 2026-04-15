package internal

import (
	"testing"
)

func TestCorrelatedSubqueryJoinWhere(t *testing.T) {
	env := newTestEnv(t)

	env.execSQL(t, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	env.execSQL(t, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT)")
	env.execSQL(t, "CREATE TABLE products (id INT PRIMARY KEY, name TEXT)")

	// users: 1=alice, 2=bob, 3=carol
	env.execSQL(t, "INSERT INTO users VALUES (1, 'alice')")
	env.execSQL(t, "INSERT INTO users VALUES (2, 'bob')")
	env.execSQL(t, "INSERT INTO users VALUES (3, 'carol')")

	// orders: user_id 1 (alice) has order, user_id 2 (bob) has order
	env.execSQL(t, "INSERT INTO orders VALUES (1, 1, 100)")
	env.execSQL(t, "INSERT INTO orders VALUES (2, 2, 150)")

	// products: id 1 and 2 exist
	env.execSQL(t, "INSERT INTO products VALUES (1, 'widget')")
	env.execSQL(t, "INSERT INTO products VALUES (2, 'gadget')")

	// Simple correlated subquery test first - users.id in subquery referencing outer users table
	t.Log("Testing simple correlated subquery (no JOIN)...")
	r1 := env.execSQL(t, "SELECT name FROM users WHERE (SELECT COUNT(*) FROM products WHERE products.id = users.id) > 0")
	t.Logf("Simple test result: %d rows", len(r1.Rows))
	for _, row := range r1.Rows {
		t.Logf("  - name: %s", row[0].Text)
	}
	if len(r1.Rows) != 2 {
		t.Errorf("Simple test: expected 2 rows, got %d", len(r1.Rows))
	}

	// Now test the JOIN case
	t.Log("Testing JOIN with correlated subquery...")
	
	// First, let's test without the correlated subquery to make sure basic JOIN works
	rBasic := env.execSQL(t, "SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id")
	t.Logf("Basic JOIN (no WHERE): %d rows", len(rBasic.Rows))
	for _, row := range rBasic.Rows {
		t.Logf("  - name: %s, amount: %d", row[0].Text, row[1].Int)
	}
	if len(rBasic.Rows) != 2 {
		t.Errorf("Basic JOIN: expected 2 rows, got %d", len(rBasic.Rows))
	}
	
	// Test with a simple non-correlated WHERE first
	rSimpleWhere := env.execSQL(t, "SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id WHERE users.id = 1")
	t.Logf("JOIN with simple WHERE (users.id = 1): %d rows", len(rSimpleWhere.Rows))
	for _, row := range rSimpleWhere.Rows {
		t.Logf("  - name: %s, amount: %d", row[0].Text, row[1].Int)
	}
	
	r := env.execSQL(t, "SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id WHERE (SELECT COUNT(*) FROM products WHERE products.id = users.id) > 0")
	t.Logf("JOIN test result: %d rows", len(r.Rows))
	for _, row := range r.Rows {
		t.Logf("  - name: %s, amount: %d", row[0].Text, row[1].Int)
	}

	// alice and bob both have orders and their ids exist in products table
	if len(r.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(r.Rows))
	}

	// Verify we got alice and bob
	names := make(map[string]bool)
	for _, row := range r.Rows {
		names[row[0].Text] = true
	}
	if !names["alice"] {
		t.Errorf("expected alice in results, got: %v", names)
	}
	if !names["bob"] {
		t.Errorf("expected bob in results, got: %v", names)
	}
}

func TestCorrelatedSubqueryOrderCount(t *testing.T) {
	env := newTestEnv(t)

	env.execSQL(t, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	env.execSQL(t, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT)")

	env.execSQL(t, "INSERT INTO users VALUES (1, 'alice')")
	env.execSQL(t, "INSERT INTO users VALUES (2, 'bob')")
	env.execSQL(t, "INSERT INTO users VALUES (3, 'carol')")
	env.execSQL(t, "INSERT INTO users VALUES (4, 'dave')")

	// alice has 2 orders, bob has 1, carol has 0, dave has 3
	env.execSQL(t, "INSERT INTO orders VALUES (1, 1, 100)")
	env.execSQL(t, "INSERT INTO orders VALUES (2, 1, 200)")
	env.execSQL(t, "INSERT INTO orders VALUES (3, 2, 150)")
	env.execSQL(t, "INSERT INTO orders VALUES (4, 4, 50)")
	env.execSQL(t, "INSERT INTO orders VALUES (5, 4, 75)")
	env.execSQL(t, "INSERT INTO orders VALUES (6, 4, 125)")

	r := env.execSQL(t, "SELECT name FROM users WHERE (SELECT COUNT(*) FROM orders WHERE orders.user_id = users.id) > 1")

	if len(r.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(r.Rows))
	}

	expected := []string{"alice", "dave"}
	for i, name := range expected {
		if i >= len(r.Rows) {
			t.Errorf("missing row at index %d: expected name %q", i, name)
			continue
		}
		got := r.Rows[i][0].Text
		if got != name {
			t.Errorf("row[%d]: expected name %q, got %q", i, name, got)
		}
	}
}
