package internal

import (
	"testing"
)

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
