package internal

import (
	"strings"
	"testing"

	sqlerrors "github.com/akzj/go-fast-kv/internal/sql/errors"
)

// ─── Parser Tests ─────────────────────────────────────────────────

func TestCheckConstraintParser(t *testing.T) {
	env := newTestEnv(t)

	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		// Column-level CHECK
		{"column check price > 0", "CREATE TABLE t1 (id INT PRIMARY KEY, price INT CHECK (price > 0))", false},
		{"column check quantity >= 0", "CREATE TABLE t2 (id INT PRIMARY KEY, quantity INT CHECK (quantity >= 0))", false},
		{"column check age BETWEEN", "CREATE TABLE t3 (id INT PRIMARY KEY, age INT CHECK (age BETWEEN 0 AND 150))", false},
		// Table-level CHECK
		{"table check price > 0", "CREATE TABLE t4 (id INT PRIMARY KEY, price INT, CHECK (price > 0))", false},
		// Multiple CHECK constraints
		{"multiple column checks", "CREATE TABLE t5 (id INT PRIMARY KEY, price INT CHECK (price > 0), quantity INT CHECK (quantity >= 0))", false},
		// Boolean expressions
		{"check with IS NOT NULL", "CREATE TABLE t6 (id INT PRIMARY KEY, name TEXT CHECK (name IS NOT NULL))", false},
		// Complex expressions
		{"check with LIKE", "CREATE TABLE t7 (id INT PRIMARY KEY, email TEXT CHECK (email LIKE '%@%'))", false},
		{"check with IN", "CREATE TABLE t8 (id INT PRIMARY KEY, status TEXT CHECK (status IN ('active', 'inactive')))", false},
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
			plan, err := env.planner.Plan(stmt)
			if err != nil {
				if !tt.wantErr {
					t.Errorf("plan error: %v", err)
				}
				return
			}
			if plan == nil {
				// Transaction control - not applicable here
				t.Errorf("expected plan, got nil")
				return
			}
			_, err = env.exec.Execute(plan)
			if tt.wantErr && err == nil {
				t.Errorf("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// ─── Basic Validation Tests ──────────────────────────────────────

func TestCheckConstraintBasic(t *testing.T) {
	env := newTestEnv(t)

	// Create table with CHECK constraint
	_, err := env.execSQLErr(t, "CREATE TABLE products (id INT PRIMARY KEY, price INT CHECK (price > 0))")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Insert valid data - should succeed
	_, err = env.execSQLErr(t, "INSERT INTO products VALUES (1, 10)")
	if err != nil {
		t.Errorf("insert valid: expected success, got %v", err)
	}

	// Insert valid data with price = 1 - should succeed
	_, err = env.execSQLErr(t, "INSERT INTO products VALUES (2, 1)")
	if err != nil {
		t.Errorf("insert price=1: expected success, got %v", err)
	}

	// Insert invalid data - price = 0 should fail
	_, err = env.execSQLErr(t, "INSERT INTO products VALUES (3, 0)")
	if err == nil {
		t.Errorf("insert price=0: expected error, got nil")
	} else if !strings.Contains(strings.ToLower(err.Error()), "check") {
		t.Errorf("error %q should mention check constraint", err.Error())
	}

	// Insert invalid data - negative price should fail
	_, err = env.execSQLErr(t, "INSERT INTO products VALUES (4, -5)")
	if err == nil {
		t.Errorf("insert negative: expected error, got nil")
	}
}

func TestCheckConstraintMultiple(t *testing.T) {
	env := newTestEnv(t)

	// Create table with multiple CHECK constraints
	_, err := env.execSQLErr(t, "CREATE TABLE orders (id INT PRIMARY KEY, quantity INT CHECK (quantity > 0), discount INT CHECK (discount >= 0))")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Valid insert
	_, err = env.execSQLErr(t, "INSERT INTO orders VALUES (1, 10, 5)")
	if err != nil {
		t.Errorf("valid insert: expected success, got %v", err)
	}

	// Invalid: quantity = 0
	_, err = env.execSQLErr(t, "INSERT INTO orders VALUES (2, 0, 5)")
	if err == nil {
		t.Errorf("quantity=0: expected error, got nil")
	}

	// Invalid: discount < 0
	_, err = env.execSQLErr(t, "INSERT INTO orders VALUES (3, 10, -1)")
	if err == nil {
		t.Errorf("discount=-1: expected error, got nil")
	}
}

func TestCheckConstraintUpdate(t *testing.T) {
	env := newTestEnv(t)

	// Create table with CHECK constraint
	_, err := env.execSQLErr(t, "CREATE TABLE items (id INT PRIMARY KEY, value INT CHECK (value >= 0))")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Insert valid initial data
	_, err = env.execSQLErr(t, "INSERT INTO items VALUES (1, 100)")
	if err != nil {
		t.Fatalf("insert initial: %v", err)
	}

	// Valid update
	_, err = env.execSQLErr(t, "UPDATE items SET value = 50 WHERE id = 1")
	if err != nil {
		t.Errorf("valid update: expected success, got %v", err)
	}

	// Update to negative value - should fail
	_, err = env.execSQLErr(t, "UPDATE items SET value = -10 WHERE id = 1")
	if err == nil {
		t.Errorf("update to negative: expected error, got nil")
	}

	// Update to zero - should pass (value >= 0, and 0 >= 0 is TRUE)
	_, err = env.execSQLErr(t, "UPDATE items SET value = 0 WHERE id = 1")
	if err != nil {
		t.Errorf("update to zero: expected success (0>=0 is TRUE), got %v", err)
	}
}

func TestCheckConstraintTableLevel(t *testing.T) {
	env := newTestEnv(t)

	// Create table with table-level CHECK constraint
	_, err := env.execSQLErr(t, "CREATE TABLE accounts1 (id INT PRIMARY KEY, balance INT, CHECK (balance >= 0))")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Valid insert
	_, err = env.execSQLErr(t, "INSERT INTO accounts1 VALUES (1, 1000)")
	if err != nil {
		t.Errorf("valid insert: expected success, got %v", err)
	}

	// Invalid insert
	_, err = env.execSQLErr(t, "INSERT INTO accounts1 VALUES (2, -100)")
	if err == nil {
		t.Errorf("negative balance: expected error, got nil")
	}
}

func TestCheckConstraintEdgeCases(t *testing.T) {
	env := newTestEnv(t)

	t.Run("NULL in check expression", func(t *testing.T) {
		_, err := env.execSQLErr(t, "CREATE TABLE t_age (id INT PRIMARY KEY, age INT CHECK (age IS NULL OR age >= 0))")
		if err != nil {
			t.Fatalf("create table: %v", err)
		}

		// NULL should pass the OR condition
		_, err = env.execSQLErr(t, "INSERT INTO t_age VALUES (1, NULL)")
		if err != nil {
			t.Errorf("NULL insert: expected success, got %v", err)
		}

		// Negative should fail
		_, err = env.execSQLErr(t, "INSERT INTO t_age VALUES (2, -1)")
		if err == nil {
			t.Errorf("negative: expected error, got nil")
		}
	})

	t.Run("CHECK with comparison", func(t *testing.T) {
		_, err := env.execSQLErr(t, "CREATE TABLE t_range1 (id INT PRIMARY KEY, low INT, high INT CHECK (high > low))")
		if err != nil {
			t.Fatalf("create table: %v", err)
		}

		// Valid: max > min
		_, err = env.execSQLErr(t, "INSERT INTO t_range1 VALUES (1, 1, 10)")
		if err != nil {
			t.Errorf("valid: expected success, got %v", err)
		}

		// Invalid: max <= min
		_, err = env.execSQLErr(t, "INSERT INTO t_range1 VALUES (2, 10, 5)")
		if err == nil {
			t.Errorf("max <= min: expected error, got nil")
		}

		// Invalid: max == min
		_, err = env.execSQLErr(t, "INSERT INTO t_range1 VALUES (3, 5, 5)")
		if err == nil {
			t.Errorf("max == min: expected error, got nil")
		}
	})

	t.Run("CHECK with logical operators", func(t *testing.T) {
		_, err := env.execSQLErr(t, "CREATE TABLE t_score (id INT PRIMARY KEY, score INT CHECK (score >= 0 AND score <= 100))")
		if err != nil {
			t.Fatalf("create table: %v", err)
		}

		// Valid: score in range
		_, err = env.execSQLErr(t, "INSERT INTO t_score VALUES (1, 50)")
		if err != nil {
			t.Errorf("valid: expected success, got %v", err)
		}

		// Invalid: score < 0
		_, err = env.execSQLErr(t, "INSERT INTO t_score VALUES (2, -1)")
		if err == nil {
			t.Errorf("score < 0: expected error, got nil")
		}

		// Invalid: score > 100
		_, err = env.execSQLErr(t, "INSERT INTO t_score VALUES (3, 101)")
		if err == nil {
			t.Errorf("score > 100: expected error, got nil")
		}
	})

	t.Run("CHECK with OR", func(t *testing.T) {
		_, err := env.execSQLErr(t, "CREATE TABLE t_status (id INT PRIMARY KEY, status TEXT CHECK (status = 'active' OR status = 'inactive'))")
		if err != nil {
			t.Fatalf("create table: %v", err)
		}

		// Valid: status = 'active'
		_, err = env.execSQLErr(t, "INSERT INTO t_status VALUES (1, 'active')")
		if err != nil {
			t.Errorf("active: expected success, got %v", err)
		}

		// Valid: status = 'inactive'
		_, err = env.execSQLErr(t, "INSERT INTO t_status VALUES (2, 'inactive')")
		if err != nil {
			t.Errorf("inactive: expected success, got %v", err)
		}

		// Invalid: status not in list
		_, err = env.execSQLErr(t, "INSERT INTO t_status VALUES (3, 'pending')")
		if err == nil {
			t.Errorf("pending: expected error, got nil")
		}
	})
}

// ─── Error Type Tests ─────────────────────────────────────────────

func TestCheckConstraintErrorType(t *testing.T) {
	env := newTestEnv(t)

	_, err := env.execSQLErr(t, "CREATE TABLE t_check (id INT PRIMARY KEY, val INT CHECK (val > 0))")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	_, err = env.execSQLErr(t, "INSERT INTO t_check VALUES (1, -1)")
	if err == nil {
		t.Fatalf("expected error, got nil")
	}

	// Should be a SQL error with check constraint info
	sqlErr, ok := err.(*sqlerrors.SQLError)
	if !ok {
		t.Logf("error type: %T", err)
		return
	}

	if sqlErr.SQLState != "23522" {
		t.Errorf("SQLState = %q, want 23522", sqlErr.SQLState)
	}
}

func TestCheckConstraintErrorMessage(t *testing.T) {
	env := newTestEnv(t)

	_, err := env.execSQLErr(t, "CREATE TABLE t_msg (id INT PRIMARY KEY, price INT CHECK (price > 0))")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	_, err = env.execSQLErr(t, "INSERT INTO t_msg VALUES (1, -5)")
	if err == nil {
		t.Fatalf("expected error, got nil")
	}

	msg := err.Error()
	// Error should mention the constraint
	if !strings.Contains(strings.ToLower(msg), "check") {
		t.Errorf("error %q should mention check constraint", msg)
	}
}
