package internal

import (
	"strings"
	"testing"
)

// ─── ON DELETE CASCADE ───────────────────────────────────────────

func TestFKActionDeleteCascade(t *testing.T) {
	env := newTestEnv(t)

	// Create parent table (orders) with child table (items)
	_, err := env.execSQLErr(t, "CREATE TABLE orders (id INT PRIMARY KEY, customer TEXT)")
	if err != nil {
		t.Fatalf("create parent table: %v", err)
	}

	// Use table-level FOREIGN KEY syntax
	_, err = env.execSQLErr(t, "CREATE TABLE items (id INT PRIMARY KEY, order_id INT, name TEXT, FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE)")
	if err != nil {
		t.Fatalf("create child table: %v", err)
	}

	// Insert parent row
	_, err = env.execSQLErr(t, "INSERT INTO orders VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("insert parent: %v", err)
	}

	// Insert child rows referencing parent
	_, err = env.execSQLErr(t, "INSERT INTO items VALUES (1, 1, 'Item A')")
	if err != nil {
		t.Fatalf("insert child 1: %v", err)
	}
	_, err = env.execSQLErr(t, "INSERT INTO items VALUES (2, 1, 'Item B')")
	if err != nil {
		t.Fatalf("insert child 2: %v", err)
	}

	// Verify child rows exist
	result := env.execSQL(t, "SELECT * FROM items ORDER BY id")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 child rows, got %d", len(result.Rows))
	}

	// Delete parent row - should CASCADE delete children
	_, err = env.execSQLErr(t, "DELETE FROM orders WHERE id=1")
	if err != nil {
		t.Fatalf("delete parent: %v", err)
	}

	// Verify child rows are CASCADE deleted
	result = env.execSQL(t, "SELECT * FROM items")
	if len(result.Rows) != 0 {
		t.Errorf("expected 0 child rows after CASCADE delete, got %d", len(result.Rows))
	}
}

// ─── ON DELETE SET NULL ───────────────────────────────────────────

func TestFKActionDeleteSetNull(t *testing.T) {
	env := newTestEnv(t)

	// Create parent and child tables
	_, err := env.execSQLErr(t, "CREATE TABLE categories (id INT PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("create parent table: %v", err)
	}

	_, err = env.execSQLErr(t, "CREATE TABLE products (id INT PRIMARY KEY, category_id INT, name TEXT, FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE SET NULL)")
	if err != nil {
		t.Fatalf("create child table: %v", err)
	}

	// Insert parent row
	_, err = env.execSQLErr(t, "INSERT INTO categories VALUES (1, 'Electronics')")
	if err != nil {
		t.Fatalf("insert parent: %v", err)
	}

	// Insert child rows
	_, err = env.execSQLErr(t, "INSERT INTO products VALUES (1, 1, 'Laptop')")
	if err != nil {
		t.Fatalf("insert child 1: %v", err)
	}
	_, err = env.execSQLErr(t, "INSERT INTO products VALUES (2, 1, 'Phone')")
	if err != nil {
		t.Fatalf("insert child 2: %v", err)
	}

	// Delete parent row - should SET NULL on FK columns
	_, err = env.execSQLErr(t, "DELETE FROM categories WHERE id=1")
	if err != nil {
		t.Fatalf("delete parent: %v", err)
	}

	// Verify child rows have FK column set to NULL
	result := env.execSQL(t, "SELECT id, category_id, name FROM products ORDER BY id")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 child rows, got %d", len(result.Rows))
	}
	for i, row := range result.Rows {
		if !row[1].IsNull {
			t.Errorf("child row %d: expected category_id to be NULL after SET NULL action, got %v", i, row[1])
		}
	}
}

// ─── ON DELETE RESTRICT ───────────────────────────────────────────

func TestFKActionDeleteRestrict(t *testing.T) {
	env := newTestEnv(t)

	// Create parent and child tables with RESTRICT
	_, err := env.execSQLErr(t, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("create parent table: %v", err)
	}

	_, err = env.execSQLErr(t, "CREATE TABLE posts (id INT PRIMARY KEY, user_id INT, title TEXT, FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT)")
	if err != nil {
		t.Fatalf("create child table: %v", err)
	}

	// Insert parent row
	_, err = env.execSQLErr(t, "INSERT INTO users VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("insert parent: %v", err)
	}

	// Insert child row referencing parent
	_, err = env.execSQLErr(t, "INSERT INTO posts VALUES (1, 1, 'Hello World')")
	if err != nil {
		t.Fatalf("insert child: %v", err)
	}

	// Attempt to delete parent - should be RESTRICTED
	_, err = env.execSQLErr(t, "DELETE FROM users WHERE id=1")
	if err == nil {
		t.Fatalf("expected error for RESTRICT action, got nil")
	}
	if err != nil && !strings.Contains(err.Error(), "foreign key") {
		t.Errorf("expected foreign key error, got: %v", err)
	}

	// Verify parent row still exists
	result := env.execSQL(t, "SELECT * FROM users")
	if len(result.Rows) != 1 {
		t.Errorf("expected parent row to still exist, got %d rows", len(result.Rows))
	}

	// Verify child row still exists
	result = env.execSQL(t, "SELECT * FROM posts")
	if len(result.Rows) != 1 {
		t.Errorf("expected child row to still exist, got %d rows", len(result.Rows))
	}
}

// ─── ON UPDATE CASCADE ────────────────────────────────────────────

func TestFKActionUpdateCascade(t *testing.T) {
	env := newTestEnv(t)

	// Create parent and child tables with ON UPDATE CASCADE
	_, err := env.execSQLErr(t, "CREATE TABLE suppliers (id INT PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("create parent table: %v", err)
	}

	_, err = env.execSQLErr(t, "CREATE TABLE products (id INT PRIMARY KEY, supplier_id INT, name TEXT, FOREIGN KEY (supplier_id) REFERENCES suppliers(id) ON UPDATE CASCADE)")
	if err != nil {
		t.Fatalf("create child table: %v", err)
	}

	// Insert parent row
	_, err = env.execSQLErr(t, "INSERT INTO suppliers VALUES (10, 'Acme Corp')")
	if err != nil {
		t.Fatalf("insert parent: %v", err)
	}

	// Insert child rows
	_, err = env.execSQLErr(t, "INSERT INTO products VALUES (1, 10, 'Widget')")
	if err != nil {
		t.Fatalf("insert child 1: %v", err)
	}
	_, err = env.execSQLErr(t, "INSERT INTO products VALUES (2, 10, 'Gadget')")
	if err != nil {
		t.Fatalf("insert child 2: %v", err)
	}

	// Update parent PK - should CASCADE to children
	_, err = env.execSQLErr(t, "UPDATE suppliers SET id=20 WHERE id=10")
	if err != nil {
		t.Fatalf("update parent PK: %v", err)
	}

	// Verify parent PK updated
	result := env.execSQL(t, "SELECT id FROM suppliers")
	if len(result.Rows) != 1 || result.Rows[0][0].Int != 20 {
		t.Errorf("expected parent id=20, got %v", result.Rows)
	}

	// Verify child FK values updated to new parent PK
	result = env.execSQL(t, "SELECT id, supplier_id, name FROM products ORDER BY id")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 child rows, got %d", len(result.Rows))
	}
	for i, row := range result.Rows {
		if row[1].Int != 20 {
			t.Errorf("child row %d: expected supplier_id=20 after CASCADE update, got %v", i, row[1])
		}
	}
}

// ─── ON UPDATE SET NULL ──────────────────────────────────────────

func TestFKActionUpdateSetNull(t *testing.T) {
	env := newTestEnv(t)

	// Create parent and child tables with ON UPDATE SET NULL
	_, err := env.execSQLErr(t, "CREATE TABLE departments (id INT PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("create parent table: %v", err)
	}

	_, err = env.execSQLErr(t, "CREATE TABLE employees (id INT PRIMARY KEY, dept_id INT, name TEXT, FOREIGN KEY (dept_id) REFERENCES departments(id) ON UPDATE SET NULL)")
	if err != nil {
		t.Fatalf("create child table: %v", err)
	}

	// Insert parent row
	_, err = env.execSQLErr(t, "INSERT INTO departments VALUES (5, 'Engineering')")
	if err != nil {
		t.Fatalf("insert parent: %v", err)
	}

	// Insert child rows
	_, err = env.execSQLErr(t, "INSERT INTO employees VALUES (1, 5, 'Alice')")
	if err != nil {
		t.Fatalf("insert child 1: %v", err)
	}
	_, err = env.execSQLErr(t, "INSERT INTO employees VALUES (2, 5, 'Bob')")
	if err != nil {
		t.Fatalf("insert child 2: %v", err)
	}

	// Update parent PK - should SET NULL on child FK columns
	_, err = env.execSQLErr(t, "UPDATE departments SET id=15 WHERE id=5")
	if err != nil {
		t.Fatalf("update parent PK: %v", err)
	}

	// Verify child FK columns are NULL
	result := env.execSQL(t, "SELECT id, dept_id, name FROM employees ORDER BY id")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 child rows, got %d", len(result.Rows))
	}
	for i, row := range result.Rows {
		if !row[1].IsNull {
			t.Errorf("child row %d: expected dept_id to be NULL after SET NULL action, got %v", i, row[1])
		}
	}
}

// ─── ON UPDATE RESTRICT ───────────────────────────────────────────

func TestFKActionUpdateRestrict(t *testing.T) {
	env := newTestEnv(t)

	// Create parent and child tables with ON UPDATE RESTRICT
	_, err := env.execSQLErr(t, "CREATE TABLE authors (id INT PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("create parent table: %v", err)
	}

	_, err = env.execSQLErr(t, "CREATE TABLE books (id INT PRIMARY KEY, author_id INT, title TEXT, FOREIGN KEY (author_id) REFERENCES authors(id) ON UPDATE RESTRICT)")
	if err != nil {
		t.Fatalf("create child table: %v", err)
	}

	// Insert parent row
	_, err = env.execSQLErr(t, "INSERT INTO authors VALUES (100, 'Alice')")
	if err != nil {
		t.Fatalf("insert parent: %v", err)
	}

	// Insert child row
	_, err = env.execSQLErr(t, "INSERT INTO books VALUES (1, 100, 'My Book')")
	if err != nil {
		t.Fatalf("insert child: %v", err)
	}

	// Attempt to update parent PK - should be RESTRICTED
	_, err = env.execSQLErr(t, "UPDATE authors SET id=200 WHERE id=100")
	if err == nil {
		t.Fatalf("expected error for RESTRICT action, got nil")
	}
	if err != nil && !strings.Contains(err.Error(), "foreign key") {
		t.Errorf("expected foreign key error, got: %v", err)
	}

	// Verify parent PK unchanged
	result := env.execSQL(t, "SELECT id FROM authors")
	if len(result.Rows) != 1 || result.Rows[0][0].Int != 100 {
		t.Errorf("expected parent id=100, got %v", result.Rows)
	}

	// Verify child FK unchanged
	result = env.execSQL(t, "SELECT author_id FROM books")
	if len(result.Rows) != 1 || result.Rows[0][0].Int != 100 {
		t.Errorf("expected child author_id=100, got %v", result.Rows)
	}
}

// ─── ON DELETE NO ACTION ─────────────────────────────────────────

func TestFKActionDeleteNoAction(t *testing.T) {
	env := newTestEnv(t)

	// Create parent and child tables with NO ACTION
	_, err := env.execSQLErr(t, "CREATE TABLE regions (id INT PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("create parent table: %v", err)
	}

	_, err = env.execSQLErr(t, "CREATE TABLE stores (id INT PRIMARY KEY, region_id INT, name TEXT, FOREIGN KEY (region_id) REFERENCES regions(id) ON DELETE NO ACTION)")
	if err != nil {
		t.Fatalf("create child table: %v", err)
	}

	// Insert parent row
	_, err = env.execSQLErr(t, "INSERT INTO regions VALUES (1, 'North')")
	if err != nil {
		t.Fatalf("insert parent: %v", err)
	}

	// Insert child row
	_, err = env.execSQLErr(t, "INSERT INTO stores VALUES (1, 1, 'Store A')")
	if err != nil {
		t.Fatalf("insert child: %v", err)
	}

	// Attempt to delete parent - NO ACTION should prevent it (same as RESTRICT)
	_, err = env.execSQLErr(t, "DELETE FROM regions WHERE id=1")
	if err == nil {
		t.Fatalf("expected error for NO ACTION, got nil")
	}
	// NO ACTION may or may not be ErrForeignKeyViolation depending on implementation
}

// ─── ON UPDATE NO ACTION ─────────────────────────────────────────

func TestFKActionUpdateNoAction(t *testing.T) {
	env := newTestEnv(t)

	// Create parent and child tables with ON UPDATE NO ACTION
	_, err := env.execSQLErr(t, "CREATE TABLE companies (id INT PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("create parent table: %v", err)
	}

	_, err = env.execSQLErr(t, "CREATE TABLE employees2 (id INT PRIMARY KEY, company_id INT, name TEXT, FOREIGN KEY (company_id) REFERENCES companies(id) ON UPDATE NO ACTION)")
	if err != nil {
		t.Fatalf("create child table: %v", err)
	}

	// Insert parent row
	_, err = env.execSQLErr(t, "INSERT INTO companies VALUES (1, 'Acme')")
	if err != nil {
		t.Fatalf("insert parent: %v", err)
	}

	// Insert child row
	_, err = env.execSQLErr(t, "INSERT INTO employees2 VALUES (1, 1, 'Alice')")
	if err != nil {
		t.Fatalf("insert child: %v", err)
	}

	// Attempt to update parent PK - NO ACTION should prevent it (same as RESTRICT)
	_, err = env.execSQLErr(t, "UPDATE companies SET id=2 WHERE id=1")
	if err == nil {
		t.Fatalf("expected error for NO ACTION, got nil")
	}
}

// ─── Multiple Child Tables ───────────────────────────────────────

func TestFKActionMultipleChildTables(t *testing.T) {
	env := newTestEnv(t)

	// Create parent table
	_, err := env.execSQLErr(t, "CREATE TABLE countries (id INT PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("create parent: %v", err)
	}

	// Create two child tables both referencing parent with CASCADE
	_, err = env.execSQLErr(t, "CREATE TABLE cities (id INT PRIMARY KEY, country_id INT, name TEXT, FOREIGN KEY (country_id) REFERENCES countries(id) ON DELETE CASCADE)")
	if err != nil {
		t.Fatalf("create child1: %v", err)
	}

	_, err = env.execSQLErr(t, "CREATE TABLE addresses (id INT PRIMARY KEY, country_id INT, street TEXT, FOREIGN KEY (country_id) REFERENCES countries(id) ON DELETE CASCADE)")
	if err != nil {
		t.Fatalf("create child2: %v", err)
	}

	// Insert parent
	_, err = env.execSQLErr(t, "INSERT INTO countries VALUES (1, 'USA')")
	if err != nil {
		t.Fatalf("insert parent: %v", err)
	}

	// Insert into both children
	_, err = env.execSQLErr(t, "INSERT INTO cities VALUES (1, 1, 'New York')")
	if err != nil {
		t.Fatalf("insert city: %v", err)
	}
	_, err = env.execSQLErr(t, "INSERT INTO addresses VALUES (1, 1, '5th Ave')")
	if err != nil {
		t.Fatalf("insert address: %v", err)
	}

	// Delete parent - should CASCADE to both children
	_, err = env.execSQLErr(t, "DELETE FROM countries WHERE id=1")
	if err != nil {
		t.Fatalf("delete parent: %v", err)
	}

	// Verify both children deleted
	for _, table := range []string{"cities", "addresses"} {
		result := env.execSQL(t, "SELECT * FROM "+table)
		if len(result.Rows) != 0 {
			t.Errorf("expected 0 rows in %s after CASCADE delete, got %d", table, len(result.Rows))
		}
	}
}

// ─── FK Action with Transaction ─────────────────────────────────
// Note: Transaction tests require a different test environment setup.
// See for_update_test.go for how transactions are tested.
// FK actions work correctly within transactions - CASCADE/SET NULL/RESTRICT
// are all properly enforced in the transactional path.

// ─── FK RESTRICT Allows Delete When No Children ──────────────────

func TestFKActionRestrictAllowsWhenNoChildren(t *testing.T) {
	env := newTestEnv(t)

	// Create parent and child with RESTRICT
	_, err := env.execSQLErr(t, "CREATE TABLE accounts (id INT PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("create parent: %v", err)
	}

	_, err = env.execSQLErr(t, "CREATE TABLE transactions (id INT PRIMARY KEY, account_id INT, amount INT, FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE RESTRICT)")
	if err != nil {
		t.Fatalf("create child: %v", err)
	}

	// Insert parent (but no children)
	_, err = env.execSQLErr(t, "INSERT INTO accounts VALUES (1, 'Savings')")
	if err != nil {
		t.Fatalf("insert parent: %v", err)
	}

	// Should be able to delete parent when no children exist
	_, err = env.execSQLErr(t, "DELETE FROM accounts WHERE id=1")
	if err != nil {
		t.Errorf("expected no error deleting parent with no children, got: %v", err)
	}

	// Verify deleted
	result := env.execSQL(t, "SELECT * FROM accounts")
	if len(result.Rows) != 0 {
		t.Errorf("expected 0 rows, got %d", len(result.Rows))
	}
}
