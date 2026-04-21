package internal

import (
	"testing"
)

// TestExec_Substring tests SUBSTRING function with both syntaxes.
func TestExec_Substring(t *testing.T) {
	env := newTestEnv(t)
	defer env.store.Close()

	env.execSQL(t, "CREATE TABLE t (id INT PRIMARY KEY, str TEXT)")

	t.Run("substring_comma_syntax", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		env.execSQL(t, "INSERT INTO t VALUES (1, 'hello')")

		// SUBSTRING(str, start, len)
		result := env.execSQL(t, "SELECT SUBSTRING(str, 1, 3) FROM t")
		if len(result.Rows) != 1 {
			t.Fatalf("rows = %d, want 1", len(result.Rows))
		}
		if result.Rows[0][0].Text != "hel" {
			t.Errorf("SUBSTRING('hello', 1, 3) = %q, want 'hel'", result.Rows[0][0].Text)
		}

		// SUBSTRING(str, 2, 2) → "el"
		result = env.execSQL(t, "SELECT SUBSTRING(str, 2, 2) FROM t")
		if result.Rows[0][0].Text != "el" {
			t.Errorf("SUBSTRING('hello', 2, 2) = %q, want 'el'", result.Rows[0][0].Text)
		}

		// SUBSTRING(str, 2) → "ello" (no length = rest of string)
		result = env.execSQL(t, "SELECT SUBSTRING(str, 2) FROM t")
		if result.Rows[0][0].Text != "ello" {
			t.Errorf("SUBSTRING('hello', 2) = %q, want 'ello'", result.Rows[0][0].Text)
		}
	})

	t.Run("substring_from_for_syntax", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		env.execSQL(t, "INSERT INTO t VALUES (1, 'world')")

		// SUBSTRING(str FROM start FOR len)
		result := env.execSQL(t, "SELECT SUBSTRING(str FROM 2 FOR 3) FROM t")
		if result.Rows[0][0].Text != "orl" {
			t.Errorf("SUBSTRING('world', 2, 3) = %q, want 'orl'", result.Rows[0][0].Text)
		}

		// SUBSTRING(str FROM start) — no FOR
		result = env.execSQL(t, "SELECT SUBSTRING(str FROM 3) FROM t")
		if result.Rows[0][0].Text != "rld" {
			t.Errorf("SUBSTRING('world', 3) = %q, want 'rld'", result.Rows[0][0].Text)
		}
	})

	t.Run("substring_null", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		env.execSQL(t, "INSERT INTO t VALUES (1, NULL)")

		result := env.execSQL(t, "SELECT SUBSTRING(str, 1, 3) FROM t")
		if !result.Rows[0][0].IsNull {
			t.Errorf("SUBSTRING(NULL, ...) should be NULL")
		}
	})

	t.Run("substring_out_of_bounds", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		env.execSQL(t, "INSERT INTO t VALUES (1, 'hi')")

		// Start beyond string length
		result := env.execSQL(t, "SELECT SUBSTRING(str, 10, 5) FROM t")
		if result.Rows[0][0].Text != "" {
			t.Errorf("SUBSTRING('hi', 10, 5) = %q, want ''", result.Rows[0][0].Text)
		}

		// Start at position > length returns empty
		result = env.execSQL(t, "SELECT SUBSTRING(str, 3) FROM t")
		if result.Rows[0][0].Text != "" {
			t.Errorf("SUBSTRING('hi', 3) = %q, want ''", result.Rows[0][0].Text)
		}
	})
}

// TestExec_Concat tests CONCAT function.
func TestExec_Concat(t *testing.T) {
	env := newTestEnv(t)
	defer env.store.Close()

	env.execSQL(t, "CREATE TABLE t (id INT PRIMARY KEY, a TEXT, b TEXT)")

	t.Run("concat_strings", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		env.execSQL(t, "INSERT INTO t VALUES (1, 'hello', 'world')")

		result := env.execSQL(t, "SELECT CONCAT(a, b) FROM t")
		if result.Rows[0][0].Text != "helloworld" {
			t.Errorf("CONCAT('hello', 'world') = %q, want 'helloworld'", result.Rows[0][0].Text)
		}
	})

	t.Run("concat_multiple", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		env.execSQL(t, "INSERT INTO t VALUES (1, 'a', 'b')")

		result := env.execSQL(t, "SELECT CONCAT(a, '-', b, '-', 'c') FROM t")
		if result.Rows[0][0].Text != "a-b-c" {
			t.Errorf("CONCAT('a', '-', 'b', '-', 'c') = %q, want 'a-b-c'", result.Rows[0][0].Text)
		}
	})

	t.Run("concat_null", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		env.execSQL(t, "INSERT INTO t VALUES (1, 'hello', NULL)")

		result := env.execSQL(t, "SELECT CONCAT(a, b) FROM t")
		if !result.Rows[0][0].IsNull {
			t.Errorf("CONCAT with NULL should return NULL")
		}
	})

	t.Run("concat_integers", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		env.execSQL(t, "INSERT INTO t VALUES (1, 'id:', '123')")

		// Integer in column (simulated with TEXT)
		result := env.execSQL(t, "SELECT CONCAT(a, b) FROM t")
		if result.Rows[0][0].Text != "id:123" {
			t.Errorf("CONCAT(...) = %q, want 'id:123'", result.Rows[0][0].Text)
		}
	})
}

// TestExec_UpperLower tests UPPER and LOWER functions.
func TestExec_UpperLower(t *testing.T) {
	env := newTestEnv(t)
	defer env.store.Close()

	env.execSQL(t, "CREATE TABLE t (id INT PRIMARY KEY, str TEXT)")

	t.Run("upper", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		env.execSQL(t, "INSERT INTO t VALUES (1, 'hello World')")

		result := env.execSQL(t, "SELECT UPPER(str) FROM t")
		if result.Rows[0][0].Text != "HELLO WORLD" {
			t.Errorf("UPPER('hello World') = %q, want 'HELLO WORLD'", result.Rows[0][0].Text)
		}
	})

	t.Run("lower", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		env.execSQL(t, "INSERT INTO t VALUES (1, 'HELLO World')")

		result := env.execSQL(t, "SELECT LOWER(str) FROM t")
		if result.Rows[0][0].Text != "hello world" {
			t.Errorf("LOWER('HELLO World') = %q, want 'hello world'", result.Rows[0][0].Text)
		}
	})

	t.Run("upper_lower_null", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		env.execSQL(t, "INSERT INTO t VALUES (1, NULL)")

		result := env.execSQL(t, "SELECT UPPER(str) FROM t")
		if !result.Rows[0][0].IsNull {
			t.Errorf("UPPER(NULL) should be NULL")
		}
	})
}

// TestExec_Length tests LENGTH function.
func TestExec_Length(t *testing.T) {
	env := newTestEnv(t)
	defer env.store.Close()

	env.execSQL(t, "CREATE TABLE t (id INT PRIMARY KEY, str TEXT)")

	t.Run("length_basic", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		env.execSQL(t, "INSERT INTO t VALUES (1, 'hello')")

		result := env.execSQL(t, "SELECT LENGTH(str) FROM t")
		if result.Rows[0][0].Int != 5 {
			t.Errorf("LENGTH('hello') = %d, want 5", result.Rows[0][0].Int)
		}
	})

	t.Run("length_empty", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		env.execSQL(t, "INSERT INTO t VALUES (1, '')")

		result := env.execSQL(t, "SELECT LENGTH(str) FROM t")
		if result.Rows[0][0].Int != 0 {
			t.Errorf("LENGTH('') = %d, want 0", result.Rows[0][0].Int)
		}
	})

	t.Run("length_unicode", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		env.execSQL(t, "INSERT INTO t VALUES (1, 'héllo')")

		// Note: Go's len() counts bytes, not runes
		result := env.execSQL(t, "SELECT LENGTH(str) FROM t")
		// "héllo" is 6 bytes in UTF-8 (h=1, é=2, l=1, l=1, o=1)
		if result.Rows[0][0].Int != 6 {
			t.Errorf("LENGTH('héllo') = %d, want 6 (bytes)", result.Rows[0][0].Int)
		}
	})

	t.Run("length_null", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		env.execSQL(t, "INSERT INTO t VALUES (1, NULL)")

		result := env.execSQL(t, "SELECT LENGTH(str) FROM t")
		if !result.Rows[0][0].IsNull {
			t.Errorf("LENGTH(NULL) should be NULL")
		}
	})
}

// TestExec_Trim tests TRIM function.
func TestExec_Trim(t *testing.T) {
	env := newTestEnv(t)
	defer env.store.Close()

	env.execSQL(t, "CREATE TABLE t (id INT PRIMARY KEY, str TEXT)")

	t.Run("trim_basic", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		env.execSQL(t, "INSERT INTO t VALUES (1, '  hello  ')")

		result := env.execSQL(t, "SELECT TRIM(str) FROM t")
		if result.Rows[0][0].Text != "hello" {
			t.Errorf("TRIM('  hello  ') = %q, want 'hello'", result.Rows[0][0].Text)
		}
	})

	t.Run("trim_leading", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		env.execSQL(t, "INSERT INTO t VALUES (1, '   hello')")

		result := env.execSQL(t, "SELECT TRIM(str) FROM t")
		if result.Rows[0][0].Text != "hello" {
			t.Errorf("TRIM('   hello') = %q, want 'hello'", result.Rows[0][0].Text)
		}
	})

	t.Run("trim_trailing", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		env.execSQL(t, "INSERT INTO t VALUES (1, 'hello   ')")

		result := env.execSQL(t, "SELECT TRIM(str) FROM t")
		if result.Rows[0][0].Text != "hello" {
			t.Errorf("TRIM('hello   ') = %q, want 'hello'", result.Rows[0][0].Text)
		}
	})

	t.Run("trim_no_whitespace", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		env.execSQL(t, "INSERT INTO t VALUES (1, 'hello')")

		result := env.execSQL(t, "SELECT TRIM(str) FROM t")
		if result.Rows[0][0].Text != "hello" {
			t.Errorf("TRIM('hello') = %q, want 'hello'", result.Rows[0][0].Text)
		}
	})

	t.Run("trim_null", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		env.execSQL(t, "INSERT INTO t VALUES (1, NULL)")

		result := env.execSQL(t, "SELECT TRIM(str) FROM t")
		if !result.Rows[0][0].IsNull {
			t.Errorf("TRIM(NULL) should be NULL")
		}
	})
}

// TestExec_StringFuncInExpressions tests string functions in complex expressions.
func TestExec_StringFuncInExpressions(t *testing.T) {
	env := newTestEnv(t)
	defer env.store.Close()

	env.execSQL(t, "CREATE TABLE t (id INT PRIMARY KEY, str TEXT)")

	t.Run("concat_upper_lower", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		env.execSQL(t, "INSERT INTO t VALUES (1, 'hello')")

		// CONCAT(UPPER(a), LOWER(b))
		result := env.execSQL(t, "SELECT CONCAT(UPPER(str), LOWER(str)) FROM t")
		if result.Rows[0][0].Text != "HELLOhello" {
			t.Errorf("CONCAT(UPPER('hello'), LOWER('hello')) = %q, want 'HELLOhello'", result.Rows[0][0].Text)
		}
	})

	t.Run("substring_concat", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		env.execSQL(t, "INSERT INTO t VALUES (1, 'world')")

		// CONCAT('Hello ', SUBSTRING(str, 1, 3))
		result := env.execSQL(t, "SELECT CONCAT('Hello ', SUBSTRING(str, 1, 3)) FROM t")
		if result.Rows[0][0].Text != "Hello wor" {
			t.Errorf("CONCAT('Hello ', SUBSTRING('world', 1, 3)) = %q, want 'Hello wor'", result.Rows[0][0].Text)
		}
	})

	t.Run("nested_functions", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM t")
		env.execSQL(t, "INSERT INTO t VALUES (1, '  HELLO  ')")

		// TRIM(UPPER(...))
		result := env.execSQL(t, "SELECT TRIM(UPPER(str)) FROM t")
		if result.Rows[0][0].Text != "HELLO" {
			t.Errorf("TRIM(UPPER('  HELLO  ')) = %q, want 'HELLO'", result.Rows[0][0].Text)
		}
	})
}

// TestExec_StringFuncWithColumn tests string functions applied to table columns.
func TestExec_StringFuncWithColumn(t *testing.T) {
	env := newTestEnv(t)
	defer env.store.Close()

	env.execSQL(t, "CREATE TABLE users (id INT PRIMARY KEY, first_name TEXT, last_name TEXT, email TEXT)")

	t.Run("substring_from_column", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM users")
		env.execSQL(t, "INSERT INTO users VALUES (1, 'John', 'Doe', 'john@example.com')")

		// Get first 3 chars of email
		result := env.execSQL(t, "SELECT SUBSTRING(email, 1, 4) FROM users")
		if result.Rows[0][0].Text != "john" {
			t.Errorf("SUBSTRING(email, 1, 4) = %q, want 'john'", result.Rows[0][0].Text)
		}
	})

	t.Run("concat_name", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM users")
		env.execSQL(t, "INSERT INTO users VALUES (1, 'John', 'Doe', 'john@example.com')")

		// Full name
		result := env.execSQL(t, "SELECT CONCAT(first_name, ' ', last_name) FROM users")
		if result.Rows[0][0].Text != "John Doe" {
			t.Errorf("CONCAT(first_name, ' ', last_name) = %q, want 'John Doe'", result.Rows[0][0].Text)
		}
	})

	t.Run("lower_email", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM users")
		env.execSQL(t, "INSERT INTO users VALUES (1, 'John', 'Doe', 'JOHN@EXAMPLE.COM')")

		// Lowercase email
		result := env.execSQL(t, "SELECT LOWER(email) FROM users")
		if result.Rows[0][0].Text != "john@example.com" {
			t.Errorf("LOWER(email) = %q, want 'john@example.com'", result.Rows[0][0].Text)
		}
	})

	t.Run("length_name", func(t *testing.T) {
		env.execSQL(t, "DELETE FROM users")
		env.execSQL(t, "INSERT INTO users VALUES (1, 'John', 'Doe', 'john@example.com')")

		// Length of first name
		result := env.execSQL(t, "SELECT LENGTH(first_name) FROM users")
		if result.Rows[0][0].Int != 4 {
			t.Errorf("LENGTH(first_name) = %d, want 4", result.Rows[0][0].Int)
		}
	})
}
