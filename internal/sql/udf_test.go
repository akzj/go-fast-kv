package sql

import (
	"fmt"
	"os"
	"testing"

	"github.com/akzj/go-fast-kv/internal/kvstore"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
)

func TestUDF_MVP(t *testing.T) {
	dir, _ := os.MkdirTemp("", "udf-*")
	defer os.RemoveAll(dir)

	store, err := kvstore.Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	db := Open(store)
	defer db.Close()

	// Test 1: CREATE FUNCTION parses without error
	fmt.Printf("=== Test 1: CREATE FUNCTION ===\n")
	_, err = db.Exec(`CREATE FUNCTION myadd(a INT, b INT) RETURNS INT AS $$ a + b $$ LANGUAGE SQL`)
	if err != nil {
		t.Fatalf("CREATE FUNCTION failed: %v", err)
	}
	fmt.Printf("CREATE FUNCTION myadd: OK\n")

	// Test 2: Function call returns correct result
	fmt.Printf("\n=== Test 2: Function call ===\n")
	rows, err := db.Query(`SELECT myadd(1, 2)`)
	if err != nil {
		t.Fatalf("myadd(1,2) failed: %v", err)
	}

	// Read the result - Result has Rows directly
	if len(rows.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows.Rows))
	}
	if len(rows.Rows[0]) != 1 {
		t.Fatalf("expected 1 column, got %d", len(rows.Rows[0]))
	}

	// Check result is 3
	v := rows.Rows[0][0]
	if v.Int != 3 {
		t.Fatalf("myadd(1,2) expected 3, got %d", v.Int)
	}
	fmt.Printf("myadd(1,2) = %d: OK\n", v.Int)

	// Test 3: DROP FUNCTION parses
	fmt.Printf("\n=== Test 3: DROP FUNCTION ===\n")
	_, err = db.Exec(`DROP FUNCTION myadd`)
	if err != nil {
		t.Fatalf("DROP FUNCTION failed: %v", err)
	}
	fmt.Printf("DROP FUNCTION myadd: OK\n")

	fmt.Printf("\n✅ MVP UDF: parse/register/call cycle complete\n")
}

func TestUDF_BlockStyle(t *testing.T) {
	dir, _ := os.MkdirTemp("", "udf-")
	defer os.RemoveAll(dir)

	store, err := kvstore.Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	db := Open(store)
	defer db.Close()

	// Test 1: CREATE FUNCTION with IF/ELSIF/ELSE block
	fmt.Printf("=== Test 1: CREATE FUNCTION grade ===\n")
	_, err = db.Exec(`CREATE FUNCTION grade(score INT) RETURNS TEXT AS $$
BEGIN
    IF score >= 90 THEN RETURN 'A';
    ELSIF score >= 80 THEN RETURN 'B';
    ELSIF score >= 70 THEN RETURN 'C';
    ELSE RETURN 'D';
    END IF;
END;
$$ LANGUAGE SQL`)
	if err != nil {
		t.Fatalf("CREATE FUNCTION grade failed: %v", err)
	}
	fmt.Printf("CREATE FUNCTION grade: OK\n")

	// Test 2: grade(95) should return 'A'
	fmt.Printf("\n=== Test 2: grade(95) ===\n")
	rows, err := db.Query(`SELECT grade(95)`)
	if err != nil {
		t.Fatalf("grade(95) failed: %v", err)
	}
	if len(rows.Rows) != 1 || len(rows.Rows[0]) != 1 {
		t.Fatalf("unexpected result shape")
	}
	v := rows.Rows[0][0]
	if v.Text != "A" {
		t.Fatalf("grade(95) expected 'A', got %q", v.Text)
	}
	fmt.Printf("grade(95) = %q: OK\n", v.Text)

	// Test 3: grade(85) should return 'B'
	fmt.Printf("\n=== Test 3: grade(85) ===\n")
	rows, err = db.Query(`SELECT grade(85)`)
	if err != nil {
		t.Fatalf("grade(85) failed: %v", err)
	}
	v = rows.Rows[0][0]
	if v.Text != "B" {
		t.Fatalf("grade(85) expected 'B', got %q", v.Text)
	}
	fmt.Printf("grade(85) = %q: OK\n", v.Text)

	// Test 4: grade(75) should return 'C'
	fmt.Printf("\n=== Test 4: grade(75) ===\n")
	rows, err = db.Query(`SELECT grade(75)`)
	if err != nil {
		t.Fatalf("grade(75) failed: %v", err)
	}
	v = rows.Rows[0][0]
	if v.Text != "C" {
		t.Fatalf("grade(75) expected 'C', got %q", v.Text)
	}
	fmt.Printf("grade(75) = %q: OK\n", v.Text)

	// Test 5: grade(55) should return 'D'
	fmt.Printf("\n=== Test 5: grade(55) ===\n")
	rows, err = db.Query(`SELECT grade(55)`)
	if err != nil {
		t.Fatalf("grade(55) failed: %v", err)
	}
	v = rows.Rows[0][0]
	if v.Text != "D" {
		t.Fatalf("grade(55) expected 'D', got %q", v.Text)
	}
	fmt.Printf("grade(55) = %q: OK\n", v.Text)

	// Test 6: Simple IF without ELSIF
	fmt.Printf("\n=== Test 6: Simple IF ===\n")
	_, err = db.Exec(`CREATE FUNCTION sign(n INT) RETURNS TEXT AS $$
BEGIN
    IF n > 0 THEN RETURN 'positive';
    ELSE RETURN 'non-positive';
    END IF;
END;
$$ LANGUAGE SQL`)
	if err != nil {
		t.Fatalf("CREATE FUNCTION sign failed: %v", err)
	}
	fmt.Printf("CREATE FUNCTION sign: OK\n")

	rows, err = db.Query(`SELECT sign(-5)`)
	if err != nil {
		t.Fatalf("sign(-5) failed: %v", err)
	}
	v = rows.Rows[0][0]
	if v.Text != "non-positive" {
		t.Fatalf("sign(-5) expected 'non-positive', got %q", v.Text)
	}
	fmt.Printf("sign(-5) = %q: OK\n", v.Text)

	// Test 7: DROP FUNCTION
	fmt.Printf("\n=== Test 7: DROP FUNCTION ===\n")
	_, err = db.Exec(`DROP FUNCTION grade`)
	if err != nil {
		t.Fatalf("DROP FUNCTION grade failed: %v", err)
	}
	_, err = db.Exec(`DROP FUNCTION sign`)
	if err != nil {
		t.Fatalf("DROP FUNCTION sign failed: %v", err)
	}
	fmt.Printf("DROP FUNCTION: OK\n")

	fmt.Printf("\n✅ Block-style UDF: IF/ELSIF/ELSE/RETURN complete\n")
}

func TestDebugParser(t *testing.T) {
	dir, _ := os.MkdirTemp("", "udf-")
	defer os.RemoveAll(dir)

	store, err := kvstore.Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	db := Open(store)
	defer db.Close()

	// First register the function
	_, err = db.Exec(`CREATE FUNCTION grade(score INT) RETURNS TEXT AS $$
BEGIN
    IF score >= 90 THEN RETURN 'A';
    ELSIF score >= 80 THEN RETURN 'B';
    ELSE RETURN 'D';
    END IF;
END;
$$ LANGUAGE SQL`)
	if err != nil {
		t.Fatalf("CREATE failed: %v", err)
	}
	fmt.Printf("Function created successfully\n")

	// Try a simpler function first
	_, err = db.Exec(`CREATE FUNCTION simple(n INT) RETURNS TEXT AS $$
BEGIN
    IF n > 0 THEN RETURN 'yes';
    ELSE RETURN 'no';
    END IF;
END;
$$ LANGUAGE SQL`)
	if err != nil {
		fmt.Printf("CREATE simple failed: %v\n", err)
	}

	rows, err := db.Query(`SELECT simple(5)`)
	if err != nil {
		fmt.Printf("simple(5) failed: %v\n", err)
		return
	}
	fmt.Printf("simple(5) result: %+v\n", rows.Rows)
}
