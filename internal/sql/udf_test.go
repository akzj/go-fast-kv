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

func TestUDF_Phase3(t *testing.T) {
	dir, _ := os.MkdirTemp("", "udf-phase3-")
	defer os.RemoveAll(dir)

	store, err := kvstore.Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	db := Open(store)
	defer db.Close()

	// Test 1: DECLARE + FOR loop sum
	fmt.Printf("=== Test 1: DECLARE + FOR sum ===\n")
	_, err = db.Exec(`CREATE FUNCTION sum1to5() RETURNS INT AS $$
DECLARE
    result INT := 0;
BEGIN
    FOR i IN 1..5 LOOP
        result := result + i;
    END LOOP;
    RETURN result;
END;
$$ LANGUAGE SQL`)
	if err != nil {
		t.Fatalf("CREATE FUNCTION sum1to5 failed: %v", err)
	}
	fmt.Printf("CREATE FUNCTION sum1to5: OK\n")

	rows, err := db.Query(`SELECT sum1to5()`)
	if err != nil {
		t.Fatalf("sum1to5() failed: %v", err)
	}
	v := rows.Rows[0][0]
	if v.Int != 15 {
		t.Fatalf("sum1to5() expected 15, got %d", v.Int)
	}
	fmt.Printf("sum1to5() = %d: OK\n", v.Int)

	// Test 2: Factorial with FOR loop
	fmt.Printf("\n=== Test 2: Factorial ===\n")
	_, err = db.Exec(`CREATE FUNCTION factorial(n INT) RETURNS INT AS $$
DECLARE
    result INT := 1;
BEGIN
    FOR i IN 1..n LOOP
        result := result * i;
    END LOOP;
    RETURN result;
END;
$$ LANGUAGE SQL`)
	if err != nil {
		t.Fatalf("CREATE FUNCTION factorial failed: %v", err)
	}
	fmt.Printf("CREATE FUNCTION factorial: OK\n")

	rows, err = db.Query(`SELECT factorial(5)`)
	if err != nil {
		t.Fatalf("factorial(5) failed: %v", err)
	}
	v = rows.Rows[0][0]
	if v.Int != 120 {
		t.Fatalf("factorial(5) expected 120, got %d", v.Int)
	}
	fmt.Printf("factorial(5) = %d: OK\n", v.Int)

	// Test 3: LOOP with EXIT WHEN
	fmt.Printf("\n=== Test 3: LOOP + EXIT WHEN ===\n")
	_, err = db.Exec(`CREATE FUNCTION count3() RETURNS INT AS $$
DECLARE
    c INT := 0;
BEGIN
    LOOP
        c := c + 1;
        EXIT WHEN c >= 3;
    END LOOP;
    RETURN c;
END;
$$ LANGUAGE SQL`)
	if err != nil {
		t.Fatalf("CREATE FUNCTION count3 failed: %v", err)
	}
	fmt.Printf("CREATE FUNCTION count3: OK\n")

	rows, err = db.Query(`SELECT count3()`)
	if err != nil {
		t.Fatalf("count3() failed: %v", err)
	}
	v = rows.Rows[0][0]
	if v.Int != 3 {
		t.Fatalf("count3() expected 3, got %d", v.Int)
	}
	fmt.Printf("count3() = %d: OK\n", v.Int)

	// Test 4: WHILE loop
	fmt.Printf("\n=== Test 4: WHILE loop ===\n")
	_, err = db.Exec(`CREATE FUNCTION while_test() RETURNS INT AS $$
DECLARE
    i INT := 0;
BEGIN
    WHILE i < 3 LOOP
        i := i + 1;
    END LOOP;
    RETURN i;
END;
$$ LANGUAGE SQL`)
	if err != nil {
		t.Fatalf("CREATE FUNCTION while_test failed: %v", err)
	}
	fmt.Printf("CREATE FUNCTION while_test: OK\n")

	rows, err = db.Query(`SELECT while_test()`)
	if err != nil {
		t.Fatalf("while_test() failed: %v", err)
	}
	v = rows.Rows[0][0]
	if v.Int != 3 {
		t.Fatalf("while_test() expected 3, got %d", v.Int)
	}
	fmt.Printf("while_test() = %d: OK\n", v.Int)

	// Test 5: Simple assignment without DECLARE
	fmt.Printf("\n=== Test 5: Simple assignment ===\n")
	_, err = db.Exec(`CREATE FUNCTION double(n INT) RETURNS INT AS $$
DECLARE
    x INT;
BEGIN
    x := n * 2;
    RETURN x;
END;
$$ LANGUAGE SQL`)
	if err != nil {
		t.Fatalf("CREATE FUNCTION double failed: %v", err)
	}
	fmt.Printf("CREATE FUNCTION double: OK\n")

	rows, err = db.Query(`SELECT double(7)`)
	if err != nil {
		t.Fatalf("double(7) failed: %v", err)
	}
	v = rows.Rows[0][0]
	if v.Int != 14 {
		t.Fatalf("double(7) expected 14, got %d", v.Int)
	}
	fmt.Printf("double(7) = %d: OK\n", v.Int)

	// Cleanup
	fmt.Printf("\n=== Cleanup ===\n")
	_, err = db.Exec(`DROP FUNCTION sum1to5`)
	_, _ = db.Exec(`DROP FUNCTION factorial`)
	_, _ = db.Exec(`DROP FUNCTION count3`)
	_, _ = db.Exec(`DROP FUNCTION while_test`)
	_, _ = db.Exec(`DROP FUNCTION double`)
	fmt.Printf("DROP FUNCTION: OK\n")

	fmt.Printf("\n✅ Phase 3 UDF: DECLARE/FOR/WHILE/LOOP/EXIT complete\n")
}
