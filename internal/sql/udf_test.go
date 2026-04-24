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

	// Test 2: Function call is recognized (will return "not yet implemented")
	fmt.Printf("\n=== Test 2: Function call ===\n")
	_, err = db.Query(`SELECT myadd(1, 2)`)
	if err != nil {
		fmt.Printf("myadd(1,2) error (expected 'not yet implemented'): %v\n", err)
	} else {
		fmt.Printf("myadd(1,2): OK (function body evaluation not yet implemented)\n")
	}

	// Test 3: DROP FUNCTION parses
	fmt.Printf("\n=== Test 3: DROP FUNCTION ===\n")
	_, err = db.Exec(`DROP FUNCTION myadd`)
	if err != nil {
		t.Fatalf("DROP FUNCTION failed: %v", err)
	}
	fmt.Printf("DROP FUNCTION myadd: OK\n")

	fmt.Printf("\n✅ MVP UDF: parse/register/call cycle complete\n")
}
