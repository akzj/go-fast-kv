package sql_testdata

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/akzj/go-fast-kv/internal/sql/testutil"
)

func TestStringFunctions(t *testing.T) {
	dir, _ := os.Getwd()
	testDir := filepath.Join(dir, "string_func")
	if _, err := os.Stat(testDir); os.IsNotExist(err) {
		t.Skip("string_func sql_testdata not found")
	}
	testutil.RunSQLFiles(t, testDir)
}

func TestMathFunctions(t *testing.T) {
	dir, _ := os.Getwd()
	testDir := filepath.Join(dir, "math_func")
	if _, err := os.Stat(testDir); os.IsNotExist(err) {
		t.Skip("math_func sql_testdata not found")
	}
	testutil.RunSQLFiles(t, testDir)
}


