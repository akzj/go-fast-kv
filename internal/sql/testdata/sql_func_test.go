package testdata

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
		t.Skip("string_func testdata not found")
	}
	testutil.RunSQLFiles(t, testDir)
}

func TestMathFunctions(t *testing.T) {
	dir, _ := os.Getwd()
	testDir := filepath.Join(dir, "math_func")
	if _, err := os.Stat(testDir); os.IsNotExist(err) {
		t.Skip("math_func testdata not found")
	}
	testutil.RunSQLFiles(t, testDir)
}

func TestDateTimeFunctions(t *testing.T) {
	dir, _ := os.Getwd()
	testDir := filepath.Join(dir, "datetime_func")
	if _, err := os.Stat(testDir); os.IsNotExist(err) {
		t.Skip("datetime_func testdata not found")
	}
	testutil.RunSQLFiles(t, testDir)
}

func TestJsonFunctions(t *testing.T) {
	dir, _ := os.Getwd()
	testDir := filepath.Join(dir, "json_func")
	if _, err := os.Stat(testDir); os.IsNotExist(err) {
		t.Skip("json_func testdata not found")
	}
	testutil.RunSQLFiles(t, testDir)
}

func TestUDFunctions(t *testing.T) {
	dir, _ := os.Getwd()
	testDir := filepath.Join(dir, "udf")
	if _, err := os.Stat(testDir); os.IsNotExist(err) {
		t.Skip("udf testdata not found")
	}
	testutil.RunSQLFiles(t, testDir)
}
