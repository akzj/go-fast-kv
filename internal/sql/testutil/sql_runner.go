package testutil

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/akzj/go-fast-kv/internal/kvstore"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
	"github.com/akzj/go-fast-kv/internal/sql"
	"github.com/akzj/go-fast-kv/internal/sql/catalog/api"
)

// SQLTestCase represents a parsed test case from a .sql file
type SQLTestCase struct {
	Name           string
	Description    string
	SetupSQLs      []string
	SQL            string
	ExpectedRows   int
	ExpectedValues [][]string
}

// SQLTestRunner executes SQL test cases
type SQLTestRunner struct {
	t *testing.T
	db *sql.DB
	store interface {
		Close() error
	}
}

// NewSQLTestRunner creates a new test runner with an in-memory database
func NewSQLTestRunner(t *testing.T) *SQLTestRunner {
	dir, err := os.MkdirTemp("", "sql-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	store, err := kvstore.Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		store.Close()
		os.RemoveAll(dir)
	})

	return &SQLTestRunner{
		t:  t,
		db: sql.Open(store),
	}
}

// valueToString converts api.Value to a string representation
func valueToString(val interface{}) string {
	switch v := val.(type) {
	case api.Value:
		if v.IsNull {
			return "NULL"
		}
		switch v.Type {
		case api.TypeInt:
			return strconv.FormatInt(v.Int, 10)
		case api.TypeFloat:
			return strconv.FormatFloat(v.Float, 'f', -1, 64)
		case api.TypeText:
			return v.Text
		case api.TypeBlob:
			return string(v.Blob)
		case api.TypeNull:
			return "NULL"
		default:
			return fmt.Sprintf("%v", v)
		}
	default:
		return fmt.Sprintf("%v", v)
	}
}

// ParseSQLFile parses a .sql test file into a SQLTestCase
func (r *SQLTestRunner) ParseSQLFile(path string) (*SQLTestCase, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	tc := &SQLTestCase{Name: filepath.Base(path)}
	var currentSQL strings.Builder
	var inResult bool
	var values [][]string

	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for scanner.Scan() {
		line := scanner.Text()

		// Parse metadata
		if strings.HasPrefix(line, "-- name:") {
			tc.Name = strings.TrimSpace(strings.TrimPrefix(line, "-- name:"))
		} else if strings.HasPrefix(line, "-- description:") {
			tc.Description = strings.TrimSpace(strings.TrimPrefix(line, "-- description:"))
		} else if strings.HasPrefix(line, "-- setup:") {
			tc.SetupSQLs = append(tc.SetupSQLs, strings.TrimSpace(strings.TrimPrefix(line, "-- setup:")))
		} else if strings.HasPrefix(line, "-- rows:") {
			rows, _ := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "-- rows:")))
			tc.ExpectedRows = rows
		} else if strings.HasPrefix(line, "-- result:") {
			inResult = true
			values = [][]string{}
		} else if strings.TrimSpace(line) == "--" && inResult {
			inResult = false
		} else if strings.HasPrefix(line, "-- [") && inResult {
			// Parse row: -- [0][0]: value
			re := regexp.MustCompile(`-- \[(\d+)\]\[(\d+)\]: (.+)`)
			match := re.FindStringSubmatch(line)
			if match != nil {
				rowIdx, _ := strconv.Atoi(match[1])
				colIdx, _ := strconv.Atoi(match[2])
				val := match[3]

				for len(values) <= rowIdx {
					values = append(values, []string{})
				}
				for len(values[rowIdx]) <= colIdx {
					values[rowIdx] = append(values[rowIdx], "")
				}
				values[rowIdx][colIdx] = val
			}
		} else if !strings.HasPrefix(strings.TrimSpace(line), "--") && line != "" {
			if !inResult {
				currentSQL.WriteString(line)
				currentSQL.WriteString(" ")
			}
		}
	}

	tc.SQL = strings.TrimSpace(currentSQL.String())
	tc.ExpectedValues = values

	return tc, nil
}

// RunTestCase executes a single test case
func (r *SQLTestRunner) RunTestCase(tc *SQLTestCase) {
	// Setup
	for _, setup := range tc.SetupSQLs {
		if setup == "" {
			continue
		}
		_, err := r.db.Exec(setup)
		if err != nil {
			r.t.Fatalf("setup %q: %v", setup, err)
		}
	}

	// Execute
	result, err := r.db.Query(tc.SQL)
	if err != nil {
		r.t.Fatalf("execute %q: %v", tc.SQL, err)
	}

	// Verify rows count
	if len(result.Rows) != tc.ExpectedRows {
		r.t.Errorf("rows = %d, want %d", len(result.Rows), tc.ExpectedRows)
		return
	}

	// Verify values
	for i, row := range result.Rows {
		if i >= len(tc.ExpectedValues) {
			break
		}
		for j, val := range row {
			if j >= len(tc.ExpectedValues[i]) {
				break
			}
			expected := tc.ExpectedValues[i][j]
			actual := valueToString(val)
			if actual != expected {
				r.t.Errorf("row[%d][%d] = %q, want %q", i, j, actual, expected)
			}
		}
	}
}

// RunSQLFiles runs all .sql files in a directory as subtests
func RunSQLFiles(t *testing.T, dir string) {
	runner := NewSQLTestRunner(t)

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}

		path := filepath.Join(dir, entry.Name())
		tc, err := runner.ParseSQLFile(path)
		if err != nil {
			t.Fatalf("parse %s: %v", entry.Name(), err)
		}

		t.Run(tc.Name, func(t *testing.T) {
			runner.RunTestCase(tc)
		})
	}
}
