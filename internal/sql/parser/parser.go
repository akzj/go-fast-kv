// Package parser provides a hand-written recursive descent SQL parser.
//
// Supported SQL statements (Phase 1):
//   - CREATE TABLE / DROP TABLE
//   - CREATE INDEX / DROP INDEX
//   - INSERT INTO ... VALUES
//   - SELECT ... FROM ... WHERE ... ORDER BY ... LIMIT
//   - DELETE FROM ... WHERE
//   - UPDATE ... SET ... WHERE
//
// Usage:
//
//	p := parser.New()
//	stmt, err := p.Parse("SELECT * FROM users WHERE age > 18")
package parser

import (
	"github.com/akzj/go-fast-kv/internal/sql/parser/api"
	"github.com/akzj/go-fast-kv/internal/sql/parser/internal"
)

// New creates a new SQL parser.
func New() api.Parser {
	return internal.New()
}
