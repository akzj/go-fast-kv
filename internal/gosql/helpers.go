package gosql

import (
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"

	"github.com/akzj/go-fast-kv/internal/sql/catalog/api"
)

// substitutePlaceholders replaces $1, $2, etc. and :name, @name named parameters
// with the actual argument values.
// args should be []interface{} (converted from []driver.Value).
// For named parameters, args may contain driver.NamedValue structs.
func substitutePlaceholders(query string, args []interface{}) (string, error) {
	if len(args) == 0 {
		return query, nil
	}

	// Build a map of parameter names to their values (for named params).
	// Also collect ordered param list for positional fallback.
	namedValues := make(map[string]driver.Value)
	var orderedParams []driver.Value

	for _, arg := range args {
		if nv, ok := arg.(driver.NamedValue); ok {
			namedValues[nv.Name] = nv.Value
		}
		orderedParams = append(orderedParams, arg.(driver.Value))
	}

	// Find all placeholders in the query.
	var result strings.Builder
	i := 0

	// Keep track of ? position count for sequential substitution
	questionMarkCount := 0

	for i < len(query) {
		// Check for question mark placeholder (sqlx rebinds :name to ?)
		if query[i] == '?' {
			i++
			questionMarkCount++
			if questionMarkCount > len(orderedParams) {
				return "", fmt.Errorf("gosql: too many placeholders (have %d args)", len(orderedParams))
			}
			val := orderedParams[questionMarkCount-1]
			argStr, err := valueToSQLLiteral(val)
			if err != nil {
				return "", err
			}
			result.WriteString(argStr)
			continue
		}

		// Check for named parameter (:name or @name)
		if (query[i] == ':' || query[i] == '@') && i+1 < len(query) {
			start := i
			i++
			// Read the parameter name
			nameStart := i
			for i < len(query) && isIdentChar(query[i]) {
				i++
			}
			if i > nameStart {
				name := query[nameStart:i]
				// Look up the value
				val, ok := namedValues[name]
				if !ok {
					return "", fmt.Errorf("gosql: missing value for parameter :%s", name)
				}
				argStr, err := valueToSQLLiteral(val)
				if err != nil {
					return "", err
				}
				result.WriteString(argStr)
				continue
			}
			// Not a valid named param, treat as regular character
			result.WriteByte(query[start])
			i = start + 1
			continue
		}

		// Check for positional placeholder ($1, $2, etc.)
		if query[i] == '$' && i+1 < len(query) {
			// Check if it's a placeholder like $1, $2, etc.
			j := i + 1
			for j < len(query) && j < i+10 {
				if query[j] >= '0' && query[j] <= '9' {
					j++
				} else {
					break
				}
			}
			if j > i+1 {
				// It's a placeholder.
				numStr := query[i+1 : j]
				num, err := strconv.Atoi(numStr)
				if err != nil {
					return "", fmt.Errorf("gosql: invalid placeholder %s: %v", numStr, err)
				}
				if num < 1 || num > len(orderedParams) {
					return "", fmt.Errorf("gosql: placeholder $%d out of range (have %d args)", num, len(orderedParams))
				}
				// Convert the argument to SQL literal.
				val := orderedParams[num-1]
				argStr, err := valueToSQLLiteral(val)
				if err != nil {
					return "", err
				}
				result.WriteString(argStr)
				i = j
				continue
			}
		}
		result.WriteByte(query[i])
		i++
	}

	return result.String(), nil
}

// isIdentChar returns true if c can be part of an identifier.
func isIdentChar(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'
}

// valueToSQLLiteral converts a Go value to a SQL literal string.
func valueToSQLLiteral(v interface{}) (string, error) {
	if v == nil {
		return "NULL", nil
	}
	switch val := v.(type) {
	case string:
		// Escape single quotes.
		escaped := strings.ReplaceAll(val, "'", "''")
		return "'" + escaped + "'", nil
	case int:
		return fmt.Sprintf("%d", val), nil
	case int64:
		return fmt.Sprintf("%d", val), nil
	case int32:
		return fmt.Sprintf("%d", val), nil
	case float64:
		return fmt.Sprintf("%v", val), nil
	case float32:
		return fmt.Sprintf("%v", val), nil
	case bool:
		if val {
			return "1", nil
		}
		return "0", nil
	case []byte:
		// Treat as string.
		escaped := strings.ReplaceAll(string(val), "'", "''")
		return "'" + escaped + "'", nil
	default:
		return "", fmt.Errorf("gosql: unsupported argument type %T", v)
	}
}

// convertValue converts an internal Value to a Go value.
func convertValue(v interface{}) (interface{}, error) {
	if v == nil {
		return nil, nil
	}

	switch val := v.(type) {
	case api.Value:
		if val.IsNull {
			return nil, nil
		}
		switch val.Type {
		case api.TypeInt:
			return val.Int, nil
		case api.TypeFloat:
			return val.Float, nil
		case api.TypeText:
			return val.Text, nil
		case api.TypeBlob:
			return val.Blob, nil
		default:
			return nil, fmt.Errorf("gosql: unknown value type %v", val.Type)
		}
	default:
		return val, nil
	}
}