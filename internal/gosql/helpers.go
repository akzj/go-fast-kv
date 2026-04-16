package gosql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/akzj/go-fast-kv/internal/sql/catalog/api"
)

// substitutePlaceholders replaces $1, $2, etc. with the actual argument values.
// Named parameters are not supported.
// args should be []interface{} (converted from []driver.Value).
func substitutePlaceholders(query string, args []interface{}) (string, error) {
	if len(args) == 0 {
		return query, nil
	}

	// Find all placeholders in the query.
	var result strings.Builder
	i := 0

	for i < len(query) {
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
				if num < 1 || num > len(args) {
					return "", fmt.Errorf("gosql: placeholder $%d out of range (have %d args)", num, len(args))
				}
				// Convert the argument to SQL literal.
				val := args[num-1]
				argStr, err := valueToSQLLiteral(val)
				if err != nil {
					return "", err
				}
				result.WriteString(argStr)
				i = j
			} else {
				result.WriteByte(query[i])
				i++
			}
		} else {
			result.WriteByte(query[i])
			i++
		}
	}

	return result.String(), nil
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