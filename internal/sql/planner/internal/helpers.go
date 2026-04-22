package internal

import (
	"fmt"
	"strings"

	catalogapi "github.com/akzj/go-fast-kv/internal/sql/catalog/api"
	encodingapi "github.com/akzj/go-fast-kv/internal/sql/encoding/api"
	parserapi "github.com/akzj/go-fast-kv/internal/sql/parser/api"
	plannerapi "github.com/akzj/go-fast-kv/internal/sql/planner/api"
)

// ─── Column Resolution ──────────────────────────────────────────────

// findColumnIndex returns the index of a column in a table schema, or -1 if not found.
// Case-insensitive comparison (identifiers are already uppercased by parser).
func findColumnIndex(tbl *catalogapi.TableSchema, name string) int {
	upper := strings.ToUpper(name)
	for i, c := range tbl.Columns {
		if strings.ToUpper(c.Name) == upper {
			return i
		}
	}
	return -1
}

// ─── Expression Resolution ──────────────────────────────────────────

// resolveExprToValue resolves a parser expression to a catalog Value.
// Phase 1: only Literal expressions are supported.
func resolveExprToValue(expr parserapi.Expr) (catalogapi.Value, error) {
	switch e := expr.(type) {
	case *parserapi.Literal:
		return e.Value, nil
	case *parserapi.UnaryExpr:
		if e.Op == parserapi.UnaryMinus {
			inner, err := resolveExprToValue(e.Operand)
			if err != nil {
				return catalogapi.Value{}, err
			}
			switch inner.Type {
			case catalogapi.TypeInt:
				return catalogapi.Value{Type: catalogapi.TypeInt, Int: -inner.Int}, nil
			case catalogapi.TypeFloat:
				return catalogapi.Value{Type: catalogapi.TypeFloat, Float: -inner.Float}, nil
			default:
				return catalogapi.Value{}, fmt.Errorf("%w: cannot negate %v", plannerapi.ErrTypeMismatch, inner.Type)
			}
		}
		return catalogapi.Value{}, plannerapi.ErrUnsupportedExpr
	case *parserapi.CastExpr:
		// CastExpr: evaluate the inner expression and apply the cast
		inner, err := resolveExprToValue(e.Expr)
		if err != nil {
			return catalogapi.Value{}, err
		}
		if inner.IsNull {
			return catalogapi.Value{IsNull: true}, nil
		}
		switch strings.ToUpper(e.TypeName) {
		case "INT", "INTEGER":
			switch inner.Type {
			case catalogapi.TypeInt:
				return inner, nil
			case catalogapi.TypeText:
				return catalogapi.Value{}, plannerapi.ErrUnsupportedExpr
			case catalogapi.TypeFloat:
				return catalogapi.Value{Type: catalogapi.TypeInt, Int: int64(inner.Float)}, nil
			default:
				return catalogapi.Value{}, plannerapi.ErrUnsupportedExpr
			}
		case "TEXT":
			switch inner.Type {
			case catalogapi.TypeText:
				return inner, nil
			case catalogapi.TypeInt:
				return catalogapi.Value{Type: catalogapi.TypeText, Text: fmt.Sprintf("%d", inner.Int)}, nil
			case catalogapi.TypeFloat:
				return catalogapi.Value{Type: catalogapi.TypeText, Text: fmt.Sprintf("%v", inner.Float)}, nil
			default:
				return catalogapi.Value{}, plannerapi.ErrUnsupportedExpr
			}
		case "FLOAT":
			switch inner.Type {
			case catalogapi.TypeFloat:
				return inner, nil
			case catalogapi.TypeInt:
				return catalogapi.Value{Type: catalogapi.TypeFloat, Float: float64(inner.Int)}, nil
			case catalogapi.TypeText:
				return catalogapi.Value{}, plannerapi.ErrUnsupportedExpr
			default:
				return catalogapi.Value{}, plannerapi.ErrUnsupportedExpr
			}
		case "BLOB":
			return catalogapi.Value{}, plannerapi.ErrUnsupportedExpr
		default:
			return catalogapi.Value{}, plannerapi.ErrUnsupportedExpr
		}
	default:
		return catalogapi.Value{}, plannerapi.ErrUnsupportedExpr
	}
}

// serializeExprToSQL converts an expression to SQL string representation.
// This is used to store expression indexes in the catalog.
func serializeExprToSQL(expr parserapi.Expr) string {
	if expr == nil {
		return ""
	}
	switch e := expr.(type) {
	case *parserapi.Literal:
		switch e.Value.Type {
		case catalogapi.TypeText:
			return fmt.Sprintf("%q", e.Value.Text)
		case catalogapi.TypeInt:
			return fmt.Sprintf("%d", e.Value.Int)
		case catalogapi.TypeFloat:
			return fmt.Sprintf("%v", e.Value.Float)
		case catalogapi.TypeBlob:
			return "NULL"
		default:
			return "NULL"
		}
	case *parserapi.StringFuncExpr:
		args := make([]string, len(e.Args))
		for i, arg := range e.Args {
			args[i] = serializeExprToSQL(arg)
		}
		return fmt.Sprintf("%s(%s)", strings.ToUpper(e.Func), strings.Join(args, ", "))
	case *parserapi.BinaryExpr:
		return fmt.Sprintf("(%s %v %s)", serializeExprToSQL(e.Left), e.Op, serializeExprToSQL(e.Right))
	case *parserapi.UnaryExpr:
		return fmt.Sprintf("(%v%s)", e.Op, serializeExprToSQL(e.Operand))
	case *parserapi.ColumnRef:
		return e.Column
	case *parserapi.ParamRef:
		return "?"
	default:
		return "?"
	}
}

// extractColumnFromExpr extracts the first column reference from an expression.
// Used to determine which table column an expression index primarily refers to.
func extractColumnFromExpr(expr parserapi.Expr) string {
	if expr == nil {
		return ""
	}
	switch e := expr.(type) {
	case *parserapi.ColumnRef:
		return e.Column
	case *parserapi.StringFuncExpr:
		if len(e.Args) > 0 {
			return extractColumnFromExpr(e.Args[0])
		}
	case *parserapi.BinaryExpr:
		// Prefer left side
		if e.Left != nil {
			return extractColumnFromExpr(e.Left)
		}
	case *parserapi.UnaryExpr:
		return extractColumnFromExpr(e.Operand)
	default:
	}
	return ""
}

// ─── Type Resolution ────────────────────────────────────────────────

// resolveTypeName converts a parser type name string to a catalog Type.
func resolveTypeName(name string) (catalogapi.Type, error) {
	switch strings.ToUpper(name) {
	case "INT", "INTEGER":
		return catalogapi.TypeInt, nil
	case "FLOAT":
		return catalogapi.TypeFloat, nil
	case "TEXT":
		return catalogapi.TypeText, nil
	case "BLOB":
		return catalogapi.TypeBlob, nil
	default:
		return 0, fmt.Errorf("%w: unknown type %q", plannerapi.ErrTypeMismatch, name)
	}
}

// checkType validates that a value's type matches the expected column type.
// NULL values always pass (they're valid for any column type in Phase 1).
func checkType(val catalogapi.Value, expected catalogapi.Type) error {
	if val.IsNull {
		return nil
	}
	if val.Type != expected {
		return fmt.Errorf("%w: expected %v, got %v", plannerapi.ErrTypeMismatch, expected, val.Type)
	}
	return nil
}

// ─── WHERE Clause Helpers ───────────────────────────────────────────

// flattenAnd decomposes a WHERE expression into AND-connected parts.
// e.g., "a AND b AND c" → [a, b, c]
func flattenAnd(expr parserapi.Expr) []parserapi.Expr {
	if bin, ok := expr.(*parserapi.BinaryExpr); ok && bin.Op == parserapi.BinAnd {
		left := flattenAnd(bin.Left)
		right := flattenAnd(bin.Right)
		return append(left, right...)
	}
	return []parserapi.Expr{expr}
}

// buildAndChain combines multiple expressions with AND.
func buildAndChain(parts []parserapi.Expr) parserapi.Expr {
	if len(parts) == 0 {
		return nil
	}
	result := parts[0]
	for i := 1; i < len(parts); i++ {
		result = &parserapi.BinaryExpr{
			Left:  result,
			Op:    parserapi.BinAnd,
			Right: parts[i],
		}
	}
	return result
}

// ─── Operator Conversion ────────────────────────────────────────────

// binOpToCompareOp converts a parser BinaryOp to an encoding CompareOp.
// Returns false if the op is not a comparison operator (e.g., AND, OR).
func binOpToCompareOp(op parserapi.BinaryOp) (encodingapi.CompareOp, bool) {
	switch op {
	case parserapi.BinEQ:
		return encodingapi.OpEQ, true
	case parserapi.BinNE:
		return encodingapi.OpNE, true
	case parserapi.BinLT:
		return encodingapi.OpLT, true
	case parserapi.BinLE:
		return encodingapi.OpLE, true
	case parserapi.BinGT:
		return encodingapi.OpGT, true
	case parserapi.BinGE:
		return encodingapi.OpGE, true
	default:
		return 0, false
	}
}

// flipCompareOp reverses a comparison for "literal OP column" → "column OP' literal".
// e.g., 5 < age → age > 5
func flipCompareOp(op encodingapi.CompareOp) encodingapi.CompareOp {
	switch op {
	case encodingapi.OpLT:
		return encodingapi.OpGT
	case encodingapi.OpLE:
		return encodingapi.OpGE
	case encodingapi.OpGT:
		return encodingapi.OpLT
	case encodingapi.OpGE:
		return encodingapi.OpLE
	default:
		return op // EQ, NE are symmetric
	}
}
