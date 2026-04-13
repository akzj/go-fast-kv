package internal

import (
	"fmt"
	"strings"

	catalogapi "github.com/akzj/go-fast-kv/internal/sql/catalog/api"
	"github.com/akzj/go-fast-kv/internal/sql/encoding"
	encodingapi "github.com/akzj/go-fast-kv/internal/sql/encoding/api"
	engineapi "github.com/akzj/go-fast-kv/internal/sql/engine/api"
	executorapi "github.com/akzj/go-fast-kv/internal/sql/executor/api"
	parserapi "github.com/akzj/go-fast-kv/internal/sql/parser/api"
)

// ─── Expression Evaluation ──────────────────────────────────────────

// evalExpr evaluates an expression against a row and returns a Value.
func evalExpr(expr parserapi.Expr, row *engineapi.Row, columns []catalogapi.ColumnDef) (catalogapi.Value, error) {
	switch e := expr.(type) {
	case *parserapi.ColumnRef:
		return evalColumnRef(e, row, columns)
	case *parserapi.Literal:
		return e.Value, nil
	case *parserapi.BinaryExpr:
		return evalBinaryExpr(e, row, columns)
	case *parserapi.UnaryExpr:
		return evalUnaryExpr(e, row, columns)
	case *parserapi.IsNullExpr:
		return evalIsNullExpr(e, row, columns)
	default:
		return catalogapi.Value{}, fmt.Errorf("%w: unsupported expression type %T", executorapi.ErrExecFailed, expr)
	}
}

// evalColumnRef looks up a column value from the row.
func evalColumnRef(ref *parserapi.ColumnRef, row *engineapi.Row, columns []catalogapi.ColumnDef) (catalogapi.Value, error) {
	upper := strings.ToUpper(ref.Column)
	for i, col := range columns {
		if strings.ToUpper(col.Name) == upper {
			if i < len(row.Values) {
				return row.Values[i], nil
			}
			return catalogapi.Value{IsNull: true}, nil
		}
	}
	return catalogapi.Value{}, fmt.Errorf("%w: column %q not found", executorapi.ErrExecFailed, ref.Column)
}

// evalBinaryExpr evaluates a binary expression (AND, OR, comparisons).
func evalBinaryExpr(expr *parserapi.BinaryExpr, row *engineapi.Row, columns []catalogapi.ColumnDef) (catalogapi.Value, error) {
	// Short-circuit AND/OR
	switch expr.Op {
	case parserapi.BinAnd:
		left, err := evalExpr(expr.Left, row, columns)
		if err != nil {
			return catalogapi.Value{}, err
		}
		if !isTruthy(left) {
			return intVal(0), nil
		}
		right, err := evalExpr(expr.Right, row, columns)
		if err != nil {
			return catalogapi.Value{}, err
		}
		if isTruthy(right) {
			return intVal(1), nil
		}
		return intVal(0), nil

	case parserapi.BinOr:
		left, err := evalExpr(expr.Left, row, columns)
		if err != nil {
			return catalogapi.Value{}, err
		}
		if isTruthy(left) {
			return intVal(1), nil
		}
		right, err := evalExpr(expr.Right, row, columns)
		if err != nil {
			return catalogapi.Value{}, err
		}
		if isTruthy(right) {
			return intVal(1), nil
		}
		return intVal(0), nil
	}

	// Comparison operators
	left, err := evalExpr(expr.Left, row, columns)
	if err != nil {
		return catalogapi.Value{}, err
	}
	right, err := evalExpr(expr.Right, row, columns)
	if err != nil {
		return catalogapi.Value{}, err
	}

	// NULL comparison: any comparison with NULL yields false (SQL semantics)
	if left.IsNull || right.IsNull {
		// != is special: NULL != X → false (not true)
		return intVal(0), nil
	}

	cmp, err := encoding.CompareValues(left, right)
	if err != nil {
		return catalogapi.Value{}, fmt.Errorf("%w: %v", executorapi.ErrExecFailed, err)
	}

	var result bool
	switch expr.Op {
	case parserapi.BinEQ:
		result = cmp == 0
	case parserapi.BinNE:
		result = cmp != 0
	case parserapi.BinLT:
		result = cmp < 0
	case parserapi.BinLE:
		result = cmp <= 0
	case parserapi.BinGT:
		result = cmp > 0
	case parserapi.BinGE:
		result = cmp >= 0
	default:
		return catalogapi.Value{}, fmt.Errorf("%w: unsupported binary op %d", executorapi.ErrExecFailed, expr.Op)
	}

	if result {
		return intVal(1), nil
	}
	return intVal(0), nil
}

// evalUnaryExpr evaluates NOT and unary minus.
func evalUnaryExpr(expr *parserapi.UnaryExpr, row *engineapi.Row, columns []catalogapi.ColumnDef) (catalogapi.Value, error) {
	operand, err := evalExpr(expr.Operand, row, columns)
	if err != nil {
		return catalogapi.Value{}, err
	}

	switch expr.Op {
	case parserapi.UnaryNot:
		if operand.IsNull {
			return catalogapi.Value{IsNull: true}, nil
		}
		if isTruthy(operand) {
			return intVal(0), nil
		}
		return intVal(1), nil

	case parserapi.UnaryMinus:
		if operand.IsNull {
			return catalogapi.Value{IsNull: true}, nil
		}
		switch operand.Type {
		case catalogapi.TypeInt:
			return catalogapi.Value{Type: catalogapi.TypeInt, Int: -operand.Int}, nil
		case catalogapi.TypeFloat:
			return catalogapi.Value{Type: catalogapi.TypeFloat, Float: -operand.Float}, nil
		default:
			return catalogapi.Value{}, fmt.Errorf("%w: cannot negate type %d", executorapi.ErrExecFailed, operand.Type)
		}

	default:
		return catalogapi.Value{}, fmt.Errorf("%w: unsupported unary op %d", executorapi.ErrExecFailed, expr.Op)
	}
}

// evalIsNullExpr evaluates IS NULL / IS NOT NULL.
func evalIsNullExpr(expr *parserapi.IsNullExpr, row *engineapi.Row, columns []catalogapi.ColumnDef) (catalogapi.Value, error) {
	inner, err := evalExpr(expr.Expr, row, columns)
	if err != nil {
		return catalogapi.Value{}, err
	}

	isNull := inner.IsNull
	if expr.Not {
		isNull = !isNull
	}
	if isNull {
		return intVal(1), nil
	}
	return intVal(0), nil
}

// ─── Helpers ────────────────────────────────────────────────────────

// isTruthy returns true if a value is considered "true" in SQL boolean context.
// A value is truthy if: not NULL, and (Int != 0 or Float != 0.0 or Text != "" or len(Blob) > 0).
func isTruthy(v catalogapi.Value) bool {
	if v.IsNull {
		return false
	}
	switch v.Type {
	case catalogapi.TypeInt:
		return v.Int != 0
	case catalogapi.TypeFloat:
		return v.Float != 0.0
	case catalogapi.TypeText:
		return v.Text != ""
	case catalogapi.TypeBlob:
		return len(v.Blob) > 0
	default:
		return false
	}
}

// intVal creates an integer Value.
func intVal(n int64) catalogapi.Value {
	return catalogapi.Value{Type: catalogapi.TypeInt, Int: n}
}

// matchFilter evaluates a filter expression against a row.
// Returns true if the row passes the filter (or filter is nil).
func matchFilter(filter parserapi.Expr, row *engineapi.Row, columns []catalogapi.ColumnDef) (bool, error) {
	if filter == nil {
		return true, nil
	}
	val, err := evalExpr(filter, row, columns)
	if err != nil {
		return false, err
	}
	return isTruthy(val), nil
}

// binOpToCompareOp converts a parser BinaryOp to an encoding CompareOp.
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
