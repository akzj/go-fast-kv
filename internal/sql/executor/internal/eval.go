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

// walkExpr recursively visits all Expr nodes in an expression tree,
// calling fn on each one.
func walkExpr(expr parserapi.Expr, fn func(parserapi.Expr)) {
	if expr == nil {
		return
	}
	fn(expr)
	switch e := expr.(type) {
	case *parserapi.BinaryExpr:
		walkExpr(e.Left, fn)
		walkExpr(e.Right, fn)
	case *parserapi.UnaryExpr:
		walkExpr(e.Operand, fn)
	case *parserapi.IsNullExpr:
		walkExpr(e.Expr, fn)
	case *parserapi.LikeExpr:
		walkExpr(e.Expr, fn)
	case *parserapi.BetweenExpr:
		walkExpr(e.Expr, fn)
		walkExpr(e.Low, fn)
		walkExpr(e.High, fn)
	case *parserapi.InExpr:
		walkExpr(e.Expr, fn)
		for _, v := range e.Values {
			walkExpr(v, fn)
		}
	case *parserapi.SubqueryExpr:
		// Subquery body not walked here (would need Statement visitor)
	}
}

// evalExpr evaluates an expression against a row and returns a Value.
// subqueryResults provides pre-computed values for SubqueryExpr nodes.
func evalExpr(expr parserapi.Expr, row *engineapi.Row, columns []catalogapi.ColumnDef,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}) (catalogapi.Value, error) {
	switch e := expr.(type) {
	case *parserapi.ColumnRef:
		return evalColumnRef(e, row, columns)
	case *parserapi.Literal:
		return e.Value, nil
	case *parserapi.BinaryExpr:
		return evalBinaryExpr(e, row, columns, subqueryResults)
	case *parserapi.UnaryExpr:
		return evalUnaryExpr(e, row, columns, subqueryResults)
	case *parserapi.IsNullExpr:
		return evalIsNullExpr(e, row, columns, subqueryResults)
	case *parserapi.LikeExpr:
		return evalLikeExpr(e, row, columns, subqueryResults)
	case *parserapi.AggregateCallExpr:
		return catalogapi.Value{}, fmt.Errorf("%w: aggregate %s() must be used in a GROUP BY context", executorapi.ErrExecFailed, e.Func)
	case *parserapi.BetweenExpr:
		return evalBetweenExpr(e, row, columns, subqueryResults)
	case *parserapi.InExpr:
		return evalInExpr(e, row, columns, subqueryResults)
	case *parserapi.SubqueryExpr:
		// Pre-computed by execSelect pre-plan pass.
		// Scalar subqueries store a single catalogapi.Value.
		// IN-list subqueries store a []catalogapi.Value (handled by evalInExpr).
		if vals, ok := subqueryResults[e]; ok {
			switch v := vals.(type) {
			case catalogapi.Value:
				// Scalar subquery result
				return v, nil
			case []catalogapi.Value:
				// IN-list subquery — should not reach here directly;
				// evalInExpr expands these into individual checks.
				return catalogapi.Value{IsNull: true}, nil
			default:
				return catalogapi.Value{IsNull: true}, nil
			}
		}
		// Not pre-computed: treat as NULL (should not happen with precomputeSubqueries)
		return catalogapi.Value{IsNull: true}, nil
	default:
		return catalogapi.Value{}, fmt.Errorf("%w: unsupported expression type %T", executorapi.ErrExecFailed, expr)
	}
}

// evalColumnRef looks up a column value from the row.
func evalColumnRef(ref *parserapi.ColumnRef, row *engineapi.Row, columns []catalogapi.ColumnDef) (catalogapi.Value, error) {
	upper := strings.ToUpper(ref.Column)
	upperTable := strings.ToUpper(ref.Table)

	// If table qualifier provided, match both table AND column name
	if ref.Table != "" {
		for i, col := range columns {
			if strings.ToUpper(col.Name) == upper && strings.EqualFold(col.Table, upperTable) {
				if i < len(row.Values) {
					return row.Values[i], nil
				}
				return catalogapi.Value{IsNull: true}, nil
			}
		}
		return catalogapi.Value{}, fmt.Errorf("%w: column %q not found in table %q", executorapi.ErrExecFailed, ref.Column, ref.Table)
	}

	// Unqualified column name — find first match (original behavior)
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
func evalBinaryExpr(expr *parserapi.BinaryExpr, row *engineapi.Row, columns []catalogapi.ColumnDef, subqueryResults map[*parserapi.SubqueryExpr]interface{}) (catalogapi.Value, error) {
	// SQL three-valued logic for AND/OR:
	//   AND: FALSE AND x → FALSE; TRUE AND x → x; NULL AND FALSE → FALSE; NULL AND TRUE → NULL; NULL AND NULL → NULL
	//   OR:  TRUE OR x → TRUE; FALSE OR x → x; NULL OR TRUE → TRUE; NULL OR FALSE → NULL; NULL OR NULL → NULL
	switch expr.Op {
	case parserapi.BinAnd:
		left, err := evalExpr(expr.Left, row, columns, subqueryResults)
		if err != nil {
			return catalogapi.Value{}, err
		}
		leftNull := left.IsNull
		leftTrue := !leftNull && isTruthy(left)
		if !leftNull && !leftTrue {
			// Left is definitely FALSE → result is FALSE regardless of right
			return intVal(0), nil
		}
		right, err := evalExpr(expr.Right, row, columns, subqueryResults)
		if err != nil {
			return catalogapi.Value{}, err
		}
		rightNull := right.IsNull
		rightTrue := !rightNull && isTruthy(right)
		if !rightNull && !rightTrue {
			// Right is definitely FALSE → result is FALSE
			return intVal(0), nil
		}
		if leftNull || rightNull {
			// At least one side is NULL, neither side is FALSE → result is NULL
			return catalogapi.Value{IsNull: true}, nil
		}
		// Both are TRUE
		if leftTrue && rightTrue {
			return intVal(1), nil
		}
		return intVal(0), nil

	case parserapi.BinOr:
		left, err := evalExpr(expr.Left, row, columns, subqueryResults)
		if err != nil {
			return catalogapi.Value{}, err
		}
		leftNull := left.IsNull
		leftTrue := !leftNull && isTruthy(left)
		if leftTrue {
			// Left is definitely TRUE → result is TRUE regardless of right
			return intVal(1), nil
		}
		right, err := evalExpr(expr.Right, row, columns, subqueryResults)
		if err != nil {
			return catalogapi.Value{}, err
		}
		rightNull := right.IsNull
		rightTrue := !rightNull && isTruthy(right)
		if rightTrue {
			// Right is definitely TRUE → result is TRUE
			return intVal(1), nil
		}
		if leftNull || rightNull {
			// At least one side is NULL, neither side is TRUE → result is NULL
			return catalogapi.Value{IsNull: true}, nil
		}
		// Both are FALSE
		return intVal(0), nil
	}

	// Comparison operators
	left, err := evalExpr(expr.Left, row, columns, subqueryResults)
	if err != nil {
		return catalogapi.Value{}, err
	}
	right, err := evalExpr(expr.Right, row, columns, subqueryResults)
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
func evalUnaryExpr(expr *parserapi.UnaryExpr, row *engineapi.Row, columns []catalogapi.ColumnDef, subqueryResults map[*parserapi.SubqueryExpr]interface{}) (catalogapi.Value, error) {
	operand, err := evalExpr(expr.Operand, row, columns, subqueryResults)
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
			if operand.Int == -9223372036854775808 { return catalogapi.Value{}, fmt.Errorf("%w: integer overflow: cannot negate %d", executorapi.ErrExecFailed, operand.Int) }; return catalogapi.Value{Type: catalogapi.TypeInt, Int: -operand.Int}, nil
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
func evalIsNullExpr(expr *parserapi.IsNullExpr, row *engineapi.Row, columns []catalogapi.ColumnDef, subqueryResults map[*parserapi.SubqueryExpr]interface{}) (catalogapi.Value, error) {
	inner, err := evalExpr(expr.Expr, row, columns, subqueryResults)
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

// literalToExpr converts a catalogapi.Value to a parserapi.Literal Expr.
// Returns nil for NULL values (no point adding NULL to IN list).
func literalToExpr(v catalogapi.Value) parserapi.Expr {
	if v.IsNull {
		return nil
	}
	return &parserapi.Literal{Value: v}
}

// matchFilter evaluates a filter expression against a row.
// Returns true if the row passes the filter (or filter is nil).
func matchFilter(filter parserapi.Expr, row *engineapi.Row, columns []catalogapi.ColumnDef,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}) (bool, error) {
	if filter == nil {
		return true, nil
	}
	val, err := evalExpr(filter, row, columns, subqueryResults)
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

// evalLikeExpr evaluates a LIKE expression.
// Returns true (1), false (0), or NULL if the input is NULL.
func evalLikeExpr(expr *parserapi.LikeExpr, row *engineapi.Row, columns []catalogapi.ColumnDef, subqueryResults map[*parserapi.SubqueryExpr]interface{}) (catalogapi.Value, error) {
	val, err := evalExpr(expr.Expr, row, columns, subqueryResults)
	if err != nil {
		return catalogapi.Value{}, err
	}
	// NULL input → NULL result
	if val.IsNull {
		return catalogapi.Value{Type: catalogapi.TypeInt, IsNull: true}, nil
	}
	// Must be text
	if val.Type != catalogapi.TypeText {
		return catalogapi.Value{}, fmt.Errorf("LIKE requires text, got %v", val.Type)
	}
	matched := matchLike(val.Text, expr.Pattern, expr.Escape)
	if matched {
		return catalogapi.Value{Type: catalogapi.TypeInt, Int: 1}, nil
	}
	return catalogapi.Value{Type: catalogapi.TypeInt, Int: 0}, nil
}

// matchLike implements LIKE pattern matching.
// Wildcards: % matches any sequence (incl. empty), _ matches exactly one char.
// Escape char (non-zero) disables special meaning of % and _.
func matchLike(value, pattern string, escape byte) bool {
	// Dynamic programming: dp[i][j] = does pattern[:j] match value[:i]?
	// dp[0][0] = true (empty pattern matches empty value)
	// dp[0][j] = dp[0][j-1] && pattern[j-1] == '%'  (trailing % matches empty)
	pLen, vLen := len(pattern), len(value)
	dp := make([][]bool, vLen+1)
	for i := range dp {
		dp[i] = make([]bool, pLen+1)
	}
	dp[0][0] = true
	// Empty value against non-empty pattern: only % can match
	for j := 1; j <= pLen; j++ {
		dp[0][j] = dp[0][j-1] && pattern[j-1] == '%'
	}
	for i := 1; i <= vLen; i++ {
		for j := 1; j <= pLen; j++ {
			pat := pattern[j-1]
			if escape != 0 && pat == escape {
				// Escape: treat next char literally
				if j == pLen {
					// Trailing escape: treat as literal escape char
					dp[i][j] = dp[i-1][j-1] && i > 0 && value[i-1] == escape
				} else {
					// Escape the next character
					nextPat := pattern[j]
					dp[i][j] = dp[i-1][j-1] && value[i-1] == nextPat
				}
			} else if pat == '%' {
				// % matches: empty sequence OR one more char from value
				dp[i][j] = dp[i][j-1] || dp[i-1][j]
			} else if pat == '_' {
				// _ matches any single char
				dp[i][j] = dp[i-1][j-1]
			} else {
				// Literal match
				dp[i][j] = dp[i-1][j-1] && value[i-1] == pat
			}
		}
	}
	return dp[vLen][pLen]
}

// evalBetweenExpr evaluates col BETWEEN low AND high.
// Semantics: col >= low AND col <= low. Returns int(1) or int(0).
// If col, low, or high is NULL, the result is NULL.
func evalBetweenExpr(expr *parserapi.BetweenExpr, row *engineapi.Row, columns []catalogapi.ColumnDef, subqueryResults map[*parserapi.SubqueryExpr]interface{}) (catalogapi.Value, error) {
	col, err := evalExpr(expr.Expr, row, columns, subqueryResults)
	if err != nil {
		return catalogapi.Value{}, err
	}
	low, err := evalExpr(expr.Low, row, columns, subqueryResults)
	if err != nil {
		return catalogapi.Value{}, err
	}
	high, err := evalExpr(expr.High, row, columns, subqueryResults)
	if err != nil {
		return catalogapi.Value{}, err
	}
	// NULL input → NULL result
	if col.IsNull || low.IsNull || high.IsNull {
		return catalogapi.Value{Type: catalogapi.TypeInt, IsNull: true}, nil
	}
	// low > high → 0 rows (standard SQL semantics, no auto-swap)
	cmp, err := encoding.CompareValues(low, high)
	if err != nil {
		return catalogapi.Value{}, err
	}
	if cmp > 0 {
		return intVal(0), nil
	}
	// col >= low
	geLow, err := evalBinaryExpr(&parserapi.BinaryExpr{Left: expr.Expr, Op: parserapi.BinGE, Right: expr.Low}, row, columns, subqueryResults)
	if err != nil {
		return catalogapi.Value{}, err
	}
	if geLow.IsNull {
		return catalogapi.Value{Type: catalogapi.TypeInt, IsNull: true}, nil
	}
	if !isTruthy(geLow) {
		// col < low
		if expr.Not {
			return intVal(1), nil // NOT BETWEEN: outside range → true
		}
		return intVal(0), nil
	}
	// col <= high
	leHigh, err := evalBinaryExpr(&parserapi.BinaryExpr{Left: expr.Expr, Op: parserapi.BinLE, Right: expr.High}, row, columns, subqueryResults)
	if err != nil {
		return catalogapi.Value{}, err
	}
	if leHigh.IsNull {
		return catalogapi.Value{Type: catalogapi.TypeInt, IsNull: true}, nil
	}
	if isTruthy(leHigh) {
		if expr.Not {
			return intVal(0), nil // NOT BETWEEN: in range → false
		}
		return intVal(1), nil
	}
	// col > high
	if expr.Not {
		return intVal(1), nil // NOT BETWEEN: outside range → true
	}
	return intVal(0), nil
}

// evalInExpr evaluates col IN (val1, val2, ...).
// Semantics: col = val1 OR col = val2 OR ...
// NULL: if col is NULL, result is NULL. If any val is NULL, comparison yields NULL.
// SubqueryExpr in Values: subqueryResults map provides pre-computed subquery value lists.
func evalInExpr(expr *parserapi.InExpr, row *engineapi.Row, columns []catalogapi.ColumnDef, subqueryResults map[*parserapi.SubqueryExpr]interface{}) (catalogapi.Value, error) {
	if len(expr.Values) == 0 {
		return catalogapi.Value{}, fmt.Errorf("%w: IN list cannot be empty", executorapi.ErrExecFailed)
	}
	col, err := evalExpr(expr.Expr, row, columns, subqueryResults)
	if err != nil {
		return catalogapi.Value{}, err
	}
	// NULL column → NULL result
	if col.IsNull {
		return catalogapi.Value{Type: catalogapi.TypeInt, IsNull: true}, nil
	}

	// Build the full list of values to check: literals + subquery results
	var valuesToCheck []parserapi.Expr
	for _, valExpr := range expr.Values {
		if sq, ok := valExpr.(*parserapi.SubqueryExpr); ok {
			// Expand subquery results into individual values
			if vals, ok := subqueryResults[sq]; ok {
				switch v := vals.(type) {
				case []catalogapi.Value:
					for _, subVal := range v {
						lit := literalToExpr(subVal)
						if lit != nil {
							valuesToCheck = append(valuesToCheck, lit)
						}
					}
				}
			} else {
			}
		} else {
			valuesToCheck = append(valuesToCheck, valExpr)
		}
	}

	// col = val1 OR col = val2 OR ...
	anyNull := false
	for _, valExpr := range valuesToCheck {
		eq, err := evalBinaryExpr(&parserapi.BinaryExpr{Left: expr.Expr, Op: parserapi.BinEQ, Right: valExpr}, row, columns, subqueryResults)
		if err != nil {
			// Type mismatch: treat as no match, continue
			continue
		}
		// CR-F: track NULL comparisons (standard SQL: any NULL in IN/NOT IN list → NULL)
		if eq.IsNull {
			anyNull = true
			continue
		}
		if eq.Type == catalogapi.TypeInt && eq.Int == 1 {
			// TRUE match found
			if expr.Not {
				return intVal(0), nil // NOT IN: match -> false
			}
			return intVal(1), nil
		}
		// eq is FALSE — continue checking
	}
	// CR-F: NOT IN with NULL → NULL (standard SQL three-valued logic)
	if expr.Not && anyNull {
		return catalogapi.Value{Type: catalogapi.TypeInt, IsNull: true}, nil
	}
	// No TRUE match found; result is FALSE
	if expr.Not {
		return intVal(1), nil // NOT IN: no match -> true
	}
	return intVal(0), nil
}
