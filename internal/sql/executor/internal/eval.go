package internal

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"

	catalogapi "github.com/akzj/go-fast-kv/internal/sql/catalog/api"
	"github.com/akzj/go-fast-kv/internal/sql/encoding"
	encodingapi "github.com/akzj/go-fast-kv/internal/sql/encoding/api"
	engineapi "github.com/akzj/go-fast-kv/internal/sql/engine/api"
	executorapi "github.com/akzj/go-fast-kv/internal/sql/executor/api"
	parserapi "github.com/akzj/go-fast-kv/internal/sql/parser/api"
	plannerapi "github.com/akzj/go-fast-kv/internal/sql/planner/api"
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
	case *parserapi.CoalesceExpr:
		for _, arg := range e.Args {
			walkExpr(arg, fn)
		}
	case *parserapi.StringFuncExpr:
		for _, arg := range e.Args {
			walkExpr(arg, fn)
		}
		walkExpr(e.Start, fn)
		walkExpr(e.Len, fn)
	case *parserapi.JsonFuncExpr:
		for _, arg := range e.Args {
			walkExpr(arg, fn)
		}
	case *parserapi.CaseExpr:
		for _, w := range e.Whens {
			walkExpr(w.Cond, fn)
			walkExpr(w.Val, fn)
		}
		if e.Else != nil {
			walkExpr(e.Else, fn)
		}
	case *parserapi.ExistsExpr:
		// Subquery body not walked here (would need Statement visitor)
	case *parserapi.CastExpr:
		walkExpr(e.Expr, fn)
	}
}

// evalExpr evaluates an expression against a row and returns a Value.
// subqueryResults provides pre-computed values for SubqueryExpr nodes.
// ex provides access to outerCols/outerVals for correlated subquery resolution.
func evalExpr(expr parserapi.Expr, row *engineapi.Row, columns []catalogapi.ColumnDef,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}, ex *executor) (catalogapi.Value, error) {
	switch node := expr.(type) {
	case *parserapi.ColumnRef:
		return evalColumnRef(node, row, columns, ex)
	case *parserapi.Literal:
		return node.Value, nil
	case *parserapi.ParamRef:
		// Resolve positional parameter: $1 → params[0], $2 → params[1], etc.
		if ex.params == nil {
			return catalogapi.Value{}, fmt.Errorf("%w: no parameters provided for $%d", executorapi.ErrExecFailed, node.Index)
		}
		if node.Index < 1 || node.Index > len(ex.params) {
			return catalogapi.Value{}, fmt.Errorf("%w: parameter index %d out of range (have %d parameters)", executorapi.ErrExecFailed, node.Index, len(ex.params))
		}
		return ex.params[node.Index-1], nil
	case *parserapi.BinaryExpr:
		return evalBinaryExpr(node, row, columns, subqueryResults, ex)
	case *parserapi.UnaryExpr:
		return evalUnaryExpr(node, row, columns, subqueryResults, ex)
	case *parserapi.IsNullExpr:
		return evalIsNullExpr(node, row, columns, subqueryResults, ex)
	case *parserapi.LikeExpr:
		return evalLikeExpr(node, row, columns, subqueryResults, ex)
	case *parserapi.AggregateCallExpr:
		return catalogapi.Value{}, fmt.Errorf("%w: aggregate %s() must be used in a GROUP BY context", executorapi.ErrExecFailed, node.Func)
	case *parserapi.WindowFuncExpr:
		// Look up pre-computed window function value from windowResults
		result, exists := ex.windowResults[node]
		if !exists || result == nil || len(result.Values) == 0 {
			return catalogapi.Value{}, fmt.Errorf("%w: window function %s() not yet computed", executorapi.ErrExecFailed, node.Func)
		}
		// Find the index of this row in the result
		idx, ok := result.rowIndexMap[row]
		if !ok {
			// Fallback: iterate to find row pointer match (should be rare)
			for j := range result.Values {
				return result.Values[j], nil
			}
			return catalogapi.Value{}, fmt.Errorf("%w: window function %s() row not found", executorapi.ErrExecFailed, node.Func)
		}
		return result.Values[idx], nil
	case *parserapi.BetweenExpr:
		return evalBetweenExpr(node, row, columns, subqueryResults, ex)
	case *parserapi.InExpr:
		return evalInExpr(node, row, columns, subqueryResults, ex)
	case *parserapi.CoalesceExpr:
		return evalCoalesceExpr(node, row, columns, subqueryResults, ex)
	case *parserapi.NullIfExpr:
		return evalNullIfExpr(node, row, columns, subqueryResults, ex)
	case *parserapi.StringFuncExpr:
		return evalStringFuncExpr(node, row, columns, subqueryResults, ex)
	case *parserapi.JsonFuncExpr:
		return evalJsonFuncExpr(node, row, columns, subqueryResults, ex)
	case *parserapi.CastExpr:
		return evalCastExpr(node, row, columns, subqueryResults, ex)
	case *parserapi.CaseExpr:
		return evalCaseExpr(node, row, columns, subqueryResults, ex)
	case *parserapi.ExistsExpr:
		return evalExistsExpr(node, row, columns, subqueryResults, ex)
	case *parserapi.SubqueryExpr:
		// Pre-computed by execSelect pre-plan pass.
		// Scalar subqueries store a single catalogapi.Value.
		// IN-list subqueries store a []catalogapi.Value (handled by evalInExpr).
		if vals, ok := subqueryResults[node]; ok {
			switch v := vals.(type) {
			case catalogapi.Value:
				return v, nil
			case []catalogapi.Value:
				return catalogapi.Value{IsNull: true}, nil
			default:
				return catalogapi.Value{IsNull: true}, nil
			}
		}
		// Not pre-computed: execute on-demand for correlated subqueries
		return ex.execSubquery(node.Plan)
	case *parserapi.FunctionCallExpr:
		return evalFunctionCall(node, row, columns, subqueryResults, ex)
	default:
		return catalogapi.Value{}, fmt.Errorf("%w: unsupported expression type %T", executorapi.ErrExecFailed, expr)
	}
}

// FunctionRegistry stores user-defined functions in memory.
type FunctionRegistry struct {
	funcs map[string]*FunctionDef
	mu    sync.RWMutex
}

// FunctionDef defines a user-defined function.
type FunctionDef struct {
	Name    string
	Args    []string // parameter names (in order)
	RetType string   // return type: "INT", "TEXT", "FLOAT", "BLOB"
	Body    string   // function body expression (e.g., "a + b")
}

// Register adds a function to the registry.
func (r *FunctionRegistry) Register(def *FunctionDef) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.funcs == nil {
		r.funcs = make(map[string]*FunctionDef)
	}
	r.funcs[strings.ToUpper(def.Name)] = def
}

// Get retrieves a function by name. Returns nil if not found.
func (r *FunctionRegistry) Get(name string) (*FunctionDef, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	fn, ok := r.funcs[strings.ToUpper(name)]
	return fn, ok
}

// NewFunctionRegistry creates a new empty FunctionRegistry.
func NewFunctionRegistry() *FunctionRegistry {
	return &FunctionRegistry{}
}

// evalFunctionCall evaluates a user-defined function call.
func evalFunctionCall(call *parserapi.FunctionCallExpr, row *engineapi.Row, columns []catalogapi.ColumnDef,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}, ex *executor) (catalogapi.Value, error) {
	// Get function from registry
	fn, ok := ex.funcRegistry.Get(call.Name)
	if !ok || fn == nil {
		return catalogapi.Value{}, fmt.Errorf("%w: function %q not found", executorapi.ErrExecFailed, call.Name)
	}

	// Bind arguments to parameter names
	env := make(map[string]parserapi.Expr)
	for i, arg := range call.Args {
		val, err := evalExpr(arg, row, columns, subqueryResults, ex)
		if err != nil {
			return catalogapi.Value{}, err
		}
		if i < len(fn.Args) {
			env[fn.Args[i]] = &parserapi.Literal{Value: val}
		}
	}

	// Parse body as expression using a simple approach:
	// Re-parse the body expression with parameter substitutions pre-evaluated.
	// For MVP, body must be a single expression.
	// Note: Full implementation would substitute parameters in the body string.
	// Here we return the body string for now (MVP: not yet implemented).
	return catalogapi.Value{}, fmt.Errorf("%w: function execution: body evaluation not yet implemented", executorapi.ErrExecFailed)
}

// evalColumnRef looks up a column value from the row.
// e provides access to outerCols/outerVals for correlated subquery resolution,
// and triggerNewCols/triggerNewVals/OLD for row-level trigger references.
func evalColumnRef(ref *parserapi.ColumnRef, row *engineapi.Row, columns []catalogapi.ColumnDef, e *executor) (catalogapi.Value, error) {
	upper := strings.ToUpper(ref.Column)
	upperTable := strings.ToUpper(ref.Table)

	// Helper to find column in given columns
	findIn := func(cols []catalogapi.ColumnDef, vals []catalogapi.Value) (catalogapi.Value, bool) {
		if cols == nil || vals == nil {
			return catalogapi.Value{}, false
		}
		// First pass: table-qualified match (both table and column name)
		if upperTable != "" {
			for i, col := range cols {
				if strings.ToUpper(col.Name) == upper && strings.EqualFold(col.Table, upperTable) {
					if i < len(vals) {
						return vals[i], true
					}
					return catalogapi.Value{IsNull: true}, true
				}
			}
		}
		// Second pass: unqualified fallback (name only).
		// Skip this fallback when the ref has a table qualifier AND the
		// columns have Table fields set — the qualified match above should
		// have found it. This prevents misresolving e.g. "users.id" as
		// "orders.id" when both tables have an "id" column.
		if upperTable != "" {
			// Check if any column in this set has a Table field set.
			// If so, the qualified match was authoritative — do not fallback.
			for _, col := range cols {
				if col.Table != "" {
					return catalogapi.Value{}, false
				}
			}
		}
		for i, col := range cols {
			if strings.ToUpper(col.Name) == upper {
				if i < len(vals) {
					return vals[i], true
				}
				return catalogapi.Value{IsNull: true}, true
			}
		}
		return catalogapi.Value{}, false
	}

	// Try current table columns first (skip if row is nil)
	if row != nil {
		if val, ok := findIn(columns, row.Values); ok {
			return val, nil
		}
	}

	// Try NEW. reference for INSERT/UPDATE triggers
	if upperTable == "NEW" && e != nil {
		if val, ok := findIn(e.triggerNewCols, e.triggerNewVals); ok {
			return val, nil
		}
	}

	// Try OLD. reference for UPDATE/DELETE triggers
	if upperTable == "OLD" && e != nil {
		if val, ok := findIn(e.triggerOldCols, e.triggerOldVals); ok {
			return val, nil
		}
	}

	// Fall back to outer columns for correlated subqueries
	if e != nil {
		if val, ok := findIn(e.outerCols, e.outerVals); ok {
			return val, nil
		}
	}

	return catalogapi.Value{}, fmt.Errorf("%w: column %q not found", executorapi.ErrExecFailed, ref.Column)
}

// evalBinaryExpr evaluates a binary expression (AND, OR, comparisons).
func evalBinaryExpr(expr *parserapi.BinaryExpr, row *engineapi.Row, columns []catalogapi.ColumnDef,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}, ex *executor) (catalogapi.Value, error) {
	// SQL three-valued logic for AND/OR:
	//   AND: FALSE AND x → FALSE; TRUE AND x → x; NULL AND FALSE → FALSE; NULL AND TRUE → NULL; NULL AND NULL → NULL
	//   OR:  TRUE OR x → TRUE; FALSE OR x → x; NULL OR TRUE → TRUE; NULL OR FALSE → NULL; NULL OR NULL → NULL
	switch expr.Op {
	case parserapi.BinAnd:
		left, err := evalExpr(expr.Left, row, columns, subqueryResults, ex)
		if err != nil {
			return catalogapi.Value{}, err
		}
		leftNull := left.IsNull
		leftTrue := !leftNull && isTruthy(left)
		if !leftNull && !leftTrue {
			return intVal(0), nil
		}
		right, err := evalExpr(expr.Right, row, columns, subqueryResults, ex)
		if err != nil {
			return catalogapi.Value{}, err
		}
		rightNull := right.IsNull
		rightTrue := !rightNull && isTruthy(right)
		if !rightNull && !rightTrue {
			return intVal(0), nil
		}
		if leftNull || rightNull {
			return catalogapi.Value{IsNull: true}, nil
		}
		if leftTrue && rightTrue {
			return intVal(1), nil
		}
		return intVal(0), nil

	case parserapi.BinOr:
		left, err := evalExpr(expr.Left, row, columns, subqueryResults, ex)
		if err != nil {
			return catalogapi.Value{}, err
		}
		leftNull := left.IsNull
		leftTrue := !leftNull && isTruthy(left)
		if leftTrue {
			return intVal(1), nil
		}
		right, err := evalExpr(expr.Right, row, columns, subqueryResults, ex)
		if err != nil {
			return catalogapi.Value{}, err
		}
		rightNull := right.IsNull
		rightTrue := !rightNull && isTruthy(right)
		if rightTrue {
			return intVal(1), nil
		}
		if leftNull || rightNull {
			return catalogapi.Value{IsNull: true}, nil
		}
		return intVal(0), nil
	}

	// Comparison operators
	left, err := evalExpr(expr.Left, row, columns, subqueryResults, ex)
	if err != nil {
		return catalogapi.Value{}, err
	}
	right, err := evalExpr(expr.Right, row, columns, subqueryResults, ex)
	if err != nil {
		return catalogapi.Value{}, err
	}

	// Any comparison with NULL returns NULL (SQL three-valued logic)
	if left.IsNull || right.IsNull {
		return catalogapi.Value{IsNull: true}, nil
	}

	cmp, err := encoding.CompareValues(left, right)
	if err != nil {
		return catalogapi.Value{}, fmt.Errorf("%w: %v", executorapi.ErrExecFailed, err)
	}

	switch expr.Op {
	case parserapi.BinEQ:
		if cmp == 0 {
			return intVal(1), nil
		}
		return intVal(0), nil
	case parserapi.BinNE:
		if cmp != 0 {
			return intVal(1), nil
		}
		return intVal(0), nil
	case parserapi.BinLT:
		if cmp < 0 {
			return intVal(1), nil
		}
		return intVal(0), nil
	case parserapi.BinLE:
		if cmp <= 0 {
			return intVal(1), nil
		}
		return intVal(0), nil
	case parserapi.BinGT:
		if cmp > 0 {
			return intVal(1), nil
		}
		return intVal(0), nil
	case parserapi.BinGE:
		if cmp >= 0 {
			return intVal(1), nil
		}
		return intVal(0), nil
	case parserapi.BinAdd, parserapi.BinSub, parserapi.BinMul, parserapi.BinDiv:
		// Arithmetic operators: use already-evaluated left/right from above.
		if left.IsNull || right.IsNull {
			return catalogapi.Value{IsNull: true}, nil
		}
		switch left.Type {
		case catalogapi.TypeInt:
			switch right.Type {
			case catalogapi.TypeInt:
				switch expr.Op {
				case parserapi.BinAdd:
					return catalogapi.Value{Type: catalogapi.TypeInt, Int: left.Int + right.Int}, nil
				case parserapi.BinSub:
					return catalogapi.Value{Type: catalogapi.TypeInt, Int: left.Int - right.Int}, nil
				case parserapi.BinMul:
					return catalogapi.Value{Type: catalogapi.TypeInt, Int: left.Int * right.Int}, nil
				case parserapi.BinDiv:
					if right.Int == 0 {
						return catalogapi.Value{}, fmt.Errorf("%w: division by zero", executorapi.ErrExecFailed)
					}
					return catalogapi.Value{Type: catalogapi.TypeInt, Int: left.Int / right.Int}, nil
				}
			case catalogapi.TypeFloat:
				switch expr.Op {
				case parserapi.BinAdd:
					return catalogapi.Value{Type: catalogapi.TypeFloat, Float: float64(left.Int) + right.Float}, nil
				case parserapi.BinSub:
					return catalogapi.Value{Type: catalogapi.TypeFloat, Float: float64(left.Int) - right.Float}, nil
				case parserapi.BinMul:
					return catalogapi.Value{Type: catalogapi.TypeFloat, Float: float64(left.Int) * right.Float}, nil
				case parserapi.BinDiv:
					return catalogapi.Value{Type: catalogapi.TypeFloat, Float: float64(left.Int) / right.Float}, nil
				}
			}
		case catalogapi.TypeFloat:
			switch right.Type {
			case catalogapi.TypeInt:
				switch expr.Op {
				case parserapi.BinAdd:
					return catalogapi.Value{Type: catalogapi.TypeFloat, Float: left.Float + float64(right.Int)}, nil
				case parserapi.BinSub:
					return catalogapi.Value{Type: catalogapi.TypeFloat, Float: left.Float - float64(right.Int)}, nil
				case parserapi.BinMul:
					return catalogapi.Value{Type: catalogapi.TypeFloat, Float: left.Float * float64(right.Int)}, nil
				case parserapi.BinDiv:
					return catalogapi.Value{Type: catalogapi.TypeFloat, Float: left.Float / float64(right.Int)}, nil
				}
			case catalogapi.TypeFloat:
				switch expr.Op {
				case parserapi.BinAdd:
					return catalogapi.Value{Type: catalogapi.TypeFloat, Float: left.Float + right.Float}, nil
				case parserapi.BinSub:
					return catalogapi.Value{Type: catalogapi.TypeFloat, Float: left.Float - right.Float}, nil
				case parserapi.BinMul:
					return catalogapi.Value{Type: catalogapi.TypeFloat, Float: left.Float * right.Float}, nil
				case parserapi.BinDiv:
					return catalogapi.Value{Type: catalogapi.TypeFloat, Float: left.Float / right.Float}, nil
				}
			}
		}
		return catalogapi.Value{}, fmt.Errorf("%w: cannot perform arithmetic on non-numeric types", executorapi.ErrExecFailed)
	default:
		return catalogapi.Value{}, fmt.Errorf("%w: unsupported binary op %d", executorapi.ErrExecFailed, expr.Op)
	}
}

// evalUnaryExpr evaluates NOT and unary minus.
func evalUnaryExpr(expr *parserapi.UnaryExpr, row *engineapi.Row, columns []catalogapi.ColumnDef,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}, ex *executor) (catalogapi.Value, error) {
	operand, err := evalExpr(expr.Operand, row, columns, subqueryResults, ex)
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
func evalIsNullExpr(expr *parserapi.IsNullExpr, row *engineapi.Row, columns []catalogapi.ColumnDef,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}, ex *executor) (catalogapi.Value, error) {
	inner, err := evalExpr(expr.Expr, row, columns, subqueryResults, ex)
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
// ex provides access to outerCols/outerVals for correlated subquery resolution.
func matchFilter(filter parserapi.Expr, row *engineapi.Row, columns []catalogapi.ColumnDef,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}, ex *executor) (bool, error) {
	if filter == nil {
		return true, nil
	}
	val, err := evalExpr(filter, row, columns, subqueryResults, ex)
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
func evalLikeExpr(expr *parserapi.LikeExpr, row *engineapi.Row, columns []catalogapi.ColumnDef,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}, ex *executor) (catalogapi.Value, error) {
	val, err := evalExpr(expr.Expr, row, columns, subqueryResults, ex)
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
	if expr.Not {
		matched = !matched
	}
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
func evalBetweenExpr(expr *parserapi.BetweenExpr, row *engineapi.Row, columns []catalogapi.ColumnDef,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}, ex *executor) (catalogapi.Value, error) {
	col, err := evalExpr(expr.Expr, row, columns, subqueryResults, ex)
	if err != nil {
		return catalogapi.Value{}, err
	}
	low, err := evalExpr(expr.Low, row, columns, subqueryResults, ex)
	if err != nil {
		return catalogapi.Value{}, err
	}
	high, err := evalExpr(expr.High, row, columns, subqueryResults, ex)
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
	geLow, err := evalBinaryExpr(&parserapi.BinaryExpr{Left: expr.Expr, Op: parserapi.BinGE, Right: expr.Low}, row, columns, subqueryResults, ex)
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
	leHigh, err := evalBinaryExpr(&parserapi.BinaryExpr{Left: expr.Expr, Op: parserapi.BinLE, Right: expr.High}, row, columns, subqueryResults, ex)
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
func evalInExpr(expr *parserapi.InExpr, row *engineapi.Row, columns []catalogapi.ColumnDef,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}, ex *executor) (catalogapi.Value, error) {
	if len(expr.Values) == 0 {
		return catalogapi.Value{}, fmt.Errorf("%w: IN list cannot be empty", executorapi.ErrExecFailed)
	}
	col, err := evalExpr(expr.Expr, row, columns, subqueryResults, ex)
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
		eq, err := evalBinaryExpr(&parserapi.BinaryExpr{Left: expr.Expr, Op: parserapi.BinEQ, Right: valExpr}, row, columns, subqueryResults, ex)
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

// evalCoalesceExpr evaluates COALESCE(expr1, expr2, ...).
// Returns the first non-NULL value. If all are NULL, returns NULL.
func evalCoalesceExpr(expr *parserapi.CoalesceExpr, row *engineapi.Row, columns []catalogapi.ColumnDef,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}, ex *executor) (catalogapi.Value, error) {
	// Defensive check: COALESCE requires at least one argument
	if len(expr.Args) == 0 {
		return catalogapi.Value{}, fmt.Errorf("%w: COALESCE requires at least one argument", executorapi.ErrExecFailed)
	}
	for _, arg := range expr.Args {
		val, err := evalExpr(arg, row, columns, subqueryResults, ex)
		if err != nil {
			return catalogapi.Value{}, err
		}
		if !val.IsNull {
			return val, nil
		}
	}
	// All values were NULL
	return catalogapi.Value{IsNull: true}, nil
}

// evalNullIfExpr evaluates NULLIF(a, b).
// Returns a if a != b, returns NULL if a == b.
// NULLIF is equivalent to CASE WHEN a = b THEN NULL ELSE a END.
func evalNullIfExpr(expr *parserapi.NullIfExpr, row *engineapi.Row, columns []catalogapi.ColumnDef,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}, ex *executor) (catalogapi.Value, error) {
	// Evaluate left operand
	left, err := evalExpr(expr.Left, row, columns, subqueryResults, ex)
	if err != nil {
		return catalogapi.Value{}, err
	}
	// NULL input → NULL result
	if left.IsNull {
		return catalogapi.Value{IsNull: true}, nil
	}
	// Evaluate right operand
	right, err := evalExpr(expr.Right, row, columns, subqueryResults, ex)
	if err != nil {
		return catalogapi.Value{}, err
	}
	// NULL input → NULL result
	if right.IsNull {
		return catalogapi.Value{IsNull: true}, nil
	}
	// Compare for equality (returns -1, 0, 1)
	eq := compareValues(left, right)
	// If equal → return NULL; if not equal → return left
	if eq == 0 {
		return catalogapi.Value{IsNull: true}, nil
	}
	return left, nil
}

// evalStringFuncExpr evaluates string functions: SUBSTRING, CONCAT, UPPER, LOWER, LENGTH, TRIM.
func evalStringFuncExpr(expr *parserapi.StringFuncExpr, row *engineapi.Row, columns []catalogapi.ColumnDef,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}, ex *executor) (catalogapi.Value, error) {
	switch expr.Func {
	case "SUBSTRING":
		return evalSubstring(expr, row, columns, subqueryResults, ex)
	case "CONCAT":
		return evalConcat(expr, row, columns, subqueryResults, ex)
	case "UPPER":
		return evalUpper(expr, row, columns, subqueryResults, ex)
	case "LOWER":
		return evalLower(expr, row, columns, subqueryResults, ex)
	case "LENGTH":
		return evalLength(expr, row, columns, subqueryResults, ex)
	case "TRIM":
		return evalTrim(expr, row, columns, subqueryResults, ex)
	default:
		return catalogapi.Value{}, fmt.Errorf("%w: unknown string function %s", executorapi.ErrExecFailed, expr.Func)
	}
}

// evalSubstring evaluates SUBSTRING(str FROM start [FOR len]) or SUBSTRING(str, start [, len]).
// SQL positions are 1-indexed. Substring is: str[start-1 : start-1+len]
func evalSubstring(expr *parserapi.StringFuncExpr, row *engineapi.Row, columns []catalogapi.ColumnDef,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}, ex *executor) (catalogapi.Value, error) {
	if len(expr.Args) == 0 {
		return catalogapi.Value{}, fmt.Errorf("%w: SUBSTRING requires at least one argument", executorapi.ErrExecFailed)
	}
	// Evaluate the string argument
	strVal, err := evalExpr(expr.Args[0], row, columns, subqueryResults, ex)
	if err != nil {
		return catalogapi.Value{}, err
	}
	if strVal.IsNull {
		return catalogapi.Value{IsNull: true}, nil
	}
	if strVal.Type != catalogapi.TypeText {
		return catalogapi.Value{}, fmt.Errorf("%w: SUBSTRING requires text argument, got %v", executorapi.ErrExecFailed, strVal.Type)
	}
	str := strVal.Text

	// Evaluate start position
	if expr.Start == nil {
		return catalogapi.Value{}, fmt.Errorf("%w: SUBSTRING requires start position", executorapi.ErrExecFailed)
	}
	startVal, err := evalExpr(expr.Start, row, columns, subqueryResults, ex)
	if err != nil {
		return catalogapi.Value{}, err
	}
	if startVal.IsNull {
		return catalogapi.Value{IsNull: true}, nil
	}
	if startVal.Type != catalogapi.TypeInt {
		return catalogapi.Value{}, fmt.Errorf("%w: SUBSTRING start position must be integer, got %v", executorapi.ErrExecFailed, startVal.Type)
	}
	start := int(startVal.Int)
	// Convert 1-indexed to 0-indexed
	startIdx := start - 1
	if startIdx < 0 {
		startIdx = 0
	}
	if startIdx > len(str) {
		startIdx = len(str)
	}

	// Optional length
	if expr.Len != nil {
		lenVal, err := evalExpr(expr.Len, row, columns, subqueryResults, ex)
		if err != nil {
			return catalogapi.Value{}, err
		}
		if lenVal.IsNull {
			return catalogapi.Value{IsNull: true}, nil
		}
		if lenVal.Type != catalogapi.TypeInt {
			return catalogapi.Value{}, fmt.Errorf("%w: SUBSTRING length must be integer, got %v", executorapi.ErrExecFailed, lenVal.Type)
		}
		length := int(lenVal.Int)
		if length < 0 {
			length = 0
		}
		endIdx := startIdx + length
		if endIdx > len(str) {
			endIdx = len(str)
		}
		return catalogapi.Value{Type: catalogapi.TypeText, Text: str[startIdx:endIdx]}, nil
	}

	// No length: return from start to end
	return catalogapi.Value{Type: catalogapi.TypeText, Text: str[startIdx:]}, nil
}

// evalConcat evaluates CONCAT(str1, str2, ...). Concatenates all arguments.
func evalConcat(expr *parserapi.StringFuncExpr, row *engineapi.Row, columns []catalogapi.ColumnDef,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}, ex *executor) (catalogapi.Value, error) {
	var sb strings.Builder
	for _, arg := range expr.Args {
		val, err := evalExpr(arg, row, columns, subqueryResults, ex)
		if err != nil {
			return catalogapi.Value{}, err
		}
		if val.IsNull {
			// If any argument is NULL, CONCAT returns NULL
			return catalogapi.Value{IsNull: true}, nil
		}
		switch val.Type {
		case catalogapi.TypeText:
			sb.WriteString(val.Text)
		case catalogapi.TypeInt:
			sb.WriteString(strconv.FormatInt(val.Int, 10))
		case catalogapi.TypeFloat:
			sb.WriteString(strconv.FormatFloat(val.Float, 'f', -1, 64))
		case catalogapi.TypeBlob:
			sb.Write(val.Blob)
		}
	}
	return catalogapi.Value{Type: catalogapi.TypeText, Text: sb.String()}, nil
}

// evalUpper evaluates UPPER(str). Converts string to uppercase.
func evalUpper(expr *parserapi.StringFuncExpr, row *engineapi.Row, columns []catalogapi.ColumnDef,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}, ex *executor) (catalogapi.Value, error) {
	if len(expr.Args) == 0 {
		return catalogapi.Value{}, fmt.Errorf("%w: UPPER requires one argument", executorapi.ErrExecFailed)
	}
	val, err := evalExpr(expr.Args[0], row, columns, subqueryResults, ex)
	if err != nil {
		return catalogapi.Value{}, err
	}
	if val.IsNull {
		return catalogapi.Value{IsNull: true}, nil
	}
	if val.Type != catalogapi.TypeText {
		return catalogapi.Value{}, fmt.Errorf("%w: UPPER requires text argument, got %v", executorapi.ErrExecFailed, val.Type)
	}
	return catalogapi.Value{Type: catalogapi.TypeText, Text: strings.ToUpper(val.Text)}, nil
}

// evalLower evaluates LOWER(str). Converts string to lowercase.
func evalLower(expr *parserapi.StringFuncExpr, row *engineapi.Row, columns []catalogapi.ColumnDef,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}, ex *executor) (catalogapi.Value, error) {
	if len(expr.Args) == 0 {
		return catalogapi.Value{}, fmt.Errorf("%w: LOWER requires one argument", executorapi.ErrExecFailed)
	}
	val, err := evalExpr(expr.Args[0], row, columns, subqueryResults, ex)
	if err != nil {
		return catalogapi.Value{}, err
	}
	if val.IsNull {
		return catalogapi.Value{IsNull: true}, nil
	}
	if val.Type != catalogapi.TypeText {
		return catalogapi.Value{}, fmt.Errorf("%w: LOWER requires text argument, got %v", executorapi.ErrExecFailed, val.Type)
	}
	return catalogapi.Value{Type: catalogapi.TypeText, Text: strings.ToLower(val.Text)}, nil
}

// evalLength evaluates LENGTH(str). Returns the length of the string.
func evalLength(expr *parserapi.StringFuncExpr, row *engineapi.Row, columns []catalogapi.ColumnDef,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}, ex *executor) (catalogapi.Value, error) {
	if len(expr.Args) == 0 {
		return catalogapi.Value{}, fmt.Errorf("%w: LENGTH requires one argument", executorapi.ErrExecFailed)
	}
	val, err := evalExpr(expr.Args[0], row, columns, subqueryResults, ex)
	if err != nil {
		return catalogapi.Value{}, err
	}
	if val.IsNull {
		return catalogapi.Value{IsNull: true}, nil
	}
	switch val.Type {
	case catalogapi.TypeText:
		return catalogapi.Value{Type: catalogapi.TypeInt, Int: int64(len(val.Text))}, nil
	case catalogapi.TypeBlob:
		return catalogapi.Value{Type: catalogapi.TypeInt, Int: int64(len(val.Blob))}, nil
	default:
		return catalogapi.Value{}, fmt.Errorf("%w: LENGTH requires text or blob argument, got %v", executorapi.ErrExecFailed, val.Type)
	}
}

// evalTrim evaluates TRIM(str). Removes leading and trailing whitespace.
func evalTrim(expr *parserapi.StringFuncExpr, row *engineapi.Row, columns []catalogapi.ColumnDef,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}, ex *executor) (catalogapi.Value, error) {
	if len(expr.Args) == 0 {
		return catalogapi.Value{}, fmt.Errorf("%w: TRIM requires one argument", executorapi.ErrExecFailed)
	}
	val, err := evalExpr(expr.Args[0], row, columns, subqueryResults, ex)
	if err != nil {
		return catalogapi.Value{}, err
	}
	if val.IsNull {
		return catalogapi.Value{IsNull: true}, nil
	}
	if val.Type != catalogapi.TypeText {
		return catalogapi.Value{}, fmt.Errorf("%w: TRIM requires text argument, got %v", executorapi.ErrExecFailed, val.Type)
	}
	return catalogapi.Value{Type: catalogapi.TypeText, Text: strings.TrimSpace(val.Text)}, nil
}

// evalCastExpr evaluates CAST(expr AS type).
// Converts a value from one type to another.
func evalCastExpr(expr *parserapi.CastExpr, row *engineapi.Row, columns []catalogapi.ColumnDef,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}, ex *executor) (catalogapi.Value, error) {
	// Evaluate the expression first
	val, err := evalExpr(expr.Expr, row, columns, subqueryResults, ex)
	if err != nil {
		return catalogapi.Value{}, err
	}
	// NULL input → NULL output
	if val.IsNull {
		return catalogapi.Value{IsNull: true}, nil
	}

	switch expr.TypeName {
	case "INT", "INTEGER":
		return castToInt(val)
	case "TEXT":
		return castToText(val)
	case "FLOAT":
		return castToFloat(val)
	case "BLOB":
		return castToBlob(val)
	default:
		return catalogapi.Value{}, fmt.Errorf("%w: unsupported cast type %s", executorapi.ErrExecFailed, expr.TypeName)
	}
}

// castToInt converts a value to INT.
func castToInt(val catalogapi.Value) (catalogapi.Value, error) {
	switch val.Type {
	case catalogapi.TypeInt:
		return val, nil
	case catalogapi.TypeText:
		i, err := strconv.ParseInt(val.Text, 10, 64)
		if err != nil {
			return catalogapi.Value{}, fmt.Errorf("%w: cannot cast %q to INT", executorapi.ErrExecFailed, val.Text)
		}
		return catalogapi.Value{Type: catalogapi.TypeInt, Int: i}, nil
	case catalogapi.TypeFloat:
		return catalogapi.Value{Type: catalogapi.TypeInt, Int: int64(val.Float)}, nil
	case catalogapi.TypeBlob:
		return catalogapi.Value{}, fmt.Errorf("%w: cannot cast BLOB to INT", executorapi.ErrExecFailed)
	default:
		return catalogapi.Value{}, fmt.Errorf("%w: cannot cast %v to INT", executorapi.ErrExecFailed, val.Type)
	}
}

// castToText converts a value to TEXT.
func castToText(val catalogapi.Value) (catalogapi.Value, error) {
	switch val.Type {
	case catalogapi.TypeText:
		return val, nil
	case catalogapi.TypeInt:
		return catalogapi.Value{Type: catalogapi.TypeText, Text: strconv.FormatInt(val.Int, 10)}, nil
	case catalogapi.TypeFloat:
		return catalogapi.Value{Type: catalogapi.TypeText, Text: strconv.FormatFloat(val.Float, 'f', -1, 64)}, nil
	case catalogapi.TypeBlob:
		return catalogapi.Value{Type: catalogapi.TypeText, Text: string(val.Blob)}, nil
	default:
		return catalogapi.Value{}, fmt.Errorf("%w: cannot cast %v to TEXT", executorapi.ErrExecFailed, val.Type)
	}
}

// castToFloat converts a value to FLOAT.
func castToFloat(val catalogapi.Value) (catalogapi.Value, error) {
	switch val.Type {
	case catalogapi.TypeFloat:
		return val, nil
	case catalogapi.TypeInt:
		return catalogapi.Value{Type: catalogapi.TypeFloat, Float: float64(val.Int)}, nil
	case catalogapi.TypeText:
		f, err := strconv.ParseFloat(val.Text, 64)
		if err != nil {
			return catalogapi.Value{}, fmt.Errorf("%w: cannot cast %q to FLOAT", executorapi.ErrExecFailed, val.Text)
		}
		return catalogapi.Value{Type: catalogapi.TypeFloat, Float: f}, nil
	case catalogapi.TypeBlob:
		return catalogapi.Value{}, fmt.Errorf("%w: cannot cast BLOB to FLOAT", executorapi.ErrExecFailed)
	default:
		return catalogapi.Value{}, fmt.Errorf("%w: cannot cast %v to FLOAT", executorapi.ErrExecFailed, val.Type)
	}
}

// castToBlob converts a value to BLOB.
func castToBlob(val catalogapi.Value) (catalogapi.Value, error) {
	switch val.Type {
	case catalogapi.TypeBlob:
		return val, nil
	case catalogapi.TypeText:
		return catalogapi.Value{Type: catalogapi.TypeBlob, Blob: []byte(val.Text)}, nil
	case catalogapi.TypeInt:
		return catalogapi.Value{Type: catalogapi.TypeBlob, Blob: []byte(strconv.FormatInt(val.Int, 10))}, nil
	case catalogapi.TypeFloat:
		return catalogapi.Value{Type: catalogapi.TypeBlob, Blob: []byte(strconv.FormatFloat(val.Float, 'f', -1, 64))}, nil
	default:
		return catalogapi.Value{}, fmt.Errorf("%w: cannot cast %v to BLOB", executorapi.ErrExecFailed, val.Type)
	}
}

// evalCaseExpr evaluates a CASE expression.
// SQL three-valued logic: WHEN condition must evaluate to TRUE (not NULL, not FALSE).
// If condition is NULL → falls through to next WHEN/ELSE.
// If condition is FALSE → falls through to next WHEN/ELSE.
func evalCaseExpr(expr *parserapi.CaseExpr, row *engineapi.Row, columns []catalogapi.ColumnDef,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}, ex *executor) (catalogapi.Value, error) {
	for _, w := range expr.Whens {
		condVal, err := evalExpr(w.Cond, row, columns, subqueryResults, ex)
		if err != nil {
			return catalogapi.Value{}, err
		}
		// SQL: WHEN condition must be TRUE (not NULL, not FALSE)
		if !condVal.IsNull && isTruthy(condVal) {
			return evalExpr(w.Val, row, columns, subqueryResults, ex)
		}
		// NULL or FALSE → fall through
	}
	// No WHEN matched — return ELSE or NULL
	if expr.Else != nil {
		return evalExpr(expr.Else, row, columns, subqueryResults, ex)
	}
	return catalogapi.Value{IsNull: true}, nil
}

// evalExistsExpr evaluates EXISTS (SELECT ...) or NOT EXISTS (SELECT ...).
// EXISTS returns TRUE if subquery returns at least 1 row, FALSE otherwise.
// The subquery is executed with outer row context for correlated subqueries.
func evalExistsExpr(expr *parserapi.ExistsExpr, row *engineapi.Row, columns []catalogapi.ColumnDef,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}, ex *executor) (catalogapi.Value, error) {
	// Set outer context for correlated subquery resolution
	if ex != nil {
		if row != nil {
			ex.outerVals = row.Values
		}
		if columns != nil {
			ex.outerCols = columns
		}
	}
	typedPlan, ok := expr.Subquery.Plan.(plannerapi.Plan)
	if !ok {
		return catalogapi.Value{}, fmt.Errorf("exists subquery plan has wrong type: %T", expr.Subquery.Plan)
	}
	// Save outer context — inner Execute will overwrite e.outerCols/e.outerVals.
	savedOuterCols := ex.outerCols
	savedOuterVals := ex.outerVals
	result, err := ex.Execute(typedPlan)
	// Restore outer context after inner execution.
	ex.outerCols = savedOuterCols
	ex.outerVals = savedOuterVals
	if err != nil {
		return catalogapi.Value{}, err
	}
	hasRows := len(result.Rows) >= 1
	if expr.Not {
		hasRows = !hasRows
	}
	if hasRows {
		return intVal(1), nil
	}
	return intVal(0), nil
}

// evalJsonFuncExpr evaluates JSON functions: JSON_EXTRACT, JSON_SET, JSON_INSERT, JSON_REMOVE, JSON_TYPE.
func evalJsonFuncExpr(expr *parserapi.JsonFuncExpr, row *engineapi.Row, columns []catalogapi.ColumnDef,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}, ex *executor) (catalogapi.Value, error) {
	switch expr.Func {
	case "JSON_EXTRACT":
		return evalJsonExtract(expr, row, columns, subqueryResults, ex)
	case "JSON_TYPE":
		return evalJsonType(expr, row, columns, subqueryResults, ex)
	case "JSON_SET":
		return evalJsonSet(expr, row, columns, subqueryResults, ex)
	case "JSON_INSERT":
		return evalJsonInsert(expr, row, columns, subqueryResults, ex)
	case "JSON_REMOVE":
		return evalJsonRemove(expr, row, columns, subqueryResults, ex)
	default:
		return catalogapi.Value{}, fmt.Errorf("%w: unknown JSON function %s", executorapi.ErrExecFailed, expr.Func)
	}
}

// evalJsonExtract evaluates JSON_EXTRACT(json, path).
// Extracts a value from JSON based on the path.
// Paths use SQLite syntax: $.name, $.a.b, $[0], $[1].name
func evalJsonExtract(expr *parserapi.JsonFuncExpr, row *engineapi.Row, columns []catalogapi.ColumnDef,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}, ex *executor) (catalogapi.Value, error) {
	if len(expr.Args) < 2 {
		return catalogapi.Value{}, fmt.Errorf("%w: JSON_EXTRACT requires 2 arguments", executorapi.ErrExecFailed)
	}
	// Evaluate JSON document
	jsonVal, err := evalExpr(expr.Args[0], row, columns, subqueryResults, ex)
	if err != nil {
		return catalogapi.Value{}, err
	}
	if jsonVal.IsNull {
		return catalogapi.Value{IsNull: true}, nil
	}
	if jsonVal.Type != catalogapi.TypeText {
		return catalogapi.Value{}, fmt.Errorf("%w: JSON_EXTRACT requires text argument for JSON document", executorapi.ErrExecFailed)
	}
	// Evaluate path
	pathVal, err := evalExpr(expr.Args[1], row, columns, subqueryResults, ex)
	if err != nil {
		return catalogapi.Value{}, err
	}
	if pathVal.IsNull {
		return catalogapi.Value{IsNull: true}, nil
	}
	if pathVal.Type != catalogapi.TypeText {
		return catalogapi.Value{}, fmt.Errorf("%w: JSON_EXTRACT path must be text", executorapi.ErrExecFailed)
	}

	// Navigate to the path and return the raw JSON
	result, err := jsonNavigate(jsonVal.Text, pathVal.Text)
	if err != nil {
		return catalogapi.Value{}, fmt.Errorf("%w: %v", executorapi.ErrExecFailed, err)
	}
	return catalogapi.Value{Type: catalogapi.TypeText, Text: string(result)}, nil
}

// jsonNavigate extracts a value from JSON based on a path.
// Supports SQLite JSON paths: $.name, $.a.b, $[0], $[1].name, $
func jsonNavigate(jsonStr, path string) (json.RawMessage, error) {
	// Parse JSON
	var doc interface{}
	if err := json.Unmarshal([]byte(jsonStr), &doc); err != nil {
		return nil, fmt.Errorf("invalid JSON: %v", err)
	}

	// Parse path: strip leading $, then split by . and []
	path = strings.TrimPrefix(path, "$")
	if path == "" {
		// Just $, return the whole document
		return json.Marshal(doc)
	}

	// Navigate through path segments
	current := doc
	for len(path) > 0 {
		if path[0] == '.' {
			// Object access: .key
			path = path[1:]
			if len(path) == 0 {
				return nil, fmt.Errorf("invalid path: expected key name after '.'")
			}
			// Find the key name (until [ or . or end)
			end := 0
			for end < len(path) && path[end] != '[' && path[end] != '.' {
				end++
			}
			key := path[:end]
			path = path[end:]

			m, ok := current.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("path error: expected object, got %T", current)
			}
			val, ok := m[key]
			if !ok {
				return nil, nil // Path not found
			}
			current = val

		} else if path[0] == '[' {
			// Array access: [index]
			path = path[1:]
			if len(path) == 0 || path[0] == ']' {
				return nil, fmt.Errorf("invalid path: expected index after '['")
			}
			// Parse index
			end := 0
			for end < len(path) && path[end] != ']' {
				end++
			}
			if end > len(path) {
				return nil, fmt.Errorf("invalid path: missing ']'")
			}
			idxStr := path[:end]
			path = path[end:]
			if path == "" || path[0] != ']' {
				return nil, fmt.Errorf("invalid path: missing ']'")
			}
			path = path[1:] // consume ']'

			arr, ok := current.([]interface{})
			if !ok {
				return nil, fmt.Errorf("path error: expected array, got %T", current)
			}
			idx := 0
			for _, c := range idxStr {
				if c < '0' || c > '9' {
					return nil, fmt.Errorf("invalid path: expected integer index")
				}
				idx = idx*10 + int(c-'0')
			}
			if idx < 0 || idx >= len(arr) {
				return nil, nil // Index out of range
			}
			current = arr[idx]

		} else {
			return nil, fmt.Errorf("invalid path: expected '.' or '[', got %c", path[0])
		}
	}

	return json.Marshal(current)
}

// jsonPathSegment represents a single segment in a JSON path.
type jsonPathSegment struct {
	Key     string // For object access: .key
	Index   int    // For array access: [index]
	IsArray bool   // True if this is an array index access
}

// parseJsonPath parses a SQLite JSON path into segments.
// Supports: $.name, $.a.b, $[0], $[1].name
func parseJsonPath(path string) ([]jsonPathSegment, error) {
	path = strings.TrimPrefix(path, "$")
	if path == "" {
		return nil, nil // Just $, empty path
	}

	var segments []jsonPathSegment
	for len(path) > 0 {
		if path[0] == '.' {
			path = path[1:]
			if len(path) == 0 {
				return nil, fmt.Errorf("invalid path: expected key name after '.'")
			}
			end := 0
			for end < len(path) && path[end] != '[' && path[end] != '.' {
				end++
			}
			segments = append(segments, jsonPathSegment{Key: path[:end]})
			path = path[end:]
		} else if path[0] == '[' {
			path = path[1:]
			if len(path) == 0 || path[0] == ']' {
				return nil, fmt.Errorf("invalid path: expected index after '['")
			}
			end := 0
			for end < len(path) && path[end] != ']' {
				end++
			}
			if end > len(path) {
				return nil, fmt.Errorf("invalid path: missing ']'")
			}
			idxStr := path[:end]
			path = path[end:]
			if path == "" || path[0] != ']' {
				return nil, fmt.Errorf("invalid path: missing ']'")
			}
			path = path[1:] // consume ']'

			idx := 0
			for _, c := range idxStr {
				if c < '0' || c > '9' {
					return nil, fmt.Errorf("invalid path: expected integer index")
				}
				idx = idx*10 + int(c-'0')
			}
			segments = append(segments, jsonPathSegment{Index: idx, IsArray: true})
		} else {
			return nil, fmt.Errorf("invalid path: expected '.' or '[', got %c", path[0])
		}
	}
	return segments, nil
}

// jsonSet sets a value at the given path, creating the path if it doesn't exist.
// JSON_SET('{"name":"Alice"}', '$.name', 'Bob') → {"name":"Bob"}
// JSON_SET('{"name":"Alice"}', '$.age', 30) → {"name":"Alice","age":30}
func jsonSet(jsonStr string, path string, value interface{}) (string, error) {
	var doc interface{}
	if err := json.Unmarshal([]byte(jsonStr), &doc); err != nil {
		return "", fmt.Errorf("invalid JSON: %v", err)
	}

	segments, err := parseJsonPath(path)
	if err != nil {
		return "", err
	}

	// Navigate to parent of target path
	current := doc
	for i := 0; i < len(segments)-1; i++ {
		seg := segments[i]
		if seg.IsArray {
			arr, ok := current.([]interface{})
			if !ok {
				return "", fmt.Errorf("path error: expected array at segment %d", i)
			}
			if seg.Index < 0 || seg.Index >= len(arr) {
				return "", fmt.Errorf("path error: index %d out of range", seg.Index)
			}
			current = arr[seg.Index]
		} else {
			m, ok := current.(map[string]interface{})
			if !ok {
				return "", fmt.Errorf("path error: expected object at segment %d", i)
			}
			val, ok := m[seg.Key]
			if !ok {
				// Create intermediate object
				m[seg.Key] = make(map[string]interface{})
				val = m[seg.Key]
			}
			current = val
		}
	}

	// Set the value at the final segment
	if len(segments) == 0 {
		// Path is just $, replace the whole document
		doc = convertToJSONValue(value)
	} else {
		last := segments[len(segments)-1]
		if last.IsArray {
			arr, ok := current.([]interface{})
			if !ok {
				return "", fmt.Errorf("path error: expected array at final segment")
			}
			// Extend array if needed
			for len(arr) <= last.Index {
				arr = append(arr, nil)
			}
			arr[last.Index] = convertToJSONValue(value)
		} else {
			m, ok := current.(map[string]interface{})
			if !ok {
				return "", fmt.Errorf("path error: expected object at final segment")
			}
			m[last.Key] = convertToJSONValue(value)
		}
	}

	result, err := json.Marshal(doc)
	if err != nil {
		return "", fmt.Errorf("failed to marshal result: %v", err)
	}
	return string(result), nil
}

// jsonInsert inserts a value at the given path only if the path doesn't exist.
// JSON_INSERT('{"name":"Alice"}', '$.name', 'Bob') → {"name":"Alice"} (no change)
// JSON_INSERT('{"name":"Alice"}', '$.age', 30) → {"name":"Alice","age":30}
func jsonInsert(jsonStr string, path string, value interface{}) (string, error) {
	var doc interface{}
	if err := json.Unmarshal([]byte(jsonStr), &doc); err != nil {
		return "", fmt.Errorf("invalid JSON: %v", err)
	}

	segments, err := parseJsonPath(path)
	if err != nil {
		return "", err
	}

	// Navigate to target
	current := doc
	for i := 0; i < len(segments); i++ {
		seg := segments[i]
		if seg.IsArray {
			arr, ok := current.([]interface{})
			if !ok {
				return "", fmt.Errorf("path error: expected array at segment %d", i)
			}
			if seg.Index < 0 || seg.Index >= len(arr) {
				// Path doesn't exist, insert it
				if i == len(segments)-1 {
					// Extend array if needed and insert
					for len(arr) <= seg.Index {
						arr = append(arr, nil)
					}
					arr[seg.Index] = convertToJSONValue(value)
				} else {
					return "", fmt.Errorf("path error: intermediate index %d out of range", seg.Index)
				}
				return string(mustMarshal(doc)), nil
			}
			if i == len(segments)-1 {
				// Path exists, don't modify
				return jsonStr, nil
			}
			current = arr[seg.Index]
		} else {
			m, ok := current.(map[string]interface{})
			if !ok {
				return "", fmt.Errorf("path error: expected object at segment %d", i)
			}
			val, ok := m[seg.Key]
			if !ok {
				// Path doesn't exist, insert it
				m[seg.Key] = convertToJSONValue(value)
				return string(mustMarshal(doc)), nil
			}
			if i == len(segments)-1 {
				// Path exists, don't modify
				return jsonStr, nil
			}
			current = val
		}
	}

	// Path is just $, replace the whole document
	return jsonStr, nil
}

// jsonRemove removes a value at the given path.
// JSON_REMOVE('{"name":"Alice","age":30}', '$.age') → {"name":"Alice"}
func jsonRemove(jsonStr string, path string) (string, error) {
	var doc interface{}
	if err := json.Unmarshal([]byte(jsonStr), &doc); err != nil {
		return "", fmt.Errorf("invalid JSON: %v", err)
	}

	segments, err := parseJsonPath(path)
	if err != nil {
		return "", err
	}

	if len(segments) == 0 {
		return "", fmt.Errorf("path error: cannot remove root $")
	}

	// For array removal, we need to track parent pointers
	// Use a copy of doc that we'll mutate
	docCopy := doc

	// Navigate to parent of target, tracking parent pointers for arrays
	type parentInfo struct {
		Parent interface{}
		Key    string
		Index  int
		IsArray bool
	}
	
	if len(segments) == 1 {
		// Direct child of root
		last := segments[0]
		if last.IsArray {
			arr, ok := docCopy.([]interface{})
			if !ok {
				return "", fmt.Errorf("path error: expected array at root")
			}
			if last.Index < 0 || last.Index >= len(arr) {
				return "", fmt.Errorf("path error: index %d out of range", last.Index)
			}
			docCopy = append(arr[:last.Index], arr[last.Index+1:]...)
		} else {
			m, ok := docCopy.(map[string]interface{})
			if !ok {
				return "", fmt.Errorf("path error: expected object at root")
			}
			delete(m, last.Key)
		}
	} else {
		// Navigate to parent of target
		current := docCopy
		for i := 0; i < len(segments)-1; i++ {
			seg := segments[i]
			if seg.IsArray {
				arr, ok := current.([]interface{})
				if !ok {
					return "", fmt.Errorf("path error: expected array at segment %d", i)
				}
				if seg.Index < 0 || seg.Index >= len(arr) {
					return "", fmt.Errorf("path error: index %d out of range", seg.Index)
				}
				current = arr[seg.Index]
			} else {
				m, ok := current.(map[string]interface{})
				if !ok {
					return "", fmt.Errorf("path error: expected object at segment %d", i)
				}
				val, ok := m[seg.Key]
				if !ok {
					return "", fmt.Errorf("path error: key %s not found", seg.Key)
				}
				current = val
			}
		}

		// Remove the value at the final segment
		last := segments[len(segments)-1]
		if last.IsArray {
			arr, ok := current.([]interface{})
			if !ok {
				return "", fmt.Errorf("path error: expected array at final segment")
			}
			if last.Index < 0 || last.Index >= len(arr) {
				return "", fmt.Errorf("path error: index %d out of range", last.Index)
			}
			arr = append(arr[:last.Index], arr[last.Index+1:]...)
		} else {
			m, ok := current.(map[string]interface{})
			if !ok {
				return "", fmt.Errorf("path error: expected object at final segment")
			}
			delete(m, last.Key)
		}
	}

	result, err := json.Marshal(docCopy)
	if err != nil {
		return "", fmt.Errorf("failed to marshal result: %v", err)
	}
	return string(result), nil
}

// convertToJSONValue converts a Go value to a JSON-serializable value.
func convertToJSONValue(v interface{}) interface{} {
	switch val := v.(type) {
	case int:
		return float64(val)
	case int8:
		return float64(val)
	case int16:
		return float64(val)
	case int32:
		return float64(val)
	case int64:
		return float64(val)
	case uint:
		return float64(val)
	case uint8:
		return float64(val)
	case uint16:
		return float64(val)
	case uint32:
		return float64(val)
	case uint64:
		return float64(val)
	case float32:
		return float64(val)
	case float64:
		return val
	case bool:
		return val
	case string:
		return val
	case nil:
		return nil
	default:
		return val
	}
}

// mustMarshal is a helper that panics on marshal error.
func mustMarshal(v interface{}) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// evalJsonSet evaluates JSON_SET(json, path, value).
func evalJsonSet(expr *parserapi.JsonFuncExpr, row *engineapi.Row, columns []catalogapi.ColumnDef,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}, ex *executor) (catalogapi.Value, error) {
	if len(expr.Args) < 3 {
		return catalogapi.Value{}, fmt.Errorf("%w: JSON_SET requires 3 arguments", executorapi.ErrExecFailed)
	}
	jsonVal, err := evalExpr(expr.Args[0], row, columns, subqueryResults, ex)
	if err != nil {
		return catalogapi.Value{}, err
	}
	if jsonVal.IsNull {
		return catalogapi.Value{IsNull: true}, nil
	}
	pathVal, err := evalExpr(expr.Args[1], row, columns, subqueryResults, ex)
	if err != nil {
		return catalogapi.Value{}, err
	}
	if pathVal.IsNull {
		return catalogapi.Value{IsNull: true}, nil
	}
	value, err := evalExpr(expr.Args[2], row, columns, subqueryResults, ex)
	if err != nil {
		return catalogapi.Value{}, err
	}

	result, err := jsonSet(jsonVal.Text, pathVal.Text, extractInterfaceValue(value))
	if err != nil {
		return catalogapi.Value{}, fmt.Errorf("%w: %v", executorapi.ErrExecFailed, err)
	}
	return catalogapi.Value{Type: catalogapi.TypeText, Text: result}, nil
}

// evalJsonInsert evaluates JSON_INSERT(json, path, value).
func evalJsonInsert(expr *parserapi.JsonFuncExpr, row *engineapi.Row, columns []catalogapi.ColumnDef,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}, ex *executor) (catalogapi.Value, error) {
	if len(expr.Args) < 3 {
		return catalogapi.Value{}, fmt.Errorf("%w: JSON_INSERT requires 3 arguments", executorapi.ErrExecFailed)
	}
	jsonVal, err := evalExpr(expr.Args[0], row, columns, subqueryResults, ex)
	if err != nil {
		return catalogapi.Value{}, err
	}
	if jsonVal.IsNull {
		return catalogapi.Value{IsNull: true}, nil
	}
	pathVal, err := evalExpr(expr.Args[1], row, columns, subqueryResults, ex)
	if err != nil {
		return catalogapi.Value{}, err
	}
	if pathVal.IsNull {
		return catalogapi.Value{IsNull: true}, nil
	}
	value, err := evalExpr(expr.Args[2], row, columns, subqueryResults, ex)
	if err != nil {
		return catalogapi.Value{}, err
	}

	result, err := jsonInsert(jsonVal.Text, pathVal.Text, extractInterfaceValue(value))
	if err != nil {
		return catalogapi.Value{}, fmt.Errorf("%w: %v", executorapi.ErrExecFailed, err)
	}
	return catalogapi.Value{Type: catalogapi.TypeText, Text: result}, nil
}

// evalJsonRemove evaluates JSON_REMOVE(json, path).
func evalJsonRemove(expr *parserapi.JsonFuncExpr, row *engineapi.Row, columns []catalogapi.ColumnDef,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}, ex *executor) (catalogapi.Value, error) {
	if len(expr.Args) < 2 {
		return catalogapi.Value{}, fmt.Errorf("%w: JSON_REMOVE requires 2 arguments", executorapi.ErrExecFailed)
	}
	jsonVal, err := evalExpr(expr.Args[0], row, columns, subqueryResults, ex)
	if err != nil {
		return catalogapi.Value{}, err
	}
	if jsonVal.IsNull {
		return catalogapi.Value{IsNull: true}, nil
	}
	pathVal, err := evalExpr(expr.Args[1], row, columns, subqueryResults, ex)
	if err != nil {
		return catalogapi.Value{}, err
	}
	if pathVal.IsNull {
		return catalogapi.Value{IsNull: true}, nil
	}

	result, err := jsonRemove(jsonVal.Text, pathVal.Text)
	if err != nil {
		return catalogapi.Value{}, fmt.Errorf("%w: %v", executorapi.ErrExecFailed, err)
	}
	return catalogapi.Value{Type: catalogapi.TypeText, Text: result}, nil
}

// extractInterfaceValue extracts a Go value from a catalogapi.Value.
func extractInterfaceValue(v catalogapi.Value) interface{} {
	if v.IsNull {
		return nil
	}
	switch v.Type {
	case catalogapi.TypeInt:
		return v.Int
	case catalogapi.TypeFloat:
		return v.Float
	case catalogapi.TypeText:
		return v.Text
	case catalogapi.TypeBlob:
		return v.Blob
	}
	return v.Text
}// evalJsonType evaluates JSON_TYPE(json, path).
// Returns the type of the value at the given path.
// Returns: null, integer, real, text, blob, array, object
func evalJsonType(expr *parserapi.JsonFuncExpr, row *engineapi.Row, columns []catalogapi.ColumnDef,
	subqueryResults map[*parserapi.SubqueryExpr]interface{}, ex *executor) (catalogapi.Value, error) {
	if len(expr.Args) < 2 {
		return catalogapi.Value{}, fmt.Errorf("%w: JSON_TYPE requires 2 arguments", executorapi.ErrExecFailed)
	}
	// Evaluate JSON document
	jsonVal, err := evalExpr(expr.Args[0], row, columns, subqueryResults, ex)
	if err != nil {
		return catalogapi.Value{}, err
	}
	if jsonVal.IsNull {
		return catalogapi.Value{IsNull: true}, nil
	}
	if jsonVal.Type != catalogapi.TypeText {
		return catalogapi.Value{}, fmt.Errorf("%w: JSON_TYPE requires text argument for JSON document", executorapi.ErrExecFailed)
	}
	// Evaluate path
	pathVal, err := evalExpr(expr.Args[1], row, columns, subqueryResults, ex)
	if err != nil {
		return catalogapi.Value{}, err
	}
	if pathVal.IsNull {
		return catalogapi.Value{IsNull: true}, nil
	}
	if pathVal.Type != catalogapi.TypeText {
		return catalogapi.Value{}, fmt.Errorf("%w: JSON_TYPE path must be text", executorapi.ErrExecFailed)
	}

	// Navigate to get the value
	raw, err := jsonNavigate(jsonVal.Text, pathVal.Text)
	if err != nil {
		return catalogapi.Value{}, fmt.Errorf("%w: %v", executorapi.ErrExecFailed, err)
	}
	if raw == nil {
		return catalogapi.Value{Type: catalogapi.TypeText, Text: "null"}, nil
	}

	// Determine the type
	var val interface{}
	if err := json.Unmarshal(raw, &val); err != nil {
		return catalogapi.Value{Type: catalogapi.TypeText, Text: "null"}, nil
	}

	typeName := "null"
	switch v := val.(type) {
	case nil:
		typeName = "null"
	case bool:
		typeName = "integer" // SQLite uses integer for boolean
	case float64:
		// Check if it's actually an integer
		if v == floatValue(int64(v)) {
			typeName = "integer"
		} else {
			typeName = "real"
		}
	case string:
		typeName = "text"
	case []interface{}:
		typeName = "array"
	case map[string]interface{}:
		typeName = "object"
	}
	return catalogapi.Value{Type: catalogapi.TypeText, Text: typeName}, nil
}

// floatValue checks if a float64 is exactly representable as an int64.
func floatValue(i int64) float64 {
	return float64(i)
}
