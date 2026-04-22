// Package errors defines SQLSTATE error codes and structured SQL errors.
package errors

import (
	"fmt"
)

// ─── SQLSTATE Constants ─────────────────────────────────────────────
// SQLSTATE is a 5-character error code that follows the SQL standard.
// See: https://www.postgresql.org/docs/current/errcodes-appendix.html

const (
	// Class 00 — Successful Completion
	SQLStateSuccessfulCompletion = "00000"

	// Class 02 — No Data (but this isn't an error, so we map it to successful)
	SQLStateNoData = "02000"

	// Class 21 — Cardinality Violation
	SQLStateCardinalityViolation = "21000" // wrong number of result columns

	// Class 22 — Data Exception
	SQLStateDataException          = "22000"
	SQLStateDivisionByZero          = "22012"
	SQLStateInvalidTextRepresentation = "22P02" // invalid input syntax
	SQLStateNullValueNotAllowed     = "22004" // null value in NOT NULL column (general)

	// Class 23 — Integrity Constraint Violation
	SQLStateUniqueViolation          = "23505"
	SQLStateNotNullViolation         = "23502"
	SQLStateCheckViolation           = "23522" // CHECK constraint violation
	SQLStateForeignKeyViolation      = "23503"
	SQLStateCheckConstraintViolation = "23514"
	SQLStateExclusionViolation       = "23P01"

	// Class 34 — Program Name Not Found
	SQLStateInvalidCursorName = "34000"

	// Class 38 — External Routine Exception
	SQLStateExternalRoutineException = "38000"

	// Class 40 — Transaction Rollback
	SQLStateTransactionRollback     = "40000"
	SQLStateDeadlockDetected        = "40P01"
	SQLStateSerializationFailure   = "40001"
	SQLStateStatementCompletionUnknown = "40002"

	// Class 42 — Syntax Error or Access Rule Violation
	SQLStateSyntaxError           = "42601"
	SQLStateUndefinedTable        = "42P01"
	SQLStateUndefinedColumn       = "42703"
	SQLStateUndefinedFunction     = "42883"
	SQLStateDuplicateColumn        = "42701"
	SQLStateDuplicateAlias         = "42712"
	SQLStateDuplicateObject        = "42710"
	SQLStateAmbiguousColumn        = "42703"
	SQLStateAmbiguousFunction      = "42725"
	SQLStateInvalidColumnReference = "42P10"
	SQLStateInvalidObjectName      = "42602"

	// Class 44 — WITH CHECK OPTION Violation
	SQLStateWithCheckOptionViolation = "44000"

	// Class 53 — Insufficient Resources
	SQLStateInsufficientResources = "53000"
	SQLStateOutOfMemory            = "53100"
	SQLStateDiskFull               = "53200"

	// Class 54 — Program Limit Exceeded
	SQLStateProgramLimitExceeded = "54000"
	SQLStateTooManyColumns        = "54011"
	SQLStateTooManyArguments      = "54023"

	// Class 55 — Object Not In Prerequisite State
	SQLStateObjectNotInPrerequisiteState = "55000"
	SQLStateLockTimeout            = "55P03"
	SQLStateUnsafeNewEnumValueUsage = "55P02"

	// Class 57 — Operator Intervention
	SQLStateOperatorIntervention = "57000"
	SQLStateSystemError           = "58000"
	SQLStateIOError              = "58030"

	// Class F0 — Configuration File Error
	SQLStateConfigurationFileError = "F0000"

	// Class P0 — PL/pgSQL Error
	SQLStatePLPGSQLError = "P0000"

	// Class 0A — Feature Not Supported
	SQLStateFeatureNotSupported = "0A000"

	// Class XX — Internal Error
	SQLStateInternalError = "XX000"
)

// ─── SQLError ──────────────────────────────────────────────────────

// SQLError wraps an error with a SQLSTATE code for standardized error reporting.
// It implements the error interface and supports error wrapping.
type SQLError struct {
	SQLState string
	Message string
	Err     error
}

// Error returns the error message.
func (e *SQLError) Error() string {
	return e.Message
}

// Unwrap returns the underlying error, if any.
func (e *SQLError) Unwrap() error {
	return e.Err
}

// GetSQLState returns the SQLSTATE code for this error.
func (e *SQLError) GetSQLState() string {
	return e.SQLState
}

// ─── Constructor Helpers ───────────────────────────────────────────

// ErrTableNotFound returns an error for a table that does not exist.
func ErrTableNotFound(tableName string) *SQLError {
	return &SQLError{
		SQLState: SQLStateUndefinedTable,
		Message:  fmt.Sprintf("table %q does not exist", tableName),
	}
}

// ErrColumnNotFound returns an error for a column that does not exist.
func ErrColumnNotFound(tableName, columnName string) *SQLError {
	return &SQLError{
		SQLState: SQLStateUndefinedColumn,
		Message:  fmt.Sprintf("column %q does not exist in table %q", columnName, tableName),
	}
}

// ErrTableExists returns an error for a table that already exists.
func ErrTableExists(tableName string) *SQLError {
	return &SQLError{
		SQLState: SQLStateDuplicateObject,
		Message:  fmt.Sprintf("table %q already exists", tableName),
	}
}

// ErrIndexExists returns an error for an index that already exists.
func ErrIndexExists(indexName string) *SQLError {
	return &SQLError{
		SQLState: SQLStateDuplicateObject,
		Message:  fmt.Sprintf("index %q already exists", indexName),
	}
}

// ErrIndexNotFound returns an error for an index that does not exist.
func ErrIndexNotFound(tableName, indexName string) *SQLError {
	return &SQLError{
		SQLState: SQLStateUndefinedTable, // 42P01 can be used for indexes too
		Message:  fmt.Sprintf("index %q on table %q does not exist", indexName, tableName),
	}
}

// ErrTypeMismatch returns an error for a type mismatch.
func ErrTypeMismatch(expected, got interface{}) *SQLError {
	return &SQLError{
		SQLState: SQLStateDataException,
		Message:  fmt.Sprintf("type mismatch: expected %v, got %v", expected, got),
	}
}

// ErrColumnCountMismatch returns an error for wrong column count in INSERT.
func ErrColumnCountMismatch(expected, got int) *SQLError {
	return &SQLError{
		SQLState: SQLStateCardinalityViolation,
		Message:  fmt.Sprintf("column count mismatch: expected %d columns, got %d", expected, got),
	}
}

// ErrSyntaxError returns an error for a SQL syntax error.
func ErrSyntaxError(message string) *SQLError {
	return &SQLError{
		SQLState: SQLStateSyntaxError,
		Message:  message,
	}
}

// ErrParseError returns an error for a parse error at a specific position.
func ErrParseError(message string, pos int) *SQLError {
	return &SQLError{
		SQLState: SQLStateSyntaxError,
		Message:  fmt.Sprintf("parse error at position %d: %s", pos, message),
	}
}

// ErrUniqueViolation returns an error for a unique constraint violation.
func ErrUniqueViolation(tableName, columnName string) *SQLError {
	return &SQLError{
		SQLState: SQLStateUniqueViolation,
		Message:  fmt.Sprintf("unique constraint violated on table %q, column %q", tableName, columnName),
	}
}

// ErrNotNullViolation returns an error for a NOT NULL constraint violation.
func ErrNotNullViolation(tableName, columnName string) *SQLError {
	return &SQLError{
		SQLState: SQLStateNotNullViolation,
		Message:  fmt.Sprintf("null value in column %q violates NOT NULL constraint on table %q", columnName, tableName),
	}
}

// ErrCheckViolation returns an error for a CHECK constraint violation.
func ErrCheckViolation(tableName, expr string) *SQLError {
	return &SQLError{
		SQLState: SQLStateCheckViolation,
		Message:  fmt.Sprintf("new row for table %q violates CHECK constraint %q", tableName, expr),
	}
}

// ErrForeignKeyViolation returns an error for a foreign key constraint violation.
func ErrForeignKeyViolation(message string) *SQLError {
	return &SQLError{
		SQLState: SQLStateForeignKeyViolation,
		Message:  message,
	}
}

// ErrDeadlockDetected returns an error when a deadlock is detected.
func ErrDeadlockDetected() *SQLError {
	return &SQLError{
		SQLState: SQLStateDeadlockDetected,
		Message:  "deadlock detected",
	}
}

// ErrLockTimeout returns an error when lock acquisition times out.
func ErrLockTimeout(tableName string) *SQLError {
	return &SQLError{
		SQLState: SQLStateLockTimeout,
		Message:  fmt.Sprintf("lock acquisition timed out for table %q", tableName),
	}
}

// ErrSerializationFailure returns an error for serialization failure.
func ErrSerializationFailure() *SQLError {
	return &SQLError{
		SQLState: SQLStateSerializationFailure,
		Message:  "could not serialize access due to concurrent update",
	}
}

// ErrInvalidPlan returns an error when a plan cannot be created.
func ErrInvalidPlan(reason string) *SQLError {
	return &SQLError{
		SQLState: SQLStateInternalError,
		Message:  fmt.Sprintf("planner: cannot create valid plan — %s", reason),
	}
}

// ErrExecFailed returns a generic execution failure error.
func ErrExecFailed(wrapped error) *SQLError {
	return &SQLError{
		SQLState: SQLStateInternalError,
		Message:  fmt.Sprintf("execution failed: %v", wrapped),
		Err:      wrapped,
	}
}

// ErrUnsupportedExpr returns an error for unsupported expressions.
func ErrUnsupportedExpr(feature string) *SQLError {
	return &SQLError{
		SQLState: SQLStateSyntaxError,
		Message:  fmt.Sprintf("unsupported expression: %s", feature),
	}
}

// ErrEmptyTable returns an error when creating a table with no columns.
func ErrEmptyTable() *SQLError {
	return &SQLError{
		SQLState: SQLStateSyntaxError,
		Message:  "table must have at least one column",
	}
}

// ErrInternalError returns an error for internal errors.
func ErrInternalError(message string) *SQLError {
	return &SQLError{
		SQLState: SQLStateInternalError,
		Message:  message,
	}
}

// ErrDivisionByZero returns an error for division by zero.
func ErrDivisionByZero() *SQLError {
	return &SQLError{
		SQLState: SQLStateDivisionByZero,
		Message:  "division by zero",
	}
}

// ErrTransactionRollback returns an error when a transaction must be rolled back.
func ErrTransactionRollback(reason string) *SQLError {
	return &SQLError{
		SQLState: SQLStateTransactionRollback,
		Message:  fmt.Sprintf("current transaction is aborted, %s", reason),
	}
}

// ErrFeatureNotSupported returns an error for unsupported SQL features.
func ErrFeatureNotSupported(feature string) *SQLError {
	return &SQLError{
		SQLState: SQLStateFeatureNotSupported,
		Message:  fmt.Sprintf("feature not supported: %s", feature),
	}
}

// ErrInvalidTextRepresentation returns an error for invalid text input syntax.
func ErrInvalidTextRepresentation(input, targetType string) *SQLError {
	return &SQLError{
		SQLState: SQLStateInvalidTextRepresentation,
		Message:  fmt.Sprintf("invalid input syntax for type %s: %q", targetType, input),
	}
}

// ErrTooManyRows returns an error when a subquery returns more than one row.
func ErrTooManyRows() *SQLError {
	return &SQLError{
		SQLState: SQLStateCardinalityViolation,
		Message:  "query returns more than one row",
	}
}

// ErrAmbiguousColumn returns an error when a column reference is ambiguous.
func ErrAmbiguousColumn(columnName string) *SQLError {
	return &SQLError{
		SQLState: SQLStateAmbiguousColumn,
		Message:  fmt.Sprintf("column reference %q is ambiguous", columnName),
	}
}

// ErrAmbiguousFunction returns an error when a function call is ambiguous.
func ErrAmbiguousFunction(funcName string) *SQLError {
	return &SQLError{
		SQLState: SQLStateAmbiguousFunction,
		Message:  fmt.Sprintf("function name %q is ambiguous", funcName),
	}
}

// ErrDuplicateAlias returns an error when a table/alias is duplicated in a query.
func ErrDuplicateAlias(aliasName string) *SQLError {
	return &SQLError{
		SQLState: SQLStateDuplicateAlias,
		Message:  fmt.Sprintf("table alias %q specified more than once", aliasName),
	}
}

// ErrTooManyArguments returns an error when a function call has too many arguments.
func ErrTooManyArguments(funcName string, maxArgs int) *SQLError {
	return &SQLError{
		SQLState: SQLStateTooManyArguments,
		Message:  fmt.Sprintf("function %q called with too many arguments (max %d)", funcName, maxArgs),
	}
}

// ErrOutOfMemory returns an error when the database runs out of memory.
func ErrOutOfMemory() *SQLError {
	return &SQLError{
		SQLState: SQLStateOutOfMemory,
		Message:  "out of memory",
	}
}

// ErrDiskFull returns an error when the disk is full.
func ErrDiskFull() *SQLError {
	return &SQLError{
		SQLState: SQLStateDiskFull,
		Message:  "disk is full",
	}
}

// ErrTriggerNotFound returns an error for a trigger that does not exist.
func ErrTriggerNotFound(triggerName string) *SQLError {
	return &SQLError{
		SQLState: SQLStateUndefinedTable,
		Message:  fmt.Sprintf("trigger %q does not exist", triggerName),
	}
}

// ErrTriggerExists returns an error for a trigger that already exists.
func ErrTriggerExists(triggerName string) *SQLError {
	return &SQLError{
		SQLState: SQLStateDuplicateObject,
		Message:  fmt.Sprintf("trigger %q already exists", triggerName),
	}
}

// Wrap wraps an existing error with a SQLSTATE code.
func Wrap(err error, sqlState string, message string) *SQLError {
	return &SQLError{
		SQLState: sqlState,
		Message:  message,
		Err:     err,
	}
}