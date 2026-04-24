# SQL Layer — Design Notes for Future Developers

Patterns, pitfalls, and conventions discovered during the deep code review. Read this before modifying the SQL layer.

---

## 1. Two Execution Paths: Transactional vs Non-Transactional

Every DML operation has two separate code paths: non-transactional (WriteBatch) and transactional (PutWithXID/DeleteWithXID). **Rule**: When modifying DML execution, search for `if e.txnCtx != nil` and ensure both branches are updated.

---

## 2. Filter Location: Scan Plan vs Residual Filter

The planner embeds WHERE filters in two places: inside the scan plan and as a separate SelectPlan.Filter field. `planDelete` and `planUpdate` discard the returned residual because the filter is already embedded in the scan plan.

---

## 3. LIMIT/OFFSET Pushdown Constraints

Only safe when: no ORDER BY, no GROUP BY, no residual filter, no DISTINCT. When adding new features requiring full-scan semantics, add them to the pushdown guard.

---

## 4. Catalog Key Conventions

```
_sql:table:{tableName}              → TableSchema JSON
_sql:index:{tableName}:{indexName}  → IndexSchema JSON
_sql:trigger:{triggerName}          → TriggerSchema JSON
_sql:view:{viewName}                → ViewSchema JSON
_sql:tableID                        → next table ID (uint32)
```

Indexes keyed by table name → RenameTable must re-key. Triggers keyed by trigger name → DropTable must enumerate.

---

## 5. RowID Allocation: Two Different APIs

`InsertInto()` handles everything in one call. `AllocRowID()` + `IncrementCounter()` are separate — **must call IncrementCounter after each AllocRowID**.

---

## 6. NULL Semantics

SQL three-valued logic implemented in `evalBinaryExpr`. `matchFilter` converts NULL to "not truthy" (row excluded). Correct for WHERE but may be wrong for CHECK constraints.

---

## 7. Schema Cache Is Read-Through, Not Write-Through

Populated on read, invalidated on write. Returns raw pointer — all callers must treat as immutable.

---

## 8. Expression Evaluation Is Recursive and Unbounded

`evalExpr` has no depth limit. Pathological SQL could cause stack overflow.

---

## 9. JOIN Column Resolution Depends on Table Name Tagging

`evalColumnRef` resolves `table.column` by matching `ColumnDef.Table`. Must be populated for all scan contexts.

---

## 10. Error Sentinel Comparison Uses `==`, Not `errors.Is()`

Replace all `err == sentinel` with `errors.Is(err, sentinel)` in the planner.

---

## 11. Executor Size and Complexity

~7300 lines in a single file. Consider splitting into sub-files for navigability.
