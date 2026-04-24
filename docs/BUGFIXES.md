# Bug Fixes Record

All bugs found and fixed during the deep code review (2026). Total: 30 correctness fixes across KV and SQL layers.

---

## KV Layer Fixes (17 commits)

All bugs found during the 6-phase deep code review (2026).

---

### Bug 1: LSM CAS WAL Ordering
**Commit:** `2aca7b5`

**Root cause:** `CompareAndSetPageMapping()` and `CompareAndSetBlobMapping()` in `internal/lsm/internal/lsm.go` appended the new mapping to the WAL collector BEFORE performing the compare-and-set check. If the CAS failed (current value != expected value), the WAL record remained in the collector and would be persisted on the next flush.

**Impact:** On crash recovery, phantom WAL records would replay, overwriting legitimate mappings with stale GC-relocated values. This could silently corrupt page-to-segment mappings after a crash that occurred during concurrent GC.

**Fix:** Move WAL collector append to AFTER the CAS succeeds. Only append the record if the compare-and-set actually changes the mapping.

---

### Bug 2: BlobStore CAS Race
**Commit:** `505729f`

**Root cause:** `CompareAndSetBlobMapping()` in `internal/blobstore/internal/blobstore.go` held only a sharded CAS lock (`casLocks[idx]`), while `Write()` held the global `bs.mu`. These are independent lock domains. If `Write()` triggered `ensureCapacity()` (which replaces the `bs.mapping` slice with a larger one), a concurrent CAS would read from or write to the old (orphaned) slice, silently losing the GC mapping update.

**Impact:** GC blob relocation could be silently lost — the blob mapping would still point to the old segment, preventing space reclamation. In the worst case, if the old segment was subsequently deleted, blob reads would fail.

**Fix:** Have `CompareAndSetBlobMapping` acquire `bs.mu.RLock()` in addition to the shard lock. RLock is sufficient to prevent `ensureCapacity()` from replacing the slice during CAS.

---

### Bug 3: DeleteBlobMapping Missing Mutex
**Commit:** `6c113b2`

**Root cause:** `DeleteBlobMapping()` in `internal/lsm/internal/lsm.go` appended to the WAL collector without acquiring the per-goroutine mutex (`getOrCreateCollectorMu`), unlike all other `Set`/`CAS` methods which correctly acquired the mutex before appending.

**Impact:** Concurrent `DeleteBlobMapping` + `SetBlobMapping` on the same goroutine could corrupt the collector slice (data race on append).

**Fix:** Acquire the per-goroutine collector mutex before appending, consistent with all other methods.

---

### Bug 4: GC liveMap Unused
**Commit:** `056d69f`

**Root cause:** In `internal/gc/internal/page_gc.go`, the `liveMap` variable was populated during the segment scan (`liveMap[pageID] = newPacked`) but never read. The intent was to deduplicate — if the same pageID appears multiple times in a segment (from successive writes before rotation), only the last should be considered live. But the liveMap was never consulted during the scan loop.

**Impact:** If a pageID had multiple records in the same segment, GC would copy both the old and new version, wasting I/O and segment space. The CAS would protect correctness (only the latest mapping wins), but extra I/O was wasted, causing write amplification.

**Fix:** Consult `liveMap` during the scan to skip stale records that have already been superseded within the same segment.

---

### Bug 5: packBlobMeta VAddr Truncation
**Commit:** `468465c`

**Root cause:** `packBlobMeta(vaddr, size)` in `internal/lsm/internal/memtable.go` did `(vaddr << 32) | uint64(size)`. VAddr is already a 64-bit value (`segID<<32 | offset`). Shifting left by 32 destroyed the segment ID (upper 32 bits). `unpackBlobMeta` did `vaddr = packed >> 32`, which returned only the original lower 32 bits (the offset), losing the segment ID entirely.

**Impact:** Latent bug — the LSM blob mapping is a shadow copy that's written but never meaningfully read in the current codebase (blob reads go through BlobStore's own in-memory dense array). However, it was a ticking bomb if anyone routed blob lookups through LSM.

**Fix:** Use a wider packing scheme that preserves the full 64-bit VAddr, or remove the LSM blob mapping if it's not needed.

---

### Bug 6: checkpointLocked Omits Page Mappings
**Commit:** `f9d5c2c`

**Root cause:** `checkpointLocked()` in `internal/kvstore/internal/checkpoint.go` (called from `Close()`) built `checkpointData` but did NOT populate `data.Pages` (page mappings). The checkpoint was written with zero page mappings. Then WAL was truncated. On recovery, `recover()` loaded the checkpoint (no page mappings) and replayed WAL (which was truncated) — all page→VAddr mappings were lost.

**Impact:** **Data loss on normal Close/Open cycle.** After a clean `Close()` followed by `Open()`, the store could not locate any B-tree pages. The segment files still existed, but the mapping from logical page IDs to physical VAddrs was gone. All data was effectively inaccessible.

**Fix:** Populate `data.Pages` in `checkpointLocked()` by calling `lsmRecovery.GetAllPageMappings()`, consistent with the background `runCheckpoint()` method.

---

### Bug 7: Checkpoint Serialization Buffer Overflow
**Commit:** `7c41728`

**Root cause:** `serializeCheckpointV3()` in `internal/kvstore/internal/checkpoint.go` allocated `lsmCount * 16` bytes for LSM segment names. But each name was serialized as `2 (length prefix) + len(name) + padding_to_16_alignment` bytes. For any non-empty name, the minimum is 18 bytes (2 + ceil16(len)). The `deserializeCheckpoint` size validation used the same wrong formula.

**Impact:** **Panic (index out of range)** when writing a checkpoint with LSM segments. This would crash the store during `Close()` or background checkpoint if any LSM SSTables existed.

**Fix:** Calculate correct size: `2 + len(name) + pad` per segment name, where `pad = (16 - len(name)%16) % 16`. Update both serialization and deserialization size calculations.

---

### Bug 8: ScanWithParams Variable Shadowing
**Commit:** `f059dc0`

**Root cause:** In `ScanWithParams()` in `internal/kvstore/internal/kvstore.go`, the `else` branch (no active txn context) at line 884 declared `readXID, snap := s.txnMgr.ReadSnapshot()`. The `:=` created a new local variable that **shadowed** the outer `readXID` declared at line 874 (`var readXID uint64`). After the `else` block exited, the outer `readXID` was still 0.

**Impact:** (1) Scan used `readXID=0` → VisibilityChecker looked up `readSnaps[0]` → not found → fell through to weaker CLOG-only check (broken snapshot isolation). (2) The real snapshot at `readSnaps[actualReadXID]` was never deleted → memory leak (one Snapshot per call, never cleaned up).

**Fix:** Change `:=` to `=` assignment to use the outer variable: `readXID, snap = s.txnMgr.ReadSnapshot()`. Requires declaring `snap` in the outer scope.

---

### Bug 9: LSM Compaction Merge Ordering
**Commit:** `d221ac9`

**Root cause:** `runLevelCompaction()` in `internal/lsm/internal/lsm.go` built `segmentsToMerge = [L0_segment, ...L1_segments]`. The merge loop iterated in order using `mergedPages[p.key] = p.value` (last-write-wins). L0 was read first, L1 second. Since L1 values overwrote L0 values for duplicate keys, and L0 is NEWER than L1, the merge produced stale data.

**Impact:** **Silent data corruption.** After L0→L1 compaction, updated page mappings reverted to stale L1 values.

**Fix:** Reverse the iteration order: read L1 segments first, then L0.

---

### Bug 10: Compaction Silent Read Error
**Commit:** `d221ac9`

**Root cause:** In `runLevelCompaction()`, `readSSTable(segPath)` errors were silently handled with `continue`. If an SSTable file was corrupted or deleted, its entries were silently dropped from the merge result.

**Impact:** Silent data loss during compaction.

**Fix:** Abort compaction on any SSTable read error.

---

### Bug 11: Compaction File Deletion Ordering
**Commit:** `d221ac9`

**Root cause:** In `runLevelCompaction()`, `os.Remove(segPath)` was called BEFORE `s.manifest.RemoveSegment(name)`. If the process crashed between file deletion and manifest update, recovery would load a manifest referencing a deleted file.

**Impact:** Crash between the two operations caused unrecoverable data loss.

**Fix:** Remove from manifest FIRST, then delete the segment file. Use `TryDelete()` to prevent TOCTOU race.

---

### Bug 12: Row Lock Shared→Exclusive Upgrade
**Commit:** `8dc9b26`

**Root cause:** `canAcquire()` in `internal/rowlock/internal/rowlock.go` returned `true` when the requesting transaction already held the lock, regardless of other holders. `Acquire()` then upgraded `entry.mode` from Shared to Exclusive. But if another transaction also held a Shared lock, both transactions now believed they had exclusive access.

**Impact:** **Broken mutual exclusion.** Two transactions simultaneously held what they believed was an exclusive lock on the same row.

**Fix:** When upgrading Shared→Exclusive, check `len(entry.holders) == 1`.

---

### Bug 13: PageStore Dead Code Removal
**Commit:** `4661bfa`

**Root cause:** `Write()`, `WriteDirect()`, `writeViaAppend()`, and `Read()` were dead code with incompatible VAddr encoding.

**Impact:** No runtime impact (dead code), but contained bugs that would cause issues if called.

**Fix:** Remove the dead code methods.

---

### Bug 14: Manifest NextID Async Save
**Commit:** `64c8093`

**Root cause:** `NextID()` incremented `nextSegmentID` in memory, then spawned a goroutine to persist the manifest asynchronously. If the process crashed before the goroutine completed, the same ID would be allocated again.

**Impact:** SSTable overwritten on crash recovery → data loss.

**Fix:** Save the manifest synchronously in `NextID()`.

---

### Bug 15: Per-Store activeCheckpoint
**Commit:** `490ba0d`

**Root cause:** `activeCheckpoint` was a package-level `atomic.Pointer`, not tied to any store instance.

**Impact:** Multiple store instances shared checkpoint state.

**Fix:** Move `activeCheckpoint` to be a field on the `store` struct.

---

### Bug 16: Row Lock Notification Mechanism
**Commit:** `0806842`

**Root cause:** `Acquire()` blocked with `time.Sleep(10 * time.Millisecond)` in a retry loop.

**Impact:** Up to 10ms added latency per contended lock acquisition.

**Fix:** Replace sleep-based polling with channel notification.

---

### Bug 17: Row Lock Deadlock Detection
**Commit:** `0806842`

**Root cause:** No deadlock detection mechanism. Two transactions could deadlock with both blocking until the 5-second timeout.

**Impact:** Deadlocks caused 5-second hangs instead of immediate detection.

**Fix:** Implement wait-for graph based deadlock detection with DFS cycle check. Before blocking, `Acquire()` checks if the new waiter→holder edge creates a cycle. If so, returns false immediately.

---

## SQL Layer Fixes (13 commits)

All bugs found during the SQL layer deep code review (2026).

---

### Bug 1: Multi-Row Transactional INSERT Assigns Same RowID to All Rows

**Commit**: `d6a9adb`
**Files**: `executor/internal/executor.go`, `engine/internal/table.go`

**Root Cause**: `AllocRowID()` returns the cached counter value without incrementing it on subsequent calls. In the transactional INSERT loop, `AllocRowID()` is called per row, but only the first call initializes the counter. All subsequent calls return the same cached value. `IncrementCounter()` was called only once after the entire loop.

**Fix**: Call `IncrementCounter()` inside the loop after each `AllocRowID()`, so each row gets a unique, monotonically increasing rowID.

**Impact**: Multi-row `INSERT INTO t VALUES (1), (2), (3)` inside a transaction would write all three rows to the same KV key. Only the last row survives — the first two are silently overwritten. **Data loss.**

---

### Bug 2: Multi-Table JOIN mergeRows Corruption

**Commit**: `5552151`
**Files**: `executor/internal/executor.go`

**Root Cause**: `mergeRows(left, right, leftLen, rightLen)` uses `leftLen` (from `len(jplan.LeftSchema)`) as the copy offset for right-side values. For a nested 3-table join (A JOIN B JOIN C), the left side's row values contain columns from both A and B (e.g., 6 values), but `leftLen` only counts A's schema columns (e.g., 3). The `copy(result[leftLen:], right.Values)` overwrites B's columns with C's columns.

**Fix**: Use `len(left.Values)` instead of `leftLen` for the result array size and copy offset.

**Impact**: Any query joining 3+ tables produces corrupted result rows. Middle table columns are overwritten by the rightmost table's values. **Silent wrong results.**

---

### Bug 3: LIMIT Pushdown with WHERE Filter Returns Fewer Rows Than Expected

**Commit**: `134c8f8`
**Files**: `executor/internal/executor.go`

**Root Cause**: The executor pushes LIMIT/OFFSET to the storage layer when `OrderBy == nil && GroupByExprs == nil`, regardless of whether a WHERE filter exists. `ScanWithLimit(table, limit, offset)` returns at most `limit` raw rows from the KV store. The WHERE filter is then applied in Go, removing non-matching rows.

**Fix**: Also check that the scan plan has no filter: `pushedDown = ... && !scanHasFilter(scanPlan)`.

**Impact**: `SELECT * FROM t WHERE rare_condition LIMIT 10` returns fewer than 10 rows even though the table has hundreds of matching rows. **Wrong result count.**

---

### Bug 4: OFFSET >= Row Count Returns All Rows Instead of Empty Result

**Commit**: `f669ab8`
**Files**: `executor/internal/executor.go` (~12 locations)

**Root Cause**: The OFFSET guard uses `plan.Offset < len(rows)` — when offset equals or exceeds the row count, the condition is false and OFFSET is silently skipped, returning ALL rows.

**Fix**: Change to `if plan.Offset > 0 { if plan.Offset >= len(rows) { rows = rows[:0] } else { rows = rows[plan.Offset:] } }`. Applied at all ~12 locations.

**Impact**: `SELECT * FROM t OFFSET 1000` on a 5-row table returns all 5 rows instead of 0. **Any pagination query where the offset exceeds the data returns wrong results.**

---

### Bug 5: Transactional INSERT Skips BEFORE INSERT Triggers

**Commit**: `9b7ae2b`
**Files**: `executor/internal/executor.go`

**Root Cause**: The transactional INSERT code path (inside `if e.txnCtx != nil`) only fires `AFTER INSERT` triggers. The `fireTriggers(... "BEFORE", "INSERT" ...)` call that exists in the non-transactional path was never added to the transactional path.

**Fix**: Add `fireTriggers(tableName, "BEFORE", "INSERT", ...)` before constraint checks and row insertion in the transactional path.

**Impact**: Any BEFORE INSERT trigger is silently ignored when the INSERT runs inside a `BEGIN...COMMIT` transaction.

---

### Bug 6: RenameTable Orphans All Indexes

**Commit**: `866b121`
**Files**: `catalog/internal/catalog_impl.go`

**Root Cause**: Index entries are stored with keys like `_sql:index:TABLENAME:INDEXNAME`. `RenameTable` updates the table schema key but does NOT re-key the index entries. After rename, `ListIndexes("newName")` scans prefix `_sql:index:newName:` and finds nothing.

**Fix**: After renaming the table schema, iterate all indexes under the old table name prefix, re-key each one under the new table name, and delete the old entries.

**Impact**: After `ALTER TABLE foo RENAME TO bar`, all indexes on the table become invisible. Queries fall back to full table scans. **UNIQUE constraint enforcement breaks silently.**

---

### Bug 7: DropTable Does Not Clean Up Triggers

**Commit**: `ba3e189`
**Files**: `catalog/internal/catalog_impl.go`

**Root Cause**: `dropTableImpl` deletes the table schema and all indexes, but triggers are stored by trigger name (`_sql:trigger:NAME`), not by table name. There is no prefix-based cleanup for triggers.

**Fix**: After dropping indexes, call `ListTriggers(tableName)` to find all triggers for the table, then delete each one.

**Impact**: Orphaned trigger metadata persists. If a new table with the same name is created later, stale triggers from the old table execute unexpectedly.

---

### Bug 8: DISTINCT Treats NULLs of Different Types as Distinct / Type Collision

**Commit**: `2ae3a25`
**Files**: `executor/internal/executor.go`

**Root Cause**: Two issues: (1) NULLs use `fmt.Sprintf("NULL:%d", v.Type)` — NULL INT and NULL TEXT produce different keys. SQL standard says NULLs are equal for DISTINCT. (2) Non-NULL values use `fmt.Sprintf("%v", v)` — string `"1"` and integer `1` may produce the same representation.

**Fix**: Use type-prefixed keys for non-NULL values and a single `"NULL"` key for all NULL values.

**Impact**: `SELECT DISTINCT col FROM t` may return multiple NULL rows and may incorrectly merge values of different types.

---

### Bug 9: TokEOF == TokIllegal — Illegal Tokens Silently Treated as EOF

**Commit**: `72987e6`
**Files**: `parser/api/api.go`

**Root Cause**: Both `TokEOF` and `TokIllegal` are defined as constant value `64`. The parser's EOF check passes for illegal tokens, so any unrecognized character is treated as end-of-input.

**Fix**: Assign distinct values: `TokIllegal = 65`.

**Impact**: `SELECT * FROM t @garbage` parses successfully, silently ignoring everything after `t`. **Malformed SQL produces no error.**

---

### Bug 10: INNER JOIN Fails to Parse

**Commit**: `af8e2eb`
**Files**: `parser/internal/lexer.go`

**Root Cause**: The keyword `"INNER"` maps to `TokJoin` (same token as `"JOIN"`). So `INNER JOIN` produces two consecutive `TokJoin` tokens.

**Fix**: Add a distinct `TokInner` token type. Update `parseJoin` to consume `TokInner` followed by `TokJoin`.

**Impact**: Standard SQL `INNER JOIN` syntax fails with a confusing error.

---

### Bug 11: Column Constraint Order Is Rigid

**Commit**: `9c6c09e`
**Files**: `parser/internal/parser.go`

**Root Cause**: `parseColumnDef` checks column constraints in a fixed order. If the user writes `NOT NULL PRIMARY KEY`, NOT NULL is consumed but PRIMARY KEY remains and is misinterpreted.

**Fix**: Parse column constraints in a loop that accepts them in any order.

**Impact**: `CREATE TABLE t (id INT NOT NULL PRIMARY KEY)` fails. Only the specific order `PRIMARY KEY NOT NULL` works.

---

### Bug 12: Wrong SQLSTATE Codes

**Commit**: `20e60ac`
**Files**: `errors/errors.go`

**Root Cause**: (1) `SQLStateAmbiguousColumn = "42703"` duplicates `SQLStateUndefinedColumn`. Correct: `"42702"`. (2) `SQLStateCheckViolation = "23522"` is non-standard. Correct: `"23514"`.

**Fix**: Set correct standard codes. Remove unused duplicate constant.

**Impact**: Database clients cannot distinguish ambiguous column from undefined column errors.

---

### Bug 13: NOT LIKE Not Supported

**Commit**: `96a4334`
**Files**: `parser/internal/parser.go`

**Root Cause**: `parseCompareExpr` handles `NOT IN` and `NOT BETWEEN` but has no `NOT LIKE` path.

**Fix**: Add a `NOT LIKE` branch producing `LikeExpr{Not: true}`.

**Impact**: `WHERE name NOT LIKE '%test%'` fails to parse.
