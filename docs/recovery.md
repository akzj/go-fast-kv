# Crash Recovery Design

> Authoritative reference. Evidence from `internal/kvstore/internal/recovery.go`.

## 1. Overview

Recovery = checkpoint file (full snapshot) + WAL replay (delta records with LSN > checkpoint.LSN).

## 2. Recovery Flow

1. Load checkpoint (if exists): restore blob mappings, CLOG, nextXID, rootPageID, etc.
2. WAL.Replay(afterLSN = checkpoint.LSN): apply post-checkpoint operations
3. Mark in-progress transactions as aborted
4. Set btree root

## 3. Checkpoint Snapshot vs WAL Delta

- **Checkpoint**: full snapshot of blob mappings, CLOG, nextXID, rootPageID, nextPageID, nextBlobID
- **WAL**: incremental operations after checkpoint LSN

## 4. WAL Replay Behavior

With checkpoint: `afterLSN = cpData.LSN` → replays records with LSN > checkpoint.LSN
Without checkpoint: `afterLSN = 0` → replays all records

## 5. Stale Comment

`recovery.go:56` says "always replay from beginning (afterLSN=0)" — **this is incorrect.** The actual code uses `afterLSN = cpData.LSN` when checkpoint exists. Comment should be:

```go
// Replay WAL entries. Only records with LSN > checkpoint.LSN are replayed.
// The checkpoint file already contains the full state snapshot at that LSN,
// so pre-checkpoint records are redundant. The active WAL segment (which
// holds post-checkpoint records) is never deleted by Truncate().
```

## 6. Why WAL Replay From checkpoint.LSN Is Safe

`WAL.Truncate(checkpoint.LSN)` deletes sealed WAL segments with endLSN ≤ checkpoint.LSN. The **active segment is never deleted**, so all post-checkpoint WAL records survive.

## 7. Page Mappings

NOT in checkpoint (always empty). Restored via: LSM SSTable files (from previous lsm.Close()) + WAL replay of ModuleLSM records.

## 8. Blob Mappings

IN checkpoint (full snapshot). Post-checkpoint changes via WAL replay of RecordBlobMap / RecordBlobFree.

## 9. LSM RecoveryStore

The live LSM is used for WAL replay: `lsmRecovery := psRecovery.LSMLifecycle()`. WAL replay calls `ApplyPageMapping()` and `ApplyBlobMapping()` directly on the live LSM.