# Checkpoint Design

> Authoritative reference. Evidence from `internal/kvstore/internal/checkpoint.go`.

## 1. Purpose

**Checkpoint is a WAL truncation accelerator.** It has nothing to do with garbage collection.

The checkpoint file is a durable snapshot of system state at a specific WAL LSN. Its sole purpose is to allow deletion of old WAL segments. Without checkpoint, the WAL grows unbounded indefinitely. With checkpoint, WAL entries older than the checkpoint LSN can be discarded.

## 2. Data Serialized

```go
type checkpointData struct {
    LSN        uint64
    NextXID    uint64
    RootPageID uint64
    NextPageID uint64
    NextBlobID uint64
    Pages      []pageMapping // Always EMPTY in current code
    Blobs      []blobMapping  // All live blob mappings
    CLOGEntries []clogEntry
}
```

## 3. When Created

**Manual only.** No automatic timer or size-based trigger. Created when user calls `store.Checkpoint()` or `store.Close()`.

## 4. WAL Interaction

1. Write RecordCheckpoint to WAL + fsync
2. Write checkpoint file (atomic: temp → fsync → rename → dir fsync)
3. WAL.Truncate(checkpoint.LSN) — deletes sealed WAL segments ≤ LSN
4. CLOG.Truncate(safeXID)

**Active WAL segment is NEVER deleted.**

## 5. Recovery If No Checkpoint

`loadCheckpoint` returns `os.IsNotExist` → `afterLSN = 0` → replay WAL from beginning.

## 6. Recovery If Corrupt

CRC32 verification. CRC mismatch → error → store fails to open.

## 7. Page Mappings NOT in Checkpoint

```go
pageMappings := []pagestoreapi.MappingEntry(nil) // always empty
```

Page mappings are restored via LSM SSTable files + WAL replay.

## 8. CLOG Truncation

`safeXID = oldest active transaction ID`. All entries below safeXID are deleted.