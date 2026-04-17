# Garbage Collection Design

> Authoritative reference. Evidence from `internal/gc/internal/page_gc.go`, `internal/gc/internal/blob_gc.go`.

## 1. Design Goal

GC reclaims space in sealed segment files by copying live data to the active segment and removing the old segment file.

## 2. Liveness Algorithm

**VAddr pointer equality.** A record is **live** iff the current mapping still points to that exact VAddr.

```
For each record in sealed segment:
  currentVAddr = mapping[id]
  if currentVAddr == record.VAddr:
    LIVE → copy to active segment
  else:
    DEAD → skip
```

## 3. Page GC

Algorithm: scan sealed page segments (4108 bytes/record), check via `LSMLifecycle().GetPageMapping()`, copy live records to active segment, WAL batch, delete old segment.

**Current Status: NOT INTEGRATED.** kvstore never calls `CollectOne()`.

## 4. Blob GC

Algorithm: scan sealed blob segments (variable-length), check via `BlobStore.Read()`, copy live blobs to active segment, WAL batch, delete old segment.

**Current Status: NOT INTEGRATED.** kvstore never calls `CollectOne()`.

## 5. GC Safety: Double Sync Pattern

1. Sync OLD sealed segment
2. Sync ACTIVE segment (live data must be durable BEFORE WAL)
3. Write WAL batch

This guarantees no silent data loss on crash.

## 6. Trigger

**Manual only.** No automatic trigger. Background goroutine or timer needed for production use.

## 7. LSM Compaction vs GC

| | LSM Compaction | GC |
|--|--|--|
| Purpose | Persist memtable to SSTable | Reclaim sealed segment space |
| Trigger | Automatic (memtable size) | Manual |
| Reclaims disk space? | No | Yes |

## 8. The Integration Gap

gc/ package is standalone. Tests pass but nothing in kvstore calls CollectOne(). Sealed segments accumulate forever. Disk space is never reclaimed.