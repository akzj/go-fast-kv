# System Architecture

> Authoritative reference. Evidence from throughout the codebase.

## 1. High-Level Overview

```
KVStore
  ├── B-link Tree (key → value)
  ├── Transaction Manager (MVCC: XID, CLOG, Snapshot)
  ├── PageStore (PageID → VAddr, uses LSM + SegmentManager)
  │     └── LSM (memtable + SSTable)
  ├── BlobStore (BlobID → VAddr, dense array, uses SegmentManager)
  └── WAL (shared by all modules)
```

## 2. Three Independent Systems

| System | Purpose | Trigger | Status |
|--------|---------|---------|--------|
| Checkpoint | WAL truncation | Manual | Working |
| LSM Compaction | Persist mappings to SSTable | Auto (memtable size) | Working |
| Page/Blob GC | Reclaim sealed segment space | NOT INTEGRATED | Dead code |

**They do not interact.** GC does not affect checkpoint or LSM.

## 3. Pages vs Blobs Asymmetry

| | Pages | Blobs |
|--|--|--|
| Persistence | LSM (SSTable files) | Dense array (in-memory) |
| In checkpoint | NO (always empty) | YES (full snapshot) |
| GC | NOT INTEGRATED | NOT INTEGRATED |

**Why asymmetry?** Pages naturally persist via LSM SSTable. Blobs need checkpoint snapshot because there's no SSTable.

## 4. GC Is Missing

Current: writes persist, checkpoint works, recovery works, LSM persists memtable. Missing: GC never runs. Result: sealed segments accumulate forever, disk space never reclaimed.

## 5. NOT VERIFIED Items

Items marked as unverified assumptions (not backed by code reading):
- Segment size threshold for sealing (assumed 64MB)
- Auto-checkpoint trigger (design doc says 16MB, NOT implemented)
- GC concurrent-write safety
- Inline value threshold (design doc says 256 bytes)
- Vacuum for MVCC old versions
- SSI isolation implementation