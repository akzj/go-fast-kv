# go-fast-kv Architecture

## System Overview

go-fast-kv is an embedded key-value store with a full SQL layer. The system is organized into two major layers:

```
┌─────────────────────────────────────────────────────────────┐
│                     SQL Layer                                │
│  Parser → Planner → Executor → Engine → Catalog → Encoding │
├─────────────────────────────────────────────────────────────┤
│                     KV Layer                                 │
│  KVStore → B-link Tree → LSM/WAL → Segment Manager          │
└─────────────────────────────────────────────────────────────┘
```

The KV layer provides Put/Get/Delete/Scan with MVCC snapshot isolation, WAL durability, and segment-based append-only storage. The SQL layer builds a complete relational database on top of the KV API.

---

## KV Layer

### Overview

go-fast-kv is a B-link tree based key-value store with MVCC snapshot isolation, WAL durability, and segment-based storage. The architecture is organized into five layers, each with clean API boundaries:

```
┌─────────────────────────────────────────────────────────────┐
│                    KVStore (Integration)                     │
│  Per-operation WAL collectors · Auto-vacuum · Auto-GC       │
├──────────────────────┬──────────────────────────────────────┤
│   Transaction Layer  │         Tree Layer                    │
│  TxnManager (CLOG)   │  B-link Tree (slotted pages, MVCC)  │
│  Row Lock (sharded)  │  Bulk Loader · Iterator              │
├──────────────────────┴──────────────────────────────────────┤
│                    Mapping Layer                              │
│  LSM (memtable + SSTable compaction) · WAL (group commit)   │
├─────────────────────────────────────────────────────────────┤
│                    Storage Layer                             │
│  Segment Manager (mmap) · PageStore · BlobStore             │
└─────────────────────────────────────────────────────────────┘
```

### 1. Storage Layer

#### 1.1 Segment Manager (`internal/segment/`)

The Segment Manager provides append-only storage files. Each segment is a single file that grows up to `MaxSegmentSize` (default 512MB), then is sealed (immutable) and a new active segment is opened via `Rotate()`.

**Key properties:**
- **Append-only**: Data is only appended, never overwritten in place. This enables crash safety — partial writes are detectable.
- **mmap for active segment**: The active segment is memory-mapped for fast writes. Sealed segments use `file.ReadAt()` (syscall path).
- **64-byte header**: Each segment file starts with a header containing magic bytes, version, segment ID, creation timestamp, and CRC32-C integrity check.
- **VAddr (Virtual Address)**: Every piece of data written to a segment gets a VAddr — a `uint64` encoding the segment ID + offset within the segment.

**VAddr Encoding (20:30:14 compact format):**

```
Bit layout:  [SegmentID:20] [Offset:30] [RecordLen:14]
             ├── 20 bits ──┤├── 30 bits ─┤├── 14 bits ─┤

Max SegmentID  = 1,048,575 (1M segments)
Max Offset     = 1,073,741,823 (1GB per segment)
Max RecordLen  = 16,383 bytes (covers 4096-byte page + overhead)
```

This compact encoding packs segment ID, byte offset, and record length into a single `uint64`, enabling O(1) lookups without additional indirection.

**Lifecycle:**
1. `New()` → opens/creates segment directory, opens active segment with mmap
2. `Append(data)` → appends to active segment, returns `VAddr{SegmentID, Offset}`
3. `ReadAtInto(segID, offset, buf)` → reads data at the given location
4. `Rotate()` → seals active segment, opens new one
5. `Close()` → syncs and closes all segments

#### 1.2 PageStore (`internal/pagestore/`)

PageStore manages the mapping from logical **PageID** (B-tree page identifier) to physical **VAddr** (segment location). It uses the LSM layer for this mapping.

**Key properties:**
- **Compact serialization**: Pages are written via `WriteCompact()` which uses variable-length encoding. The record length is embedded in the VAddr (14-bit field), eliminating fixed-size waste.
- **LSM-backed mapping**: `pageID → packed VAddr` is stored in the LSM memtable. On lookup, the LSM is queried (memtable first, then SSTables).
- **CAS for GC**: `CompareAndSetPageMapping(pageID, oldVAddr, newVAddr)` enables lock-free GC relocation.
- **Allocator**: `AllocPage()` returns monotonically increasing page IDs via `atomic.AddUint64`.

#### 1.3 BlobStore (`internal/blobstore/`)

BlobStore handles large values that exceed the B-tree's inline threshold (default 256 bytes). Uses a **dense in-memory array** (`[]BlobMeta`) indexed by blob ID.

**Key properties:**
- **Dense array mapping**: `blobID → BlobMeta{segID, offset, size}`. O(1) lookup.
- **Sequential allocation**: Blob IDs are allocated sequentially.
- **Sharded CAS locks**: GC blob relocation uses 64 shards to reduce contention.
- **COW snapshots**: `GetSnapshotMappings()` creates a copy under a short write lock for checkpoint.

### 2. Mapping Layer

#### 2.1 LSM Store (`internal/lsm/`)

The LSM store provides the persistent mapping layer for PageStore.

**Architecture:**
```
Write path:  SetPageMapping(pageID, vaddr) → active memtable (sync.Map)
Read path:   GetPageMapping(pageID) → active memtable → immutables → SSTables (newest first)
Compaction:  memtable full → freeze → background flush to L0 SSTable
             L0 overflow → merge L0 + overlapping L1 → new L1 SSTable
```

**Per-goroutine WAL collectors:** LSM uses goroutine-keyed collectors (`sync.Map` keyed by goroutine ID) to isolate WAL entries from concurrent operations.

**Compaction levels:** L0 (max 4), L1 (max 10), L2 (max 100).

#### 2.2 WAL (`internal/wal/`)

The Write-Ahead Log provides crash durability with group commit:

```
Writer goroutines                     Consumer goroutine
┌──────────┐                         ┌──────────────────────┐
│ Put()    ├──→ reqCh (buffered) ──→ │ drainAndProcess()    │
│ Delete() │                         │  • drain all pending │
│ Batch()  │                         │  • serialize all     │
└──────────┘                         │  • single Write()    │
                                     │  • single Sync()     │
                                     │  • notify all callers│
                                     └──────────────────────┘
```

**Record format:** `[LSN:8][ModuleType:1][RecordType:1][ID:8][VAddr:8][Size:4][CRC32:4]` = 34 bytes per record.

### 3. Tree Layer

#### 3.1 B-link Tree (`internal/btree/`)

B-link tree (Lehman-Yao) with MVCC versioning and slotted page format.

**Slotted page format (4096 bytes):**
```
┌────────────────────────────────────────────┐
│ Header (16 bytes)                          │
│  flags:1 | reserved:1 | count:2 | next:8  │
│  freeEnd:2 | highKeyOff:2                  │
├────────────────────────────────────────────┤
│ Slot Array (count × 2 bytes)               │
├────────────────────────────────────────────┤
│            Free Space                       │
├────────────────────────────────────────────┤
│ Cell Content Area (grows ← from end)       │
│  Leaf: [keyLen:2][key][txnMin:8][txnMax:8] │
│        [valueType:1][valueLen:4][value]     │
│  Internal: [keyLen:2][key][rightChild:8]   │
└────────────────────────────────────────────┘
```

**MVCC**: Each leaf entry carries `TxnMin` (creating txn) and `TxnMax` (deleting txn). `TxnMax = MaxUint64` means alive.

**Concurrency**: Per-page read-write locks (16 shards). B-link right-link correction handles concurrent splits — each level holds only one lock at a time, making deadlock impossible.

### 4. Transaction Layer

#### 4.1 TxnManager (`internal/txn/`)

PostgreSQL-style MVCC with snapshot isolation:
- **XID Manager**: Monotonically increasing transaction IDs
- **CLOG**: Commit status log (InProgress / Committed / Aborted)
- **Snapshot**: Captured at `BEGIN` — defines visibility boundary (xmin, xmax, activeXIDs)
- **Visibility**: 10 rules determining if a version is visible to a snapshot

#### 4.2 Row Lock Manager (`internal/rowlock/`)

Fine-grained row locking for SQL `SELECT ... FOR UPDATE`:
- 16 lock shards to reduce contention
- Shared + Exclusive modes
- Channel-based notification (replaces 10ms polling)
- Wait-for graph deadlock detection with DFS cycle check

### 5. Integration Layer

#### 5.1 KVStore (`internal/kvstore/`)

The top-level integration layer that wires everything together:
- Per-operation WAL collectors (goroutine-local)
- Auto-vacuum (configurable threshold)
- Auto-GC (segment compaction)
- Checkpoint & Recovery (V4 format)

#### 5.2 Checkpoint & Recovery

**Checkpoint**: Full state snapshot (blob mappings, CLOG, nextXID, rootPageID) written atomically (temp → fsync → rename → dir fsync). Page mappings restored via LSM SSTables.

**Recovery flow**: Load checkpoint → WAL replay (records with LSN > checkpoint.LSN) → Mark in-progress transactions as aborted.

### Data Flow: Put Operation

```
store.Put(key, value)
  │
  ├─ 1. RegisterCollector() — per-goroutine WAL entry collector
  ├─ 2. txnMgr.BeginTxn() — allocate XID, capture snapshot
  ├─ 3. tree.Put(key, value, xid)
  │     ├─ searchPath(key) — read-lock pages top-down, B-link correction
  │     ├─ WLock(leafPID) — write-lock target leaf
  │     ├─ mvccMarkOld() — set TxnMax on previous version
  │     ├─ InsertLeafEntry() — insert into slotted page
  │     │   └─ (if page full) splitLeafPage() → propagateSplit()
  │     ├─ WritePage(leafPID, page)
  │     │   ├─ segMgr.Append(data) → VAddr
  │     │   ├─ PackPageVAddr() → packed
  │     │   └─ lsm.SetPageMapping(pageID, packed) → WAL collector
  │     └─ WUnlock(leafPID)
  │
  ├─ 4. assembleBatchFromCollectors()
  ├─ 5. wal.WriteBatch(batch) — group commit
  ├─ 6. txnMgr.Commit(xid)
  └─ 7. checkAutoVacuum()
```

### Data Flow: Get Operation

```
store.Get(key)
  │
  ├─ 1. Check activeTxnCtx (goroutine-local SQL transaction context)
  ├─ 2. txnMgr.ReadSnapshot() — snapshot WITHOUT allocating XID
  ├─ 3. tree.Get(key, readXID)
  │     ├─ Navigate: RLock → B-link correction → descend → RUnlock
  │     ├─ At leaf: binary search, MVCC visibility check
  │     └─ Return first visible entry's value
  └─ 4. Cleanup snapshot registration
```

---

## SQL Layer

The SQL layer implements a complete SQL database engine on top of go-fast-kv's key-value store. It follows a classical pipeline: **Parse → Plan → Execute**.

### Module Dependency Graph

```
sql.go (top-level API)
  ├── stmtcache (prepared statement LRU cache)
  ├── parser → AST
  ├── planner → Plan (uses catalog for schema lookup)
  ├── executor → Result (uses engine for storage I/O)
  │     ├── engine/table (row CRUD)
  │     ├── engine/index (secondary index CRUD)
  │     └── engine/fts (full-text search)
  ├── catalog (schema metadata in KV)
  ├── encoding (key encoding + row codec)
  └── errors (structured SQL errors with SQLSTATE)
```

### Parser

Hand-written recursive descent parser with hand-written lexer. ~90 token types. 28 statement types. 21 expression types.

| Category | Statements |
|----------|-----------|
| DDL | CREATE TABLE, DROP TABLE, ALTER TABLE, CREATE INDEX, DROP INDEX, CREATE VIEW, DROP VIEW |
| DML | SELECT, INSERT, UPDATE, DELETE, TRUNCATE |
| Set Operations | UNION [ALL], INTERSECT, EXCEPT |
| CTE | WITH [RECURSIVE] ... AS (SELECT ...) |
| Transaction | BEGIN, COMMIT, ROLLBACK, SAVEPOINT, RELEASE SAVEPOINT |
| Other | EXPLAIN [ANALYZE], PRAGMA, CREATE TRIGGER, DROP TRIGGER, CREATE FTS |

### Planner

Converts AST to execution plans. Scan strategy selection (in priority order):

1. FTS MATCH → full-text search scan
2. LIKE prefix → index range scan
3. Index scan → equality or range on indexed column
4. Covering index → index-only scan
5. Table scan → fallback full scan

### Executor

~7300 lines. SELECT execution pipeline:

```
subqueries → scan → filter → window functions → GROUP BY → HAVING
  → ORDER BY → OFFSET/LIMIT → DISTINCT → project
```

**JOIN strategies:**
- Nested Loop: O(n×m) — default, works for all join types
- Hash Join: O(n+m) — for equi-joins on large tables
- Index Nested Loop: O(n × log m) — when index exists on join column

### Engine

Maps SQL operations to KV operations:

- **Table Engine**: Row keys `t{tableID:4B}r{rowID:8B}` (14 bytes, fixed size). Values encoded with RowCodec (column count + null bitmap + typed values).
- **Index Engine**: Keys `t{tableID:4B}i{indexID:4B}{encodedValue}{rowID:8B}`. Values are empty (rowID is in the key).
- **FTS Engine**: Full-text search with tokenization, Porter stemming, and inverted index.

### Catalog

Schema metadata stored in KV with prefix-based keys:

| Key Pattern | Content |
|------------|---------|
| `_sql:table:{name}` | Table schema (JSON) |
| `_sql:index:{table}:{name}` | Index schema (JSON) |
| `_sql:trigger:{name}` | Trigger definition |
| `_sql:view:{name}` | View definition |

In-memory LRU cache (64 entries) for frequently accessed schemas.

### Encoding

Order-preserving value encoding for index keys:

| Type | Format | Sort Order |
|------|--------|-----------|
| NULL | `[0x00]` | Sorts first |
| Int | `[0x02][int64 XOR 0x8000000000000000]` | Correct signed ordering |
| Float | `[0x03][transformed IEEE 754]` | Correct float ordering |
| Text | `[0x04][escaped bytes][0x00][0x00]` | Lexicographic |
| Blob | `[0x05][escaped bytes][0x00][0x00]` | Lexicographic |

### Data Flow: SELECT Query

```
SQL: "SELECT name FROM users WHERE age > 25 ORDER BY name LIMIT 10"

1. Parser → SelectStmt{Table:"users", Where: BinaryExpr{age > 25}, ...}
2. Planner: catalog lookup → index on "age" → IndexScanPlan{Op:GT, Value:25}
3. Executor: index scan → fetch rows → sort by name → limit 10 → project
```

### Data Flow: INSERT Query

```
SQL: "INSERT INTO users (name, age) VALUES ('alice', 30)"

1. Parser → InsertStmt
2. Planner: resolve columns, type check → InsertPlan
3. Executor: constraints → allocate rowID → encode row → write batch → commit
```
