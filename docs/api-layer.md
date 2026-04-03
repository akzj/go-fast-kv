# API Layer Specification

## Overview

Defines the public API for the KV store: operations, error handling, iterator semantics, and transaction model. The API layer sits atop the B-link-tree index, translating user operations into tree mutations.

**Relates to:**
- `blinktree-node-format.md` — B-link-tree node operations, sibling chain traversal
- `vaddr-format.md` — VAddr format for addressing
- `page-manager.md` — PageID allocation and mapping
- `external-value-store.md` — Large value storage

## Design Principles

1. **Simple core, extensible options** — Basic operations are synchronous; async/batch are optional extensions
2. **Error types complement lower layers** — API layer defines user-facing errors; lower layers define storage errors
3. **Iterator semantics match B-link properties** — Sibling chain traversal is exposed, not hidden
4. **Transaction model is pluggable** — Single-key ops work without transactions; multi-key transactions are optional

---

## Core Interface

```go
// KVStore is the primary interface for key-value operations.
// Thread-safe: all operations are safe for concurrent use.
// 
// Invariant: Operations are atomic — either fully applied or not at all.
// Invariant: After a successful Put/Delete, subsequent Get reflects the change.
// Invariant: Scan iterator is point-in-time consistent (snapshot semantics).
//
// Why snapshot semantics for Scan?
// B-link-tree nodes are immutable; snapshot is just the root's VAddr.
// Iterator captures root VAddr at creation, traverses at that point-in-time.
type KVStore interface {
    // Get retrieves the value for key.
    // Returns (value, nil) if found, (nil, ErrKeyNotFound) if absent.
    // 
    // Why return []byte, not (value, found bool)?
    // ErrKeyNotFound is self-describing; found is redundant.
    // Callers check error, not a bool.
    Get(key []byte) ([]byte, error)

    // Put stores a key-value pair.
    // If key exists, its value is overwritten.
    // Returns nil on success, error otherwise.
    //
    // Invariant: Put is durable when this method returns nil.
    // Invariant: Put with value > 48 bytes stores externally per blinktree-node-format.md.
    //
    // Why not return previous value?
    // Append-only storage; previous value may be in sealed segment.
    // If caller needs it, they should Get before Put.
    Put(key, value []byte) error

    // Delete removes a key-value pair.
    // Returns nil if deleted, ErrKeyNotFound if key was absent.
    //
    // Invariant: After Delete returns nil, subsequent Get returns ErrKeyNotFound.
    // Invariant: Delete is idempotent — calling twice when key exists returns ErrKeyNotFound on second call.
    //
    // Why idempotent? Simplifies error handling in callers.
    // Append-only tombstone is indistinguishable from "key never existed" for reads.
    Delete(key []byte) error

    // Scan returns an iterator over keys in range [start, end).
    // If end is nil, scan continues to the last key.
    // If start is nil, scan begins at the first key.
    //
    // Invariant: Iterator reflects state at time of Scan() call (snapshot semantics).
    // Invariant: Iterator is safe to use after concurrent modifications to store.
    //
    // Iterator lifecycle:
    //   iter := store.Scan(start, end)
    //   for iter.Next() {
    //       key, value := iter.Key(), iter.Value()
    //       // process
    //   }
    //   if err := iter.Error(); err != nil {
    //       // handle error
    //   }
    //   iter.Close()
    //
    // Why not return slice of key-value pairs?
    // Memory efficiency for large ranges; lazy evaluation.
    // Iterator is the standard pattern for range queries in storage systems.
    Scan(start, end []byte) (Iterator, error)

    // Close releases resources held by the store.
    // After Close, all operations return ErrStoreClosed.
    Close() error
}
```

---

## Error Types

```go
// API-layer errors are user-facing and complement lower-layer errors.
// Lower layers (storage, page manager) define transport/persistence errors;
// API layer defines semantic/business logic errors.
//
// Invariant: Error strings are stable (no dynamic content in error message).
// Invariant: Use sentinel errors for cases callers check explicitly.

var (
    // ErrKeyNotFound indicates the requested key does not exist.
    // Returned by: Get, Delete
    // Why not return zero value from Get?
    // Zero value is ambiguous — caller can't distinguish missing key from empty value.
    ErrKeyNotFound = errors.New("key not found")

    // ErrStoreClosed indicates the store has been closed.
    // Returned by: all operations after Close()
    // Why check before operation?
    // Prevents goroutine leaks and use-after-free scenarios.
    ErrStoreClosed = errors.New("store is closed")

    // ErrTransactionAborted indicates the transaction was explicitly aborted.
    // Returned by: Transaction.Commit (if Abort was called)
    // See: Transaction model section.
    ErrTransactionAborted = errors.New("transaction aborted")

    // ErrStoreFull indicates storage capacity has been reached.
    // Returned by: Put
    // Why not auto-expand?
    // Append-only storage may require compaction before new allocations.
    ErrStoreFull = errors.New("store is full")

    // ErrKeyTooLarge indicates the key exceeds maximum allowed size.
    // Returned by: Put
    // Maximum key size is implementation-defined (typically 1KB).
    ErrKeyTooLarge = errors.New("key too large")

    // ErrValueTooLarge indicates the value exceeds maximum allowed size.
    // Returned by: Put
    // Maximum value size from external-value-store.md: DefaultMaxValueSize (64MB).
    // Note: Small values (<=48 bytes) are stored inline; this error
    // applies to values exceeding the external store's limit.
    ErrValueTooLarge = errors.New("value too large")
)

// IsNotFound is a helper for checking ErrKeyNotFound.
func IsNotFound(err error) bool {
    return errors.Is(err, ErrKeyNotFound)
}

// IsClosed is a helper for checking ErrStoreClosed.
func IsClosed(err error) bool {
    return errors.Is(err, ErrStoreClosed)
}
```

### Error Type Hierarchy

```
errors package (standard)
    │
    ├── ErrKeyNotFound       ← API semantic error
    ├── ErrStoreClosed       ← API state error
    ├── ErrTransactionAborted
    ├── ErrStoreFull
    ├── ErrKeyTooLarge
    ├── ErrValueTooLarge
    │
    └── wrapped lower-layer errors (storage, checksum, etc.)
```

**Why not define custom error type?**
- Sentinel errors with `errors.Is` satisfy most use cases.
- Custom error types add complexity without benefit for this use count.
- Lower layers may wrap with `fmt.Errorf("page manager: %w", err)`.

---

## Iterator Interface

```go
// Iterator provides sequential access to a range of key-value pairs.
// Created by KVStore.Scan().
//
// Invariant: Iterator is point-in-time consistent (snapshot semantics).
// Invariant: Iterator is independent of concurrent store modifications.
// Invariant: Iterator must be closed to release resources.
//
// Why B-link sibling chain matters for iterators:
// B-link trees allow concurrent splits. During iteration:
//   1. Current node may split while iterating
//   2. Sibling chain links connect old node → new right node
//   3. Iterator follows sibling chain to continue past split
//
// This is why we expose Next() as a loop, not a single-pass method.
type Iterator interface {
    // Next advances the iterator to the next key-value pair.
    // Returns true if positioned at a valid entry, false if exhausted.
    // Returns false when:
    //   - Iteration reached end bound
    //   - Iteration encountered an error (check Error())
    //   - Iterator was closed
    //
    // Why return bool, not error?
    // Exhaustion is normal; error is exceptional.
    // Caller pattern: for iter.Next() { process } is cleaner.
    Next() bool

    // Key returns the key at the current position.
    // Valid only after Next() returns true.
    // 
    // Why return copy?
    // Iterator may advance or close, invalidating underlying reference.
    // Caller receives independent copy.
    Key() []byte

    // Value returns the value at the current position.
    // Valid only after Next() returns true.
    //
    // Why return copy?
    // Same as Key() — iterator state may change.
    Value() []byte

    // Error returns any error encountered during iteration.
    // Returns nil if iteration completed successfully.
    // 
    // Why separate from Next()?
    // Next() returning false could mean exhaustion OR error.
    // Error() disambiguates — caller can distinguish.
    Error() error

    // Close releases resources held by the iterator.
    // Idempotent — safe to call multiple times.
    Close()
}

// Why not a range-based for loop?
// Go doesn't support custom iteration syntax.
// Iterator pattern (Next/Key/Value) is the standard Go approach.
//
// Why not an iterator that returns channel?
// Channel-based iterators are less flexible:
//   - Can't easily skip items
//   - Can't easily peek ahead
//   - Error handling is awkward
//   - Not cursor-positionable
```

### Iterator Implementation Notes

```go
// Iterator traverses B-link tree using sibling chains.
// 
// Key invariant: Sibling chain continuity.
// When a leaf splits:
//   - Old leaf continues to exist (immutable)
//   - New right leaf is appended
//   - Old leaf's HighSibling points to new leaf
//   - New leaf may have its own HighSibling
//
// Iterator traversal:
//   for node := search(root, start); node != nil; node = node.HighSibling {
//       for i := searchWithin(node, currentKey); i < node.Count; i++ {
//           yield node.Entries[i]
//           if end != nil && keyCmp(Entries[i].Key, end) >= 0 {
//               return  // past end bound
//           }
//       }
//   }
//
// Why not use parent pointer?
// Parent may split/change during iteration.
// Sibling chain is stable — links to immutable nodes.
//
// Why not restart from root?
// Correct but inefficient — O(n log n) for full scan.
// Sibling chain provides O(n) full scan.
```

---

## Transaction Model

```go
// Transaction provides atomic multi-key operations.
// Transactions are optional — single-key operations work without them.
//
// Invariant: Transaction commits atomically or not at all.
// Invariant: Uncommitted changes are invisible to other readers.
// Invariant: Committed changes are immediately visible to new readers.
//
// Transaction lifecycle:
//   tx, err := store.Begin()
//   if err != nil { /* handle */ }
//   // ... perform operations
//   if err := tx.Commit(); err != nil {
//       tx.Rollback()  // cleanup
//   }
//
// Why not auto-commit?
// Explicit commit gives caller control over durability.
// Implicit commit on close would hide errors.
type Transaction interface {
    // Get retrieves value within this transaction.
    // Sees uncommitted changes from this transaction.
    // Does not see uncommitted changes from other transactions.
    Get(key []byte) ([]byte, error)

    // Put stores value within this transaction.
    // Change is not visible until Commit succeeds.
    Put(key, value []byte) error

    // Delete removes key within this transaction.
    // Change is not visible until Commit succeeds.
    Delete(key []byte) error

    // Scan returns an iterator within this transaction.
    // Iterator is consistent with uncommitted changes.
    Scan(start, end []byte) (Iterator, error)

    // Commit makes all changes in this transaction visible to other operations.
    // After Commit, transaction is closed.
    // Returns ErrTransactionAborted if transaction was already aborted.
    Commit() error

    // Rollback cancels all changes in this transaction.
    // After Rollback, transaction is closed.
    // Idempotent — safe to call even after Commit (which becomes no-op).
    Rollback()

    // TxID returns the transaction's unique identifier.
    // Used for debugging and transaction ordering.
    TxID() uint64
}

// KVStoreWithTransactions extends KVStore with transaction support.
// Not all implementations may support transactions.
//
// Why separate interface?
// - Simpler implementations can implement only KVStore
// - Transaction support may have additional resource costs
// - Clear capability indication for callers
type KVStoreWithTransactions interface {
    KVStore

    // Begin starts a new transaction.
    // Returns ErrStoreFull if transaction limit is reached.
    //
    // Why not auto-detect need?
    // Explicit Begin makes transaction boundaries clear.
    // Callers know when they enter/exit transactional mode.
    Begin() (Transaction, error)

    // BeginWithOptions starts a transaction with configuration.
    // Options control isolation level, timeout, etc.
    BeginWithOptions(opts TransactionOptions) (Transaction, error)
}

// TransactionOptions controls transaction behavior.
type TransactionOptions struct {
    // IsolationLevel (not yet implemented — reserved for future).
    // Default: Snapshot isolation.
    IsolationLevel IsolationLevel

    // Timeout sets maximum duration for commit.
    // If 0, uses implementation default.
    // If negative, no timeout.
    Timeout time.Duration

    // ReadOnly prevents writes within this transaction.
    // Implementation may optimize for read-only workloads.
    ReadOnly bool
}

// IsolationLevel is reserved for future concurrency control.
// Currently: Snapshot isolation only.
type IsolationLevel int

const (
    IsolationSnapshot IsolationLevel = iota  // Default; reads see committed state at tx start
    // Future: IsolationSerializable
)

// Why this transaction model?
// - MVCC (Multi-Version Concurrency Control) pairs naturally with append-only storage.
// - Each transaction reads from a snapshot (root VAddr at Begin).
// - Writes create new versions; old versions persist until compaction.
// - No locking during reads — readers never block writers.
// - Writers may block if they need to update the same node (latch).
```

---

## Batch Operations

```go
// Batch performs multiple operations atomically.
// Batch is simpler than Transaction but less flexible.
//
// Invariant: Batch commits atomically or not at all.
// Invariant: Batch applies operations in order (not parallelized).
// Invariant: All operations in a batch succeed or all fail.
//
// Batch lifecycle:
//   batch := store.NewBatch()
//   batch.Put(key1, value1)
//   batch.Put(key2, value2)
//   batch.Delete(key3)
//   if err := batch.Commit(); err != nil {
//       // all changes rolled back
//   }
//
// Why Batch AND Transaction?
// - Batch: simpler, lower overhead, no TxID needed
// - Transaction: explicit Begin/Commit, can span time, can rollback
// - Transaction supports reads; Batch is write-only
type Batch interface {
    // Put queues a put operation.
    // Panics if batch was already committed.
    Put(key, value []byte)

    // Delete queues a delete operation.
    // Panics if batch was already committed.
    Delete(key []byte)

    // Commit applies all queued operations atomically.
    // After Commit, batch is closed.
    Commit() error

    // Reset clears queued operations without committing.
    // Batch can be reused after Reset.
    Reset()
}

// NewBatch creates a new batch for atomic multi-operation.
type BatchCreator interface {
    NewBatch() Batch
}
```

---

## Wire Protocol (Optional Extension)

```go
// WireProtocol defines the interface for network/IPC communication.
// This is an optional extension — in-process usage does not require it.
//
// Why separate from core KVStore?
// - Core interface is synchronous, in-memory
// - Wire protocol adds serialization, transport, protocol versioning
// - Allows different transports (TCP, Unix socket, gRPC, custom)
//
// Invariant: Wire protocol is stateless — each request is independent.
// Invariant: Wire protocol does not define transactions (delegated to underlying store).
//
// Wire format (binary, big-endian):
//   ┌─────────────────────────────────────────────────────┐
//   │  Header (16 bytes)                                  │
//   ├─────────────────────────────────────────────────────┤
//   │  Magic: 0x4641544501000001 (8 bytes) "FATE\0\0\0\1"  │
//   │  Version: uint16 (currently 1)                      │
//   │  Flags: uint16                                      │
//   │  Length: uint32 (body length, big-endian)           │
//   ├─────────────────────────────────────────────────────┤
//   │  Body (variable)                                    │
//   │  └── Operation-specific payload                    │
//   └─────────────────────────────────────────────────────┘
//
// Request types: GET, PUT, DELETE, SCAN, SCAN_NEXT, BATCH, CLOSE
// Response types: OK, ERROR, VALUE, ITERATOR, BATCH_OK
type WireProtocol interface {
    // EncodeRequest serializes an operation for transport.
    EncodeRequest(op Operation) ([]byte, error)

    // DecodeRequest parses a request from transport.
    DecodeRequest(data []byte) (Operation, error)

    // EncodeResponse serializes an operation result.
    EncodeResponse(op Operation, result Result) ([]byte, error)

    // DecodeResponse parses a response from transport.
    DecodeResponse(data []byte) (Result, error)
}

// Operation and Result are wire protocol types.
// These are separate from KVStore types to allow evolution.
//
// Why not use KVStore types directly?
// Wire protocol may need versioning, compression, encryption.
// Separating allows independent evolution.
type Operation struct {
    Type    OpType
    Key     []byte
    Value   []byte
    Start   []byte  // For SCAN
    End     []byte  // For SCAN
    BatchOp []BatchEntry
}

type Result struct {
    Type   ResultType
    Value  []byte
    Error  error
    // For iterator results
    IterID uint64
    // For batch results
    BatchResult []BatchResultEntry
}

type OpType uint8

const (
    OpGet OpType = iota
    OpPut
    OpDelete
    OpScan
    OpScanNext
    OpBatch
    OpClose
)

type ResultType uint8

const (
    ResultOK ResultType = iota
    ResultError
    ResultValue
    ResultIterator
    ResultBatchOK
)

type BatchEntry struct {
    Type  OpType  // PUT or DELETE
    Key   []byte
    Value []byte
}

type BatchResultEntry struct {
    Type  OpType
    Error error
}
```

---

## Configuration

```go
// Config holds KVStore initialization parameters.
// All fields are optional; defaults are applied for zero values.
type Config struct {
    // Directory is the path to storage directory.
    // Required unless Open is called with nil Config.
    Directory string

    // MaxKeySize is the maximum key length in bytes.
    // If 0, uses DefaultMaxKeySize.
    // Default: 1024 bytes.
    MaxKeySize uint32

    // MaxValueSize is the maximum value length in bytes.
    // If 0, uses DefaultMaxValueSize (64MB from external-value-store.md).
    MaxValueSize uint64

    // ReadOnly opens the store in read-only mode.
    // Useful for read replicas or recovery verification.
    ReadOnly bool

    // SyncWrites controls durability vs performance trade-off.
    // If true, writes are synced to storage before returning.
    // If false, relies on OS page cache (faster, less durable).
    // Default: true (durable).
    SyncWrites bool

    // CacheSizeMB is the size of in-memory cache in megabytes.
    // If 0, uses implementation default (typically 256MB).
    // Cache stores recent pages for faster reads.
    CacheSizeMB uint32

    // BLinkTreeNodeSize is the size of B-link tree nodes.
    // If 0, uses DefaultNodeSize (4KB or 64KB, aligned with PageSize).
    // Larger nodes: fewer levels, more memory per node.
    // Smaller nodes: more levels, less memory per node.
    BLinkTreeNodeSize uint32
}

// Defaults
const (
    DefaultMaxKeySize   = 1024            // 1 KB
    DefaultNodeSize     = 64 * 1024       // 64 KB
    DefaultCacheSizeMB  = 256             // 256 MB
)
```

---

## Factory Functions

```go
// Open creates or opens a KVStore at the given directory.
// Applies default configuration for nil Config.
//
// Why a factory function instead of constructor?
// - Directory-based initialization is common
// - Allows deferred configuration
// - Future: could add OpenWithOptions for more control
func Open(directory string, config *Config) (KVStore, error)

// OpenWithTransactions creates a store with transaction support.
func OpenWithTransactions(directory string, config *Config) (KVStoreWithTransactions, error)

// Destroy removes all storage files in directory.
// Use with caution — this is irreversible.
func Destroy(directory string) error
```

---

## Usage Examples

### Basic Operations

```go
store, err := Open("/data/my-store", nil)
if err != nil {
    log.Fatalf("failed to open store: %v", err)
}
defer store.Close()

// Put and Get
if err := store.Put([]byte("hello"), []byte("world")); err != nil {
    log.Fatalf("put failed: %v", err)
}

value, err := store.Get([]byte("hello"))
if errors.Is(err, ErrKeyNotFound) {
    println("key not found")
} else if err != nil {
    log.Fatalf("get failed: %v", err)
}
println(string(value)) // "world"

// Delete
if err := store.Delete([]byte("hello")); err != nil {
    log.Fatalf("delete failed: %v", err)
}
```

### Range Scan

```go
iter, err := store.Scan([]byte("a"), []byte("z"))
if err != nil {
    log.Fatalf("scan failed: %v", err)
}
defer iter.Close()

for iter.Next() {
    fmt.Printf("%s: %s\n", iter.Key(), iter.Value())
}
if err := iter.Error(); err != nil {
    log.Fatalf("iteration error: %v", err)
}
```

### Batch Operations

```go
kvtx, ok := store.(BatchCreator)
if !ok {
    println("batch not supported")
    return
}

batch := kvtx.NewBatch()
batch.Put([]byte("key1"), []byte("value1"))
batch.Put([]byte("key2"), []byte("value2"))
batch.Delete([]byte("key3"))

if err := batch.Commit(); err != nil {
    log.Fatalf("batch commit failed: %v", err)
}
```

### Transactions

```go
kvtx, ok := store.(KVStoreWithTransactions)
if !ok {
    println("transactions not supported")
    return
}

tx, err := kvtx.Begin()
if err != nil {
    log.Fatalf("begin failed: %v", err)
}

// Transaction reads see uncommitted changes
val, _ := tx.Get([]byte("key1")) // sees this tx's writes

tx.Put([]byte("key1"), []byte("new-value"))
tx.Put([]byte("key2"), []byte("another"))

if err := tx.Commit(); err != nil {
    tx.Rollback()
    log.Fatalf("commit failed: %v", err)
}
```

---

## Why These Design Choices

| Decision | Alternative | Why Not |
|----------|-------------|---------|
| []byte for key/value | string | More generic; callers may have []byte anyway |
| errors.Is for error checking | error types | Simpler; covers most cases |
| Iterator vs slice return | []KV{} | Memory efficient; lazy evaluation |
| Explicit Transaction.Begin | Auto-tx on first op | Clear boundaries; explicit is safer |
| WireProtocol separate | Embedded in KVStore | Different transport needs; separable evolution |
| Config struct | many parameters | Extensible without breaking callers |

---

## Acceptance Criteria

- [ ] `KVStore` interface defined with Get, Put, Delete, Scan, Close
- [ ] Error types defined: ErrKeyNotFound, ErrStoreClosed, etc.
- [ ] `Iterator` interface defined with Next, Key, Value, Error, Close
- [ ] `Transaction` interface defined with Get, Put, Delete, Scan, Commit, Rollback
- [ ] `KVStoreWithTransactions` extends KVStore with Begin
- [ ] `Batch` interface defined with Put, Delete, Commit
- [ ] WireProtocol interface defined (separable from core)
- [ ] Config struct with all options
- [ ] Factory functions: Open, OpenWithTransactions, Destroy
- [ ] All types compile: `go build` passes
- [ ] "Why not" alternatives documented

---

*Document Status: Contract Spec*
*Last Updated: 2024*
