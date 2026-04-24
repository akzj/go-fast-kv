# SQL Layer Design Document

**Version**: 1.1
**Status**: Reviewed — ready for implementation
**Scope**: `internal/sql/` — SQL support for go-fast-kv

---

## §1 Overview & Goals

### 1.1 What We're Building

A SQL layer on top of go-fast-kv's existing KV store, enabling users to interact with the database using standard SQL statements instead of raw KV operations.

```
用户: db.Exec("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
      db.Query("SELECT name, age FROM users WHERE age > 18")

而非: store.Put([]byte("...encoded key..."), []byte("...encoded row..."))
```

### 1.2 Design Goals

1. **最小可用优先** — 先支持核心 SQL 子集，可工作后再扩展
2. **零 KV 层改动** — SQL 层完全构建在 `kvstore.Store` 接口之上
3. **模块化** — 每个组件独立可测，遵循 `api/api.go` 接口先行
4. **正确性优先** — 借鉴 SQLite 哲学：宁可慢，不可错

### 1.3 SQL 支持范围（Phase 1）

| 支持 | 不支持（后续 Phase） |
|------|---------------------|
| `CREATE TABLE` / `DROP TABLE` | `ALTER TABLE` |
| `CREATE INDEX` / `DROP INDEX` | 复合索引（多列） |
| `INSERT INTO ... VALUES (...)` | `INSERT ... SELECT` |
| `SELECT ... FROM ... WHERE ...` | `JOIN` |
| `DELETE FROM ... WHERE ...` | 子查询 |
| `UPDATE ... SET ... WHERE ...` | `GROUP BY` / `HAVING` |
| `ORDER BY` (单列) | `ORDER BY` 多列 |
| `LIMIT` | `OFFSET` |
| 单表查询 | 多表查询 |
| 比较运算: `=`, `!=`, `<`, `>`, `<=`, `>=` | `LIKE`, `IN`, `BETWEEN` |
| 逻辑运算: `AND`, `OR`, `NOT` | 聚合函数 |
| `NULL` / `IS NULL` / `IS NOT NULL` | `CASE` 表达式 |
| 自动主键 (auto-increment rowID) | 复合主键 |
| 二级索引（单列） | 覆盖索引 |

### 1.4 Non-Goals

- 不实现 VDBE 字节码虚拟机 — 使用树遍历解释器（tree-walking interpreter）直接执行
- 不实现复杂查询优化器 — 简单启发式：有索引用索引，否则全表扫描
- 不实现自己的事务管理 — 复用 KV 层的 WriteBatch（原子性）和 MVCC（隔离性）
- 不实现网络协议 — 嵌入式 API 调用

---

## §2 Architecture

### 2.1 Layer Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                        User API                              │
│           db.Exec(sql) / db.Query(sql)                      │
└──────────────────────────┬──────────────────────────────────┘
                           │
┌──────────────────────────┴──────────────────────────────────┐
│                     SQL Layer (NEW)                           │
│                                                              │
│  ┌──────────┐   ┌──────┐   ┌─────────┐   ┌──────────────┐  │
│  │  Parser  │──▶│ AST  │──▶│ Planner │──▶│   Executor   │  │
│  │          │   │      │   │         │   │              │  │
│  └──────────┘   └──────┘   └────┬────┘   └──────┬───────┘  │
│                                 │                │          │
│                           ┌─────┴─────┐   ┌─────┴───────┐  │
│                           │  Catalog  │   │   Engine     │  │
│                           │  (EXISTS) │   │ (table+index)│  │
│                           └─────┬─────┘   └──────┬──────┘  │
│                                 │                │          │
│                           ┌─────┴────────────────┴──────┐  │
│                           │        Encoding              │  │
│                           │  (key encoder + row codec)   │  │
│                           └──────────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
                           │
┌──────────────────────────┴──────────────────────────────────┐
│                   kvstore.Store (EXISTING)                    │
│        Put / Get / Delete / Scan / WriteBatch                │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 Execution Pipeline

借鉴 SQLite 的单向流水线，简化为 5 个阶段：

```
SQL Text
  │
  ▼
┌──────────────────────────────────────────────┐
│ 1. Tokenize (lexer)                          │
│    "SELECT name FROM users WHERE age > 18"   │
│    → [SELECT] [name] [FROM] [users] ...      │
└──────────────────┬───────────────────────────┘
                   ▼
┌──────────────────────────────────────────────┐
│ 2. Parse (recursive descent)                 │
│    Token stream → AST                        │
│    → SelectStmt{Columns, Table, Where}       │
└──────────────────┬───────────────────────────┘
                   ▼
┌──────────────────────────────────────────────┐
│ 3. Plan (simple heuristic)                   │
│    AST + Catalog → Execution Plan            │
│    → IndexScan(idx_age, >18) or TableScan    │
└──────────────────┬───────────────────────────┘
                   ▼
┌──────────────────────────────────────────────┐
│ 4. Execute (tree-walking)                    │
│    Plan → KV operations → Result rows        │
│    → engine.Scan() → filter → project        │
└──────────────────┬───────────────────────────┘
                   ▼
┌──────────────────────────────────────────────┐
│ 5. Return results                            │
│    []Row → user callback / iterator          │
└──────────────────────────────────────────────┘
```

### 2.3 Module Structure

```
internal/sql/
├── catalog/          ← EXISTS (347 lines, 7 tests)
│   ├── api/api.go       CatalogManager, TableSchema, ColumnDef, IndexSchema, Value, Type
│   ├── internal/        catalog_impl.go, catalog_test.go
│   └── catalog.go       re-export
│
├── encoding/         ← NEW: Key encoding + Row codec
│   ├── api/api.go       KeyEncoder, RowCodec interfaces + wire formats
│   ├── internal/        encoding_impl.go, encoding_test.go
│   └── encoding.go      re-export
│
├── parser/           ← NEW: Tokenizer + Recursive descent parser
│   ├── api/api.go       Token types, AST node types, Parser interface
│   ├── internal/        lexer.go, parser.go, parser_test.go
│   └── parser.go        re-export
│
├── planner/          ← NEW: AST → Execution plan
│   ├── api/api.go       Plan types (TableScanPlan, IndexScanPlan, etc.)
│   ├── internal/        planner.go, planner_test.go
│   └── planner.go       re-export
│
├── executor/         ← NEW: Execute plan via KV ops
│   ├── api/api.go       Executor interface, Result types
│   ├── internal/        executor.go, executor_test.go
│   └── executor.go      re-export
│
├── engine/           ← NEW: Table/Index CRUD mapped to KV
│   ├── api/api.go       TableEngine, IndexEngine interfaces
│   ├── internal/        table.go, index.go, engine_test.go
│   └── engine.go        re-export
│
└── sql.go            ← NEW: Composition root (DB struct)
```

### 2.4 Dependency Graph

```
Layer 0 (standalone):    encoding (pure algorithm, no external deps)
Layer 1 (catalog dep):   catalog → kvstore.Store  [EXISTS]
Layer 2 (encoding dep):  engine → encoding, catalog, kvstore.Store
Layer 3 (AST dep):       parser → (standalone, defines AST types in api/)
Layer 4 (plan dep):      planner → parser/api (AST types), catalog, engine/api
Layer 5 (composition):   executor → planner/api, engine, catalog
Layer 6 (top):           sql.go → ALL above (composition root)
```

Implementation order (bottom-up):
```
Phase 1: encoding     (standalone — key encoder + row codec)
Phase 2: engine       (depends on encoding + catalog + kvstore)
Phase 3: parser       (standalone — tokenizer + recursive descent)
Phase 4: planner      (depends on parser AST + catalog + engine API)
Phase 5: executor     (depends on planner + engine)
Phase 6: sql.go       (composition root — wires everything together)
```

---

## §3 Component Specifications

### §3.1 Encoding — Key Encoder + Row Codec

**Responsibility**: Order-preserving encoding of typed values into `[]byte` keys, and serialization of row data into `[]byte` values.

**Why this matters**: The B-tree stores keys in `bytes.Compare()` lexicographic order. For SQL to work, encoded keys must preserve the logical sort order of the original typed values. `int(9) < int(10)` must hold after encoding.

#### §3.1.1 Key Encoding Format

Inspired by TiDB/CockroachDB's memcomparable format:

```
Table row key:     t{tableID}r{rowID}
Index entry key:   t{tableID}i{indexID}{encodedValue}{rowID}
Metadata key:      _sql:table:{NAME}  (existing catalog format)
                   _sql:index:{TABLE}:{INDEX}  (existing catalog format)
```

Where:
- `tableID`: uint32, big-endian (4 bytes) — assigned at CREATE TABLE time
- `indexID`: uint32, big-endian (4 bytes) — assigned at CREATE INDEX time
- `rowID`: uint64, big-endian (8 bytes) — auto-increment per table

Prefix bytes for key type disambiguation:
```
't' (0x74) = table data prefix
```

Full row key layout:
```
[0]      byte    't' (0x74)
[1:5]    uint32  tableID (big-endian)
[5]      byte    'r' (0x72)
[6:14]   uint64  rowID (big-endian)
─────────────────────────────
Total: 14 bytes (fixed size)
```

Full index key layout:
```
[0]      byte    't' (0x74)
[1:5]    uint32  tableID (big-endian)
[5]      byte    'i' (0x69)
[6:10]   uint32  indexID (big-endian)
[10:..}  []byte  encodedColumnValue (variable, order-preserving)
[..:+8]  uint64  rowID (big-endian, suffix for uniqueness)
```

#### §3.1.2 Value Encoding (Order-Preserving)

Each typed value is encoded with a 1-byte type tag followed by the encoded value:

```
NULL:    [0x00]
         — sorts before all non-NULL values

Int:     [0x02] [8 bytes: int64 XOR 0x8000000000000000]
         — XOR flips sign bit so negative < 0 < positive in unsigned comparison
         — big-endian

Float:   [0x03] [8 bytes: transformed float64]
         — IEEE 754 → comparable: if negative, flip all bits; if positive, flip sign bit
         — big-endian

Text:    [0x04] [escaped bytes] [0x00] [0x00]
         — bytes escaped: 0x00 → 0x00 0xFF
         — terminated by 0x00 0x00 (unescaped)
         — preserves lexicographic order

Blob:    [0x05] [escaped bytes] [0x00] [0x00]
         — same escaping as Text
```

**Properties**:
- NULL < Int < Float < Text < Blob (by type tag)
- Within each type, values sort correctly via `bytes.Compare()`
- Variable-length types (Text, Blob) are self-delimiting via escape + terminator

#### §3.1.3 Row Codec (Value Serialization)

Row values are stored as the KV value. Unlike keys, these do NOT need to be order-preserving — they just need to be compact and fast to encode/decode.

```
Row value wire format:
[0:2]     uint16    column count (big-endian)
[2:2+N]   []byte    null bitmap (ceil(columnCount/8) bytes, bit=1 means NULL)
[...]     []byte    column values, concatenated:

Per-column encoding:
  NULL:    (skipped — indicated by null bitmap)
  Int:     [8 bytes int64, big-endian]
  Float:   [8 bytes float64, IEEE 754]
  Text:    [4 bytes uint32 length] [UTF-8 bytes]
  Blob:    [4 bytes uint32 length] [raw bytes]
```

#### §3.1.4 Interface

```go
// In encoding/api/api.go

// KeyEncoder encodes/decodes KV keys for table rows and index entries.
type KeyEncoder interface {
    // EncodeRowKey encodes a table row key: t{tableID}r{rowID}
    EncodeRowKey(tableID uint32, rowID uint64) []byte

    // DecodeRowKey extracts tableID and rowID from a row key.
    DecodeRowKey(key []byte) (tableID uint32, rowID uint64, err error)

    // EncodeIndexKey encodes an index entry key: t{tableID}i{indexID}{value}{rowID}
    EncodeIndexKey(tableID uint32, indexID uint32, value catalogapi.Value, rowID uint64) []byte

    // DecodeIndexKey extracts components from an index key.
    DecodeIndexKey(key []byte) (tableID uint32, indexID uint32, value catalogapi.Value, rowID uint64, err error)

    // EncodeRowPrefix returns the prefix for all rows of a table: t{tableID}r
    // Used for table scans: Scan(EncodeRowPrefix(tid), EncodeRowPrefixEnd(tid))
    EncodeRowPrefix(tableID uint32) []byte

    // EncodeRowPrefixEnd returns the exclusive end key for row prefix scan.
    EncodeRowPrefixEnd(tableID uint32) []byte

    // EncodeIndexPrefix returns the prefix for all entries of an index.
    EncodeIndexPrefix(tableID uint32, indexID uint32) []byte

    // EncodeIndexPrefixEnd returns the exclusive end key for index prefix scan.
    EncodeIndexPrefixEnd(tableID uint32, indexID uint32) []byte

    // EncodeValue encodes a single value in order-preserving format (for index keys).
    EncodeValue(v catalogapi.Value) []byte
}

// RowCodec encodes/decodes row data (KV values, not keys).
type RowCodec interface {
    // EncodeRow encodes column values into a byte slice.
    // values[i] corresponds to columns[i]. NULL values use IsNull=true.
    EncodeRow(values []catalogapi.Value) []byte

    // DecodeRow decodes a byte slice back into column values.
    // Returns values aligned with the column definitions.
    DecodeRow(data []byte, columns []catalogapi.ColumnDef) ([]catalogapi.Value, error)
}

// Errors
var (
    ErrInvalidKey    = errors.New("encoding: invalid key format")
    ErrInvalidRow    = errors.New("encoding: invalid row format")
    ErrTypeMismatch  = errors.New("encoding: type mismatch")
)
```

#### §3.1.5 Dependencies

- `catalog/api` — for `Value`, `ColumnDef`, `Type` types
- No other dependencies (pure encoding logic)

---

### §3.2 Engine — Table & Index Operations

**Responsibility**: Map SQL row/index CRUD operations to KV operations. This is the bridge between SQL semantics and KV storage.

#### §3.2.1 Table Engine

```go
// In engine/api/api.go

// Row represents a single table row.
type Row struct {
    RowID  uint64
    Values []catalogapi.Value  // aligned with table columns
}

// TableEngine provides row-level CRUD on a table.
type TableEngine interface {
    // Insert inserts a row, assigning a new auto-increment rowID.
    // Returns the assigned rowID.
    // If the table has a primary key column, the PK value is used as rowID
    // (must be TypeInt).
    Insert(table *catalogapi.TableSchema, values []catalogapi.Value) (uint64, error)

    // Get retrieves a single row by rowID.
    Get(table *catalogapi.TableSchema, rowID uint64) (*Row, error)

    // Scan returns an iterator over all rows in the table.
    // Caller must call Close() on the returned iterator.
    Scan(table *catalogapi.TableSchema) (RowIterator, error)

    // Delete deletes a row by rowID.
    Delete(table *catalogapi.TableSchema, rowID uint64) error

    // Update updates a row's values (delete old + insert new with same rowID).
    Update(table *catalogapi.TableSchema, rowID uint64, values []catalogapi.Value) error

    // DropTableData deletes all row data for a table.
    // Uses kvstore.DeleteRange for efficiency.
    DropTableData(tableID uint32) error

    // NextRowID returns the next auto-increment rowID for a table.
    // Stored as a special KV entry: t{tableID}m (metadata).
    NextRowID(tableID uint32) (uint64, error)
}

// RowIterator iterates over table rows.
type RowIterator interface {
    Next() bool
    Row() *Row
    Err() error
    Close()
}
```

#### §3.2.2 Index Engine

```go
// IndexEngine provides secondary index CRUD.
type IndexEngine interface {
    // Insert adds an index entry for a row.
    Insert(index *catalogapi.IndexSchema, tableID uint32, indexID uint32,
           value catalogapi.Value, rowID uint64) error

    // Delete removes an index entry for a row.
    Delete(index *catalogapi.IndexSchema, tableID uint32, indexID uint32,
           value catalogapi.Value, rowID uint64) error

    // Scan returns rowIDs matching a range condition on the indexed column.
    // op is one of: =, <, >, <=, >=
    // For '=': returns all rowIDs where indexedColumn == value
    // For '<': returns all rowIDs where indexedColumn < value
    // etc.
    Scan(tableID uint32, indexID uint32, op CompareOp, value catalogapi.Value) (RowIDIterator, error)

    // ScanRange returns rowIDs where indexedColumn is in [start, end).
    // If start is nil, scan from beginning. If end is nil, scan to end.
    ScanRange(tableID uint32, indexID uint32,
              start *catalogapi.Value, end *catalogapi.Value) (RowIDIterator, error)

    // DropIndexData deletes all entries for an index.
    DropIndexData(tableID uint32, indexID uint32) error
}

// CompareOp represents a comparison operator.
type CompareOp int

const (
    OpEQ CompareOp = iota  // =
    OpNE                    // != (not used in index scan, falls back to table scan)
    OpLT                    // <
    OpLE                    // <=
    OpGT                    // >
    OpGE                    // >=
)

// RowIDIterator iterates over rowIDs from an index scan.
type RowIDIterator interface {
    Next() bool
    RowID() uint64
    Err() error
    Close()
}

// Errors
var (
    ErrRowNotFound    = errors.New("engine: row not found")
    ErrDuplicateKey   = errors.New("engine: duplicate key in unique index")
    ErrTableIDNotSet  = errors.New("engine: table has no assigned ID")
)
```

#### §3.2.3 Table ID Assignment

Each table needs a persistent `tableID` (uint32) for key encoding. Options:

**Chosen approach**: Store a global counter in KV:
```
Key:   _sql:meta:next_table_id
Value: uint32 big-endian
```

On `CREATE TABLE`:
1. Read counter (or start at 1 if not exists)
2. Assign `tableID = counter`
3. Increment counter
4. Store counter + table schema atomically via WriteBatch

The `tableID` is stored in an extended `TableSchema`:
```go
// Addition to catalog/api/api.go:
type TableSchema struct {
    Name       string
    Columns    []ColumnDef
    PrimaryKey string
    TableID    uint32    // NEW: persistent table ID for key encoding
}
```

Similarly, `IndexSchema` gets an `IndexID uint32`.

#### §3.2.4 Auto-Increment RowID

Each table maintains a next-rowID counter:
```
Key:   t{tableID}m  (metadata key, 'm' = 0x6D)
Value: uint64 big-endian (next rowID)
```

On `INSERT`:
1. If table has integer PK column and user provides value → use that as rowID
2. Otherwise → read next-rowID, use it, increment and store

#### §3.2.5 Engine Internal: How Operations Map to KV

```
INSERT INTO users (name, age) VALUES ('Alice', 30)
  → rowID = NextRowID(tableID)                    // read + increment counter
  → key = EncodeRowKey(tableID, rowID)             // t{tid}r{rid}
  → val = EncodeRow([Value{Text:"Alice"}, Value{Int:30}])
  → batch.Put(key, val)
  → for each index on 'users':
      → idxKey = EncodeIndexKey(tableID, indexID, columnValue, rowID)
      → batch.Put(idxKey, []byte{})               // index value is empty
  → batch.Commit()

SELECT name FROM users WHERE age > 18
  → if index on 'age':
      → rowIDs = IndexEngine.Scan(tableID, indexID, OpGT, Value{Int:18})
      → for each rowID: row = TableEngine.Get(tableID, rowID)
  → else:
      → rows = TableEngine.Scan(tableID)
      → filter: row.Values[ageColIdx].Int > 18
  → project: extract 'name' column from each row

DELETE FROM users WHERE id = 5
  → row = TableEngine.Get(tableID, 5)
  → batch.Delete(EncodeRowKey(tableID, 5))
  → for each index: batch.Delete(EncodeIndexKey(..., oldValue, 5))
  → batch.Commit()

UPDATE users SET age = 31 WHERE id = 1
  → oldRow = TableEngine.Get(tableID, 1)
  → batch.Delete(old index entries for changed columns)
  → newRow = merge(oldRow, {age: 31})
  → batch.Put(EncodeRowKey(tableID, 1), EncodeRow(newRow))
  → batch.Put(new index entries for changed columns)
  → batch.Commit()
```

#### §3.2.6 Dependencies

- `encoding/api` — KeyEncoder, RowCodec
- `catalog/api` — TableSchema, IndexSchema, Value, Type
- `kvstore/api` — Store, WriteBatch, Iterator

---

### §3.3 Parser — SQL Text to AST

**Responsibility**: Convert SQL text into a structured AST (Abstract Syntax Tree). Hand-written recursive descent parser — zero external dependencies, full control.

#### §3.3.1 Why Hand-Written

- Zero dependencies (aligns with project philosophy)
- Full control over error messages
- SQL subset is small enough (Phase 1: ~8 statement types)
- SQLite itself uses a hand-written tokenizer (Lemon generates the parser, but tokenizer is manual)
- Easier to extend incrementally

#### §3.3.2 Token Types

```go
// In parser/api/api.go

type TokenType int

const (
    // Literals
    TokInteger    TokenType = iota  // 42
    TokFloat                        // 3.14
    TokString                       // 'hello'
    TokIdent                        // users, name, age

    // Keywords
    TokSelect
    TokFrom
    TokWhere
    TokInsert
    TokInto
    TokValues
    TokDelete
    TokUpdate
    TokSet
    TokCreate
    TokDrop
    TokTable
    TokIndex
    TokOn
    TokAnd
    TokOr
    TokNot
    TokNull
    TokIs
    TokOrder
    TokBy
    TokAsc
    TokDesc
    TokLimit
    TokInt       // INT type keyword
    TokTextKw    // TEXT type keyword
    TokFloatKw   // FLOAT type keyword
    TokBlobKw    // BLOB type keyword
    TokPrimary
    TokKey
    TokUnique
    TokIf
    TokExists

    // Operators
    TokEQ        // =
    TokNE        // != or <>
    TokLT        // <
    TokLE        // <=
    TokGT        // >
    TokGE        // >=
    TokPlus      // +
    TokMinus     // -
    TokStar      // *
    TokComma     // ,
    TokLParen    // (
    TokRParen    // )
    TokSemicolon // ;

    // Special
    TokEOF
    TokIllegal
)

type Token struct {
    Type    TokenType
    Literal string   // raw text
    Pos     int      // byte offset in source
}
```

#### §3.3.3 AST Node Types

```go
// Statement types (top-level)
type Statement interface {
    stmtNode()
}

// CREATE TABLE name (col1 type1, col2 type2, ..., PRIMARY KEY (col))
type CreateTableStmt struct {
    Table      string
    Columns    []ColumnDef
    PrimaryKey string       // optional
    IfNotExists bool
}

// DROP TABLE name
type DropTableStmt struct {
    Table    string
    IfExists bool
}

// CREATE INDEX name ON table (column)
type CreateIndexStmt struct {
    Index    string
    Table    string
    Column   string
    Unique   bool
    IfNotExists bool
}

// DROP INDEX name ON table
type DropIndexStmt struct {
    Index    string
    Table    string
    IfExists bool
}

// INSERT INTO table (col1, col2) VALUES (val1, val2)
type InsertStmt struct {
    Table   string
    Columns []string         // optional — if empty, values align with table columns
    Values  [][]Expr         // multiple rows: VALUES (a,b), (c,d)
}

// SELECT columns FROM table WHERE condition ORDER BY col LIMIT n
type SelectStmt struct {
    Columns  []SelectColumn  // column names or *
    Table    string
    Where    Expr            // nil if no WHERE
    OrderBy  *OrderByClause  // nil if no ORDER BY
    Limit    Expr            // nil if no LIMIT
}

type SelectColumn struct {
    Expr  Expr               // column reference or *
    Alias string             // optional AS alias
}

type OrderByClause struct {
    Column string
    Desc   bool
}

// DELETE FROM table WHERE condition
type DeleteStmt struct {
    Table string
    Where Expr               // nil = delete all rows
}

// UPDATE table SET col1=val1, col2=val2 WHERE condition
type UpdateStmt struct {
    Table       string
    Assignments []Assignment
    Where       Expr         // nil = update all rows
}

type Assignment struct {
    Column string
    Value  Expr
}

// Expression types
type Expr interface {
    exprNode()
}

// Column reference: "age", "users.age"
type ColumnRef struct {
    Table  string  // optional qualifier
    Column string
}

// Literal value: 42, 3.14, 'hello', NULL
type Literal struct {
    Value catalogapi.Value
}

// Binary expression: age > 18, name = 'Alice'
type BinaryExpr struct {
    Left  Expr
    Op    BinaryOp
    Right Expr
}

type BinaryOp int

const (
    BinEQ BinaryOp = iota  // =
    BinNE                   // !=
    BinLT                   // <
    BinLE                   // <=
    BinGT                   // >
    BinGE                   // >=
    BinAnd                  // AND
    BinOr                   // OR
)

// Unary expression: NOT condition, -42
type UnaryExpr struct {
    Op      UnaryOp
    Operand Expr
}

type UnaryOp int

const (
    UnaryNot   UnaryOp = iota  // NOT
    UnaryMinus                  // -
)

// IS NULL / IS NOT NULL
type IsNullExpr struct {
    Expr Expr
    Not  bool   // true = IS NOT NULL
}

// Star expression: SELECT *
type StarExpr struct{}

// ColumnDef in CREATE TABLE (reused from catalog, but parser needs its own for AST)
type ColumnDef struct {
    Name       string
    TypeName   string   // "INT", "TEXT", "FLOAT", "BLOB"
    PrimaryKey bool
}
```

#### §3.3.4 Parser Interface

```go
// Parser parses SQL text into AST statements.
type Parser interface {
    // Parse parses a single SQL statement.
    // Returns the AST and any parse error.
    Parse(sql string) (Statement, error)
}

// Errors
var (
    ErrUnexpectedToken = errors.New("parser: unexpected token")
    ErrUnexpectedEOF   = errors.New("parser: unexpected end of input")
)

// ParseError provides detailed error information.
type ParseError struct {
    Message  string
    Pos      int      // byte offset in source
    Token    Token    // the problematic token
}

func (e *ParseError) Error() string
```

#### §3.3.5 Parser Grammar (Simplified EBNF)

```
statement     = create_table | drop_table | create_index | drop_index
              | insert | select | delete | update

create_table  = "CREATE" "TABLE" ["IF" "NOT" "EXISTS"] ident
                "(" column_def {"," column_def} ["," "PRIMARY" "KEY" "(" ident ")"] ")"

drop_table    = "DROP" "TABLE" ["IF" "EXISTS"] ident

create_index  = "CREATE" ["UNIQUE"] "INDEX" ["IF" "NOT" "EXISTS"] ident
                "ON" ident "(" ident ")"

drop_index    = "DROP" "INDEX" ["IF" "EXISTS"] ident "ON" ident

insert        = "INSERT" "INTO" ident ["(" ident {"," ident} ")"]
                "VALUES" "(" expr {"," expr} ")" {"," "(" expr {"," expr} ")"}

select        = "SELECT" select_columns "FROM" ident
                ["WHERE" expr] ["ORDER" "BY" ident ["ASC"|"DESC"]] ["LIMIT" expr]

delete        = "DELETE" "FROM" ident ["WHERE" expr]

update        = "UPDATE" ident "SET" assignment {"," assignment} ["WHERE" expr]

column_def    = ident type_name ["PRIMARY" "KEY"]
type_name     = "INT" | "TEXT" | "FLOAT" | "BLOB"
assignment    = ident "=" expr
select_columns = "*" | expr ["AS" ident] {"," expr ["AS" ident]}

expr          = or_expr
or_expr       = and_expr {"OR" and_expr}
and_expr      = not_expr {"AND" not_expr}
not_expr      = ["NOT"] compare_expr
compare_expr  = primary [("=" | "!=" | "<>" | "<" | "<=" | ">" | ">=") primary]
              | primary "IS" ["NOT"] "NULL"
primary       = literal | ident ["." ident] | "(" expr ")" | "-" primary | "*"
literal       = INTEGER | FLOAT | STRING | "NULL"
```

#### §3.3.6 Dependencies

- `catalog/api` — for `Value`, `Type` types (used in Literal AST nodes)
- No other dependencies

---

### §3.4 Planner — AST to Execution Plan

**Responsibility**: Convert a parsed AST into an execution plan. Simple heuristic-based planning — no cost model, no statistics.

#### §3.4.1 Plan Types

```go
// In planner/api/api.go

// Plan represents an execution plan for a SQL statement.
type Plan interface {
    planNode()
}

// DDL plans (pass-through — executed directly by catalog)
type CreateTablePlan struct {
    Schema catalogapi.TableSchema
}

type DropTablePlan struct {
    Table    string
    IfExists bool
}

type CreateIndexPlan struct {
    Schema   catalogapi.IndexSchema
    IfNotExists bool
}

type DropIndexPlan struct {
    Index    string
    Table    string
    IfExists bool
}

// DML plans

type InsertPlan struct {
    Table   *catalogapi.TableSchema
    Rows    [][]catalogapi.Value  // resolved values, aligned with columns
}

type DeletePlan struct {
    Table   *catalogapi.TableSchema
    Scan    ScanPlan              // how to find rows to delete
}

type UpdatePlan struct {
    Table       *catalogapi.TableSchema
    Assignments map[int]Expr      // columnIndex → new value expression
    Scan        ScanPlan          // how to find rows to update
}

type SelectPlan struct {
    Table      *catalogapi.TableSchema
    Scan       ScanPlan           // how to scan rows
    Columns    []int              // column indices to project (-1 = all)
    Filter     Expr               // residual filter (conditions not handled by index)
    OrderBy    *OrderByPlan       // nil if no ORDER BY
    Limit      int                // -1 if no LIMIT
}

// ScanPlan describes how to find rows.
type ScanPlan interface {
    scanNode()
}

// TableScanPlan: full table scan (no index available).
type TableScanPlan struct {
    TableID uint32
    Filter  Expr   // filter applied during scan
}

// IndexScanPlan: use an index to narrow the scan.
type IndexScanPlan struct {
    TableID  uint32
    IndexID  uint32
    Index    *catalogapi.IndexSchema
    Op       engineapi.CompareOp
    Value    catalogapi.Value     // comparison value
    // After index scan, remaining filter (if WHERE has AND conditions
    // and only one was handled by index)
    ResidualFilter Expr
}

type OrderByPlan struct {
    ColumnIndex int
    Desc        bool
}
```

#### §3.4.2 Planning Algorithm

```
Plan(stmt, catalog):
  match stmt:
    CreateTableStmt → validate columns → CreateTablePlan
    DropTableStmt   → DropTablePlan
    CreateIndexStmt → validate table+column exist → CreateIndexPlan
    DropIndexStmt   → DropIndexPlan

    InsertStmt:
      1. Resolve table via catalog.GetTable()
      2. Validate column count matches
      3. Type-check values against column types
      4. → InsertPlan{table, resolvedValues}

    SelectStmt:
      1. Resolve table via catalog.GetTable()
      2. Resolve column references → column indices
      3. Analyze WHERE clause:
         a. Extract simple conditions: "column OP literal"
         b. For each condition, check if an index exists on that column
         c. If index found → IndexScanPlan (best index wins)
         d. If no index → TableScanPlan with filter
      4. Resolve ORDER BY → column index
      5. Resolve LIMIT → integer
      6. → SelectPlan

    DeleteStmt:
      1. Resolve table
      2. Plan scan (same as SELECT WHERE analysis)
      3. → DeletePlan{table, scan}

    UpdateStmt:
      1. Resolve table
      2. Resolve assignments → column indices
      3. Plan scan (same as SELECT WHERE analysis)
      4. → UpdatePlan{table, assignments, scan}
```

#### §3.4.3 Index Selection Heuristic

Simple rule (no cost model):

```
For WHERE clause:
  1. Decompose into AND-connected conditions
  2. For each condition of form "column OP literal":
     - Check catalog.GetIndexByColumn(table, column)
     - If index exists AND op is one of {=, <, >, <=, >=}:
       → candidate index scan
  3. Priority: '=' index scan > range index scan > table scan
  4. Only ONE index is used per query (no index intersection)
  5. Remaining conditions become residual filter
```

#### §3.4.4 Planner Interface

```go
type Planner interface {
    // Plan converts a parsed AST statement into an execution plan.
    Plan(stmt parserapi.Statement) (Plan, error)
}

// Errors
var (
    ErrTableNotFound  = errors.New("planner: table not found")
    ErrColumnNotFound = errors.New("planner: column not found")
    ErrTypeMismatch   = errors.New("planner: type mismatch in expression")
    ErrInvalidPlan    = errors.New("planner: cannot create valid plan")
)
```

#### §3.4.5 Dependencies

- `parser/api` — AST types (Statement, Expr, etc.)
- `catalog/api` — CatalogManager, TableSchema, IndexSchema
- `engine/api` — CompareOp, ScanPlan types

---

### §3.5 Executor — Execute Plan via KV Operations

**Responsibility**: Take an execution plan and execute it, producing results. This is a tree-walking interpreter — no bytecode, no VM.

#### §3.5.1 Interface

```go
// In executor/api/api.go

// Result represents the result of executing a SQL statement.
type Result struct {
    // For SELECT: column names and rows
    Columns []string
    Rows    [][]catalogapi.Value

    // For INSERT/UPDATE/DELETE: affected row count
    RowsAffected int64

    // For DDL: success indicator
    // (no additional data)
}

// Executor executes SQL plans.
type Executor interface {
    // Execute executes a plan and returns the result.
    Execute(plan plannerapi.Plan) (*Result, error)
}

// Errors
var (
    ErrExecFailed = errors.New("executor: execution failed")
)
```

#### §3.5.2 Execution Logic

```
Execute(plan):
  match plan:
    CreateTablePlan:
      1. Assign tableID (read + increment global counter)
      2. catalog.CreateTable(schema with tableID)
      3. → Result{RowsAffected: 0}

    DropTablePlan:
      1. Get table schema (for tableID)
      2. engine.DropTableData(tableID)      // delete all row data
      3. Drop all indexes: engine.DropIndexData(tableID, indexID) for each
      4. catalog.DropTable(name)
      5. → Result{RowsAffected: 0}

    InsertPlan:
      1. batch = store.NewWriteBatch()
      2. For each row:
         a. rowID = engine.NextRowID(tableID) or PK value
         b. batch.Put(EncodeRowKey(tableID, rowID), EncodeRow(values))
         c. For each index: batch.Put(EncodeIndexKey(...), []byte{})
         d. Increment rowID counter
      3. batch.Commit()
      4. → Result{RowsAffected: len(rows)}

    SelectPlan:
      1. Scan rows (via TableScan or IndexScan)
      2. Apply residual filter
      3. Project selected columns
      4. If ORDER BY: sort in memory
      5. If LIMIT: truncate
      6. → Result{Columns, Rows}

    DeletePlan:
      1. Scan rows to delete (same as SELECT scan)
      2. batch = store.NewWriteBatch()
      3. For each row:
         a. batch.Delete(EncodeRowKey(tableID, rowID))
         b. For each index: batch.Delete(EncodeIndexKey(...))
      4. batch.Commit()
      5. → Result{RowsAffected: count}

    UpdatePlan:
      1. Scan rows to update
      2. batch = store.NewWriteBatch()
      3. For each row:
         a. Delete old index entries for changed columns
         b. Apply assignments → new values
         c. batch.Put(EncodeRowKey(tableID, rowID), EncodeRow(newValues))
         d. Insert new index entries for changed columns
      4. batch.Commit()
      5. → Result{RowsAffected: count}
```

#### §3.5.3 Expression Evaluation

```go
// evalExpr evaluates an expression against a row.
func evalExpr(expr Expr, row *Row, columns []ColumnDef) (Value, error)

// For BinaryExpr:
//   Evaluate left and right, then apply operator
//   AND/OR use short-circuit evaluation
//   Comparison: use Value.Compare() method

// For ColumnRef:
//   Look up column index in table schema
//   Return row.Values[columnIndex]

// For Literal:
//   Return the literal value directly

// For IsNullExpr:
//   Evaluate inner expr, check IsNull flag

// For UnaryExpr:
//   NOT: evaluate operand, negate boolean result
//   -: evaluate operand, negate numeric value
```

#### §3.5.4 Dependencies

- `planner/api` — Plan types
- `engine/api` — TableEngine, IndexEngine
- `catalog/api` — CatalogManager, TableSchema, Value
- `encoding/api` — KeyEncoder, RowCodec (used indirectly via engine)

---

### §3.6 SQL Composition Root — `sql.go`

**Responsibility**: Wire all SQL components together. Provide the user-facing API.

#### §3.6.1 User API

```go
// In internal/sql/sql.go

// DB represents a SQL database backed by a go-fast-kv store.
type DB struct {
    store    kvstoreapi.Store
    catalog  catalogapi.CatalogManager
    encoder  encodingapi.KeyEncoder
    codec    encodingapi.RowCodec
    table    engineapi.TableEngine
    index    engineapi.IndexEngine
    parser   parserapi.Parser
    planner  plannerapi.Planner
    executor executorapi.Executor
}

// Open creates a new SQL database using the given KV store.
func Open(store kvstoreapi.Store) (*DB, error)

// Exec executes a SQL statement that does not return rows.
// Use for INSERT, UPDATE, DELETE, CREATE TABLE, DROP TABLE, etc.
func (db *DB) Exec(sql string) (*Result, error)

// Query executes a SQL query that returns rows.
// Use for SELECT.
func (db *DB) Query(sql string) (*Result, error)

// Close closes the SQL layer. Does NOT close the underlying KV store.
func (db *DB) Close() error
```

#### §3.6.2 Wiring

```go
func Open(store kvstoreapi.Store) (*DB, error) {
    cat := catalog.New(store)
    enc := encoding.NewKeyEncoder()
    codec := encoding.NewRowCodec()
    tbl := engine.NewTableEngine(store, enc, codec)
    idx := engine.NewIndexEngine(store, enc)
    p := parser.New()
    pl := planner.New(cat)
    ex := executor.New(cat, tbl, idx, enc, codec, store)

    return &DB{
        store:    store,
        catalog:  cat,
        encoder:  enc,
        codec:    codec,
        table:    tbl,
        index:    idx,
        parser:   p,
        planner:  pl,
        executor: ex,
    }, nil
}

func (db *DB) Exec(sql string) (*Result, error) {
    stmt, err := db.parser.Parse(sql)
    if err != nil { return nil, err }

    plan, err := db.planner.Plan(stmt)
    if err != nil { return nil, err }

    return db.executor.Execute(plan)
}

// Query is the same as Exec for now.
// In the future, Query could return a streaming iterator instead of materialized rows.
func (db *DB) Query(sql string) (*Result, error) {
    return db.Exec(sql)
}
```

---

## §4 Design Decisions

### 4.1 Tree-Walking Interpreter vs VDBE Bytecode

**Chosen: Tree-walking interpreter.**

SQLite compiles SQL to VDBE bytecode for good reasons (prepare-once-execute-many, introspection). But for Phase 1:
- Our SQL subset is small — no need for a full VM
- Tree-walking is simpler to implement and debug
- We can always add a bytecode layer later if performance matters
- The plan objects ARE the "compiled form" — they can be cached

### 4.2 Hand-Written Parser vs Parser Generator

**Chosen: Hand-written recursive descent.**

- Zero external dependencies
- Full control over error messages
- SQL subset is small enough (~8 statement types)
- Easier to extend incrementally
- SQLite's tokenizer is also hand-written

### 4.3 Key Encoding: TiDB-Style Prefix

**Chosen: `t{tableID}r{rowID}` / `t{tableID}i{indexID}{value}{rowID}`**

- Well-proven pattern (TiDB, CockroachDB)
- Natural prefix scan support (all rows of a table share prefix)
- Fixed-size row keys (14 bytes) — compact and fast
- Order-preserving value encoding for index keys

### 4.4 Single Index Per Query

**Chosen: At most one index per query.**

- Keeps planner simple
- Index intersection is complex and rarely needed for OLTP
- Can be extended later

### 4.5 In-Memory Sort for ORDER BY

**Chosen: Materialize all result rows, then sort in memory.**

- Simple to implement
- Fine for small-to-medium result sets
- For large results, can add external sort later

### 4.6 WriteBatch = SQL Transaction

**Chosen: Each DML statement is one WriteBatch = one atomic operation.**

- INSERT/UPDATE/DELETE each create a WriteBatch
- All row + index mutations go into the same batch
- Batch.Commit() = atomic commit
- No explicit BEGIN/COMMIT/ROLLBACK in Phase 1

---

## §5 Wire Formats Summary

### Row Key
```
[0]      0x74 ('t')
[1:5]    uint32 tableID (big-endian)
[5]      0x72 ('r')
[6:14]   uint64 rowID (big-endian)
```

### Index Key
```
[0]      0x74 ('t')
[1:5]    uint32 tableID (big-endian)
[5]      0x69 ('i')
[6:10]   uint32 indexID (big-endian)
[10:..]  encoded column value (order-preserving, variable length)
[..:+8]  uint64 rowID (big-endian)
```

### Table Metadata Key
```
[0]      0x74 ('t')
[1:5]    uint32 tableID (big-endian)
[5]      0x6D ('m')
```

### Value Encoding (Order-Preserving)
```
NULL:   [0x00]
Int:    [0x02] [8B: int64 XOR 0x8000000000000000, big-endian]
Float:  [0x03] [8B: transformed IEEE754, big-endian]
Text:   [0x04] [escaped bytes] [0x00 0x00]
Blob:   [0x05] [escaped bytes] [0x00 0x00]
```

### Row Value (Non-Order-Preserving)
```
[0:2]    uint16 column count
[2:2+N]  null bitmap (ceil(count/8) bytes)
[...]    column values:
         Int:   8B int64 big-endian
         Float: 8B float64 IEEE754
         Text:  4B length + UTF-8 bytes
         Blob:  4B length + raw bytes
```

---

## §6 Test Strategy

Each module gets its own test file. Tests are written before or alongside implementation.

### §6.1 Encoding Tests
- `TestEncodeRowKey` — encode/decode roundtrip
- `TestEncodeIndexKey` — encode/decode roundtrip
- `TestValueEncoding_IntOrder` — encoded ints sort correctly (negative, zero, positive)
- `TestValueEncoding_FloatOrder` — encoded floats sort correctly
- `TestValueEncoding_TextOrder` — encoded strings sort lexicographically
- `TestValueEncoding_NullFirst` — NULL sorts before all values
- `TestValueEncoding_TypeOrder` — NULL < Int < Float < Text < Blob
- `TestRowCodec_Roundtrip` — encode/decode row with all types
- `TestRowCodec_NullHandling` — NULL values in rows

### §6.2 Engine Tests
- `TestTableEngine_InsertGet` — insert a row, get it back
- `TestTableEngine_Scan` — insert multiple rows, scan all
- `TestTableEngine_Delete` — insert then delete
- `TestTableEngine_Update` — insert then update
- `TestTableEngine_AutoIncrement` — rowIDs increase monotonically
- `TestIndexEngine_InsertScan` — insert index entries, scan by value
- `TestIndexEngine_UniqueViolation` — duplicate key in unique index
- `TestIndexEngine_Delete` — insert then delete index entry
- `TestIndexEngine_RangeScan` — scan with <, >, <=, >= operators

### §6.3 Parser Tests
- `TestParse_CreateTable` — various CREATE TABLE forms
- `TestParse_DropTable` — with/without IF EXISTS
- `TestParse_Insert` — single row, multiple rows
- `TestParse_Select` — simple, with WHERE, ORDER BY, LIMIT
- `TestParse_Delete` — with/without WHERE
- `TestParse_Update` — SET multiple columns
- `TestParse_CreateIndex` — with/without UNIQUE
- `TestParse_Expressions` — AND, OR, NOT, IS NULL, comparisons
- `TestParse_Errors` — malformed SQL produces clear errors

### §6.4 Planner Tests
- `TestPlan_SelectTableScan` — no index → table scan
- `TestPlan_SelectIndexScan` — index on WHERE column → index scan
- `TestPlan_SelectResidualFilter` — index handles one condition, filter handles rest
- `TestPlan_Insert` — values resolved and type-checked
- `TestPlan_Delete` — scan plan for WHERE clause
- `TestPlan_Update` — assignments resolved

### §6.5 Executor Tests
- `TestExec_CreateTableInsertSelect` — full lifecycle
- `TestExec_IndexScan` — create index, verify it's used
- `TestExec_DeleteWhere` — delete specific rows
- `TestExec_UpdateWhere` — update specific rows
- `TestExec_OrderByLimit` — sorting and limiting
- `TestExec_NullHandling` — NULL in WHERE, INSERT, SELECT

### §6.6 Integration Tests (sql.go level)
- `TestSQL_FullLifecycle` — CREATE TABLE → INSERT → SELECT → UPDATE → DELETE → DROP TABLE
- `TestSQL_SecondaryIndex` — CREATE INDEX → INSERT → SELECT WHERE (uses index)
- `TestSQL_ErrorHandling` — table not found, column not found, type mismatch
- `TestSQL_MultipleRows` — INSERT multiple rows, verify SELECT returns all
- `TestSQL_NullValues` — INSERT with NULL, SELECT WHERE IS NULL

---

## §7 Implementation Order

Strict bottom-up, following dependency order. Each phase = one fork.

```
Phase 1: encoding     ← standalone, pure algorithm
Phase 2: engine       ← depends on encoding + catalog + kvstore
Phase 3: parser       ← standalone
Phase 4: planner      ← depends on parser AST + catalog + engine API
Phase 5: executor     ← depends on planner + engine
Phase 6: sql.go       ← composition root + integration tests
```

**Estimated size**: ~3,000-4,000 lines total (production code, excluding tests)

| Module | Estimated Lines |
|--------|----------------|
| encoding | ~400 |
| engine | ~600 |
| parser | ~800 |
| planner | ~500 |
| executor | ~500 |
| sql.go | ~100 |
| **Total** | **~2,900** |

---

## §8 Catalog Extension

The existing `sql/catalog` module needs minor extensions:

1. Add `TableID uint32` to `TableSchema`
2. Add `IndexID uint32` to `IndexSchema`
3. Add global counter management for table/index ID assignment

These changes are backward-compatible — existing tests should still pass.

---

## §9 Future Extensions (Post Phase 1)

| Feature | Complexity | Value |
|---------|-----------|-------|
| `JOIN` (INNER) | Medium | High |
| `GROUP BY` / aggregates (COUNT, SUM, AVG) | Medium | High |
| `ALTER TABLE ADD COLUMN` | Low | Medium |
| `LIKE` / `IN` / `BETWEEN` | Low | Medium |
| `INSERT ... SELECT` | Low | Medium |
| Prepared statements (parse once, execute many) | Medium | High |
| `BEGIN` / `COMMIT` / `ROLLBACK` (explicit transactions) | Medium | High |
| Composite indexes (multi-column) | High | Medium |
| Query plan caching | Low | Medium |
| Streaming result iterator (instead of materialized) | Medium | Medium |

---

---

## §10.5 User-Defined Functions (UDF) — MVP Implemented

**Status**: MVP Complete (feature/postgres-functions branch)

### Syntax

```sql
CREATE FUNCTION name(arg1 type1, arg2 type2) RETURNS type AS $$
    expression
$$ LANGUAGE SQL;

DROP FUNCTION name;
```

### Implementation

| Component | Status | Commit |
|-----------|--------|--------|
| Lexer: `$$...$$` dollar-quoted strings | ✅ | 41ff17a |
| Parser: `CREATE FUNCTION` / `DROP FUNCTION` | ✅ | 41ff17a |
| AST: `CreateFunctionStmt`, `FunctionCallExpr` | ✅ | 41ff17a |
| Executor: `FunctionRegistry` | ✅ | 478beda |
| Executor: `evalFunctionCall()` | ✅ | 478beda |
| Planner: `CreateFunctionPlan` | ✅ | 935fddf |
| Multi-argument function calls | ✅ | 69df681 |

### Architecture

```
Parser → CreateFunctionStmt
       → Planner → CreateFunctionPlan
       → Executor → FunctionRegistry.Register()
```

```
Parser → FunctionCallExpr
       → Planner → FunctionCallPlan
       → Executor → FunctionRegistry.Get()
       → evalFunctionCall() → returns "body evaluation not yet implemented"
```

### Limitations (MVP)

1. **No persistent function storage** — Functions exist only in-memory (`FunctionRegistry`)
2. **No body evaluation** — Function calls return error "body evaluation not yet implemented"
3. **No plpgsql** — Only SQL scalar expressions in function body

### Future Enhancements

1. **Function body evaluation** — Parse and evaluate function body expression with bound arguments
2. **Persistent function storage** — Store functions in catalog (`FunctionSchema`)
3. **Built-in functions as UDFs** — Migrate hardcoded built-in functions to UDF registry
4. **plpgsql support** — `BEGIN...END` blocks, `DECLARE`, `IF`/`CASE`/`LOOP`


## §10 Review Resolutions (v1.1)

Review identified 5 CRITICAL, 10 WARNING, 10 SUGGESTION issues. Resolutions:

### Must-Fix (addressed in implementation)

| ID | Issue | Resolution |
|----|-------|------------|
| **C1** | Catalog API lacks atomic tableID assignment | Engine layer manages ID assignment with in-memory counter (initialized from KV on Open, persisted in WriteBatch alongside row data). Catalog gets `TableID`/`IndexID` fields added to schemas (JSON backward-compatible). |
| **C3** | DecodeIndexKey boundary ambiguous for variable-length values | Decode algorithm: read type tag at [10], fixed types (NULL=1B, Int/Float=9B) → known offset; Text/Blob → scan for unescaped 0x00 0x00 terminator, rowID follows. |
| **C4** | Race on auto-increment rowID | **Single-writer model**: SQL `DB` struct holds a `sync.Mutex` serializing all DML. Matches embedded use case. In-memory per-table counters (atomic uint64), persisted on each WriteBatch. |
| **C5** | DROP TABLE not atomic | Use single WriteBatch: DeleteRange(rows) + DeleteRange(each index) + Delete(catalog keys) + Delete(metadata). Extend catalog to accept batch parameter internally. |
| **W1** | NULL dual representation | **Canonical rule**: `IsNull == true` means NULL regardless of `Type` field. All encoders check `IsNull` first. |
| **W4** | No Value.Compare() | Add `CompareValues(a, b Value) int` to `encoding/api`. Returns -1/0/+1. NULL < any non-NULL. Same-type comparison. Cross-type returns error. |
| **W5** | No ListIndexes in catalog | Add `ListIndexes(tableName string) ([]*IndexSchema, error)` to CatalogManager. |
| **W6** | INSERT duplicate PK not checked | Engine.Insert checks `Get(rowKey)` before Put. Returns `ErrDuplicateKey` if exists. |
| **W3** | Prefix-end computation undocumented | Use increment-last-byte: `t{tableID}r` → `t{tableID}s`. Safe because discriminator bytes (m,r,i) are non-adjacent in ASCII. |
| **W7** | UPDATE SET expressions | Phase 1: SET values are **literals only** (no `age = age + 1`). Documented limitation. |
| **W8** | DELETE without WHERE inefficient | Special-case: no WHERE → use DeleteRange for rows + each index range. |
| **W10** | ORDER BY no memory limit | Add `MaxSortRows` config (default 100,000). Error if exceeded. |

### Accepted Suggestions
- **S2**: Add `NOT NULL` constraint to column definition
- **S5**: Add `EXPLAIN` statement (returns plan as text)
- **S8**: Move `CompareOp` to `encoding/api` (shared types)
- **S9**: Parser normalizes identifiers to uppercase

### Deferred
- S10 (BulkLoad), S7 (streaming results) — post Phase 1
