# SQL Language Reference

This document describes the SQL language supported by the query engine.

## Table of Contents

1. [Data Types](#data-types)
2. [DDL (Data Definition Language)](#ddl-data-definition-language)
3. [DML (Data Manipulation Language)](#dml-data-manipulation-language)
4. [SELECT Statement](#select-statement)
5. [WHERE Clause](#where-clause)
6. [JOIN](#join)
7. [GROUP BY and Aggregates](#group-by-and-aggregates)
8. [HAVING Clause](#having-clause)
9. [ORDER BY](#order-by)
10. [LIMIT and OFFSET](#limit-and-offset)
11. [Set Operations (UNION, INTERSECT, EXCEPT)](#set-operations-union-intersect-except)
12. [Subqueries](#subqueries)
13. [INSERT SELECT](#insert-select)
14. [Scalar Expressions](#scalar-expressions)
15. [NULL Handling](#null-handling)

---

## Data Types

| Type | Description |
|------|-------------|
| `INT` | 64-bit signed integer |
| `TEXT` | UTF-8 string |
| `FLOAT` | 64-bit floating point |
| `BLOB` | Binary data |

---

## DDL (Data Definition Language)

### CREATE TABLE

Create a new table with columns.

```sql
CREATE TABLE table_name (
    column_name TYPE [PRIMARY KEY],
    column_name TYPE,
    ...
)
```

**Example:**
```sql
CREATE TABLE users (
    id INT PRIMARY KEY,
    name TEXT,
    age INT
)
```

### CREATE TABLE IF NOT EXISTS

Creates a table only if it doesn't already exist.

```sql
CREATE TABLE IF NOT EXISTS users (
    id INT PRIMARY KEY,
    name TEXT
)
```

### DROP TABLE

Removes a table from the database.

```sql
DROP TABLE table_name
```

### DROP TABLE IF EXISTS

Drops a table only if it exists (no error if it doesn't).

```sql
DROP TABLE IF EXISTS table_name
```

### CREATE INDEX

Creates an index on a column for faster lookups.

```sql
CREATE INDEX idx_name ON table_name (column_name)
```

### DROP INDEX

Removes an index.

```sql
DROP INDEX idx_name
```

---

## DML (Data Manipulation Language)

### INSERT

Insert rows into a table.

```sql
INSERT INTO table_name VALUES (value1, value2, ...)
INSERT INTO table_name (col1, col2) VALUES (val1, val2)
```

**Examples:**
```sql
INSERT INTO users VALUES (1, 'Alice', 30)
INSERT INTO users (id, name) VALUES (2, 'Bob')
INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')
```

### UPDATE

Update existing rows.

```sql
UPDATE table_name SET col1 = value1, col2 = value2 WHERE condition
```

**Example:**
```sql
UPDATE users SET age = 31 WHERE id = 1
```

### DELETE

Delete rows from a table.

```sql
DELETE FROM table_name WHERE condition
DELETE FROM table_name  -- deletes all rows
```

**Examples:**
```sql
DELETE FROM users WHERE id = 1
DELETE FROM users
```

---

## SELECT Statement

### Basic SELECT

```sql
SELECT column1, column2, ... FROM table_name
SELECT * FROM table_name  -- all columns
```

**Examples:**
```sql
SELECT name, age FROM users
SELECT * FROM users
```

### SELECT Without FROM

Evaluate expressions without a table.

```sql
SELECT 1 + 1
-- Returns: 2
```

### DISTINCT

Remove duplicate rows from results.

```sql
SELECT DISTINCT column FROM table_name
```

**Example:**
```sql
-- Table: users with names ['Alice', 'Bob', 'Alice', 'Bob', 'Charlie']
SELECT DISTINCT name FROM users
-- Returns: Alice, Bob, Charlie
```

### Expression Columns

```sql
SELECT column1 + column2 FROM table_name
SELECT (price * quantity) AS total FROM orders
```

---

## WHERE Clause

Filter rows based on conditions.

### Comparison Operators

| Operator | Description |
|----------|-------------|
| `=` | Equal |
| `<>` or `!=` | Not equal |
| `<` | Less than |
| `>` | Greater than |
| `<=` | Less than or equal |
| `>=` | Greater than or equal |

**Example:**
```sql
SELECT * FROM users WHERE age > 18
SELECT * FROM users WHERE name = 'Alice'
```

### AND / OR

Combine multiple conditions.

```sql
SELECT * FROM users WHERE age > 18 AND name = 'Alice'
SELECT * FROM users WHERE age < 20 OR age > 50
```

### NOT

Negate a condition.

```sql
SELECT * FROM users WHERE NOT age > 18
SELECT * FROM users WHERE NOT name = 'Alice'
```

### IN

Check if a value is in a list.

```sql
SELECT * FROM users WHERE id IN (1, 2, 3)
SELECT * FROM users WHERE name IN ('Alice', 'Bob')
```

### NOT IN

Check if a value is not in a list.

```sql
SELECT * FROM users WHERE id NOT IN (1, 2, 3)
```

### BETWEEN

Check if a value is within a range (inclusive).

```sql
SELECT * FROM users WHERE age BETWEEN 18 AND 30
```

**Equivalent to:**
```sql
SELECT * FROM users WHERE age >= 18 AND age <= 30
```

### LIKE

Pattern matching for strings.

| Pattern | Description |
|---------|-------------|
| `%` | Matches any sequence of characters |
| `_` | Matches any single character |

```sql
SELECT * FROM users WHERE name LIKE 'Alice'
SELECT * FROM users WHERE email LIKE '%@example.com'
SELECT * FROM users WHERE name LIKE '%ice'
SELECT * FROM users WHERE name LIKE 'A%'
```

### IS NULL / IS NOT NULL

Check for NULL values.

```sql
SELECT * FROM users WHERE age IS NULL
SELECT * FROM users WHERE age IS NOT NULL
```

---

## JOIN

Combines rows from two tables based on a related column.

### INNER JOIN

Returns rows when there is a match in both tables.

```sql
SELECT t1.col1, t2.col2 
FROM table1 t1 
JOIN table2 t2 ON t1.id = t2.ref_id
```

**Example:**
```sql
-- users: id=1 alice, id=2 bob, id=3 carol
-- orders: user_id=1, user_id=1, user_id=3
SELECT users.name, orders.amount 
FROM users 
JOIN orders ON users.id = orders.user_id

-- Returns:
-- alice, 100
-- alice, 200
-- carol, 50
-- (bob has no orders, so not included)
```

---

## GROUP BY and Aggregates

Group rows and compute aggregate values.

### Aggregate Functions

| Function | Description |
|----------|-------------|
| `COUNT(*)` | Count of rows |
| `COUNT(col)` | Count of non-NULL values |
| `SUM(col)` | Sum of values |
| `AVG(col)` | Average of values |
| `MIN(col)` | Minimum value |
| `MAX(col)` | Maximum value |

### Basic GROUP BY

```sql
SELECT column, AGG_FUNC(column) 
FROM table_name 
GROUP BY column
```

**Example:**
```sql
-- orders: user_id, amount
-- (1, 100), (1, 200), (2, 50), (2, 75)
SELECT user_id, COUNT(*), SUM(amount) 
FROM orders 
GROUP BY user_id

-- Returns:
-- 1, 2, 300
-- 2, 2, 125
```

### GROUP BY with WHERE

Filter rows before grouping.

```sql
SELECT user_id, SUM(amount) 
FROM orders 
WHERE amount > 50 
GROUP BY user_id
```

### Scalar Aggregates

When aggregates are used without GROUP BY, a single aggregate value is returned.

```sql
SELECT COUNT(*) FROM users
SELECT SUM(amount) FROM orders
SELECT AVG(age) FROM users
```

---

## HAVING Clause

Filter groups after aggregation (WHERE filters rows before aggregation).

```sql
SELECT user_id, SUM(amount) 
FROM orders 
GROUP BY user_id 
HAVING SUM(amount) > 100
```

**Example:**
```sql
-- Find users with total orders over 100
SELECT user_id, SUM(amount) AS total 
FROM orders 
GROUP BY user_id 
HAVING SUM(amount) > 100
```

---

## ORDER BY

Sort results by one or more columns.

```sql
SELECT * FROM table_name ORDER BY column [ASC|DESC]
SELECT * FROM table_name ORDER BY col1 ASC, col2 DESC
```

| Keyword | Description |
|---------|-------------|
| `ASC` | Ascending order (default) |
| `DESC` | Descending order |

**Examples:**
```sql
-- Sort by age ascending (default)
SELECT * FROM users ORDER BY age

-- Sort by age descending
SELECT * FROM users ORDER BY age DESC

-- Sort by multiple columns
SELECT * FROM users ORDER BY age DESC, name ASC
```

---

## LIMIT and OFFSET

### LIMIT

Restrict the number of rows returned.

```sql
SELECT * FROM users ORDER BY age LIMIT 10
```

### OFFSET

Skip a number of rows before returning results.

```sql
SELECT * FROM users ORDER BY age LIMIT 5 OFFSET 10
```

### LIMIT with OFFSET (Pagination)

```sql
-- Page 1 (rows 1-5)
SELECT * FROM users ORDER BY id LIMIT 5 OFFSET 0

-- Page 2 (rows 6-10)
SELECT * FROM users ORDER BY id LIMIT 5 OFFSET 5

-- Page 3 (rows 11-15)
SELECT * FROM users ORDER BY id LIMIT 5 OFFSET 10
```

---

## Set Operations (UNION, INTERSECT, EXCEPT)

Combine results from multiple SELECT statements.

### UNION

Returns all rows from both queries, with duplicates removed.

```sql
SELECT col FROM table1
UNION
SELECT col FROM table2
```

**Example:**
```sql
SELECT 1 UNION SELECT 2
-- Returns: 1, 2
```

### UNION ALL

Returns all rows from both queries, including duplicates.

```sql
SELECT col FROM table1
UNION ALL
SELECT col FROM table2
```

**Example:**
```sql
SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
-- Returns: 1, 1, 1 (no deduplication)
```

### INTERSECT

Returns rows that appear in BOTH queries (duplicates removed).

```sql
SELECT col FROM table1
INTERSECT
SELECT col FROM table2
```

**Examples:**
```sql
SELECT 1 INTERSECT SELECT 1
-- Returns: 1

SELECT 1 INTERSECT SELECT 2
-- Returns: (empty)

SELECT NULL INTERSECT SELECT NULL
-- Returns: NULL (two NULLs are considered equal)
```

### EXCEPT

Returns rows from the first query that do NOT appear in the second query.

```sql
SELECT col FROM table1
EXCEPT
SELECT col FROM table2
```

**Examples:**
```sql
SELECT 1 EXCEPT SELECT 2
-- Returns: 1

SELECT 1 EXCEPT SELECT 1
-- Returns: (empty)
```

### Combining Set Operations

Set operations are right-associative.

```sql
-- Evaluated as: SELECT 1 UNION (SELECT 2 INTERSECT SELECT 2)
SELECT 1 UNION SELECT 2 INTERSECT SELECT 2
-- Returns: 1, 2

-- Evaluated as: SELECT 1 UNION (SELECT 2 EXCEPT SELECT 1)
SELECT 1 UNION SELECT 2 EXCEPT SELECT 1
-- Returns: 1, 2
```

---

## Subqueries

Queries nested inside other queries.

### Subquery in WHERE (IN / NOT IN)

```sql
-- Find users who have orders
SELECT * FROM users 
WHERE id IN (SELECT user_id FROM orders)

-- Find users who don't have orders
SELECT * FROM users 
WHERE id NOT IN (SELECT user_id FROM orders)
```

### Scalar Subquery

A subquery that returns a single value.

```sql
SELECT name, 
       (SELECT SUM(amount) FROM orders WHERE orders.user_id = users.id) AS total
FROM users
```

### Correlated Subquery

A subquery that references columns from the outer query.

```sql
-- Find users with more than 1 order
SELECT name FROM users 
WHERE (SELECT COUNT(*) FROM orders WHERE orders.user_id = users.id) > 1

-- Find users who also appear in products table
SELECT name FROM users 
WHERE (SELECT COUNT(*) FROM products WHERE products.id = users.id) > 0
```

### EXISTS / NOT EXISTS

Check if a subquery returns any rows.

```sql
-- Users who have at least one order
SELECT name FROM users 
WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)

-- Users who have no orders
SELECT name FROM users 
WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)
```

### Derived Tables (Subquery in FROM)

Subqueries in the FROM clause.

```sql
SELECT * FROM (
    SELECT user_id, SUM(amount) AS total 
    FROM orders 
    GROUP BY user_id
) AS order_totals
WHERE total > 100
```

---

## INSERT SELECT

Insert rows from a SELECT query into a table.

### Basic INSERT SELECT

```sql
INSERT INTO destination_table SELECT * FROM source_table
```

**Example:**
```sql
-- Copy all rows from t1 to t2
INSERT INTO t2 SELECT * FROM t1
```

### INSERT SELECT with Column List

```sql
INSERT INTO destination_table (col1, col2) SELECT expr1, expr2 FROM source_table
```

**Example:**
```sql
INSERT INTO dst (id, label) SELECT a, b FROM src
```

### INSERT SELECT with WHERE

```sql
INSERT INTO t2 SELECT * FROM t1 WHERE condition
```

---

## Scalar Expressions

### Arithmetic Operators

| Operator | Description |
|----------|-------------|
| `+` | Addition |
| `-` | Subtraction |
| `*` | Multiplication |
| `/` | Division |

**Examples:**
```sql
SELECT 1 + 1           -- Returns: 2
SELECT 10 - 3          -- Returns: 7
SELECT 2 * 3           -- Returns: 6
SELECT 10 / 2          -- Returns: 5
SELECT 1 + 2 * 3       -- Returns: 7 (correct precedence: * before +)
SELECT 1 < 2 + 3      -- Returns: 1 (TRUE, since 1 < 5)
```

### Operator Precedence

Multiplication and division have higher precedence than addition and subtraction:

```
* /    (highest)
+ -
```

### COALESCE

Returns the first non-NULL value from a list of arguments.

```sql
COALESCE(expr1, expr2, ..., exprN)
```

**Examples:**
```sql
-- If price is NULL, use 0
SELECT name, COALESCE(price, 0) FROM products

-- Multiple fallbacks
SELECT COALESCE(price, discount, 0) FROM products

-- In calculations (NULL would otherwise propagate)
SELECT COALESCE(price, 0) * quantity FROM orders
```

### CASE / WHEN

Conditional expressions (similar to if-then-else).

### Searched CASE

```sql
CASE 
    WHEN condition1 THEN result1
    WHEN condition2 THEN result2
    ...
    ELSE default_result
END
```

**Examples:**
```sql
-- Basic categorization
SELECT 
    CASE 
        WHEN age < 18 THEN 'minor'
        WHEN age < 65 THEN 'adult'
        ELSE 'senior'
    END AS category
FROM users

-- CASE with equality
SELECT 
    CASE 
        WHEN status = 1 THEN 'active'
        WHEN status = 0 THEN 'inactive'
        ELSE 'unknown'
    END AS status_text
FROM accounts

-- CASE without ELSE (returns NULL when no WHEN matches)
SELECT CASE WHEN val > 0 THEN 'positive' END FROM t
```

---

## NULL Handling

NULL represents missing or unknown values.

### NULL in Comparisons

| Expression | Result |
|------------|--------|
| `NULL = NULL` | NULL (not TRUE) |
| `NULL <> NULL` | NULL (not TRUE) |
| `NULL > 5` | NULL |
| `NULL AND TRUE` | NULL |
| `NULL AND FALSE` | FALSE |
| `NULL OR FALSE` | NULL |
| `NULL OR TRUE` | TRUE |

### NULL in Set Operations

In UNION, INTERSECT, and EXCEPT, two NULL values are considered equal.

```sql
SELECT NULL INTERSECT SELECT NULL
-- Returns: NULL (single row)
```

### NULL Propagation in Arithmetic

Arithmetic with NULL returns NULL.

```sql
SELECT 5 + NULL  -- Returns: NULL
SELECT 10 * NULL -- Returns: NULL
```

---

## EXPLAIN

Analyze and inspect query execution plans.

### EXPLAIN

Shows the logical execution plan for a statement without executing it.

```sql
EXPLAIN <statement>
```

**Example:**
```sql
EXPLAIN SELECT * FROM users WHERE id = 1
```

**Actual Output:**
```
EXPLAIN
└─ SELECT * FROM users
└─ TABLE SCAN table=1
└─ FILTER: (ID 0 {1 1 0  [] false})
```

### EXPLAIN ANALYZE

Executes the statement and returns both the execution plan and actual runtime statistics.

```sql
EXPLAIN ANALYZE <statement>
```

**Example:**
```sql
EXPLAIN ANALYZE SELECT name FROM users WHERE age > 18
```

**Actual Output (SELECT with WHERE):**
```
┌──────────────────────────────────────────────────────────────────────┐
│ EXPLAIN ANALYZE                                                        │
├──────────────────────────────────────────────────────────────────────┤
│ EXPLAIN ANALYZE                                                        │
│ └─ SELECT * FROM users                                                 │
│ └─ TABLE SCAN table=1                                                  │
│ └─ FILTER: (ID 0 {1 1 0  [] false})                                    │
├──────────────────────────────────────────────────────────────────────┤
│ actual rows=1                                                          │
│ actual time=0.032ms                                                    │
└──────────────────────────────────────────────────────────────────────┘
```

**Actual Output (INNER HASH JOIN):**
```
┌──────────────────────────────────────────────────────────────────────┐
│ EXPLAIN ANALYZE                                                        │
├──────────────────────────────────────────────────────────────────────┤
│ EXPLAIN ANALYZE                                                        │
│ └─ SELECT * FROM INNER HASH JOIN (optimized) users × orders            │
│ ├─ ON: (ID 0 USER_ID)                                                  │
│ └─ hash keys: users[col0] = orders[col1]                               │
├──────────────────────────────────────────────────────────────────────┤
│ actual rows=3                                                           │
│ actual time=0.094ms                                                    │
└──────────────────────────────────────────────────────────────────────┘
```

**Actual Output (GROUP BY with aggregate):**
```
┌──────────────────────────────────────────────────────────────────────┐
│ EXPLAIN ANALYZE                                                        │
├──────────────────────────────────────────────────────────────────────┤
│ EXPLAIN ANALYZE                                                        │
│ └─ SELECT 2 columns FROM employees                                     │
│ └─ TABLE SCAN table=3                                                  │
│ └─ GROUP BY                                                            │
├──────────────────────────────────────────────────────────────────────┤
│ actual rows=2                                                           │
│ actual time=0.023ms                                                    │
└──────────────────────────────────────────────────────────────────────┘
```

**Actual Output (INSERT):**
```
┌──────────────────────────────────────────────────────────────────────┐
│ EXPLAIN ANALYZE                                                        │
├──────────────────────────────────────────────────────────────────────┤
│ EXPLAIN ANALYZE                                                        │
│ └─ INSERT INTO users                                                   │
├──────────────────────────────────────────────────────────────────────┤
│ actual rows=0                                                          │
│ rows affected=1                                                        │
│ actual time=2.872ms                                                    │
└──────────────────────────────────────────────────────────────────────┘
```

**Actual Output (UPDATE):**
```
┌──────────────────────────────────────────────────────────────────────┐
│ EXPLAIN ANALYZE                                                        │
├──────────────────────────────────────────────────────────────────────┤
│ EXPLAIN ANALYZE                                                        │
│ └─ UPDATE users                                                        │
├──────────────────────────────────────────────────────────────────────┤
│ actual rows=0                                                          │
│ rows affected=1                                                        │
│ actual time=2.807ms                                                    │
└──────────────────────────────────────────────────────────────────────┘
```

**Actual Output (DELETE):**
```
┌──────────────────────────────────────────────────────────────────────┐
│ EXPLAIN ANALYZE                                                        │
├──────────────────────────────────────────────────────────────────────┤
│ EXPLAIN ANALYZE                                                        │
│ └─ DELETE FROM users                                                   │
├──────────────────────────────────────────────────────────────────────┤
│ actual rows=0                                                          │
│ rows affected=1                                                        │
│ actual time=2.870ms                                                    │
└──────────────────────────────────────────────────────────────────────┘
```

### Plan Node Types

| Node Type | Description |
|-----------|-------------|
| `SELECT` | Query selection node |
| `TABLE SCAN` | Full table scan operation |
| `FILTER` | Row filtering condition |
| `INNER HASH JOIN` | Hash-based inner join with optimization |
| `GROUP BY` | Aggregation grouping |
| `INSERT` | Row insertion |
| `UPDATE` | Row modification |
| `DELETE` | Row removal |

### EXPLAIN ANALYZE Statistics

| Field | Description |
|-------|-------------|
| `actual rows` | Number of rows produced by the operation |
| `actual time` | Time taken to execute (in milliseconds) |
| `rows affected` | Number of rows modified (INSERT/UPDATE/DELETE)

---

## Complete Examples

### Example 1: Sales Report

```sql
-- Create tables
CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price INT);
CREATE TABLE orders (id INT PRIMARY KEY, product_id INT, quantity INT, discount INT);
CREATE TABLE users (id INT PRIMARY KEY, name TEXT);

-- Insert data
INSERT INTO products VALUES (1, 'Widget', 100), (2, 'Gadget', 200);
INSERT INTO orders VALUES (1, 1, 2, 10), (2, 1, 1, 0), (3, 2, 3, 5);
INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');

-- Complex query with JOIN, GROUP BY, HAVING, ORDER BY
SELECT 
    p.name AS product,
    COUNT(*) AS order_count,
    SUM(o.quantity) AS total_quantity,
    COALESCE(SUM(o.quantity * (p.price - COALESCE(o.discount, 0))), 0) AS revenue
FROM products p
JOIN orders o ON p.id = o.product_id
GROUP BY p.id, p.name
HAVING SUM(o.quantity) > 1
ORDER BY revenue DESC;
```

### Example 2: User Analysis

```sql
-- Find top users with order statistics
SELECT 
    u.name,
    COALESCE(order_stats.order_count, 0) AS order_count,
    COALESCE(order_stats.total_amount, 0) AS total_amount,
    CASE 
        WHEN order_stats.order_count > 5 THEN 'VIP'
        WHEN order_stats.order_count > 0 THEN 'Regular'
        ELSE 'New'
    END AS category
FROM users u
LEFT JOIN (
    SELECT user_id, COUNT(*) AS order_count, SUM(amount) AS total_amount
    FROM orders
    GROUP BY user_id
) AS order_stats ON u.id = order_stats.user_id
ORDER BY total_amount DESC NULLS LAST
LIMIT 10 OFFSET 0;
```

### Example 3: Set Operations

```sql
-- Find customers who ordered from both Q1 and Q2
SELECT customer_id FROM orders_2024_q1
INTERSECT
SELECT customer_id FROM orders_2024_q2;

-- Find customers who ordered in Q1 but not Q2
SELECT customer_id FROM orders_2024_q1
EXCEPT
SELECT customer_id FROM orders_2024_q2;

-- Combine all unique customers from multiple quarters
SELECT customer_id FROM orders_2024_q1
UNION
SELECT customer_id FROM orders_2024_q2
UNION
SELECT customer_id FROM orders_2024_q3;
```
