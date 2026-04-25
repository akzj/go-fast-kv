-- name: double_value_udf
-- description: UDF for doubling a value

-- setup: CREATE FUNCTION double(x INT) RETURNS INT AS $$x * 2$$
SELECT double(21);
-- result:
-- rows: 1
-- [0][0]: 42
