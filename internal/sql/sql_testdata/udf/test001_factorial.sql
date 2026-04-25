-- name: factorial_udf
-- description: UDF for calculating factorial

-- setup: CREATE FUNCTION factorial(n INT) RETURNS INT AS $$CASE WHEN n <= 1 THEN 1 ELSE n * factorial(n - 1) END$$
SELECT factorial(5);
-- result:
-- rows: 1
-- [0][0]: 120
