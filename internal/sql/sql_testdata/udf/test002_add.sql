-- name: udf_add
-- description: UDF that adds two numbers

CREATE FUNCTION add(a INT, b INT) RETURNS INT AS $$ a + b $$;

SELECT add(3, 4);
-- result:
-- rows: 1
-- [0][0]: 7
