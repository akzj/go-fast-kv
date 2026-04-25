-- name: udf_sqltest
-- description: UDF with SQL expression body

CREATE FUNCTION calc(a INT, b INT) RETURNS INT AS $$ (a + b) * 2 - 1 $$;

SELECT calc(2, 3);
-- result:
-- rows: 1
-- [0][0]: 9
