-- name: udf_double
-- description: UDF that doubles a number

CREATE FUNCTION double(x INT) RETURNS INT AS $$ x * 2 $$;

SELECT double(5);
-- result:
-- rows: 1
-- [0][0]: 10
