-- name: udf_max
-- description: UDF that returns maximum of two values

CREATE FUNCTION my_max(a INT, b INT) RETURNS INT AS $$ 
  CASE WHEN a > b THEN a ELSE b END
$$;

SELECT my_max(10, 20);
-- result:
-- rows: 1
-- [0][0]: 20
