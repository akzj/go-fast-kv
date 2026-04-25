-- name: udf_factorial
-- description: UDF that calculates factorial

CREATE FUNCTION factorial(n INT) RETURNS INT AS $$ 
  CASE WHEN n <= 1 THEN 1 ELSE n * factorial(n - 1) END
$$;

SELECT factorial(5);
-- result:
-- rows: 1
-- [0][0]: 120
