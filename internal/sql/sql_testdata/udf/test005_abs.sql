-- name: udf_abs
-- description: UDF that returns absolute value

CREATE FUNCTION my_abs(n INT) RETURNS INT AS $$ CASE WHEN n < 0 THEN -n ELSE n END $$;

SELECT my_abs(-7);
-- result:
-- rows: 1
-- [0][0]: 7
