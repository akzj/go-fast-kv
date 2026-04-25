-- name: udf_is_even
-- description: UDF that checks if number is even

CREATE FUNCTION is_even(n INT) RETURNS BOOLEAN AS $$ n % 2 = 0 $$;

SELECT is_even(4);
-- result:
-- rows: 1
-- [0][0]: true
