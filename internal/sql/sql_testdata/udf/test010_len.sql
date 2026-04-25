-- name: udf_len
-- description: UDF that returns string length

CREATE FUNCTION str_len(s TEXT) RETURNS INT AS $$ LENGTH(s) $$;

SELECT str_len('hello');
-- result:
-- rows: 1
-- [0][0]: 5
