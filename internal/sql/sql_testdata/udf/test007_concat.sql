-- name: udf_concat
-- description: UDF that concatenates strings

CREATE FUNCTION concat_str(a TEXT, b TEXT) RETURNS TEXT AS $$ a || b $$;

SELECT concat_str('Hello', 'World');
-- result:
-- rows: 1
-- [0][0]: HelloWorld
