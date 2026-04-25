-- name: udf_greet
-- description: UDF that returns greeting

CREATE FUNCTION greet(name TEXT) RETURNS TEXT AS $$ 'Hello, ' || name $$;

SELECT greet('World');
-- result:
-- rows: 1
-- [0][0]: Hello, World
