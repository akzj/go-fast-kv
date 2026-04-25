-- name: jsonb_typeof_object
-- description: JSONB_TYPEOF returns type of JSON value

SELECT JSONB_TYPEOF('{"name": "Alice"}');
-- result:
-- rows: 1
-- [0][0]: object

SELECT JSONB_TYPEOF('[1, 2, 3]');
-- result:
-- rows: 1
-- [0][0]: array

SELECT JSONB_TYPEOF('null');
-- result:
-- rows: 1
-- [0][0]: null

SELECT JSONB_TYPEOF('123');
-- result:
-- rows: 1
-- [0][0]: number
