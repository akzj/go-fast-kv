-- name: jsonb_typeof_string
-- description: JSONB_TYPEOF returns string for string type

SELECT JSONB_TYPEOF('"hello"');
-- result:
-- rows: 1
-- [0][0]: string
