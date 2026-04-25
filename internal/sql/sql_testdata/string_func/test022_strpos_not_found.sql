-- name: strpos_not_found
-- description: STRPOS returns 0 when pattern not found

SELECT STRPOS('hello', 'x');
-- result:
-- rows: 1
-- [0][0]: 0