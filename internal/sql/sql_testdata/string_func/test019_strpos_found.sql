-- name: strpos_found
-- description: STRPOS returns position when found

SELECT STRPOS('hello', 'll');
-- result:
-- rows: 1
-- [0][0]: 3
