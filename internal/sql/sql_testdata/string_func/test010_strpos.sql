-- name: strpos_basic
-- description: STRPOS returns 0-indexed position of substring

SELECT STRPOS('hello', 'el');
-- result:
-- rows: 1
-- [0][0]: 2
