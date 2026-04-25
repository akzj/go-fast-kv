-- name: trim_concat
-- description: TRIM with CONCAT

SELECT TRIM(CONCAT(' ', 'hello', ' '));
-- result:
-- rows: 1
-- [0][0]: hello
