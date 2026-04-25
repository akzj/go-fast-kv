-- name: upper_nested_concat
-- description: UPPER with nested CONCAT

SELECT UPPER(CONCAT('hello', ' ', 'world'));
-- result:
-- rows: 1
-- [0][0]: HELLO WORLD
