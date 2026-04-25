-- name: lower_nested_concat
-- description: LOWER with nested CONCAT

SELECT LOWER(CONCAT('HELLO', 'WORLD'));
-- result:
-- rows: 1
-- [0][0]: helloworld
