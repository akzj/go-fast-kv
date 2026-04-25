-- name: concat_nested
-- description: Nested CONCAT with UPPER

SELECT CONCAT(UPPER('hello'), ' world');
-- result:
-- rows: 1
-- [0][0]: HELLO world
