-- name: substring_edge_cases
-- description: SUBSTRING handles edge cases

SELECT SUBSTRING('hello', 3, 2);
-- result:
-- rows: 1
-- [0][0]: ll