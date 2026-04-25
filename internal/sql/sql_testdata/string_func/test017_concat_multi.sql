-- name: concat_multi
-- description: CONCAT joins multiple strings

SELECT CONCAT('A', 'B', 'C', 'D');
-- result:
-- rows: 1
-- [0][0]: ABCD