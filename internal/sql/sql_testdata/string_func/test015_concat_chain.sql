-- name: concat_chain
-- description: CONCAT with multiple string arguments

SELECT CONCAT('A', 'B', 'C');
-- result:
-- rows: 1
-- [0][0]: ABC
