-- name: length_chain
-- description: Multiple LENGTH calls on same expression

SELECT LENGTH(CONCAT('ab', 'cd'));
-- result:
-- rows: 1
-- [0][0]: 4
