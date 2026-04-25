-- name: greatest_chain
-- description: GREATEST with multiple values

SELECT GREATEST(1, 2, 3, 4, 5);
-- result:
-- rows: 1
-- [0][0]: 5
