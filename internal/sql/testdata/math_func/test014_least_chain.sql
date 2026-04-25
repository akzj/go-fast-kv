-- name: least_chain
-- description: LEAST with multiple values

SELECT LEAST(5, 4, 3, 2, 1);
-- result:
-- rows: 1
-- [0][0]: 1
