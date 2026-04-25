-- name: least_negative
-- description: LEAST returns smallest among negative numbers

SELECT LEAST(-5, -10, -3);
-- result:
-- rows: 1
-- [0][0]: -10