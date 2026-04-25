-- name: mixed_greatest_least
-- description: Mixed positive and negative in GREATEST

SELECT GREATEST(-10, 5, -5, 10);
-- result:
-- rows: 1
-- [0][0]: 10
