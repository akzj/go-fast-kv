-- name: greatest_least_combined
-- description: GREATEST with LEAST nested

SELECT GREATEST(LEAST(1, 3), 2, LEAST(5, 4));
-- result:
-- rows: 1
-- [0][0]: 4
