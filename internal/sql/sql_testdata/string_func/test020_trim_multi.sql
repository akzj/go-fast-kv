-- name: trim_multi_direction
-- description: TRIM removes spaces from both sides

SELECT TRIM('  hello  ');
-- result:
-- rows: 1
-- [0][0]: hello