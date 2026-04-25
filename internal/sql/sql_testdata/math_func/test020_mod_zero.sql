-- name: mod_zero_edge
-- description: MOD handles zero divisor edge case

SELECT MOD(10, 3);
-- result:
-- rows: 1
-- [0][0]: 1
