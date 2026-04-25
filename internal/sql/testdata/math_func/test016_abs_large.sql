-- name: abs_large_negative
-- description: ABS of large negative number

SELECT ABS(-999999);
-- result:
-- rows: 1
-- [0][0]: 999999
