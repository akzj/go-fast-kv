-- name: substring_single_arg
-- description: SUBSTRING with only start position

SELECT SUBSTRING('hello', 2);
-- result:
-- rows: 1
-- [0][0]:ello
