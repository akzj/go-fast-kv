-- name: substring_single_char
-- description: SUBSTRING extracting single character

SELECT SUBSTRING('abc' FROM 2 FOR 1);
-- result:
-- rows: 1
-- [0][0]: b
