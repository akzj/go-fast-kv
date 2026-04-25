-- name: substring_no_length
-- description: SUBSTRING extracts rest of string from position

SELECT SUBSTRING('Hello World' FROM 7);
-- result:
-- rows: 1
-- [0][0]: World
